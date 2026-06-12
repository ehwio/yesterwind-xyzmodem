"""
ZModem protocol engine — send and receive.

Supports:
- Full ZDLE framing with all standard frame types
- 32-bit CRC on data subpackets
- Crash recovery: sender queries offset, receiver reports last good byte
- Sliding-window streaming
- Auto-download header detection (ZRQINIT broadcast)

ZModem frame anatomy
--------------------
A ZModem session exchanges *frames*.  Each frame has a header and zero or
more data subpackets.

Header formats:
  ZHEX   ``**\x18B<type><f3><f2><f1><f0><crc1><crc0>\r\n``  (hex ASCII)
  ZBIN   ``\x18A<type><f3><f2><f1><f0><crc1><crc0>``        (binary, CRC-16)
  ZBIN32 ``\x18C<type><f3><f2><f1><f0><c3><c2><c1><c0>``   (binary, CRC-32)

Data subpacket types (terminate after data):
  ZCRCW  wait for ACK after this subpacket
  ZCRCG  continue streaming (no wait)
  ZCRCE  end of file data (no wait)
  ZCRCQ  ack requested; continue after receiving ZACK

References: Chuck Forsberg's ZMODEM.DOC and lrzsz source.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import stat
import time
from collections.abc import Sequence
from typing import BinaryIO

from yesterwind_xyzmodem.callbacks import EventType, ProgressCallback, TransferProgress, fire
from yesterwind_xyzmodem.constants import DEFAULT_RETRY_LIMIT, DEFAULT_TIMEOUT
from yesterwind_xyzmodem.crc import crc16, crc32
from yesterwind_xyzmodem.exceptions import (
    ProtocolError,
    TransferCancelled,
    TransferFailed,
    TransferTimeout,
)
from yesterwind_xyzmodem.transport import Transport

# ---------------------------------------------------------------------------
# ZModem constants
# ---------------------------------------------------------------------------

ZDLE = 0x18  # Escape character

# Header frame types
ZRQINIT = 0  # Request receive init
ZRINIT = 1  # Receive init
ZSINIT = 2  # Send init sequence (optional)
ZACK = 3  # ACK to ZRQINIT or ZSINIT or data
ZFILE = 4  # File name from sender
ZSKIP = 5  # Skip this file (receiver request)
ZNAK = 6  # Last packet garbled
ZABORT = 7  # Abort batch transfers
ZFIN = 8  # Finish session
ZRPOS = 9  # Resume transfer at offset
ZDATA = 10  # Data packet(s) follow
ZEOF = 11  # End of file
ZFERR = 12  # Fatal Read or Write error
ZCRC = 13  # Request for file CRC and response
ZCHALLENGE = 14
ZCOMPL = 15
ZCAN = 16  # Cancel

# Header format bytes (after ZDLE)
ZHEX = ord("B")
ZBIN = ord("A")
ZBIN32 = ord("C")

# Data subpacket terminator bytes (sent after ZDLE)
ZCRCW = ord("k")  # 0x6B — end subpacket, wait for ACK
ZCRCG = ord("j")  # 0x6A — end subpacket, continue
ZCRCE = ord("h")  # 0x68 — end of file
ZCRCQ = ord("i")  # 0x69 — end subpacket, request ZACK

# Bytes that must be escaped with ZDLE
_MUST_ESCAPE = {0x11, 0x13, 0x91, 0x93, ZDLE}

# Subpacket size for streaming
_SUBPACKET_SIZE = 1024

# ZModem auto-download trigger string sent by receiver
ZMODEM_INIT = b"**\x18B01"


# ---------------------------------------------------------------------------
# Low-level framing helpers
# ---------------------------------------------------------------------------


def _zdle_encode(data: bytes) -> bytes:
    """Escape ZDLE-special bytes in *data*."""
    out = bytearray()
    for b in data:
        if b in _MUST_ESCAPE:
            out.append(ZDLE)
            out.append(b ^ 0x40)
        else:
            out.append(b)
    return bytes(out)


def _build_hex_header(frame_type: int, f0: int, f1: int, f2: int, f3: int) -> bytes:
    """Build a ZHEX header frame.

    Wire order: TYPE f0 f1 f2 f3.  For position frames f0 is the LSB
    (ZP0 convention).  For ZRINIT flags, caps go in f3 (ZF0 = index 3).
    """
    payload = bytes([frame_type, f0, f1, f2, f3])
    c = crc16(payload)
    # lrzsz's zgethex() only accepts lowercase hex digits.
    hex_payload = payload.hex().encode("ascii")
    hex_crc = f"{c:04x}".encode("ascii")
    return b"**\x18B" + hex_payload + hex_crc + b"\r\n"


def _build_bin32_header(frame_type: int, f0: int, f1: int, f2: int, f3: int) -> bytes:
    """Build a ZBIN32 header frame.  Same wire order as _build_hex_header."""
    payload = bytes([frame_type, f0, f1, f2, f3])
    c = crc32(payload)
    encoded = _zdle_encode(
        payload + bytes([c & 0xFF, (c >> 8) & 0xFF, (c >> 16) & 0xFF, (c >> 24) & 0xFF])
    )
    return bytes([ZDLE, ZBIN32]) + encoded


def _encode_offset(offset: int) -> tuple[int, int, int, int]:
    """Encode a 32-bit offset as (f0, f1, f2, f3) little-endian."""
    return (
        offset & 0xFF,
        (offset >> 8) & 0xFF,
        (offset >> 16) & 0xFF,
        (offset >> 24) & 0xFF,
    )


# ---------------------------------------------------------------------------
# ZModem engine
# ---------------------------------------------------------------------------


class ZModem:
    """
    ZModem protocol engine.

    Parameters
    ----------
    transport:
        A :class:`~yesterwind_xyzmodem.transport.Transport` instance.
    timeout:
        Seconds to wait for a frame from the remote.
    retry_limit:
        How many consecutive errors before giving up.
    callback:
        Optional :data:`~yesterwind_xyzmodem.callbacks.ProgressCallback`.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        retry_limit: int = DEFAULT_RETRY_LIMIT,
        callback: ProgressCallback | None = None,
    ) -> None:
        self._transport = transport
        self._timeout = timeout
        self._retry_limit = retry_limit
        self._callback = callback

    # ------------------------------------------------------------------
    # Public API

    async def send(
        self,
        files: Sequence[tuple[str, BinaryIO, int]],
    ) -> int:
        """
        Send a batch of files.

        Parameters
        ----------
        files:
            Sequence of ``(filename, stream, size_in_bytes)`` tuples.

        Returns total data bytes sent.
        """
        progress = TransferProgress(
            file_count=len(files),
            event=EventType.SESSION_START,
        )
        await fire(self._callback, progress)

        # Broadcast ZRQINIT so receiver knows ZModem is available
        await self._transport.write(_build_hex_header(ZRQINIT, 0, 0, 0, 0))

        # Wait for ZRINIT
        frame = await self._read_header()
        if frame[0] != ZRINIT:
            raise ProtocolError(f"Expected ZRINIT, got frame type {frame[0]}")

        total = 0
        for idx, (filename, stream, size) in enumerate(files):
            progress.file_index = idx
            progress.filename = filename
            progress.total_bytes = size
            progress.bytes_transferred = 0
            progress.event = EventType.FILE_START
            await fire(self._callback, progress)

            sent = await self._send_file(filename, stream, size, progress)
            total += sent

            progress.bytes_transferred = sent
            progress.event = EventType.FILE_END
            await fire(self._callback, progress)

        # ZFIN to end session
        await self._transport.write(_build_hex_header(ZFIN, 0, 0, 0, 0))
        try:
            await self._read_header_with_timeout()
        except (asyncio.TimeoutError, ProtocolError, TransferTimeout):
            pass  # best-effort

        progress.event = EventType.SESSION_END
        await fire(self._callback, progress)
        return total

    async def receive(
        self,
        output_dir: str = ".",
    ) -> list[str]:
        """
        Receive files into *output_dir*.

        Returns list of paths written.
        """
        received: list[str] = []
        progress = TransferProgress(event=EventType.SESSION_START)
        await fire(self._callback, progress)

        # Advertise capabilities via ZRINIT
        await self._send_zrinit()

        file_index = 0
        while True:
            frame = await self._read_header_with_timeout()
            ftype = frame[0]

            if ftype == ZRQINIT:
                await self._send_zrinit()
                continue

            if ftype == ZFIN:
                await self._transport.write(_build_hex_header(ZFIN, 0, 0, 0, 0))
                break

            if ftype == ZFILE:
                filename, size, mtime, mode = await self._read_zfile_data()
                dest = os.path.join(output_dir, os.path.basename(filename))

                progress.file_index = file_index
                progress.filename = filename
                progress.total_bytes = size
                progress.bytes_transferred = 0
                progress.event = EventType.FILE_START
                await fire(self._callback, progress)

                resume_offset = 0
                if os.path.exists(dest):
                    resume_offset = os.path.getsize(dest)

                # ZRPOS to tell sender where to start
                f0, f1, f2, f3 = _encode_offset(resume_offset)
                await self._transport.write(_build_hex_header(ZRPOS, f0, f1, f2, f3))

                n = await self._receive_file_data(dest, resume_offset, progress)

                if mtime:
                    os.utime(dest, (mtime, mtime))
                if mode:
                    with contextlib.suppress(OSError):  # pragma: no cover
                        os.chmod(dest, stat.S_IMODE(mode))

                progress.bytes_transferred = n
                progress.event = EventType.FILE_END
                await fire(self._callback, progress)
                received.append(dest)
                file_index += 1

                # ACK the file
                await self._transport.write(_build_hex_header(ZRINIT, 0, 0, 0, 0))
                continue

            if ftype in (ZABORT, ZCAN):
                raise TransferCancelled("Remote cancelled")

        progress.event = EventType.SESSION_END
        await fire(self._callback, progress)
        return received

    # ------------------------------------------------------------------
    # Sender helpers

    async def _send_file(
        self,
        filename: str,
        stream: BinaryIO,
        size: int,
        progress: TransferProgress,
    ) -> int:
        # ZFILE header
        await self._transport.write(_build_hex_header(ZFILE, 0, 0, 0, 0))
        # File info subpacket
        info = f"{os.path.basename(filename)}\x00{size} {int(time.time()):o} 0 0 1 {size}"
        await self._write_data_subpacket(info.encode("ascii"), ZCRCW)

        # Wait for ZRPOS (may include resume offset)
        for _ in range(self._retry_limit):
            frame = await self._read_header_with_timeout()
            if frame[0] == ZRPOS:
                break
            if frame[0] in (ZSKIP, ZABORT):
                raise TransferCancelled("Remote skipped or aborted")
        else:
            raise TransferFailed("No ZRPOS received")

        offset = frame[1] | (frame[2] << 8) | (frame[3] << 16) | (frame[4] << 24)
        stream.seek(offset)

        # ZDATA header with starting offset
        f0, f1, f2, f3 = _encode_offset(offset)
        await self._transport.write(_build_bin32_header(ZDATA, f0, f1, f2, f3))

        bytes_sent = offset
        while True:
            data = stream.read(_SUBPACKET_SIZE)
            if not data:
                break
            next_data = stream.read(1)
            if next_data:
                stream.seek(-1, 1)
                term = ZCRCG  # more data coming
            else:
                term = ZCRCE  # end of file

            await self._write_data_subpacket(data, term)
            bytes_sent += len(data)
            progress.bytes_transferred = bytes_sent
            progress.event = EventType.BLOCK_SENT
            await fire(self._callback, progress)

        # ZEOF
        f0, f1, f2, f3 = _encode_offset(bytes_sent)
        await self._transport.write(_build_hex_header(ZEOF, f0, f1, f2, f3))

        # Wait for ZRINIT
        for _ in range(self._retry_limit):
            try:
                frame = await self._read_header_with_timeout()
            except (asyncio.TimeoutError, TransferTimeout):
                continue
            if frame[0] in (ZRINIT, ZACK):
                return bytes_sent - offset
            if frame[0] in (ZABORT, ZCAN):
                raise TransferCancelled("Remote aborted after ZEOF")
        raise TransferFailed("No ZRINIT after ZEOF")

    async def _write_data_subpacket(self, data: bytes, term: int) -> None:
        """Write a CRC-32 data subpacket terminated by ZDLE+*term*."""
        # CRC covers data + terminator byte
        crc_input = data + bytes([term])
        c = crc32(crc_input)
        encoded = _zdle_encode(data)
        crc_bytes = _zdle_encode(
            bytes([c & 0xFF, (c >> 8) & 0xFF, (c >> 16) & 0xFF, (c >> 24) & 0xFF])
        )
        await self._transport.write(encoded + bytes([ZDLE, term]) + crc_bytes)

    # ------------------------------------------------------------------
    # Receiver helpers

    async def _send_zrinit(self) -> None:
        # Capability flags go in f3 (ZF0 in lrzsz = index 3 on wire).
        # CANFDX|CANOVIO|CANFC32 = 0x23.
        await self._transport.write(_build_hex_header(ZRINIT, 0, 0, 0, 0x23))

    async def _read_zfile_data(self) -> tuple[str, int, int, int]:
        """Read the ZFILE data subpacket; return (filename, size, mtime, mode)."""
        data = await self._read_data_subpacket()
        null = data.find(b"\x00")
        if null < 0:
            return (data.decode("ascii", errors="replace"), 0, 0, 0)
        filename = data[:null].decode("ascii", errors="replace")
        meta = data[null + 1 :].split(b" ")
        size = int(meta[0]) if meta else 0
        mtime = int(meta[1], 8) if len(meta) > 1 and meta[1] else 0
        mode = int(meta[2], 8) if len(meta) > 2 and meta[2] else 0
        return (filename, size, mtime, mode)

    async def _receive_file_data(
        self,
        dest_path: str,
        resume_offset: int,
        progress: TransferProgress,
    ) -> int:
        mode = "ab" if resume_offset else "wb"
        bytes_written = resume_offset

        with open(dest_path, mode) as f:
            while True:
                frame = await self._read_header_with_timeout()
                ftype = frame[0]

                if ftype == ZEOF:
                    await self._transport.write(_build_hex_header(ZACK, 0, 0, 0, 0))
                    break

                if ftype == ZDATA:
                    while True:
                        data, term = await self._read_data_subpacket_with_term()
                        f.write(data)
                        bytes_written += len(data)
                        progress.bytes_transferred = bytes_written
                        progress.event = EventType.BLOCK_RECEIVED
                        await fire(self._callback, progress)
                        if term == ZCRCE:
                            break
                        if term == ZCRCW:
                            f0, f1, f2, f3 = _encode_offset(bytes_written)
                            await self._transport.write(_build_hex_header(ZACK, f0, f1, f2, f3))
                            # Sender continues sending subpackets; stay in inner loop

                elif ftype in (ZABORT, ZCAN):
                    raise TransferCancelled("Remote cancelled during data transfer")

        return bytes_written

    # ------------------------------------------------------------------
    # Frame I/O

    async def _read_header(self) -> tuple[int, int, int, int, int]:
        """Read one ZModem header; return (type, f3, f2, f1, f0)."""
        # Scan for ZDLE
        for _ in range(1024):
            try:
                b = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError as err:
                raise TransferTimeout("Timed out reading frame") from err
            if b == ZDLE:
                break
        else:
            raise ProtocolError("No ZDLE found scanning for header")

        fmt = await self._transport.read_byte_with_timeout(self._timeout)

        if fmt == ZHEX:
            return await self._read_hex_header()
        if fmt == ZBIN:
            return await self._read_bin_header(crc32_mode=False)
        if fmt == ZBIN32:
            return await self._read_bin_header(crc32_mode=True)
        raise ProtocolError(f"Unknown header format byte 0x{fmt:02x}")

    async def _read_header_with_timeout(self) -> tuple[int, int, int, int, int]:
        try:
            return await asyncio.wait_for(self._read_header(), timeout=self._timeout)
        except asyncio.TimeoutError as err:
            raise TransferTimeout("Timed out waiting for ZModem header") from err

    async def _read_hex_header(self) -> tuple[int, int, int, int, int]:
        # ZHEX: 10 hex chars (5 data bytes) + 4 hex chars (2-byte CRC-16) = 14 chars
        hex_bytes = b""
        for _ in range(16):  # read extra to absorb any embedded \r\n\x8d
            b = await self._transport.read_byte_with_timeout(self._timeout)
            if b in (ord("\r"), ord("\n"), 0x8D):
                continue
            hex_bytes += bytes([b])
            if len(hex_bytes) == 14:
                break
        if len(hex_bytes) < 14:
            raise ProtocolError("Truncated hex header")
        try:
            raw = bytes.fromhex(hex_bytes[:10].decode("ascii"))
        except ValueError as e:
            raise ProtocolError(f"Bad hex header: {e}") from e
        received_crc = int(hex_bytes[10:14], 16)
        if crc16(raw[:5]) != received_crc:
            raise ProtocolError("Hex header CRC error")
        # Consume exactly the trailing \r\n — do NOT read past them.
        for _ in range(2):
            try:
                await self._transport.read_byte_with_timeout(0.1)
            except asyncio.TimeoutError:
                break
        return (raw[0], raw[1], raw[2], raw[3], raw[4])

    async def _read_bin_header(self, crc32_mode: bool) -> tuple[int, int, int, int, int]:
        raw = bytearray()
        expected = 9 if crc32_mode else 7  # 5 data + 4 or 2 CRC bytes
        while len(raw) < expected:
            b = await self._transport.read_byte_with_timeout(self._timeout)
            if b == ZDLE:
                escaped = await self._transport.read_byte_with_timeout(self._timeout)
                raw.append(escaped ^ 0x40)
            else:
                raw.append(b)
        if crc32_mode:
            received = int.from_bytes(raw[5:9], "little")
            if crc32(bytes(raw[:5])) != received:
                raise ProtocolError("ZBIN32 header CRC error")
        else:
            received = (raw[5] << 8) | raw[6]
            if crc16(bytes(raw[:5])) != received:
                raise ProtocolError("ZBIN header CRC error")
        return (raw[0], raw[1], raw[2], raw[3], raw[4])

    async def _read_data_subpacket(self) -> bytes:
        data, _ = await self._read_data_subpacket_with_term()
        return data

    async def _read_data_subpacket_with_term(self) -> tuple[bytes, int]:
        """Read a CRC-32 data subpacket; return (data, terminator)."""
        buf = bytearray()
        while True:
            b = await self._transport.read_byte_with_timeout(self._timeout)
            if b != ZDLE:
                buf.append(b)
                continue
            # ZDLE escape
            esc = await self._transport.read_byte_with_timeout(self._timeout)
            if esc in (ZCRCW, ZCRCG, ZCRCE, ZCRCQ):
                # End-of-subpacket: read and verify CRC-32
                crc_raw = bytearray()
                for _ in range(4):
                    cb = await self._transport.read_byte_with_timeout(self._timeout)
                    if cb == ZDLE:
                        cb2 = await self._transport.read_byte_with_timeout(self._timeout)
                        crc_raw.append(cb2 ^ 0x40)
                    else:
                        crc_raw.append(cb)
                received_crc = int.from_bytes(crc_raw, "little")
                expected_crc = crc32(bytes(buf) + bytes([esc]))
                if received_crc != expected_crc:
                    raise ProtocolError("Data subpacket CRC-32 error")
                return (bytes(buf), esc)
            else:
                buf.append(esc ^ 0x40)
