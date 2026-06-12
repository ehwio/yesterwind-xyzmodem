"""
YModem protocol engine — send and receive.

Supports:
- YModem batch (multiple files per session)
- YModem-G (streaming, no per-block ACK — for reliable links)

Block 0 carries a NUL-terminated filename followed by optional metadata
(file size, modification time) as an ASCII string.  The engine reads and
writes this metadata automatically.
"""

from __future__ import annotations

import asyncio
import io
import os
import time
from collections.abc import Sequence
from typing import BinaryIO

from yesterwind_xyzmodem.callbacks import EventType, ProgressCallback, TransferProgress, fire
from yesterwind_xyzmodem.constants import (
    ACK,
    CAN,
    CRC_MODE,
    DEFAULT_RETRY_LIMIT,
    DEFAULT_TIMEOUT,
    EOT,
    NAK,
    SOH,
    STX,
    SUB,
)
from yesterwind_xyzmodem.crc import crc16
from yesterwind_xyzmodem.exceptions import (
    ProtocolError,
    TransferCancelled,
    TransferFailed,
    TransferTimeout,
)
from yesterwind_xyzmodem.transport import Transport

# YModem always uses 1024-byte data blocks (STX) except for block 0 (SOH 128)
_DATA_BLOCK_SIZE = 1024
_HEADER_BLOCK_SIZE = 128


class YModem:
    """
    YModem protocol engine.

    Parameters
    ----------
    transport:
        A :class:`~yesterwind_xyzmodem.transport.Transport` instance.
    timeout:
        Seconds to wait for a byte from the remote.
    retry_limit:
        How many consecutive NAKs / timeouts before giving up.
    g_mode:
        If True, use YModem-G (streaming, no per-block ACK).
    callback:
        Optional :data:`~yesterwind_xyzmodem.callbacks.ProgressCallback`.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        retry_limit: int = DEFAULT_RETRY_LIMIT,
        g_mode: bool = False,
        callback: ProgressCallback | None = None,
    ) -> None:
        self._transport = transport
        self._timeout = timeout
        self._retry_limit = retry_limit
        self._g_mode = g_mode
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
            Pass ``size=0`` if unknown.

        Returns total data bytes sent.
        """
        progress = TransferProgress(
            file_count=len(files),
            event=EventType.SESSION_START,
        )
        await fire(self._callback, progress)

        # Wait for receiver 'G' or 'C'
        await self._sender_handshake()

        total_bytes = 0
        for idx, (filename, stream, size) in enumerate(files):
            progress.file_index = idx
            progress.filename = filename
            progress.total_bytes = size
            progress.bytes_transferred = 0
            progress.event = EventType.FILE_START
            await fire(self._callback, progress)

            sent = await self._send_file(filename, stream, size, progress)
            total_bytes += sent

            progress.bytes_transferred = sent
            progress.event = EventType.FILE_END
            await fire(self._callback, progress)

        # Send empty block 0 to signal end of batch
        await self._send_block0("", 0, 0)
        await self._wait_for_ack_or_ignore()

        progress.event = EventType.SESSION_END
        await fire(self._callback, progress)
        return total_bytes

    async def receive(
        self,
        output_dir: str = ".",
    ) -> list[str]:
        """
        Receive a batch of files into *output_dir*.

        Returns list of received filenames.
        """
        received: list[str] = []
        progress = TransferProgress(event=EventType.SESSION_START)
        await fire(self._callback, progress)

        # Advertise capability
        init_char = ord("G") if self._g_mode else CRC_MODE
        await self._transport.write(bytes([init_char]))

        file_index = 0
        while True:
            filename, size, mtime = await self._receive_block0()
            if not filename:
                # Empty filename = end of batch
                await self._transport.write(bytes([ACK]))
                break

            dest_path = os.path.join(output_dir, os.path.basename(filename))
            progress.file_index = file_index
            progress.filename = filename
            progress.total_bytes = size
            progress.bytes_transferred = 0
            progress.event = EventType.FILE_START
            await fire(self._callback, progress)

            # ACK block 0, then send C to start data
            await self._transport.write(bytes([ACK]))
            await self._transport.write(bytes([init_char]))

            with open(dest_path, "wb") as f:
                n = await self._receive_data_blocks(f, size, progress)

            if mtime:
                os.utime(dest_path, (mtime, mtime))

            progress.bytes_transferred = n
            progress.event = EventType.FILE_END
            await fire(self._callback, progress)

            received.append(dest_path)
            file_index += 1

            # Ready for next file
            await self._transport.write(bytes([init_char]))

        progress.event = EventType.SESSION_END
        await fire(self._callback, progress)
        return received

    # ------------------------------------------------------------------
    # Sender helpers

    async def _sender_handshake(self) -> None:
        for _ in range(self._retry_limit):
            try:
                byte = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError:
                continue
            if byte in (CRC_MODE, ord("G")):
                self._g_mode = byte == ord("G")
                return
            if byte == CAN:
                raise TransferCancelled("Remote cancelled during handshake")
        raise TransferFailed("No handshake response from receiver")

    async def _send_file(
        self,
        filename: str,
        stream: BinaryIO,
        size: int,
        progress: TransferProgress,
    ) -> int:
        await self._send_block0(filename, size, int(time.time()))
        if not self._g_mode:
            await self._wait_for_ack()
            # Receiver will send another 'C' to start data
            await self._wait_for_c()

        return await self._send_data_blocks(stream, progress)

    async def _send_block0(self, filename: str, size: int, mtime: int) -> None:
        """Build and send YModem block 0 (file header)."""
        meta = f"{filename}\x00{size} {mtime:o}" if filename else ""
        payload = meta.encode("ascii", errors="replace")
        payload = payload[:_HEADER_BLOCK_SIZE].ljust(_HEADER_BLOCK_SIZE, b"\x00")
        c = crc16(payload)
        frame = bytes([SOH, 0x00, 0xFF]) + payload + bytes([c >> 8, c & 0xFF])
        await self._transport.write(frame)

    async def _send_data_blocks(
        self,
        stream: BinaryIO,
        progress: TransferProgress,
    ) -> int:
        block_num = 1
        bytes_sent = 0

        while True:
            data = stream.read(_DATA_BLOCK_SIZE)
            if not data:
                break
            if len(data) < _DATA_BLOCK_SIZE:
                data = data + bytes([SUB] * (_DATA_BLOCK_SIZE - len(data)))

            c = crc16(data)
            frame = bytes([STX, block_num, (~block_num) & 0xFF]) + data + bytes([c >> 8, c & 0xFF])

            if self._g_mode:
                await self._transport.write(frame)
            else:
                await self._send_block_with_retry(frame, block_num, progress)

            bytes_sent += _DATA_BLOCK_SIZE
            block_num = (block_num + 1) & 0xFF
            progress.bytes_transferred = bytes_sent
            progress.block_number = block_num
            progress.event = EventType.BLOCK_SENT
            await fire(self._callback, progress)

        # EOT sequence: first EOT may be NAK'd; second must be ACK'd
        for _ in range(self._retry_limit):
            await self._transport.write(bytes([EOT]))
            if self._g_mode:
                return bytes_sent
            try:
                resp = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError:
                continue
            if resp == ACK:
                return bytes_sent
            if resp == CAN:
                raise TransferCancelled("Remote cancelled on EOT")
        raise TransferFailed("EOT not acknowledged")

    async def _send_block_with_retry(
        self,
        frame: bytes,
        block_num: int,
        progress: TransferProgress,
    ) -> None:
        for _attempt in range(self._retry_limit):
            await self._transport.write(frame)
            try:
                resp = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError:
                progress.event = EventType.TIMEOUT
                await fire(self._callback, progress)
                continue
            if resp == ACK:
                return
            if resp == CAN:
                raise TransferCancelled("Remote cancelled")
            progress.event = EventType.BLOCK_NAK
            await fire(self._callback, progress)
        raise TransferFailed(
            f"Block {block_num} not acknowledged after {self._retry_limit} attempts"
        )

    async def _wait_for_ack(self) -> None:
        for _ in range(self._retry_limit):
            try:
                b = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError:
                continue
            if b == ACK:
                return
            if b == CAN:
                raise TransferCancelled("Remote cancelled")
        raise TransferFailed("Expected ACK, got nothing")

    async def _wait_for_c(self) -> None:
        for _ in range(self._retry_limit):
            try:
                b = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError:
                continue
            if b == CRC_MODE:
                return
            if b == CAN:
                raise TransferCancelled("Remote cancelled")
        raise TransferFailed("Expected 'C', got nothing")

    async def _wait_for_ack_or_ignore(self) -> None:
        try:
            await self._transport.read_byte_with_timeout(self._timeout)
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------------
    # Receiver helpers

    async def _receive_block0(self) -> tuple[str, int, int]:
        """Read block 0; return (filename, size, mtime). Empty filename = end."""
        for _ in range(self._retry_limit):
            try:
                header = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError:
                await self._transport.write(bytes([NAK]))
                continue

            if header == CAN:
                raise TransferCancelled("Remote cancelled")
            if header != SOH:
                await self._transport.write(bytes([NAK]))
                continue

            try:
                rest = await self._transport.read_with_timeout(
                    _HEADER_BLOCK_SIZE + 4, self._timeout
                )
            except asyncio.TimeoutError:
                await self._transport.write(bytes([NAK]))
                continue

            if len(rest) < _HEADER_BLOCK_SIZE + 4:  # pragma: no cover — only with StreamTransport
                await self._transport.write(bytes([NAK]))
                continue

            block_num = rest[0]
            block_comp = rest[1]
            if block_num != 0x00 or block_comp != 0xFF:
                await self._transport.write(bytes([NAK]))
                continue

            payload = rest[2 : 2 + _HEADER_BLOCK_SIZE]
            received_crc = (rest[-2] << 8) | rest[-1]
            if crc16(payload) != received_crc:
                await self._transport.write(bytes([NAK]))
                continue

            # Decode filename and optional metadata
            null_idx = payload.find(b"\x00")
            if null_idx < 0:
                return ("", 0, 0)
            filename = payload[:null_idx].decode("ascii", errors="replace")
            if not filename:
                return ("", 0, 0)

            meta_bytes = payload[null_idx + 1 :].rstrip(b"\x00")
            size = 0
            mtime = 0
            if meta_bytes:
                parts = meta_bytes.decode("ascii", errors="replace").split()
                if parts:
                    try:
                        size = int(parts[0])
                    except ValueError:
                        pass
                if len(parts) > 1:
                    try:
                        mtime = int(parts[1], 8)
                    except ValueError:
                        pass

            return (filename, size, mtime)

        raise TransferFailed("Could not receive valid block 0")

    async def _receive_data_blocks(
        self,
        stream: BinaryIO,
        expected_size: int,
        progress: TransferProgress,
    ) -> int:
        expected_block = 1
        bytes_received = 0
        buf = io.BytesIO()
        consecutive_errors = 0

        while True:
            try:
                header = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError as err:
                consecutive_errors += 1
                if consecutive_errors >= self._retry_limit:
                    raise TransferTimeout("Timed out waiting for data") from err
                await self._transport.write(bytes([NAK]))
                continue

            if header == EOT:
                await self._transport.write(bytes([ACK]))
                break
            if header == CAN:
                raise TransferCancelled("Remote cancelled")
            if header not in (SOH, STX):
                consecutive_errors += 1
                await self._transport.write(bytes([NAK]))
                continue

            block_size = 128 if header == SOH else _DATA_BLOCK_SIZE
            try:
                rest = await self._transport.read_with_timeout(block_size + 4, self._timeout)
            except asyncio.TimeoutError:
                consecutive_errors += 1
                await self._transport.write(bytes([NAK]))
                continue

            block_num = rest[0]
            block_comp = rest[1]
            if block_num ^ block_comp != 0xFF:
                consecutive_errors += 1
                await self._transport.write(bytes([NAK]))
                continue

            data = rest[2 : 2 + block_size]
            received_crc = (rest[-2] << 8) | rest[-1]
            if crc16(data) != received_crc:
                consecutive_errors += 1
                progress.event = EventType.CRC_ERROR
                await fire(self._callback, progress)
                await self._transport.write(bytes([NAK]))
                continue

            consecutive_errors = 0

            if block_num == (expected_block - 1) & 0xFF:
                # Duplicate
                await self._transport.write(bytes([ACK]))
                continue

            if block_num != expected_block:
                await self._transport.write(bytes([CAN, CAN]))
                raise ProtocolError(f"Expected block {expected_block}, got {block_num}")

            buf.write(data)
            bytes_received += block_size
            expected_block = (expected_block + 1) & 0xFF
            progress.bytes_transferred = bytes_received
            progress.block_number = block_num
            progress.event = EventType.BLOCK_RECEIVED
            await fire(self._callback, progress)

            if not self._g_mode:
                await self._transport.write(bytes([ACK]))

        payload = buf.getvalue()
        if expected_size and len(payload) > expected_size:
            payload = payload[:expected_size]
        else:
            payload = payload.rstrip(bytes([SUB]))
        stream.write(payload)
        return len(payload)
