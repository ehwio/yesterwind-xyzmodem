"""Tests for the ZModem protocol engine."""

from __future__ import annotations

import asyncio
import io
import time

import pytest

from yesterwind_xyzmodem.callbacks import EventType
from yesterwind_xyzmodem.crc import crc32
from yesterwind_xyzmodem.exceptions import (
    ProtocolError,
    TransferCancelled,
)
from yesterwind_xyzmodem.zmodem import (
    ZABORT,
    ZACK,
    ZBIN32,
    ZCRCE,
    ZCRCG,
    ZCRCW,
    ZDATA,
    ZDLE,
    ZEOF,
    ZFILE,
    ZFIN,
    ZRINIT,
    ZRPOS,
    ZRQINIT,
    ZModem,
    _build_bin32_header,
    _build_hex_header,
    _encode_offset,
    _zdle_encode,
)

# ---------------------------------------------------------------------------
# Unit tests for framing helpers
# ---------------------------------------------------------------------------


class TestFramingHelpers:
    def test_zdle_encode_passthrough(self):
        data = bytes(range(32, 128))
        assert _zdle_encode(data) == data

    def test_zdle_encode_escapes_zdle(self):
        encoded = _zdle_encode(bytes([ZDLE]))
        assert encoded == bytes([ZDLE, ZDLE ^ 0x40])

    def test_zdle_encode_escapes_xon_xoff(self):
        for b in (0x11, 0x13, 0x91, 0x93):
            enc = _zdle_encode(bytes([b]))
            assert enc[0] == ZDLE
            assert enc[1] == b ^ 0x40

    def test_build_hex_header_structure(self):
        frame = _build_hex_header(ZRINIT, 0x23, 0, 0, 0)
        assert frame[:4] == b"**\x18B"
        assert frame.endswith(b"\r\n")

    def test_build_bin32_header_structure(self):
        frame = _build_bin32_header(ZDATA, 0, 0, 0, 0)
        assert frame[:2] == bytes([ZDLE, ZBIN32])

    def test_encode_offset_roundtrip(self):
        for offset in (0, 1, 1024, 0xDEADBEEF & 0xFFFFFFFF):
            f0, f1, f2, f3 = _encode_offset(offset)
            recovered = f0 | (f1 << 8) | (f2 << 16) | (f3 << 24)
            assert recovered == offset


# ---------------------------------------------------------------------------
# Integration: full send/receive over piped transport
# ---------------------------------------------------------------------------


class TestZModemSend:
    async def test_send_single_file(self, piped, event_log):
        """Sender completes a single-file transfer with the receiver."""
        file_data = b"Z" * 128
        sender = ZModem(piped.side_a, callback=event_log.callback)

        async def receiver():
            # Wait for ZRQINIT
            frame = await _read_hex_frame(piped.side_b)
            assert frame[0] == ZRQINIT
            # Send ZRINIT
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            # Receive ZFILE header
            frame = await _read_hex_frame(piped.side_b)
            assert frame[0] == ZFILE
            # Read and discard ZFILE data subpacket
            await _read_subpacket(piped.side_b)
            # Send ZRPOS offset=0
            await piped.side_b.write(_build_hex_header(ZRPOS, 0, 0, 0, 0))
            # Receive ZDATA header
            frame = await _read_bin32_frame(piped.side_b)
            assert frame[0] == ZDATA
            # Read all data subpackets until ZCRCE
            received = b""
            while True:
                data, term = await _read_subpacket_with_term(piped.side_b)
                received += data
                if term == ZCRCE:
                    break
            assert received == file_data
            # Receive ZEOF
            frame = await _read_hex_frame(piped.side_b)
            assert frame[0] == ZEOF
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            # Receive ZFIN
            frame = await _read_hex_frame(piped.side_b)
            assert frame[0] == ZFIN
            await piped.side_b.write(_build_hex_header(ZFIN, 0, 0, 0, 0))

        await asyncio.gather(
            sender.send([("test.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )
        types = event_log.types()
        assert EventType.FILE_START in types
        assert EventType.FILE_END in types
        assert EventType.SESSION_END in types

    async def test_send_resume_from_offset(self, piped):
        """Sender resumes from ZRPOS offset instead of the beginning."""
        file_data = b"R" * 512
        resume_at = 256
        sender = ZModem(piped.side_a)

        async def receiver():
            frame = await _read_hex_frame(piped.side_b)
            assert frame[0] == ZRQINIT
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            frame = await _read_hex_frame(piped.side_b)
            assert frame[0] == ZFILE
            await _read_subpacket(piped.side_b)
            # Request resume from offset 1024
            f0, f1, f2, f3 = _encode_offset(resume_at)
            await piped.side_b.write(_build_hex_header(ZRPOS, f0, f1, f2, f3))
            frame = await _read_bin32_frame(piped.side_b)
            assert frame[0] == ZDATA
            # Verify offset in header
            offset = frame[1] | (frame[2] << 8) | (frame[3] << 16) | (frame[4] << 24)
            assert offset == resume_at
            # Drain subpackets
            while True:
                _, term = await _read_subpacket_with_term(piped.side_b)
                if term == ZCRCE:
                    break
            frame = await _read_hex_frame(piped.side_b)
            assert frame[0] == ZEOF
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            frame = await _read_hex_frame(piped.side_b)
            assert frame[0] == ZFIN
            await piped.side_b.write(_build_hex_header(ZFIN, 0, 0, 0, 0))

        stream = io.BytesIO(file_data)
        await asyncio.gather(
            sender.send([("r.bin", stream, len(file_data))]),
            receiver(),
        )

    async def test_send_cancel_on_zabort(self, piped):
        """Sender raises TransferCancelled when receiver sends ZABORT."""
        sender = ZModem(piped.side_a, timeout=1.0)

        async def receiver():
            await _read_hex_frame(piped.side_b)  # ZRQINIT
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_hex_frame(piped.side_b)  # ZFILE
            await _read_subpacket(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZABORT, 0, 0, 0, 0))

        with pytest.raises(TransferCancelled):
            await asyncio.gather(
                sender.send([("f.bin", io.BytesIO(b"x" * 512), 512)]),
                receiver(),
            )

    async def test_send_no_zrinit_raises(self, piped):
        """Sender raises ProtocolError if ZRQINIT is not answered with ZRINIT."""
        sender = ZModem(piped.side_a, timeout=0.1, retry_limit=1)

        async def receiver():
            await _read_hex_frame(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZFIN, 0, 0, 0, 0))

        with pytest.raises(ProtocolError):
            await asyncio.gather(
                sender.send([("f.bin", io.BytesIO(b"x"), 1)]),
                receiver(),
            )


class TestZModemReceive:
    async def test_receive_single_file(self, piped, event_log, tmp_path):
        """Receiver writes file correctly from sender."""
        file_data = b"Y" * 200
        receiver_engine = ZModem(piped.side_b, callback=event_log.callback)

        async def sender():
            # Wait for ZRINIT
            frame = await _read_hex_frame(piped.side_a)
            assert frame[0] == ZRINIT
            # Send ZFILE
            await piped.side_a.write(_build_hex_header(ZFILE, 0, 0, 0, 0))
            info = f"data.bin\x00{len(file_data)} {int(time.time()):o} 0 0 1 {len(file_data)}"
            await _write_subpacket(piped.side_a, info.encode(), ZCRCW)
            # Wait for ZRPOS
            frame = await _read_hex_frame(piped.side_a)
            assert frame[0] == ZRPOS
            # Send ZDATA
            await piped.side_a.write(_build_bin32_header(ZDATA, 0, 0, 0, 0))
            # Send data in one ZCRCE subpacket
            await _write_subpacket(piped.side_a, file_data, ZCRCE)
            # Send ZEOF
            f0, f1, f2, f3 = _encode_offset(len(file_data))
            await piped.side_a.write(_build_hex_header(ZEOF, f0, f1, f2, f3))
            # Wait for ZACK / ZRINIT
            await _read_hex_frame(piped.side_a)
            # ZFIN
            await piped.side_a.write(_build_hex_header(ZFIN, 0, 0, 0, 0))
            await _read_hex_frame(piped.side_a)  # consume ZFIN ack

        paths = await asyncio.gather(
            receiver_engine.receive(str(tmp_path)),
            sender(),
        )
        with open(paths[0][0], "rb") as f:
            assert f.read() == file_data
        assert EventType.FILE_START in event_log.types()
        assert EventType.SESSION_END in event_log.types()

    async def test_receive_rqinit_triggers_zrinit(self, piped, tmp_path):
        """Receiver re-sends ZRINIT when it gets ZRQINIT."""
        receiver_engine = ZModem(piped.side_b, timeout=1.0)

        async def sender():
            # First frame is ZRINIT (initial advertisement)
            frame = await _read_hex_frame(piped.side_a)
            assert frame[0] == ZRINIT
            # Send ZRQINIT to solicit another ZRINIT
            await piped.side_a.write(_build_hex_header(ZRQINIT, 0, 0, 0, 0))
            # Should receive ZRINIT again
            frame = await _read_hex_frame(piped.side_a)
            assert frame[0] == ZRINIT
            # End session
            await piped.side_a.write(_build_hex_header(ZFIN, 0, 0, 0, 0))
            await _read_hex_frame(piped.side_a)

        await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())

    async def test_receive_cancel(self, piped, tmp_path):
        """Receiver raises TransferCancelled on ZABORT."""
        receiver_engine = ZModem(piped.side_b, timeout=1.0)

        async def sender():
            await _read_hex_frame(piped.side_a)  # ZRINIT
            await piped.side_a.write(_build_hex_header(ZABORT, 0, 0, 0, 0))

        with pytest.raises(TransferCancelled):
            await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())

    async def test_receive_resume(self, piped, tmp_path):
        """Receiver sends correct resume offset when partial file exists."""
        file_data = b"W" * 512
        partial = file_data[:256]
        dest = tmp_path / "w.bin"
        dest.write_bytes(partial)
        receiver_engine = ZModem(piped.side_b, timeout=1.0)

        async def sender():
            await _read_hex_frame(piped.side_a)  # ZRINIT
            await piped.side_a.write(_build_hex_header(ZFILE, 0, 0, 0, 0))
            info = f"w.bin\x00{len(file_data)} {int(time.time()):o} 0 0 1 {len(file_data)}"
            await _write_subpacket(piped.side_a, info.encode(), ZCRCW)
            frame = await _read_hex_frame(piped.side_a)
            assert frame[0] == ZRPOS
            offset = frame[1] | (frame[2] << 8) | (frame[3] << 16) | (frame[4] << 24)
            assert offset == 256
            # Send remaining data starting at the resume offset
            rf0, rf1, rf2, rf3 = _encode_offset(offset)
            await piped.side_a.write(_build_bin32_header(ZDATA, rf0, rf1, rf2, rf3))
            await _write_subpacket(piped.side_a, file_data[offset:], ZCRCE)
            f0, f1, f2, f3 = _encode_offset(len(file_data))
            await piped.side_a.write(_build_hex_header(ZEOF, f0, f1, f2, f3))
            await _read_hex_frame(piped.side_a)
            await piped.side_a.write(_build_hex_header(ZFIN, 0, 0, 0, 0))
            await _read_hex_frame(piped.side_a)

        paths = await asyncio.gather(
            receiver_engine.receive(str(tmp_path)),
            sender(),
        )
        with open(paths[0][0], "rb") as f:
            assert f.read() == file_data

    async def test_receive_zcrcw_sends_zack(self, piped, tmp_path):
        """Receiver sends ZACK when it gets a ZCRCW subpacket."""
        file_data = b"X" * 128
        receiver_engine = ZModem(piped.side_b, timeout=1.0)

        async def sender():
            await _read_hex_frame(piped.side_a)
            await piped.side_a.write(_build_hex_header(ZFILE, 0, 0, 0, 0))
            info = f"x.bin\x00{len(file_data)} {int(time.time()):o}"
            await _write_subpacket(piped.side_a, info.encode(), ZCRCW)
            await _read_hex_frame(piped.side_a)  # ZRPOS
            await piped.side_a.write(_build_bin32_header(ZDATA, 0, 0, 0, 0))
            # First subpacket with ZCRCW (requests window ACK)
            await _write_subpacket(piped.side_a, file_data[:256], ZCRCW)
            ack = await _read_hex_frame(piped.side_a)
            assert ack[0] == ZACK
            # Second subpacket with ZCRCE
            await _write_subpacket(piped.side_a, file_data[256:], ZCRCE)
            f0, f1, f2, f3 = _encode_offset(len(file_data))
            await piped.side_a.write(_build_hex_header(ZEOF, f0, f1, f2, f3))
            await _read_hex_frame(piped.side_a)
            await piped.side_a.write(_build_hex_header(ZFIN, 0, 0, 0, 0))
            await _read_hex_frame(piped.side_a)

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        with open(paths[0][0], "rb") as f:
            assert f.read() == file_data


# ---------------------------------------------------------------------------
# Low-level helpers for the receiver side of tests
# ---------------------------------------------------------------------------


async def _scan_to_zdle(transport, timeout: float = 10.0) -> None:
    for _ in range(512):
        b = await transport.read_byte_with_timeout(timeout)
        if b == ZDLE:
            return
    raise AssertionError("ZDLE not found")


async def _read_hex_frame(transport, timeout: float = 10.0) -> tuple:
    await _scan_to_zdle(transport, timeout)
    fmt = await transport.read_byte_with_timeout(timeout)
    assert fmt == ord("B"), f"Expected hex frame, got format byte 0x{fmt:02x}"
    hex_bytes = b""
    for _ in range(16):
        b = await transport.read_byte_with_timeout(timeout)
        if b in (ord("\r"), ord("\n"), 0x8D):
            continue
        hex_bytes += bytes([b])
        if len(hex_bytes) == 14:
            break
    raw = bytes.fromhex(hex_bytes[:10].decode("ascii"))
    # Consume exactly the trailing \r\n — do NOT read past them.
    for _ in range(2):
        try:
            await transport.read_byte_with_timeout(0.05)
        except asyncio.TimeoutError:
            break
    return tuple(raw)


async def _read_bin32_frame(transport, timeout: float = 10.0) -> tuple:
    await _scan_to_zdle(transport, timeout)
    fmt = await transport.read_byte_with_timeout(timeout)
    assert fmt == ord("C"), f"Expected ZBIN32 frame, got 0x{fmt:02x}"
    raw = bytearray()
    while len(raw) < 9:
        b = await transport.read_byte_with_timeout(timeout)
        if b == ZDLE:
            esc = await transport.read_byte_with_timeout(timeout)
            raw.append(esc ^ 0x40)
        else:
            raw.append(b)
    return tuple(raw[:5])


async def _read_subpacket(transport, timeout: float = 10.0) -> bytes:
    data, _ = await _read_subpacket_with_term(transport, timeout)
    return data


async def _read_subpacket_with_term(transport, timeout: float = 10.0) -> tuple[bytes, int]:
    buf = bytearray()
    while True:
        b = await transport.read_byte_with_timeout(timeout)
        if b != ZDLE:
            buf.append(b)
            continue
        esc = await transport.read_byte_with_timeout(timeout)
        if esc in (ZCRCW, ZCRCG, ZCRCE, ord("i")):
            # Read and discard 4 CRC bytes
            crc_raw = bytearray()
            for _ in range(4):
                cb = await transport.read_byte_with_timeout(timeout)
                if cb == ZDLE:
                    cb2 = await transport.read_byte_with_timeout(timeout)
                    crc_raw.append(cb2 ^ 0x40)
                else:
                    crc_raw.append(cb)
            return (bytes(buf), esc)
        else:
            buf.append(esc ^ 0x40)


async def _write_subpacket(transport, data: bytes, term: int) -> None:
    from yesterwind_xyzmodem.zmodem import _zdle_encode

    crc_input = data + bytes([term])
    c = crc32(crc_input)
    encoded = _zdle_encode(data)
    crc_bytes = _zdle_encode(bytes([c & 0xFF, (c >> 8) & 0xFF, (c >> 16) & 0xFF, (c >> 24) & 0xFF]))
    await transport.write(encoded + bytes([ZDLE, term]) + crc_bytes)
