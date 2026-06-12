"""
Third and final batch of coverage tests — closes the remaining 8 uncovered lines.
"""

from __future__ import annotations

import asyncio
import io
import os
import time

import pytest

from yesterwind_xyzmodem.constants import ACK, CAN, CRC_MODE, EOT, NAK, SOH, STX, SUB
from yesterwind_xyzmodem.crc import crc16, crc32
from yesterwind_xyzmodem.exceptions import (
    ProtocolError,
    TransferCancelled,
    TransferFailed,
    TransferTimeout,
)
from yesterwind_xyzmodem.transport import MemoryTransport
from yesterwind_xyzmodem.ymodem import YModem, _DATA_BLOCK_SIZE, _HEADER_BLOCK_SIZE
from yesterwind_xyzmodem.zmodem import (
    ZABORT,
    ZACK,
    ZBIN,
    ZBIN32,
    ZCRCE,
    ZCRCG,
    ZCRCQ,
    ZCRCW,
    ZDATA,
    ZDLE,
    ZEOF,
    ZFIN,
    ZFILE,
    ZHEX,
    ZRPOS,
    ZRINIT,
    ZRQINIT,
    ZModem,
    _build_bin32_header,
    _build_hex_header,
    _encode_offset,
    _zdle_encode,
)
from tests.test_zmodem import (
    _read_hex_frame,
    _read_bin32_frame,
    _read_subpacket,
    _read_subpacket_with_term,
    _write_subpacket,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_header_block(filename: str, size: int, mtime: int) -> bytes:
    meta = f"{filename}\x00{size} {mtime:o}"
    payload = meta.encode("ascii")[:_HEADER_BLOCK_SIZE].ljust(_HEADER_BLOCK_SIZE, b"\x00")
    c = crc16(payload)
    return bytes([SOH, 0x00, 0xFF]) + payload + bytes([c >> 8, c & 0xFF])


def _make_data_block(block_num: int, data: bytes) -> bytes:
    assert len(data) == _DATA_BLOCK_SIZE
    c = crc16(data)
    return bytes([STX, block_num, (~block_num) & 0xFF]) + data + bytes([c >> 8, c & 0xFF])


def _pad1k(payload: bytes) -> bytes:
    return payload + bytes([SUB] * (_DATA_BLOCK_SIZE - len(payload)))


def _empty_header() -> bytes:
    payload = bytes(_HEADER_BLOCK_SIZE)
    c = crc16(payload)
    return bytes([SOH, 0x00, 0xFF]) + payload + bytes([c >> 8, c & 0xFF])


# ---------------------------------------------------------------------------
# YModem: ymodem.py 426-428 — garbage header byte in data receive loop
# ---------------------------------------------------------------------------

async def test_ymodem_receive_data_garbage_header(piped, tmp_path):
    """426-428: non-SOH/STX/EOT/CAN data header byte → NAK → retry."""
    file_data = b"A" * 128
    mtime = int(time.time())
    receiver_engine = YModem(piped.side_b, timeout=0.5, retry_limit=5)

    async def sender():
        await piped.side_a.read_byte()  # C
        await piped.side_a.write(_make_header_block("a.bin", len(file_data), mtime))
        await piped.side_a.read_byte()  # ACK
        await piped.side_a.read_byte()  # C
        # Send garbage header byte
        await piped.side_a.write(bytes([0xFF]))
        await piped.side_a.read_byte()  # NAK
        # Retry with valid data block
        await piped.side_a.write(_make_data_block(1, _pad1k(file_data)))
        await piped.side_a.read_byte()  # ACK
        await piped.side_a.write(bytes([EOT]))
        await piped.side_a.read_byte()
        await piped.side_a.read_byte()
        await piped.side_a.write(_empty_header())
        await piped.side_a.read_byte()

    paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
    assert len(paths[0]) == 1


# ---------------------------------------------------------------------------
# ZModem: 431->422, 439->413 — ZCRCG and unknown frame in _receive_file_data
# ---------------------------------------------------------------------------

async def test_zmodem_receive_zcrcg_subpacket(piped, tmp_path):
    """431->422: ZCRCG subpacket in _receive_file_data continues inner loop."""
    file_data = b"B" * 2048  # > _SUBPACKET_SIZE, forces ZCRCG subpackets
    receiver_engine = ZModem(piped.side_b, timeout=5.0)

    async def sender():
        await _read_hex_frame(piped.side_a)  # ZRINIT
        await piped.side_a.write(_build_hex_header(ZFILE, 0, 0, 0, 0))
        info = f"b.bin\x00{len(file_data)}"
        await _write_subpacket(piped.side_a, info.encode(), ZCRCW)
        await _read_hex_frame(piped.side_a)  # ZRPOS
        await piped.side_a.write(_build_bin32_header(ZDATA, 0, 0, 0, 0))
        # Send first subpacket with ZCRCG (more data follows)
        first_chunk = file_data[:1024]
        await _write_subpacket(piped.side_a, first_chunk, ZCRCG)
        # Send second subpacket with ZCRCE (final)
        second_chunk = file_data[1024:]
        await _write_subpacket(piped.side_a, second_chunk, ZCRCE)
        f0, f1, f2, f3 = _encode_offset(len(file_data))
        await piped.side_a.write(_build_hex_header(ZEOF, f0, f1, f2, f3))
        await _read_hex_frame(piped.side_a)  # ZRINIT
        await piped.side_a.write(_build_hex_header(ZFIN, 0, 0, 0, 0))
        await _read_hex_frame(piped.side_a)

    paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
    assert len(paths[0]) == 1
    assert os.path.getsize(paths[0][0]) == len(file_data)


async def test_zmodem_receive_unknown_ftype_in_data_phase(piped, tmp_path):
    """439->413: unknown frame type in _receive_file_data → loop continues."""
    file_data = b"C" * 64
    receiver_engine = ZModem(piped.side_b, timeout=1.0)

    async def sender():
        await _read_hex_frame(piped.side_a)
        await piped.side_a.write(_build_hex_header(ZFILE, 0, 0, 0, 0))
        info = f"c.bin\x00{len(file_data)}"
        await _write_subpacket(piped.side_a, info.encode(), ZCRCW)
        await _read_hex_frame(piped.side_a)
        await piped.side_a.write(_build_bin32_header(ZDATA, 0, 0, 0, 0))
        await _write_subpacket(piped.side_a, file_data, ZCRCE)
        # Send ZRQINIT (unexpected, not ZEOF/ZDATA/ZABORT) → fallthrough loop
        await piped.side_a.write(_build_hex_header(ZRQINIT, 0, 0, 0, 0))
        # Then send ZEOF
        f0, f1, f2, f3 = _encode_offset(len(file_data))
        await piped.side_a.write(_build_hex_header(ZEOF, f0, f1, f2, f3))
        await _read_hex_frame(piped.side_a)
        await piped.side_a.write(_build_hex_header(ZFIN, 0, 0, 0, 0))
        await _read_hex_frame(piped.side_a)

    paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
    assert len(paths[0]) == 1


# ---------------------------------------------------------------------------
# ZModem: 454 — TransferTimeout in _read_header scan
# ---------------------------------------------------------------------------

async def test_zmodem_header_scan_partial_timeout():
    """454: read_byte_with_timeout raises TimeoutError mid-scan → TransferTimeout."""
    # Feed 50 non-ZDLE bytes — scan loop runs out of buffer mid-way, not at 1024
    t = MemoryTransport(bytes([0x41] * 50))
    engine = ZModem(t, timeout=1.0)
    with pytest.raises(TransferTimeout):
        await engine._read_header()


# ---------------------------------------------------------------------------
# ZModem: 499-500 — trailing \r\n timeout in _read_hex_header
# ---------------------------------------------------------------------------

async def test_zmodem_hex_header_no_trailing_crlf():
    """499-500: valid hex header with no trailing \\r\\n → timeout break → return ok."""
    frame_type = ZRINIT
    raw = bytes([frame_type, 0x23, 0, 0, 0])
    c = crc16(raw)
    hex_data = raw.hex().encode("ascii")     # 10 chars
    hex_crc = f"{c:04X}".encode("ascii")    # 4 chars
    # No trailing \r\n — causes timeout in trailing read loop
    frame = bytes([ZDLE, ZHEX]) + hex_data + hex_crc
    t = MemoryTransport(frame)
    engine = ZModem(t, timeout=1.0)
    result = await engine._read_header()
    assert result[0] == ZRINIT


# ---------------------------------------------------------------------------
# ZModem: 509-510 — ZDLE escape in _read_bin_header
# ---------------------------------------------------------------------------

async def test_zmodem_bin32_header_with_escaped_bytes():
    """509-510: ZBIN32 header where a payload byte requires ZDLE escaping."""
    # Use ZDLE (0x18) as f0 — it'll be escaped as \x18\x58 in the header
    frame_type = ZRINIT
    f0, f1, f2, f3 = 0x18, 0, 0, 0  # f0=ZDLE will be escaped
    header = _build_bin32_header(frame_type, f0, f1, f2, f3)
    t = MemoryTransport(header)
    engine = ZModem(t, timeout=1.0)
    result = await engine._read_header()
    assert result[0] == ZRINIT
    assert result[1] == f0  # decoded correctly
