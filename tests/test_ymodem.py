"""Tests for the YModem protocol engine."""

from __future__ import annotations

import asyncio
import io
import os
import time

import pytest

from yesterwind_xyzmodem.callbacks import EventType
from yesterwind_xyzmodem.constants import ACK, CAN, CRC_MODE, EOT, NAK, SOH, STX, SUB
from yesterwind_xyzmodem.crc import crc16
from yesterwind_xyzmodem.exceptions import TransferCancelled, TransferFailed, TransferTimeout
from yesterwind_xyzmodem.ymodem import YModem, _DATA_BLOCK_SIZE, _HEADER_BLOCK_SIZE


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
    """End-of-batch block 0 with empty filename."""
    payload = bytes(_HEADER_BLOCK_SIZE)
    c = crc16(payload)
    return bytes([SOH, 0x00, 0xFF]) + payload + bytes([c >> 8, c & 0xFF])


# ---------------------------------------------------------------------------
# Send tests
# ---------------------------------------------------------------------------

class TestYModemSend:

    async def test_send_single_file(self, piped, event_log):
        """Sender transfers one file to a standard receiver."""
        file_data = b"A" * 512
        sender = YModem(piped.side_a, callback=event_log.callback)

        async def receiver():
            # Handshake
            await piped.side_b.write(bytes([CRC_MODE]))
            # Receive block 0
            hdr_frame = await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            assert hdr_frame[0] == SOH
            assert hdr_frame[1] == 0x00
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.write(bytes([CRC_MODE]))  # ready for data
            # Data block
            data_frame = await piped.side_b.read(3 + _DATA_BLOCK_SIZE + 2)
            assert data_frame[0] == STX
            await piped.side_b.write(bytes([ACK]))
            # EOT
            eot = await piped.side_b.read_byte()
            assert eot == EOT
            await piped.side_b.write(bytes([ACK]))
            # Empty batch-end header
            end_hdr = await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(
            sender.send([("test.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )
        types = event_log.types()
        assert EventType.FILE_START in types
        assert EventType.FILE_END in types
        assert EventType.SESSION_END in types

    async def test_send_g_mode_detected(self, piped):
        """Sender activates G mode when receiver sends 'G'."""
        file_data = b"B" * _DATA_BLOCK_SIZE
        sender = YModem(piped.side_a)

        async def receiver():
            await piped.side_b.write(bytes([ord("G")]))
            # Block 0
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            # In G mode, no ACK/C exchange after block 0
            # Data comes immediately
            await piped.side_b.read(3 + _DATA_BLOCK_SIZE + 2)
            # EOT
            eot = await piped.side_b.read_byte()
            assert eot == EOT
            # End-of-batch header
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)

        await asyncio.gather(
            sender.send([("g.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )
        assert sender._g_mode is True

    async def test_send_cancel_during_handshake(self, piped):
        sender = YModem(piped.side_a, timeout=1.0)

        async def receiver():
            await piped.side_b.write(bytes([CAN]))

        with pytest.raises(TransferCancelled):
            await asyncio.gather(
                sender.send([("f.bin", io.BytesIO(b"x" * 128), 128)]),
                receiver(),
            )

    async def test_send_no_handshake_raises(self, piped):
        sender = YModem(piped.side_a, timeout=0.05, retry_limit=2)
        with pytest.raises(TransferFailed):
            await sender.send([("f.bin", io.BytesIO(b"x"), 1)])

    async def test_send_block_nak_retry(self, piped):
        file_data = b"C" * _DATA_BLOCK_SIZE
        sender = YModem(piped.side_a, retry_limit=5)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)  # block 0
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.write(bytes([CRC_MODE]))
            # NAK first data block
            await piped.side_b.read(3 + _DATA_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([NAK]))
            # ACK retry
            await piped.side_b.read(3 + _DATA_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read_byte()  # EOT
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(
            sender.send([("c.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )

    async def test_send_can_during_transfer(self, piped):
        file_data = b"D" * _DATA_BLOCK_SIZE
        sender = YModem(piped.side_a, timeout=1.0)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + _DATA_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([CAN]))

        with pytest.raises(TransferCancelled):
            await asyncio.gather(
                sender.send([("d.bin", io.BytesIO(file_data), len(file_data))]),
                receiver(),
            )

    async def test_send_eot_retry_exhausted(self, piped):
        file_data = b"E" * _DATA_BLOCK_SIZE
        sender = YModem(piped.side_a, timeout=0.05, retry_limit=2)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + _DATA_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))
            # Never ACK EOT

        with pytest.raises(TransferFailed):
            await asyncio.gather(
                sender.send([("e.bin", io.BytesIO(file_data), len(file_data))]),
                receiver(),
            )


# ---------------------------------------------------------------------------
# Receive tests
# ---------------------------------------------------------------------------

class TestYModemReceive:

    async def test_receive_single_file(self, piped, event_log, tmp_path):
        """Receiver writes correct file data and metadata."""
        file_data = b"F" * 200
        mtime = int(time.time()) - 100
        receiver_engine = YModem(piped.side_b, callback=event_log.callback)

        async def sender():
            # Wait for 'C'
            init = await piped.side_a.read_byte()
            assert init == CRC_MODE
            # Send block 0
            await piped.side_a.write(_make_header_block("hello.bin", len(file_data), mtime))
            # Wait for ACK + C
            ack = await piped.side_a.read_byte()
            assert ack == ACK
            c = await piped.side_a.read_byte()
            assert c == CRC_MODE
            # Send padded data block
            padded = _pad1k(file_data)
            await piped.side_a.write(_make_data_block(1, padded))
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()  # ACK
            # Wait for next 'C' then send empty header
            c2 = await piped.side_a.read_byte()
            assert c2 == CRC_MODE
            await piped.side_a.write(_empty_header())
            ack2 = await piped.side_a.read_byte()
            assert ack2 == ACK

        paths = await asyncio.gather(
            receiver_engine.receive(str(tmp_path)),
            sender(),
        )
        assert len(paths[0]) == 1
        dest = paths[0][0]
        assert open(dest, "rb").read() == file_data
        types = event_log.types()
        assert EventType.FILE_START in types
        assert EventType.FILE_END in types

    async def test_receive_bad_crc_block0_then_good(self, piped, tmp_path):
        """Receiver NAKs corrupt block 0 then accepts retransmission."""
        file_data = b"G" * 128
        mtime = int(time.time())
        receiver_engine = YModem(piped.side_b, timeout=0.5, retry_limit=5)

        async def sender():
            await piped.side_a.read_byte()  # 'C'
            good_hdr = _make_header_block("g.bin", len(file_data), mtime)
            bad_hdr = bytearray(good_hdr)
            bad_hdr[-1] ^= 0xFF
            await piped.side_a.write(bytes(bad_hdr))
            nak = await piped.side_a.read_byte()
            assert nak == NAK
            await piped.side_a.write(good_hdr)
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.read_byte()  # C
            padded = _pad1k(file_data)
            await piped.side_a.write(_make_data_block(1, padded))
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()  # C for next file
            await piped.side_a.write(_empty_header())
            await piped.side_a.read_byte()

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert open(paths[0][0], "rb").read() == file_data

    async def test_receive_cancel_during_data(self, piped, tmp_path):
        """Receiver raises TransferCancelled when CAN arrives during data blocks."""
        receiver_engine = YModem(piped.side_b, timeout=1.0)

        async def sender():
            await piped.side_a.read_byte()
            await piped.side_a.write(
                _make_header_block("x.bin", 1024, int(time.time()))
            )
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.read_byte()  # C
            await piped.side_a.write(bytes([CAN]))

        with pytest.raises(TransferCancelled):
            await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())

    async def test_receive_data_timeout(self, piped, tmp_path):
        """Receiver raises TransferTimeout if no data arrives after block 0."""
        receiver_engine = YModem(piped.side_b, timeout=0.05, retry_limit=2)

        async def sender():
            await piped.side_a.read_byte()
            await piped.side_a.write(
                _make_header_block("y.bin", 1024, int(time.time()))
            )
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.read_byte()  # C
            # Send nothing

        with pytest.raises((TransferTimeout, TransferFailed)):
            await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())

    async def test_receive_crc_error_in_data(self, piped, event_log, tmp_path):
        """Receiver fires CRC_ERROR and NAKs corrupt data block."""
        file_data = b"H" * _DATA_BLOCK_SIZE
        receiver_engine = YModem(piped.side_b, timeout=0.5, retry_limit=5,
                                  callback=event_log.callback)

        async def sender():
            await piped.side_a.read_byte()
            await piped.side_a.write(
                _make_header_block("h.bin", len(file_data), int(time.time()))
            )
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            # Corrupt block
            good = _make_data_block(1, file_data)
            bad = bytearray(good)
            bad[-1] ^= 0xFF
            await piped.side_a.write(bytes(bad))
            nak = await piped.side_a.read_byte()
            assert nak == NAK
            # Send correct block
            await piped.side_a.write(good)
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            await piped.side_a.write(_empty_header())
            await piped.side_a.read_byte()

        await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert EventType.CRC_ERROR in event_log.types()

    async def test_receive_size_trimming(self, piped, tmp_path):
        """Receiver trims padding to match declared file size."""
        actual = b"I" * 300
        receiver_engine = YModem(piped.side_b, timeout=0.5)

        async def sender():
            await piped.side_a.read_byte()
            await piped.side_a.write(_make_header_block("i.bin", 300, int(time.time())))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            await piped.side_a.write(_make_data_block(1, _pad1k(actual)))
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            await piped.side_a.write(_empty_header())
            await piped.side_a.read_byte()

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert open(paths[0][0], "rb").read() == actual
