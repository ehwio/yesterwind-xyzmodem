"""
Second batch of targeted coverage tests.

Covers remaining YModem and ZModem branches after the first batch.
Uses a mix of piped (two-party async) and MemoryTransport (direct unit) tests.
"""

from __future__ import annotations

import asyncio
import io
import os
import time

import pytest

from tests.test_zmodem import (
    _read_bin32_frame,
    _read_hex_frame,
    _read_subpacket,
    _read_subpacket_with_term,
    _write_subpacket,
)
from yesterwind_xyzmodem.constants import ACK, CAN, CRC_MODE, EOT, NAK, SOH, STX, SUB
from yesterwind_xyzmodem.crc import crc16, crc32
from yesterwind_xyzmodem.exceptions import (
    ProtocolError,
    TransferCancelled,
    TransferFailed,
)
from yesterwind_xyzmodem.transport import MemoryTransport
from yesterwind_xyzmodem.ymodem import _DATA_BLOCK_SIZE, _HEADER_BLOCK_SIZE, YModem
from yesterwind_xyzmodem.zmodem import (
    ZABORT,
    ZACK,
    ZBIN,
    ZBIN32,
    ZCRCE,
    ZCRCW,
    ZDATA,
    ZDLE,
    ZEOF,
    ZFILE,
    ZFIN,
    ZHEX,
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


def _zbin_header(frame_type: int, f0: int, f1: int, f2: int, f3: int) -> bytes:
    """Build a ZBIN (CRC-16) encoded header."""
    payload = bytes([frame_type, f0, f1, f2, f3])
    c = crc16(payload)
    crc_bytes = bytes([c >> 8, c & 0xFF])
    return bytes([ZDLE, ZBIN]) + _zdle_encode(payload + crc_bytes)


# ---------------------------------------------------------------------------
# YModem sender: retry loop paths
# ---------------------------------------------------------------------------


class TestYModemSenderLoops:
    async def test_wait_for_handshake_junk_byte(self, piped):
        """201->193: junk byte in handshake loop causes retry."""
        file_data = b"A" * _DATA_BLOCK_SIZE
        sender = YModem(piped.side_a, timeout=1.0)

        async def receiver():
            await piped.side_b.write(bytes([0xFF]))  # junk (not C/G/CAN)
            await piped.side_b.write(bytes([CRC_MODE]))  # correct handshake
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + _DATA_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))
            eot = await piped.side_b.read_byte()
            assert eot == EOT
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)  # end block0
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(
            sender.send([("a.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )

    async def test_eot_nak_then_ack(self, piped):
        """273->263: NAK on first EOT → retry → ACK."""
        file_data = b"B" * _DATA_BLOCK_SIZE
        sender = YModem(piped.side_a, timeout=1.0)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + _DATA_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))
            eot1 = await piped.side_b.read_byte()
            assert eot1 == EOT
            await piped.side_b.write(bytes([NAK]))  # NAK first EOT
            eot2 = await piped.side_b.read_byte()
            assert eot2 == EOT
            await piped.side_b.write(bytes([ACK]))  # ACK second EOT
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(
            sender.send([("b.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )

    async def test_wait_for_ack_nak_then_ack(self, piped):
        """307->300: NAK in _wait_for_ack → loop → ACK (no block resend)."""
        file_data = b"C" * _DATA_BLOCK_SIZE
        sender = YModem(piped.side_a, timeout=1.0)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([NAK]))  # _wait_for_ack loops on NAK
            await piped.side_b.write(bytes([ACK]))  # then ACK (sender doesn't resend)
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + _DATA_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))
            eot = await piped.side_b.read_byte()
            assert eot == EOT
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(
            sender.send([("c.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )

    async def test_wait_for_c_junk_then_c(self, piped):
        """319->312: junk byte in _wait_for_c loop → retry → C."""
        file_data = b"D" * _DATA_BLOCK_SIZE
        sender = YModem(piped.side_a, timeout=1.0)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.write(bytes([NAK]))  # junk instead of C
            await piped.side_b.write(bytes([CRC_MODE]))  # correct C
            await piped.side_b.read(3 + _DATA_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))
            eot = await piped.side_b.read_byte()
            assert eot == EOT
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read(3 + _HEADER_BLOCK_SIZE + 2)
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(
            sender.send([("d.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )


# ---------------------------------------------------------------------------
# YModem receiver: block0 error paths
# ---------------------------------------------------------------------------


class TestYModemReceiverBlock0:
    async def test_receive_block0_can(self, piped, tmp_path):
        """342: CAN received as block0 header byte raises TransferCancelled."""
        receiver_engine = YModem(piped.side_b, timeout=0.5, retry_limit=3)

        async def sender():
            await piped.side_a.read_byte()  # 'C'
            await piped.side_a.write(bytes([CAN]))

        with pytest.raises(TransferCancelled):
            await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())

    async def test_receive_block0_rest_timeout(self, piped, tmp_path):
        """351-353: timeout reading block0 rest → NAK → retry → success."""
        file_data = b"E" * 128
        mtime = int(time.time())
        receiver_engine = YModem(piped.side_b, timeout=0.08, retry_limit=5)

        async def sender():
            await piped.side_a.read_byte()  # 'C'
            # Partial: send header byte only, let receiver timeout
            await piped.side_a.write(bytes([SOH]))
            await asyncio.sleep(0.15)
            await piped.side_a.read_byte()  # NAK
            # Send full valid block0
            await piped.side_a.write(_make_header_block("e.bin", len(file_data), mtime))
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.read_byte()  # C
            await piped.side_a.write(_make_data_block(1, _pad1k(file_data)))
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()  # C for next file
            await piped.side_a.write(_empty_header())
            await piped.side_a.read_byte()

        await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())

    async def test_receive_block0_no_meta(self, piped, tmp_path):
        """382->395: meta_bytes is empty → (filename, 0, 0)."""
        file_data = b"F" * 128
        receiver_engine = YModem(piped.side_b, timeout=0.5, retry_limit=3)

        async def sender():
            await piped.side_a.read_byte()
            # Block0 with filename but NO metadata after null
            meta = "f.bin\x00"
            payload = meta.encode("ascii").ljust(_HEADER_BLOCK_SIZE, b"\x00")
            c = crc16(payload)
            frame = bytes([SOH, 0x00, 0xFF]) + payload + bytes([c >> 8, c & 0xFF])
            await piped.side_a.write(frame)
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.read_byte()  # C
            await piped.side_a.write(_make_data_block(1, _pad1k(file_data)))
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            await piped.side_a.write(_empty_header())
            await piped.side_a.read_byte()

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert len(paths[0]) == 1

    async def test_receive_block0_empty_parts(self, piped, tmp_path):
        """384->389: whitespace-only metadata → empty parts list."""
        file_data = b"G" * 128
        receiver_engine = YModem(piped.side_b, timeout=0.5, retry_limit=3)

        async def sender():
            await piped.side_a.read_byte()
            # Block0 with filename + null + only whitespace (no numeric fields)
            meta = "g.bin\x00   "
            payload = meta.encode("ascii").ljust(_HEADER_BLOCK_SIZE, b"\x00")
            c = crc16(payload)
            frame = bytes([SOH, 0x00, 0xFF]) + payload + bytes([c >> 8, c & 0xFF])
            await piped.side_a.write(frame)
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            await piped.side_a.write(_make_data_block(1, _pad1k(file_data)))
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            await piped.side_a.write(_empty_header())
            await piped.side_a.read_byte()

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert len(paths[0]) == 1

    async def test_receive_block0_size_only(self, piped, tmp_path):
        """389->395: only one metadata field (size, no mtime)."""
        file_data = b"H" * 128
        receiver_engine = YModem(piped.side_b, timeout=0.5, retry_limit=3)

        async def sender():
            await piped.side_a.read_byte()
            # Block0 with filename + null + size only (no mtime or mode)
            meta = f"h.bin\x00{len(file_data)}"
            payload = meta.encode("ascii").ljust(_HEADER_BLOCK_SIZE, b"\x00")
            c = crc16(payload)
            frame = bytes([SOH, 0x00, 0xFF]) + payload + bytes([c >> 8, c & 0xFF])
            await piped.side_a.write(frame)
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            await piped.side_a.write(_make_data_block(1, _pad1k(file_data)))
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            await piped.side_a.write(_empty_header())
            await piped.side_a.read_byte()

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert len(paths[0]) == 1


# ---------------------------------------------------------------------------
# YModem receiver: data block error paths
# ---------------------------------------------------------------------------


class TestYModemReceiverData:
    async def test_data_rest_timeout(self, piped, tmp_path):
        """435-438: timeout reading data block rest → NAK → retry."""
        file_data = b"I" * 128
        mtime = int(time.time())
        receiver_engine = YModem(piped.side_b, timeout=0.08, retry_limit=5)

        async def sender():
            await piped.side_a.read_byte()  # C
            await piped.side_a.write(_make_header_block("i.bin", len(file_data), mtime))
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.read_byte()  # C
            # Send STX header only (timeout on rest)
            await piped.side_a.write(bytes([STX]))
            await asyncio.sleep(0.15)
            await piped.side_a.read_byte()  # NAK
            # Send full valid block
            await piped.side_a.write(_make_data_block(1, _pad1k(file_data)))
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            await piped.side_a.write(_empty_header())
            await piped.side_a.read_byte()

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert len(paths[0]) == 1

    async def test_data_bad_complement(self, piped, tmp_path):
        """443-445: bad block complement → NAK → retry."""
        file_data = b"J" * 128
        mtime = int(time.time())
        receiver_engine = YModem(piped.side_b, timeout=0.5, retry_limit=5)

        async def sender():
            await piped.side_a.read_byte()
            await piped.side_a.write(_make_header_block("j.bin", len(file_data), mtime))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            # Send block with bad complement (0x00 instead of 0xFE for block 1)
            data = _pad1k(file_data)
            c = crc16(data)
            bad_block = bytes([STX, 1, 0x00]) + data + bytes([c >> 8, c & 0xFF])
            await piped.side_a.write(bad_block)
            await piped.side_a.read_byte()  # NAK
            # Retry with correct block
            await piped.side_a.write(_make_data_block(1, _pad1k(file_data)))
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            await piped.side_a.write(_empty_header())
            await piped.side_a.read_byte()

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert len(paths[0]) == 1

    async def test_data_duplicate_block(self, piped, tmp_path):
        """460-461: duplicate block (block_num == expected - 1) → ACK silently."""
        file_data = b"K" * 128
        mtime = int(time.time())
        receiver_engine = YModem(piped.side_b, timeout=0.5, retry_limit=5)

        async def sender():
            await piped.side_a.read_byte()
            await piped.side_a.write(_make_header_block("k.bin", len(file_data), mtime))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            # Send block 0 (duplicate of the header block num, which is expected-1=0)
            data = _pad1k(file_data)
            c = crc16(data)
            dup = bytes([STX, 0, 0xFF]) + data + bytes([c >> 8, c & 0xFF])
            await piped.side_a.write(dup)
            await piped.side_a.read_byte()  # ACK (silent dup)
            # Send real block 1
            await piped.side_a.write(_make_data_block(1, _pad1k(file_data)))
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()
            await piped.side_a.read_byte()
            await piped.side_a.write(_empty_header())
            await piped.side_a.read_byte()

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert len(paths[0]) == 1

    async def test_g_mode_no_ack(self, piped, tmp_path):
        """475->410: g_mode receiver does NOT send ACK after each block."""
        file_data = b"L" * _DATA_BLOCK_SIZE
        mtime = int(time.time())
        receiver_engine = YModem(piped.side_b, timeout=0.5, g_mode=True)

        async def sender():
            await piped.side_a.read_byte()  # C
            await piped.side_a.write(_make_header_block("l.bin", len(file_data), mtime))
            await (
                piped.side_a.read_byte()
            )  # ACK (g_mode sender still sends block0 and waits for ACK)
            await piped.side_a.read_byte()  # C
            # In G mode, send data without waiting for ACKs
            await piped.side_a.write(_make_data_block(1, _pad1k(file_data)))
            # Receiver does NOT ACK in g_mode
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()  # ACK for EOT
            await piped.side_a.read_byte()  # C for batch end
            await piped.side_a.write(_empty_header())
            await piped.side_a.read_byte()

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert len(paths[0]) == 1


# ---------------------------------------------------------------------------
# ZModem sender: session-level paths
# ---------------------------------------------------------------------------


class TestZModemSenderPaths:
    async def test_send_zfin_best_effort_timeout(self, piped):
        """223-224: ZFIN response times out → pass (best-effort)."""
        sender = ZModem(piped.side_a, timeout=0.05)
        file_data = b"M" * 64

        async def receiver():
            await _read_hex_frame(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)  # ZFILE
            await _read_subpacket(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRPOS, 0, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)  # ZDATA
            while True:
                _, term = await _read_subpacket_with_term(piped.side_b)
                if term == ZCRCE:
                    break
            await _read_hex_frame(piped.side_b)  # ZEOF
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_hex_frame(piped.side_b)  # ZFIN — consume but don't respond

        total = await asyncio.gather(
            sender.send([("m.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )
        assert total[0] == len(file_data)

    async def test_send_data_zcrcg(self, piped):
        """345-346, 431->422: data > 1024 bytes → ZCRCG subpackets."""
        file_data = b"N" * 2048  # > _SUBPACKET_SIZE → multiple subpackets
        sender = ZModem(piped.side_a, timeout=5.0)

        async def receiver():
            await _read_hex_frame(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)  # ZFILE
            await _read_subpacket(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRPOS, 0, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)  # ZDATA
            while True:
                _, term = await _read_subpacket_with_term(piped.side_b)
                if term == ZCRCE:
                    break
            await _read_hex_frame(piped.side_b)  # ZEOF
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_hex_frame(piped.side_b)  # ZFIN
            await piped.side_b.write(_build_hex_header(ZFIN, 0, 0, 0, 0))

        total = await asyncio.gather(
            sender.send([("n.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )
        assert total[0] == len(file_data)

    async def test_send_zrpos_wait_unexpected_then_ok(self, piped):
        """326->322: buffered ZRINIT drained, then ZRPOS accepted."""
        file_data = b"O" * 64
        sender = ZModem(piped.side_a, timeout=1.0)

        async def receiver():
            await _read_hex_frame(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)  # ZFILE
            await _read_subpacket(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))  # buffered extra ZRINIT
            await piped.side_b.write(_build_hex_header(ZRPOS, 0, 0, 0, 0))  # correct
            await _read_bin32_frame(piped.side_b)
            while True:
                _, term = await _read_subpacket_with_term(piped.side_b)
                if term == ZCRCE:
                    break
            await _read_hex_frame(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_hex_frame(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZFIN, 0, 0, 0, 0))

        total = await asyncio.gather(
            sender.send([("o.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )
        assert total[0] == len(file_data)

    async def test_send_zrpos_exhausted(self, piped):
        """329: for-else exhaustion — keep sending ZACK (not ZRPOS)."""
        file_data = b"P" * 64
        sender = ZModem(piped.side_a, timeout=1.0, retry_limit=2)

        async def receiver():
            await _read_hex_frame(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)  # ZFILE
            await _read_subpacket(piped.side_b)
            for _ in range(3):  # more than retry_limit
                await piped.side_b.write(_build_hex_header(ZACK, 0, 0, 0, 0))

        with pytest.raises(TransferFailed):
            await asyncio.gather(
                sender.send([("p.bin", io.BytesIO(file_data), len(file_data))]),
                receiver(),
            )

    async def test_send_zeof_timeout_retry(self, piped):
        """365: timeout in ZRINIT wait → continue → then ZRINIT."""
        file_data = b"Q" * 64
        sender = ZModem(piped.side_a, timeout=0.08, retry_limit=4)

        async def receiver():
            await _read_hex_frame(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)  # ZFILE
            await _read_subpacket(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRPOS, 0, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)
            while True:
                _, term = await _read_subpacket_with_term(piped.side_b)
                if term == ZCRCE:
                    break
            await _read_hex_frame(piped.side_b)  # ZEOF
            await asyncio.sleep(0.15)  # cause timeout on ZRINIT wait
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_hex_frame(piped.side_b)  # ZFIN
            await piped.side_b.write(_build_hex_header(ZFIN, 0, 0, 0, 0))

        total = await asyncio.gather(
            sender.send([("q.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )
        assert total[0] == len(file_data)

    async def test_send_zeof_unexpected_frame_then_ok(self, piped):
        """368->361: unexpected frame (not ZRINIT/ZACK/ZABORT) in ZRINIT wait → loop."""
        file_data = b"R" * 64
        sender = ZModem(piped.side_a, timeout=1.0, retry_limit=4)

        async def receiver():
            await _read_hex_frame(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)  # ZFILE
            await _read_subpacket(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRPOS, 0, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)
            while True:
                _, term = await _read_subpacket_with_term(piped.side_b)
                if term == ZCRCE:
                    break
            await _read_hex_frame(piped.side_b)  # ZEOF
            await piped.side_b.write(_build_hex_header(ZRQINIT, 0, 0, 0, 0))  # unexpected
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_hex_frame(piped.side_b)  # ZFIN
            await piped.side_b.write(_build_hex_header(ZFIN, 0, 0, 0, 0))

        total = await asyncio.gather(
            sender.send([("r.bin", io.BytesIO(file_data), len(file_data))]),
            receiver(),
        )
        assert total[0] == len(file_data)

    async def test_send_zeof_exhausted(self, piped):
        """370: raise TransferFailed after ZRINIT wait exhausts."""
        file_data = b"S" * 64
        sender = ZModem(piped.side_a, timeout=1.0, retry_limit=2)

        async def receiver():
            await _read_hex_frame(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRINIT, 0x23, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)  # ZFILE
            await _read_subpacket(piped.side_b)
            await piped.side_b.write(_build_hex_header(ZRPOS, 0, 0, 0, 0))
            await _read_bin32_frame(piped.side_b)
            while True:
                _, term = await _read_subpacket_with_term(piped.side_b)
                if term == ZCRCE:
                    break
            await _read_hex_frame(piped.side_b)  # ZEOF
            for _ in range(3):  # keep sending unexpected frames
                await piped.side_b.write(_build_hex_header(ZRQINIT, 0, 0, 0, 0))

        with pytest.raises(TransferFailed):
            await asyncio.gather(
                sender.send([("s.bin", io.BytesIO(file_data), len(file_data))]),
                receiver(),
            )


# ---------------------------------------------------------------------------
# ZModem receiver: session-level paths
# ---------------------------------------------------------------------------


class TestZModemReceiverPaths:
    async def test_receive_mtime_zero(self, piped, tmp_path):
        """280->282: mtime == 0 → os.utime NOT called."""
        file_data = b"T" * 64
        receiver_engine = ZModem(piped.side_b, timeout=1.0)

        async def sender():
            await _read_hex_frame(piped.side_a)
            await piped.side_a.write(_build_hex_header(ZFILE, 0, 0, 0, 0))
            # No mtime (only filename + size, mtime omitted → 0)
            info = f"t.bin\x00{len(file_data)}"
            await _write_subpacket(piped.side_a, info.encode(), ZCRCW)
            await _read_hex_frame(piped.side_a)  # ZRPOS
            await piped.side_a.write(_build_bin32_header(ZDATA, 0, 0, 0, 0))
            await _write_subpacket(piped.side_a, file_data, ZCRCE)
            f0, f1, f2, f3 = _encode_offset(len(file_data))
            await piped.side_a.write(_build_hex_header(ZEOF, f0, f1, f2, f3))
            await _read_hex_frame(piped.side_a)
            await piped.side_a.write(_build_hex_header(ZFIN, 0, 0, 0, 0))
            await _read_hex_frame(piped.side_a)

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert os.path.exists(paths[0][0])

    async def test_receive_unknown_ftype_loops(self, piped, tmp_path):
        """298->247: unknown frame type (not ZRQINIT/ZFIN/ZFILE/ZABORT) → loop."""
        file_data = b"U" * 64
        receiver_engine = ZModem(piped.side_b, timeout=1.0)

        async def sender():
            await _read_hex_frame(piped.side_a)
            # Send unexpected ZACK first, then ZFILE
            await piped.side_a.write(_build_hex_header(ZACK, 0, 0, 0, 0))
            await piped.side_a.write(_build_hex_header(ZFILE, 0, 0, 0, 0))
            info = f"u.bin\x00{len(file_data)}"
            await _write_subpacket(piped.side_a, info.encode(), ZCRCW)
            await _read_hex_frame(piped.side_a)
            await piped.side_a.write(_build_bin32_header(ZDATA, 0, 0, 0, 0))
            await _write_subpacket(piped.side_a, file_data, ZCRCE)
            f0, f1, f2, f3 = _encode_offset(len(file_data))
            await piped.side_a.write(_build_hex_header(ZEOF, f0, f1, f2, f3))
            await _read_hex_frame(piped.side_a)
            await piped.side_a.write(_build_hex_header(ZFIN, 0, 0, 0, 0))
            await _read_hex_frame(piped.side_a)

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert len(paths[0]) == 1

    async def test_receive_zrqinit_resends_zrinit(self, piped, tmp_path):
        """251-252: ZRQINIT → sender re-sends ZRINIT."""
        file_data = b"V" * 64
        receiver_engine = ZModem(piped.side_b, timeout=1.0)

        async def sender():
            await _read_hex_frame(piped.side_a)  # initial ZRINIT
            await piped.side_a.write(_build_hex_header(ZRQINIT, 0, 0, 0, 0))  # provoke re-ZRINIT
            await _read_hex_frame(piped.side_a)  # re-ZRINIT
            await piped.side_a.write(_build_hex_header(ZFILE, 0, 0, 0, 0))
            info = f"v.bin\x00{len(file_data)}"
            await _write_subpacket(piped.side_a, info.encode(), ZCRCW)
            await _read_hex_frame(piped.side_a)
            await piped.side_a.write(_build_bin32_header(ZDATA, 0, 0, 0, 0))
            await _write_subpacket(piped.side_a, file_data, ZCRCE)
            f0, f1, f2, f3 = _encode_offset(len(file_data))
            await piped.side_a.write(_build_hex_header(ZEOF, f0, f1, f2, f3))
            await _read_hex_frame(piped.side_a)
            await piped.side_a.write(_build_hex_header(ZFIN, 0, 0, 0, 0))
            await _read_hex_frame(piped.side_a)

        paths = await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())
        assert len(paths[0]) == 1

    async def test_receive_zabort_in_data(self, piped, tmp_path):
        """439-440: ZABORT while waiting for ZDATA → TransferCancelled."""
        receiver_engine = ZModem(piped.side_b, timeout=1.0)

        async def sender():
            await _read_hex_frame(piped.side_a)  # ZRINIT
            await piped.side_a.write(_build_hex_header(ZFILE, 0, 0, 0, 0))
            info = b"w.bin\x00128"
            await _write_subpacket(piped.side_a, info, ZCRCW)
            await _read_hex_frame(piped.side_a)  # ZRPOS
            # Send ZABORT instead of ZDATA
            await piped.side_a.write(_build_hex_header(ZABORT, 0, 0, 0, 0))

        with pytest.raises(TransferCancelled):
            await asyncio.gather(receiver_engine.receive(str(tmp_path)), sender())


# ---------------------------------------------------------------------------
# ZModem: direct protocol-level unit tests (MemoryTransport)
# ---------------------------------------------------------------------------


class TestZModemProtocolUnit:
    async def test_read_zfile_data_no_null(self):
        """395: _read_zfile_data returns (data_as_str, 0, 0, 0) when no null byte."""
        from yesterwind_xyzmodem.zmodem import ZCRCW

        # Build a subpacket with no null byte
        data = b"justfilename"
        t = MemoryTransport()
        engine = ZModem(t, timeout=1.0)
        # Feed the subpacket bytes directly
        crc_input = data + bytes([ZCRCW])
        c = crc32(crc_input)
        encoded = _zdle_encode(data)
        crc_bytes = _zdle_encode(
            bytes([c & 0xFF, (c >> 8) & 0xFF, (c >> 16) & 0xFF, (c >> 24) & 0xFF])
        )
        t.feed(encoded + bytes([ZDLE, ZCRCW]) + crc_bytes)
        filename, size, mtime, mode = await engine._read_zfile_data()
        assert filename == "justfilename"
        assert size == 0
        assert mtime == 0

    async def test_header_scan_timeout(self):
        """454: raise TransferTimeout when ZDLE not found within limit."""
        t = MemoryTransport(bytes([0x41] * 1024))  # 1024 non-ZDLE bytes
        engine = ZModem(t, timeout=1.0)
        with pytest.raises(ProtocolError, match="No ZDLE"):
            await engine._read_header()

    async def test_zbin_header_reads_correctly(self):
        """465: ZBIN (CRC-16) header format is parsed."""
        # Build a ZBIN header manually
        frame_type = ZRINIT
        payload = bytes([frame_type, 0x23, 0, 0, 0])
        c = crc16(payload)
        crc_b = bytes([c >> 8, c & 0xFF])
        # ZBIN header: ZDLE + ZBIN + ZDLE-encoded (payload + crc16)
        raw = bytes([ZDLE, ZBIN]) + _zdle_encode(payload + crc_b)
        t = MemoryTransport(raw)
        engine = ZModem(t, timeout=1.0)
        result = await engine._read_header()
        assert result[0] == ZRINIT

    async def test_hex_header_truncated(self):
        """479->486, 482, 487: ProtocolError on truncated hex header (16 skip-bytes)."""
        # Feed 16 carriage-return bytes (all skipped) — loop exhausts without 14 hex chars
        data = bytes([ZDLE, ZHEX]) + bytes([0x0D] * 16)
        t = MemoryTransport(data)
        engine = ZModem(t, timeout=1.0)
        with pytest.raises(ProtocolError):
            await engine._read_header()

    async def test_hex_header_bad_hex_chars(self):
        """490-491: ValueError in fromhex → ProtocolError."""
        # Feed ZDLE + ZHEX + 14 invalid hex chars + \r\n
        bad = bytes([ZDLE, ZHEX]) + b"GGGGGGGGGGGGGG" + b"\r\n"
        t = MemoryTransport(bad)
        engine = ZModem(t, timeout=1.0)
        with pytest.raises(ProtocolError):
            await engine._read_header()

    async def test_hex_header_crc_error(self):
        """493-494: CRC mismatch in hex header → ProtocolError."""
        frame_type = ZRINIT
        raw = bytes([frame_type, 0, 0, 0, 0])
        hex_data = raw.hex().encode("ascii")  # 10 chars
        wrong_crc = b"FFFF"  # bad CRC
        frame = bytes([ZDLE, ZHEX]) + hex_data + wrong_crc + b"\r\n"
        t = MemoryTransport(frame)
        engine = ZModem(t, timeout=1.0)
        with pytest.raises(ProtocolError):
            await engine._read_header()

    async def test_bin32_header_crc_error(self):
        """499-500: ZBIN32 CRC-32 mismatch → ProtocolError."""
        # Build valid-looking payload but with wrong CRC-32
        payload = bytes([ZRINIT, 0, 0, 0, 0])
        wrong_crc = bytes([0xFF, 0xFF, 0xFF, 0xFF])  # definitely wrong
        raw = bytes([ZDLE, ZBIN32]) + _zdle_encode(payload + wrong_crc)
        t = MemoryTransport(raw)
        engine = ZModem(t, timeout=1.0)
        with pytest.raises(ProtocolError):
            await engine._read_header()

    async def test_bin_header_crc_error(self):
        """509-510, 518-520: ZBIN CRC-16 mismatch → ProtocolError."""
        payload = bytes([ZRINIT, 0, 0, 0, 0])
        wrong_crc = bytes([0xFF, 0xFF])  # wrong CRC16
        raw = bytes([ZDLE, ZBIN]) + _zdle_encode(payload + wrong_crc)
        t = MemoryTransport(raw)
        engine = ZModem(t, timeout=1.0)
        with pytest.raises(ProtocolError):
            await engine._read_header()

    async def test_data_subpacket_crc_error(self):
        """550: data subpacket CRC-32 mismatch → ProtocolError."""
        data = b"hello"
        term = ZCRCW
        # Build subpacket with WRONG CRC
        wrong_crc = bytes([0xFF, 0xFF, 0xFF, 0xFF])
        raw = _zdle_encode(data) + bytes([ZDLE, term]) + _zdle_encode(wrong_crc)
        t = MemoryTransport(raw)
        engine = ZModem(t, timeout=1.0)
        with pytest.raises(ProtocolError):
            await engine._read_data_subpacket()

    async def test_header_unknown_format(self):
        """468: unknown format byte after ZDLE → ProtocolError."""
        raw = bytes([ZDLE, 0x7F])  # 0x7F is not ZHEX/ZBIN/ZBIN32
        t = MemoryTransport(raw)
        engine = ZModem(t, timeout=1.0)
        with pytest.raises(ProtocolError):
            await engine._read_header()
