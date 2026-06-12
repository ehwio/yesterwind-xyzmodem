"""Tests for the XModem protocol engine."""

from __future__ import annotations

import asyncio
import io

import pytest

from yesterwind_xyzmodem.callbacks import EventType
from yesterwind_xyzmodem.constants import ACK, CAN, CRC_MODE, EOT, NAK, SOH, STX, SUB
from yesterwind_xyzmodem.crc import checksum, crc16
from yesterwind_xyzmodem.exceptions import (
    ProtocolError,
    TransferCancelled,
    TransferFailed,
    TransferTimeout,
)
from yesterwind_xyzmodem.xmodem import XModem

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_block(block_num: int, data: bytes, use_crc: bool = True, size: int = 128) -> bytes:
    """Build a valid XModem block frame."""
    assert len(data) == size
    header = SOH if size == 128 else STX
    comp = (~block_num) & 0xFF
    if use_crc:
        c = crc16(data)
        integrity = bytes([c >> 8, c & 0xFF])
    else:
        integrity = bytes([checksum(data)])
    return bytes([header, block_num, comp]) + data + integrity


def _pad(payload: bytes, size: int = 128) -> bytes:
    return payload + bytes([SUB] * (size - len(payload)))


# ---------------------------------------------------------------------------
# Send tests
# ---------------------------------------------------------------------------


class TestXModemSend:
    async def test_send_crc_mode_single_block(self, piped, event_log):
        """Sender completes a one-block transfer in CRC mode."""
        data = b"A" * 128
        sender = XModem(piped.side_a, callback=event_log.callback)

        async def receiver():
            # Handshake: send 'C'
            await piped.side_b.write(bytes([CRC_MODE]))
            # Receive block
            frame = await piped.side_b.read(3 + 128 + 2)
            assert frame[0] == SOH
            assert frame[1] == 1
            assert frame[2] == 0xFE
            # ACK
            await piped.side_b.write(bytes([ACK]))
            # EOT
            eot = await piped.side_b.read_byte()
            assert eot == EOT
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(
            sender.send(io.BytesIO(data), filename="test.bin", total_bytes=128),
            receiver(),
        )
        assert EventType.SESSION_START in event_log.types()
        assert EventType.SESSION_END in event_log.types()

    async def test_send_checksum_mode(self, piped):
        """Sender falls back to checksum mode when receiver sends NAK."""
        data = b"B" * 128
        sender = XModem(piped.side_a)

        async def receiver():
            await piped.side_b.write(bytes([NAK]))  # request checksum mode
            frame = await piped.side_b.read(3 + 128 + 1)
            assert frame[0] == SOH
            # Verify checksum
            assert frame[-1] == checksum(frame[3:-1])
            await piped.side_b.write(bytes([ACK]))
            eot = await piped.side_b.read_byte()
            assert eot == EOT
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(sender.send(io.BytesIO(data)), receiver())

    async def test_send_retries_on_nak(self, piped, event_log):
        """Sender retransmits block after NAK, then succeeds."""
        data = b"C" * 128
        sender = XModem(piped.side_a, callback=event_log.callback)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            # NAK first transmission
            await piped.side_b.read(3 + 128 + 2)
            await piped.side_b.write(bytes([NAK]))
            # ACK second transmission
            await piped.side_b.read(3 + 128 + 2)
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read_byte()  # EOT
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(sender.send(io.BytesIO(data)), receiver())
        assert EventType.BLOCK_NAK in event_log.types()

    async def test_send_cancel_on_can(self, piped):
        """Sender raises TransferCancelled when it receives CAN."""
        data = b"D" * 128
        sender = XModem(piped.side_a, timeout=1.0)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + 128 + 2)
            await piped.side_b.write(bytes([CAN]))

        with pytest.raises(TransferCancelled):
            await asyncio.gather(sender.send(io.BytesIO(data)), receiver())

    async def test_send_1k_blocks(self, piped):
        """Sender uses STX header for 1K blocks."""
        data = b"E" * 1024
        sender = XModem(piped.side_a, block_size=1024)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            frame = await piped.side_b.read(3 + 1024 + 2)
            assert frame[0] == STX
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read_byte()
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(sender.send(io.BytesIO(data)), receiver())

    async def test_send_partial_last_block_padded(self, piped):
        """Last block is padded with SUB bytes."""
        data = b"F" * 50
        sender = XModem(piped.side_a)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            frame = await piped.side_b.read(3 + 128 + 2)
            block_data = frame[3:131]
            assert block_data[:50] == b"F" * 50
            assert block_data[50:] == bytes([SUB] * 78)
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read_byte()
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(sender.send(io.BytesIO(data)), receiver())

    async def test_send_handshake_timeout_then_success(self, piped):
        """Sender waits through one timeout before receiving 'C'."""
        data = b"G" * 128
        sender = XModem(piped.side_a, timeout=0.1, retry_limit=5)

        async def receiver():
            await asyncio.sleep(0.15)  # miss first poll
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + 128 + 2)
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read_byte()
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(sender.send(io.BytesIO(data)), receiver())

    async def test_send_no_handshake_raises(self, piped):
        """Sender raises TransferFailed if no handshake response at all."""
        sender = XModem(piped.side_a, timeout=0.05, retry_limit=2)
        with pytest.raises(TransferFailed):
            await sender.send(io.BytesIO(b"x" * 128))

    async def test_send_eot_timeout_then_success(self, piped):
        """Sender retries EOT after timeout."""
        data = b"H" * 128
        sender = XModem(piped.side_a, timeout=0.1, retry_limit=5)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + 128 + 2)
            await piped.side_b.write(bytes([ACK]))
            # Ignore first EOT
            await piped.side_b.read_byte()
            await asyncio.sleep(0.15)
            # ACK second EOT
            await piped.side_b.read_byte()
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(sender.send(io.BytesIO(data)), receiver())

    async def test_send_block_size_invalid(self):
        from yesterwind_xyzmodem.transport import MemoryTransport

        t = MemoryTransport()
        with pytest.raises(ValueError):
            XModem(t, block_size=256)

    async def test_send_can_during_handshake(self, piped):
        sender = XModem(piped.side_a, timeout=1.0)

        async def receiver():
            await piped.side_b.write(bytes([CAN]))

        with pytest.raises(TransferCancelled):
            await asyncio.gather(sender.send(io.BytesIO(b"x" * 128)), receiver())

    async def test_send_can_on_eot(self, piped):
        data = b"I" * 128
        sender = XModem(piped.side_a, timeout=1.0)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + 128 + 2)
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read_byte()  # EOT
            await piped.side_b.write(bytes([CAN]))

        with pytest.raises(TransferCancelled):
            await asyncio.gather(sender.send(io.BytesIO(data)), receiver())

    async def test_send_block_retry_exhausted(self, piped):
        data = b"J" * 128
        sender = XModem(piped.side_a, timeout=0.1, retry_limit=2)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            for _ in range(3):
                try:
                    await piped.side_b.read_with_timeout(3 + 128 + 2, 0.5)
                    await piped.side_b.write(bytes([NAK]))
                except asyncio.TimeoutError:
                    break

        with pytest.raises(TransferFailed):
            await asyncio.gather(sender.send(io.BytesIO(data)), receiver())

    async def test_send_eot_retry_exhausted(self, piped):
        data = b"K" * 128
        sender = XModem(piped.side_a, timeout=0.05, retry_limit=2)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + 128 + 2)
            await piped.side_b.write(bytes([ACK]))
            # Never ACK EOT

        with pytest.raises(TransferFailed):
            await asyncio.gather(sender.send(io.BytesIO(data)), receiver())

    async def test_send_timeout_on_block_ack(self, piped, event_log):
        """Sender fires TIMEOUT event and retries when no ACK arrives."""
        data = b"L" * 128
        sender = XModem(piped.side_a, timeout=0.05, retry_limit=3, callback=event_log.callback)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + 128 + 2)  # first send, no response
            await asyncio.sleep(0.08)  # let timeout fire
            await piped.side_b.read(3 + 128 + 2)  # retry
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read_byte()
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(sender.send(io.BytesIO(data)), receiver())
        assert EventType.TIMEOUT in event_log.types()


# ---------------------------------------------------------------------------
# Receive tests
# ---------------------------------------------------------------------------


class TestXModemReceive:
    async def test_receive_crc_mode_single_block(self, piped, event_log):
        """Receiver accepts one valid CRC block and writes stripped payload."""
        payload = b"M" * 100
        padded = _pad(payload)
        receiver_engine = XModem(piped.side_b, callback=event_log.callback)

        async def sender():
            # Wait for 'C'
            init = await piped.side_a.read_byte()
            assert init == CRC_MODE
            await piped.side_a.write(_make_block(1, padded))
            ack = await piped.side_a.read_byte()
            assert ack == ACK
            await piped.side_a.write(bytes([EOT]))
            final_ack = await piped.side_a.read_byte()
            assert final_ack == ACK

        out = io.BytesIO()
        n, _ = await asyncio.gather(receiver_engine.receive(out), sender())
        assert out.getvalue() == payload
        assert EventType.BLOCK_RECEIVED in event_log.types()

    async def test_receive_checksum_mode(self, piped):
        """Receiver works in checksum mode when crc_mode=False."""
        payload = b"N" * 128
        receiver_engine = XModem(piped.side_b)

        async def sender():
            init = await piped.side_a.read_byte()
            assert init == NAK
            await piped.side_a.write(_make_block(1, payload, use_crc=False))
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()  # ACK

        out = io.BytesIO()
        await asyncio.gather(
            receiver_engine.receive(out, crc_mode=False),
            sender(),
        )
        assert out.getvalue() == payload

    async def test_receive_bad_crc_then_good(self, piped, event_log):
        """Receiver NAKs a corrupt block then accepts the retransmission."""
        payload = b"O" * 128
        receiver_engine = XModem(piped.side_b, callback=event_log.callback)

        async def sender():
            await piped.side_a.read_byte()  # 'C'
            # Send corrupt block (flip last CRC byte)
            bad = bytearray(_make_block(1, payload))
            bad[-1] ^= 0xFF
            await piped.side_a.write(bytes(bad))
            resp = await piped.side_a.read_byte()
            assert resp == NAK
            # Retransmit correct
            await piped.side_a.write(_make_block(1, payload))
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()

        out = io.BytesIO()
        await asyncio.gather(receiver_engine.receive(out), sender())
        assert out.getvalue() == payload
        assert EventType.CRC_ERROR in event_log.types()

    async def test_receive_bad_complement(self, piped):
        """Receiver NAKs block with wrong block-number complement."""
        payload = b"P" * 128
        receiver_engine = XModem(piped.side_b, timeout=0.1, retry_limit=3)

        async def sender():
            await piped.side_a.read_byte()
            c = crc16(payload)
            # Complement is wrong (0x00 instead of 0xFE)
            bad_frame = bytes([SOH, 1, 0x00]) + payload + bytes([c >> 8, c & 0xFF])
            await piped.side_a.write(bad_frame)
            await piped.side_a.read_byte()  # NAK
            # Send correct block
            await piped.side_a.write(_make_block(1, payload))
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()

        out = io.BytesIO()
        await asyncio.gather(receiver_engine.receive(out), sender())
        assert out.getvalue() == payload

    async def test_receive_duplicate_block_acked(self, piped):
        """Receiver silently ACKs a duplicate block (retransmission of block N-1)."""
        payload = b"Q" * 128
        receiver_engine = XModem(piped.side_b)

        async def sender():
            await piped.side_a.read_byte()  # 'C'
            await piped.side_a.write(_make_block(1, payload))
            await piped.side_a.read_byte()  # ACK
            # Retransmit block 1 (simulate lost ACK)
            await piped.side_a.write(_make_block(1, payload))
            await piped.side_a.read_byte()  # ACK for duplicate
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()

        out = io.BytesIO()
        await asyncio.gather(receiver_engine.receive(out), sender())
        # Data should not be duplicated
        assert out.getvalue() == payload

    async def test_receive_cancel_two_cans(self, piped):
        """Receiver raises TransferCancelled on two consecutive CAN bytes."""
        receiver_engine = XModem(piped.side_b, timeout=1.0)

        async def sender():
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([CAN, CAN]))

        with pytest.raises(TransferCancelled):
            await asyncio.gather(receiver_engine.receive(io.BytesIO()), sender())

    async def test_receive_single_can_treated_as_noise(self, piped):
        """A single CAN is treated as noise; receiver NAKs and continues."""
        payload = b"R" * 128
        receiver_engine = XModem(piped.side_b, timeout=0.5)

        async def sender():
            await piped.side_a.read_byte()  # 'C'
            await piped.side_a.write(bytes([CAN]))  # single CAN = noise
            await piped.side_a.read_byte()  # NAK
            await piped.side_a.write(_make_block(1, payload))
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()

        out = io.BytesIO()
        await asyncio.gather(receiver_engine.receive(out), sender())
        assert out.getvalue() == payload

    async def test_receive_out_of_sequence_cancels(self, piped):
        """Receiver sends CAN and raises ProtocolError on wrong block sequence."""
        receiver_engine = XModem(piped.side_b, timeout=1.0)

        async def sender():
            await piped.side_a.read_byte()
            # Send block 2 when block 1 expected
            await piped.side_a.write(_make_block(2, b"S" * 128))
            # Expect CAN CAN
            can1 = await piped.side_a.read_byte()
            can2 = await piped.side_a.read_byte()
            assert can1 == CAN
            assert can2 == CAN

        with pytest.raises(ProtocolError):
            await asyncio.gather(receiver_engine.receive(io.BytesIO()), sender())

    async def test_receive_timeout_exhausted(self, piped):
        """Receiver raises TransferTimeout after retry limit."""
        receiver_engine = XModem(piped.side_b, timeout=0.05, retry_limit=2)

        async def sender():
            await piped.side_a.read_byte()  # consume 'C'
            # Send nothing; let timeouts accumulate

        with pytest.raises(TransferTimeout):
            await asyncio.gather(receiver_engine.receive(io.BytesIO()), sender())

    async def test_receive_unexpected_header_naks(self, piped):
        """Receiver NAKs unrecognised header bytes."""
        payload = b"T" * 128
        receiver_engine = XModem(piped.side_b, timeout=0.5, retry_limit=5)

        async def sender():
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([0x55]))  # garbage
            await piped.side_a.read_byte()  # NAK
            await piped.side_a.write(_make_block(1, payload))
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()

        out = io.BytesIO()
        await asyncio.gather(receiver_engine.receive(out), sender())
        assert out.getvalue() == payload

    async def test_receive_1k_block(self, piped):
        """Receiver accepts 1K blocks (STX header)."""
        payload = b"U" * 1024
        receiver_engine = XModem(piped.side_b)

        async def sender():
            await piped.side_a.read_byte()
            await piped.side_a.write(_make_block(1, payload, size=1024))
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()

        out = io.BytesIO()
        await asyncio.gather(receiver_engine.receive(out), sender())
        assert out.getvalue() == payload

    async def test_receive_multiple_blocks(self, piped):
        """Receiver assembles multiple blocks correctly."""
        blocks = [bytes([i] * 128) for i in range(1, 4)]
        receiver_engine = XModem(piped.side_b)

        async def sender():
            await piped.side_a.read_byte()
            for i, block in enumerate(blocks, start=1):
                await piped.side_a.write(_make_block(i, block))
                await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()

        out = io.BytesIO()
        await asyncio.gather(receiver_engine.receive(out), sender())
        assert out.getvalue() == b"".join(blocks)

    async def test_receive_truncated_frame_naks(self, piped):
        """Receiver NAKs when frame data is too short."""
        receiver_engine = XModem(piped.side_b, timeout=0.1, retry_limit=3)
        payload = b"V" * 128

        async def sender():
            await piped.side_a.read_byte()
            # Send SOH + only 10 bytes (truncated)
            await piped.side_a.write(bytes([SOH, 1, 0xFE]) + b"\x00" * 10)
            await piped.side_a.read_byte()  # NAK
            # Now send correct block
            await piped.side_a.write(_make_block(1, payload))
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()

        out = io.BytesIO()
        await asyncio.gather(receiver_engine.receive(out), sender())
        assert out.getvalue() == payload

    async def test_receive_callback_fires_events(self, piped, event_log):
        """All expected event types are fired during a clean transfer."""
        payload = b"W" * 128
        receiver_engine = XModem(piped.side_b, callback=event_log.callback)

        async def sender():
            await piped.side_a.read_byte()
            await piped.side_a.write(_make_block(1, payload))
            await piped.side_a.read_byte()
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()

        out = io.BytesIO()
        await asyncio.gather(receiver_engine.receive(out, filename="w.bin"), sender())
        types = event_log.types()
        assert EventType.SESSION_START in types
        assert EventType.BLOCK_RECEIVED in types
        assert EventType.SESSION_END in types

    async def test_receive_error_limit_on_bad_header(self, piped):
        """Too many bad header bytes raises ProtocolError."""
        receiver_engine = XModem(piped.side_b, timeout=0.05, retry_limit=2)

        async def sender():
            await piped.side_a.read_byte()
            for _ in range(5):
                try:
                    await piped.side_a.write(bytes([0xAA]))
                    await piped.side_a.read_byte_with_timeout(0.1)
                except asyncio.TimeoutError:
                    break

        with pytest.raises((ProtocolError, TransferTimeout)):
            await asyncio.gather(receiver_engine.receive(io.BytesIO()), sender())


class TestXModemMissingBranches:
    async def test_send_handshake_ignores_unknown_byte(self, piped):
        """Sender ignores unknown byte during handshake and waits for C/NAK."""
        data = b"Z" * 128
        sender = XModem(piped.side_a, timeout=0.5, retry_limit=5)

        async def receiver():
            await piped.side_b.write(bytes([0xFF]))  # garbage
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + 128 + 2)
            await piped.side_b.write(bytes([ACK]))
            await piped.side_b.read_byte()
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(sender.send(io.BytesIO(data)), receiver())

    async def test_send_eot_nak_then_ack(self, piped):
        """Sender retries EOT after receiving NAK, then ACKs."""
        data = b"A" * 128
        sender = XModem(piped.side_a, timeout=1.0, retry_limit=5)

        async def receiver():
            await piped.side_b.write(bytes([CRC_MODE]))
            await piped.side_b.read(3 + 128 + 2)
            await piped.side_b.write(bytes([ACK]))
            # NAK first EOT
            await piped.side_b.read_byte()
            await piped.side_b.write(bytes([NAK]))
            # ACK second EOT
            await piped.side_b.read_byte()
            await piped.side_b.write(bytes([ACK]))

        await asyncio.gather(sender.send(io.BytesIO(data)), receiver())

    async def test_receive_truncated_frame_then_correct(self, piped):
        """Receiver NAKs a frame whose payload is too short."""
        payload = b"B" * 128
        receiver_engine = XModem(piped.side_b, timeout=0.5, retry_limit=5)

        async def sender():
            await piped.side_a.read_byte()  # 'C'
            # Send SOH + block num + comp + only 10 bytes (truncated body, missing CRC)
            await piped.side_a.write(bytes([SOH, 1, 0xFE]) + b"\x01" * 10)
            await piped.side_a.read_byte()  # NAK
            # Send correct block
            from yesterwind_xyzmodem.crc import crc16

            c = crc16(payload)
            await piped.side_a.write(bytes([SOH, 1, 0xFE]) + payload + bytes([c >> 8, c & 0xFF]))
            await piped.side_a.read_byte()  # ACK
            await piped.side_a.write(bytes([EOT]))
            await piped.side_a.read_byte()

        out = io.BytesIO()
        await asyncio.gather(receiver_engine.receive(out), sender())
        assert out.getvalue() == payload
