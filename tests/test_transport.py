"""Tests for MemoryTransport."""

import asyncio
import pytest
from yesterwind_xyzmodem.transport import MemoryTransport


async def test_read_returns_data():
    t = MemoryTransport(b"\x01\x02\x03")
    assert await t.read(2) == b"\x01\x02"
    assert await t.read(1) == b"\x03"


async def test_read_eof_raises():
    t = MemoryTransport(b"")
    with pytest.raises(EOFError):
        await t.read(1)


async def test_read_byte_returns_int():
    t = MemoryTransport(b"\xFF")
    assert await t.read_byte() == 0xFF


async def test_read_byte_eof_raises():
    t = MemoryTransport(b"")
    with pytest.raises(EOFError):
        await t.read_byte()


async def test_write_accumulates_in_sent():
    t = MemoryTransport()
    await t.write(b"hello")
    await t.write(b" world")
    assert t.sent == b"hello world"


async def test_sent_since():
    t = MemoryTransport()
    await t.write(b"abc")
    mark = len(t.sent)
    await t.write(b"def")
    assert t.sent_since(mark) == b"def"


async def test_read_with_timeout_returns_data():
    t = MemoryTransport(b"\x01\x02")
    assert await t.read_with_timeout(2, 1.0) == b"\x01\x02"


async def test_read_with_timeout_empty_raises():
    t = MemoryTransport(b"")
    with pytest.raises(asyncio.TimeoutError):
        await t.read_with_timeout(1, 1.0)


async def test_read_byte_with_timeout_returns_int():
    t = MemoryTransport(b"\xAB")
    assert await t.read_byte_with_timeout(1.0) == 0xAB


async def test_read_byte_with_timeout_empty_raises():
    t = MemoryTransport(b"")
    with pytest.raises(asyncio.TimeoutError):
        await t.read_byte_with_timeout(1.0)


async def test_feed_appends_data():
    t = MemoryTransport(b"\x01")
    t.feed(b"\x02\x03")
    assert await t.read(3) == b"\x01\x02\x03"


async def test_purge_discards_remaining():
    t = MemoryTransport(b"\x01\x02\x03")
    await t.read(1)
    await t.purge()
    with pytest.raises(EOFError):
        await t.read_byte()
