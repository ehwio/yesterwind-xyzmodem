"""Tests for the callbacks module."""

import asyncio
import pytest
from yesterwind_xyzmodem.callbacks import EventType, TransferProgress, fire


def test_percent_with_total():
    p = TransferProgress(total_bytes=200, bytes_transferred=100)
    assert p.percent == 50.0


def test_percent_clamp():
    p = TransferProgress(total_bytes=100, bytes_transferred=200)
    assert p.percent == 100.0


def test_percent_no_total():
    p = TransferProgress(total_bytes=0, bytes_transferred=50)
    assert p.percent == 0.0


async def test_fire_none_callback():
    """fire() with no callback is a no-op."""
    p = TransferProgress()
    await fire(None, p)  # must not raise


async def test_fire_sync_callback():
    called = []

    def cb(prog):
        called.append(prog)

    p = TransferProgress(event=EventType.SESSION_START)
    await fire(cb, p)
    assert called == [p]


async def test_fire_async_callback():
    called = []

    async def cb(prog):
        called.append(prog)

    p = TransferProgress(event=EventType.SESSION_END)
    await fire(cb, p)
    assert called == [p]
