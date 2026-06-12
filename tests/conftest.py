"""
Shared test helpers.

The ``loopback`` fixture wires two MemoryTransports together so that data
written to one appears as readable input to the other — simulating a
two-party serial connection without any OS I/O.
"""

from __future__ import annotations

import asyncio
import io
from dataclasses import dataclass, field
from typing import Optional

import pytest

from yesterwind_xyzmodem.callbacks import EventType, ProgressCallback, TransferProgress
from yesterwind_xyzmodem.transport import MemoryTransport


@dataclass
class EventLog:
    """Collects all progress callbacks for assertions."""
    events: list[TransferProgress] = field(default_factory=list)

    def callback(self, p: TransferProgress) -> None:
        self.events.append(TransferProgress(**p.__dict__))

    def types(self) -> list[EventType]:
        return [e.event for e in self.events]


class PipedTransport:
    """
    Two transports sharing a pair of asyncio queues.

    ``side_a`` and ``side_b`` are independent Transport objects: data
    written to A arrives at B's read, and vice versa.
    """

    def __init__(self) -> None:
        self._a_to_b: asyncio.Queue[int] = asyncio.Queue()
        self._b_to_a: asyncio.Queue[int] = asyncio.Queue()
        self.side_a = _QueueTransport(self._a_to_b, self._b_to_a)
        self.side_b = _QueueTransport(self._b_to_a, self._a_to_b)


class _QueueTransport:
    """Async transport backed by two asyncio.Queues."""

    def __init__(self, tx: asyncio.Queue, rx: asyncio.Queue) -> None:
        self._tx = tx  # outgoing bytes go here
        self._rx = rx  # incoming bytes come from here

    async def read(self, n: int) -> bytes:
        out = bytearray()
        for _ in range(n):
            out.append(await self._rx.get())
        return bytes(out)

    async def read_byte(self) -> int:
        return await self._rx.get()

    async def write(self, data: bytes) -> None:
        for b in data:
            await self._tx.put(b)

    async def read_with_timeout(self, n: int, timeout: float) -> bytes:
        return await asyncio.wait_for(self.read(n), timeout=timeout)

    async def read_byte_with_timeout(self, timeout: float) -> int:
        return await asyncio.wait_for(self.read_byte(), timeout=timeout)

    async def purge(self) -> None:
        while not self._rx.empty():
            self._rx.get_nowait()


@pytest.fixture
def piped():
    """Return a PipedTransport with two connected sides."""
    return PipedTransport()


@pytest.fixture
def event_log():
    return EventLog()
