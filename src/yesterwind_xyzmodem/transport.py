"""
Transport abstraction — all protocol engines communicate through this interface.

Any object with async read/write/drain semantics can be wrapped.  Concrete
implementations are provided for asyncio streams (sockets, ptys, serial) and
for in-memory bytes (testing).
"""

from __future__ import annotations

import asyncio
import io
from abc import ABC, abstractmethod


class Transport(ABC):
    """Base class for all transports."""

    @abstractmethod
    async def read(self, n: int) -> bytes:
        """Read exactly *n* bytes; return fewer only on EOF."""

    @abstractmethod
    async def read_byte(self) -> int:
        """Read a single byte as an integer (0–255), or raise EOFError."""

    @abstractmethod
    async def write(self, data: bytes) -> None:
        """Write *data* and flush."""

    @abstractmethod
    async def read_with_timeout(self, n: int, timeout: float) -> bytes:
        """Like read(), but raise asyncio.TimeoutError after *timeout* seconds."""

    @abstractmethod
    async def read_byte_with_timeout(self, timeout: float) -> int:
        """Like read_byte(), but raise asyncio.TimeoutError after *timeout* seconds."""

    async def purge(self) -> None:  # pragma: no cover  # noqa: B027
        """Discard any bytes currently buffered in the receive path."""


class StreamTransport(Transport):
    """Wraps an asyncio (reader, writer) pair — sockets, ptys, TCP, serial."""

    def __init__(  # pragma: no cover
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self._reader = reader  # pragma: no cover
        self._writer = writer  # pragma: no cover

    async def read(self, n: int) -> bytes:  # pragma: no cover
        return await self._reader.read(n)

    async def read_byte(self) -> int:  # pragma: no cover
        data = await self._reader.readexactly(1)
        return data[0]

    async def write(self, data: bytes) -> None:  # pragma: no cover
        self._writer.write(data)
        await self._writer.drain()

    async def read_with_timeout(self, n: int, timeout: float) -> bytes:  # pragma: no cover
        return await asyncio.wait_for(self._reader.read(n), timeout=timeout)

    async def read_byte_with_timeout(self, timeout: float) -> int:  # pragma: no cover
        data = await asyncio.wait_for(self._reader.readexactly(1), timeout=timeout)
        return data[0]

    async def purge(self) -> None:  # pragma: no cover
        try:
            while True:
                await asyncio.wait_for(self._reader.read(4096), timeout=0.05)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError):
            pass


class MemoryTransport(Transport):
    """
    In-memory transport for unit tests.

    Write to *tx_buf* to simulate bytes arriving from the remote end.
    Bytes sent by the protocol engine accumulate in *rx_buf*.
    """

    def __init__(self, tx_data: bytes = b"") -> None:
        self._tx = io.BytesIO(tx_data)  # data the protocol will *read*
        self._rx = io.BytesIO()  # data the protocol *wrote*
        self._rx_pos = 0
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Test helpers

    def feed(self, data: bytes) -> None:
        """Append *data* to the incoming buffer (simulates remote sending)."""
        pos = self._tx.tell()
        self._tx.seek(0, 2)
        self._tx.write(data)
        self._tx.seek(pos)

    @property
    def sent(self) -> bytes:
        """Everything the protocol engine has written so far."""
        pos = self._rx.tell()
        self._rx.seek(0)
        data = self._rx.read()
        self._rx.seek(pos)
        return data

    def sent_since(self, mark: int) -> bytes:
        """Bytes written after position *mark* (from a previous len(sent))."""
        self._rx.seek(mark)
        return self._rx.read()

    # ------------------------------------------------------------------
    # Transport interface

    async def read(self, n: int) -> bytes:
        data = self._tx.read(n)
        if not data:
            raise EOFError("MemoryTransport: no more data")
        return data

    async def read_byte(self) -> int:
        data = self._tx.read(1)
        if not data:
            raise EOFError("MemoryTransport: no more data")
        return data[0]

    async def write(self, data: bytes) -> None:
        self._rx.seek(0, 2)
        self._rx.write(data)

    async def read_with_timeout(self, n: int, timeout: float) -> bytes:
        # Memory reads are instant; honour timeout only in the sense that
        # missing data raises TimeoutError rather than blocking forever.
        data = self._tx.read(n)
        if not data:
            raise asyncio.TimeoutError
        return data

    async def read_byte_with_timeout(self, timeout: float) -> int:
        data = self._tx.read(1)
        if not data:
            raise asyncio.TimeoutError
        return data[0]

    async def purge(self) -> None:
        self._tx.read()  # discard remaining
