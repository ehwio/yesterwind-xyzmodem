"""
Callback types and the TransferProgress dataclass.

All protocol engines accept optional callbacks of these signatures.
Callbacks may be plain functions or coroutines; the engine calls them via
``_fire()``, which awaits coroutines transparently.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Union


class EventType(Enum):
    SESSION_START = auto()  # transfer session beginning
    SESSION_END = auto()  # transfer session complete
    FILE_START = auto()  # individual file beginning (YModem/ZModem)
    FILE_END = auto()  # individual file complete
    BLOCK_SENT = auto()  # sender: block acknowledged
    BLOCK_RECEIVED = auto()  # receiver: block accepted
    BLOCK_NAK = auto()  # block NAK'd; will retry
    BLOCK_RETRY = auto()  # retransmitting block
    CRC_ERROR = auto()  # CRC/checksum mismatch on received block
    CANCEL = auto()  # remote or local cancel
    TIMEOUT = auto()  # timeout waiting for remote


@dataclass
class TransferProgress:
    filename: str = ""
    file_index: int = 0  # 0-based index within a batch
    file_count: int = 1
    bytes_transferred: int = 0
    total_bytes: int = 0  # 0 if unknown
    block_number: int = 0
    retry_count: int = 0
    event: EventType = EventType.SESSION_START
    detail: str = ""

    @property
    def percent(self) -> float:
        if self.total_bytes:
            return min(100.0, 100.0 * self.bytes_transferred / self.total_bytes)
        return 0.0


ProgressCallback = Callable[[TransferProgress], Union[None, Coroutine]]


async def fire(callback: ProgressCallback | None, progress: TransferProgress) -> None:
    """Invoke *callback* if set; await it if it is a coroutine function."""
    if callback is None:
        return
    result = callback(progress)
    if asyncio.iscoroutine(result):
        await result
