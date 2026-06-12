"""
XModem protocol engine — send and receive.

Supports:
- XModem (128-byte blocks, arithmetic checksum)
- XModem-CRC (128-byte blocks, CRC-16)
- XModem-1K (1024-byte blocks, CRC-16)

The receiver advertises its capability by sending 'C' (CRC mode) or NAK
(checksum mode) as the initial handshake byte.  The sender honours whichever
the receiver requests.
"""

from __future__ import annotations

import asyncio
import io
from typing import BinaryIO, Optional

from yesterwind_xyzmodem.callbacks import EventType, ProgressCallback, TransferProgress, fire
from yesterwind_xyzmodem.constants import (
    ACK,
    CAN,
    CRC_MODE,
    DEFAULT_RETRY_LIMIT,
    DEFAULT_TIMEOUT,
    EOT,
    NAK,
    SOH,
    STX,
    SUB,
)
from yesterwind_xyzmodem.crc import checksum, crc16
from yesterwind_xyzmodem.exceptions import (
    ProtocolError,
    TransferCancelled,
    TransferFailed,
    TransferTimeout,
)
from yesterwind_xyzmodem.transport import Transport


class XModem:
    """
    XModem protocol engine.

    Parameters
    ----------
    transport:
        A :class:`~yesterwind_xyzmodem.transport.Transport` instance.
    timeout:
        Seconds to wait for a byte from the remote.
    retry_limit:
        How many consecutive NAKs / timeouts before giving up.
    block_size:
        128 or 1024.  1024 activates XModem-1K (STX header byte).
        Only relevant for the sender; the receiver accepts both automatically.
    callback:
        Optional :data:`~yesterwind_xyzmodem.callbacks.ProgressCallback`.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        retry_limit: int = DEFAULT_RETRY_LIMIT,
        block_size: int = 128,
        callback: Optional[ProgressCallback] = None,
    ) -> None:
        if block_size not in (128, 1024):
            raise ValueError("block_size must be 128 or 1024")
        self._transport = transport
        self._timeout = timeout
        self._retry_limit = retry_limit
        self._block_size = block_size
        self._callback = callback

    # ------------------------------------------------------------------
    # Public API

    async def send(
        self,
        stream: BinaryIO,
        filename: str = "",
        total_bytes: int = 0,
    ) -> int:
        """
        Send the contents of *stream* using XModem.

        Returns the total number of data bytes sent (excluding protocol
        overhead).  Raises on failure.
        """
        progress = TransferProgress(
            filename=filename,
            total_bytes=total_bytes,
            event=EventType.SESSION_START,
        )
        await fire(self._callback, progress)

        use_crc = await self._sender_handshake()
        bytes_sent = await self._send_blocks(stream, use_crc, progress)

        progress.event = EventType.SESSION_END
        progress.bytes_transferred = bytes_sent
        await fire(self._callback, progress)
        return bytes_sent

    async def receive(
        self,
        stream: BinaryIO,
        filename: str = "",
        total_bytes: int = 0,
        *,
        crc_mode: bool = True,
    ) -> int:
        """
        Receive data into *stream* using XModem.

        Returns the total number of data bytes written.  Raises on failure.
        """
        progress = TransferProgress(
            filename=filename,
            total_bytes=total_bytes,
            event=EventType.SESSION_START,
        )
        await fire(self._callback, progress)

        bytes_received = await self._receive_blocks(stream, crc_mode, progress)

        progress.event = EventType.SESSION_END
        progress.bytes_transferred = bytes_received
        await fire(self._callback, progress)
        return bytes_received

    # ------------------------------------------------------------------
    # Sender internals

    async def _sender_handshake(self) -> bool:
        """Wait for receiver 'C' or NAK; return True if CRC mode."""
        for _ in range(self._retry_limit):
            try:
                byte = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError:
                continue
            if byte == CRC_MODE:
                return True
            if byte == NAK:
                return False
            if byte == CAN:
                raise TransferCancelled("Remote cancelled during handshake")
        raise TransferFailed("No handshake response from receiver")

    async def _send_blocks(
        self,
        stream: BinaryIO,
        use_crc: bool,
        progress: TransferProgress,
    ) -> int:
        block_num = 1
        bytes_sent = 0

        while True:
            data = stream.read(self._block_size)
            if not data:
                break

            # Pad the last block with SUB (CTRL-Z)
            if len(data) < self._block_size:
                data = data + bytes([SUB] * (self._block_size - len(data)))

            await self._send_block(data, block_num, use_crc, progress)
            bytes_sent += min(len(data), self._block_size)
            block_num = (block_num + 1) & 0xFF

        # End of transmission
        await self._send_eot()

        progress.bytes_transferred = bytes_sent
        return bytes_sent

    async def _send_block(
        self,
        data: bytes,
        block_num: int,
        use_crc: bool,
        progress: TransferProgress,
    ) -> None:
        header = SOH if self._block_size == 128 else STX
        complement = (~block_num) & 0xFF
        if use_crc:
            c = crc16(data)
            integrity = bytes([c >> 8, c & 0xFF])
        else:
            integrity = bytes([checksum(data)])

        frame = bytes([header, block_num, complement]) + data + integrity

        for attempt in range(self._retry_limit):
            await self._transport.write(frame)

            progress.event = EventType.BLOCK_SENT if attempt == 0 else EventType.BLOCK_RETRY
            progress.block_number = block_num
            progress.retry_count = attempt
            await fire(self._callback, progress)

            try:
                resp = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError:
                progress.event = EventType.TIMEOUT
                await fire(self._callback, progress)
                continue

            if resp == ACK:
                return
            if resp == CAN:
                raise TransferCancelled("Remote cancelled during transfer")
            # NAK or unexpected: retry
            progress.event = EventType.BLOCK_NAK
            await fire(self._callback, progress)

        raise TransferFailed(f"Block {block_num} not acknowledged after {self._retry_limit} attempts")

    async def _send_eot(self) -> None:
        for attempt in range(self._retry_limit):
            await self._transport.write(bytes([EOT]))
            try:
                resp = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError:
                continue
            if resp == ACK:
                return
            if resp == CAN:
                raise TransferCancelled("Remote cancelled on EOT")
        raise TransferFailed("EOT not acknowledged")

    # ------------------------------------------------------------------
    # Receiver internals

    async def _receive_blocks(
        self,
        stream: BinaryIO,
        crc_mode: bool,
        progress: TransferProgress,
    ) -> int:
        # Send initial handshake
        init_byte = CRC_MODE if crc_mode else NAK
        await self._transport.write(bytes([init_byte]))

        expected_block = 1
        bytes_received = 0
        consecutive_errors = 0
        buffer = io.BytesIO()

        while True:
            try:
                header = await self._transport.read_byte_with_timeout(self._timeout)
            except asyncio.TimeoutError:
                consecutive_errors += 1
                if consecutive_errors >= self._retry_limit:
                    raise TransferTimeout("Timed out waiting for data block")
                progress.event = EventType.TIMEOUT
                await fire(self._callback, progress)
                await self._transport.write(bytes([NAK]))
                continue

            if header == EOT:
                await self._transport.write(bytes([ACK]))
                break

            if header == CAN:
                # Two CANs = cancel
                try:
                    next_byte = await self._transport.read_byte_with_timeout(self._timeout)
                except asyncio.TimeoutError:
                    next_byte = 0
                if next_byte == CAN:
                    raise TransferCancelled("Remote cancelled transfer")
                # Single CAN: treat as noise, send NAK
                await self._transport.write(bytes([NAK]))
                continue

            if header not in (SOH, STX):
                consecutive_errors += 1
                if consecutive_errors >= self._retry_limit:
                    raise ProtocolError(f"Unexpected header byte 0x{header:02x}")
                await self._transport.write(bytes([NAK]))
                continue

            block_size = 128 if header == SOH else 1024
            frame_size = block_size + (4 if crc_mode else 3)  # num + ~num + data + integrity

            try:
                rest = await self._transport.read_with_timeout(frame_size, self._timeout)
            except asyncio.TimeoutError:
                consecutive_errors += 1
                await self._transport.write(bytes([NAK]))
                continue

            if len(rest) < frame_size:  # pragma: no cover — only with StreamTransport
                consecutive_errors += 1
                await self._transport.write(bytes([NAK]))
                continue

            block_num = rest[0]
            block_complement = rest[1]
            data = rest[2 : 2 + block_size]

            # Validate block number complement
            if block_num ^ block_complement != 0xFF:
                consecutive_errors += 1
                progress.event = EventType.CRC_ERROR
                await fire(self._callback, progress)
                await self._transport.write(bytes([NAK]))
                continue

            # Validate integrity
            if crc_mode:
                received_crc = (rest[-2] << 8) | rest[-1]
                ok = crc16(data) == received_crc
            else:
                ok = checksum(data) == rest[-1]

            if not ok:
                consecutive_errors += 1
                progress.event = EventType.CRC_ERROR
                await fire(self._callback, progress)
                await self._transport.write(bytes([NAK]))
                continue

            consecutive_errors = 0

            # Duplicate block (sender retransmitted after our ACK was lost)
            if block_num == (expected_block - 1) & 0xFF:
                await self._transport.write(bytes([ACK]))
                continue

            if block_num != expected_block:
                # Out-of-sequence: cancel
                await self._transport.write(bytes([CAN, CAN]))
                raise ProtocolError(
                    f"Expected block {expected_block}, got {block_num}"
                )

            buffer.write(data)
            bytes_received += block_size
            expected_block = (expected_block + 1) & 0xFF

            progress.event = EventType.BLOCK_RECEIVED
            progress.block_number = block_num
            progress.bytes_transferred = bytes_received
            await fire(self._callback, progress)

            await self._transport.write(bytes([ACK]))

        # Write to the output stream, stripping trailing SUB padding
        payload = buffer.getvalue().rstrip(bytes([SUB]))
        stream.write(payload)
        return len(payload)
