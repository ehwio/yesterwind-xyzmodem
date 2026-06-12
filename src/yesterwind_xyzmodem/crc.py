"""CRC-16/CCITT and CRC-32 routines used by all three protocol engines."""

from __future__ import annotations

# ---------------------------------------------------------------------------
# CRC-16/CCITT  (poly 0x1021, init 0x0000) — XModem / YModem
# ---------------------------------------------------------------------------

_CRC16_TABLE: list[int] = []


def _build_crc16_table() -> None:
    for i in range(256):
        crc = i << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if (crc & 0x8000) else (crc << 1)
        _CRC16_TABLE.append(crc & 0xFFFF)


_build_crc16_table()


def crc16(data: bytes | bytearray) -> int:
    crc = 0
    for byte in data:
        crc = ((crc << 8) ^ _CRC16_TABLE[(crc >> 8) ^ byte]) & 0xFFFF
    return crc


def crc16_valid(data: bytes | bytearray, received_crc: int) -> bool:
    return crc16(data) == received_crc


# ---------------------------------------------------------------------------
# Simple arithmetic checksum — XModem fallback
# ---------------------------------------------------------------------------


def checksum(data: bytes | bytearray) -> int:
    return sum(data) & 0xFF


# ---------------------------------------------------------------------------
# CRC-32 — ZModem
# ---------------------------------------------------------------------------

_CRC32_TABLE: list[int] = []


def _build_crc32_table() -> None:
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ 0xEDB88320 if (crc & 1) else (crc >> 1)
        _CRC32_TABLE.append(crc)


_build_crc32_table()


def crc32(data: bytes | bytearray, initial: int = 0xFFFFFFFF) -> int:
    crc = initial
    for byte in data:
        crc = (crc >> 8) ^ _CRC32_TABLE[(crc ^ byte) & 0xFF]
    return crc ^ 0xFFFFFFFF


def crc32_valid(data: bytes | bytearray, received_crc: int) -> bool:
    return crc32(data) == received_crc
