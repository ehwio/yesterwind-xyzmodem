# Roadmap

## v0.1.0 — XModem Core

Foundation: solid, tested XModem that the rest of the stack builds on.

- XModem send and receive (128-byte blocks)
- CRC-16 and checksum error detection, auto-negotiated
- XModem-1K (1024-byte blocks)
- Retry logic with configurable limits
- Timeout handling
- Progress callback API: `on_progress(bytes_sent, total_bytes, block_num)`
- Error/event callback API: `on_event(event_type, detail)`
- 100% test coverage across Python 3.9–3.12

## v0.2.0 — YModem

Batch transfers and improved throughput.

- YModem send and receive (batch mode, multiple files in one session)
- Block-0 metadata: filename, file size, modification time
- YModem-G streaming variant (no per-block ACK, for reliable links)
- Per-file and per-batch progress callbacks
- Integrity: CRC-16 verified on every block; full-file CRC check on receipt
- Demo: `yw-send` and `yw-receive` CLI tools wired to `sz`/`rz` via a pty

## v0.3.0 — ZModem

Full ZModem with crash recovery — the gold standard for modem transfers.

- ZModem send and receive
- ZDLE framing and all subpacket types
- Crash recovery: resume interrupted transfers from last confirmed byte
- ZModem auto-download header detection (ZRQINIT)
- Sliding-window / streaming mode for maximum throughput
- Per-file and session-level progress callbacks
- Integrity: full 32-bit CRC on data subpackets and end-of-file
- Demo: end-to-end transfer against `sz`/`rz` reference implementations with
  visible progress bars (using `rich`)

## v0.4.0 — Telnet BBS Client

A self-contained demo that proves the stack works in the wild.

- Async Telnet client (asyncio) with full IAC negotiation
- ANSI / VT100 terminal emulation sufficient for BBS menu navigation
- Interactive TUI (using `textual` or `rich`): connect, browse, initiate
  transfer
- Trigger XModem / YModem / ZModem send or receive directly from the BBS
  file-transfer prompt
- Auto-detect protocol from BBS invitation string
- Upload and download demonstrated against at least two live public BBSes
- Screencap / recording support for demo GIFs

## v1.0.0 — Production Release

API and packaging hardened for long-term use as a library.

- Public API fully type-annotated and stable
- Async-native API alongside synchronous wrappers
- Protocol-agnostic `transfer()` facade with auto-negotiation
- Configurable transport layer: raw serial, pty, socket, asyncio stream —
  anything with a `read`/`write` interface
- Structured logging via stdlib `logging`; no print statements in library code
- Comprehensive docstrings; Sphinx docs published to Read the Docs
- PyPI package published (`yesterwind-xyzmodem`)
- GitHub Actions CI matrix: Python 3.9 / 3.10 / 3.11 / 3.12, Ubuntu / macOS
- 100% branch coverage enforced in CI

## Future Considerations

- Serial port support via `pyserial` (optional dependency)
- SSH tunneling demo (transfer files through an SSH channel)
- ZMODEM over WebSocket for browser-based terminal emulators
- Benchmark suite: throughput vs. `lrzsz` reference at various error rates
