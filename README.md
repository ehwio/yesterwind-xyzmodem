# Yesterwind XYZ-modem

[![CI](https://github.com/ehwio/yesterwind-xyzmodem/actions/workflows/ci.yml/badge.svg)](https://github.com/ehwio/yesterwind-xyzmodem/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/yesterwind-xyzmodem)](https://pypi.org/project/yesterwind-xyzmodem/)
[![Python](https://img.shields.io/pypi/pyversions/yesterwind-xyzmodem)](https://pypi.org/project/yesterwind-xyzmodem/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)](https://github.com/ehwio/yesterwind-xyzmodem/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Pure-Python async implementation of the X-, Y-, and Z-Modem file transfer
protocols.  No subprocesses, no external tools — everything is implemented
in Python 3.9+.

## Installation

```bash
pip install yesterwind-xyzmodem
```

To also get the `yw-send`, `yw-receive`, and `yw-bbs` command-line tools:

```bash
pip install 'yesterwind-xyzmodem[demos]'
```

## Usage

### ZModem receive (from any transport)

```python
from yesterwind_xyzmodem.transport import StreamTransport
from yesterwind_xyzmodem.zmodem import ZModem

transport = StreamTransport(reader, writer)
zmodem = ZModem(transport)
received_paths = await zmodem.receive("./downloads")
```

### ZModem send

```python
zmodem = ZModem(transport)
await zmodem.send([("file1.bin", open("file1.bin", "rb"), size)])
```

### XModem / YModem

```python
from yesterwind_xyzmodem.xmodem import XModem
from yesterwind_xyzmodem.ymodem import YModem

xmodem = XModem(transport)
await xmodem.send("file.bin")
await xmodem.receive("file.bin")

ymodem = YModem(transport)
await ymodem.send([("a.bin", stream_a, size_a), ("b.txt", stream_b, size_b)])
paths = await ymodem.receive("./downloads")
```

### Progress callbacks

```python
from yesterwind_xyzmodem.callbacks import EventType

def on_progress(p):
    if p.event == EventType.BLOCK_RECEIVED:
        print(f"{p.filename}: {p.bytes_transferred}/{p.total_bytes}")

zmodem = ZModem(transport, callback=on_progress)
```

## Demo tools

Install the `[demos]` extra to get three command-line tools:

```bash
pip install 'yesterwind-xyzmodem[demos]'
```

| Command | Description |
|---|---|
| `yw-send` | ZModem TCP server — send files to any `rz` client |
| `yw-receive` | ZModem TCP client — receive files from any `sz` server |
| `yw-bbs` | Telnet BBS client with automatic ZModem receive |

If you run a `yw-*` command without the `[demos]` extra installed, you'll get
a clear message explaining what to install rather than a bare traceback.

### `yw-send` — ZModem TCP server

Equivalent to `sz --tcp-server`. The OS assigns a free port unless you
specify one with `--port`.

```bash
# Single-shot: accept one client, send file, exit
yw-send myfile.bin

# Serve forever: keep listening, handle multiple clients in parallel
yw-send --serve-forever -p 12345 file1.bin file2.bin

# Connect with the reference rz or our own client:
rz --tcp-client <host>:12345
yw-receive <host> 12345
```

### `yw-receive` — ZModem TCP client

Equivalent to `rz --tcp-client`. Connects to a `yw-send` or
`sz --tcp-server` instance and downloads files with a progress panel.

```bash
# yw-send prints the port when it starts listening
yw-send myfile.bin
# → Listening on 0.0.0.0:54321

yw-receive <host> 54321
yw-receive <host> 54321 -d ~/Downloads
```

### `yw-bbs` — telnet BBS client with auto ZModem receive

Connects to any telnet BBS. ZModem transfers initiated by the BBS are
detected automatically and a retro progress panel appears, just like
PCPlus or XTalk4 — no user action required.

```bash
yw-bbs bbs.fozztexx.com
yw-bbs bbs.fozztexx.com 23 -d ~/Downloads/bbs
```

Press `Ctrl+]` to disconnect.

### Running demos from a repo clone

If you're working from a clone rather than a pip install, use `uv run`:

```bash
uv run demos/sz.py myfile.bin
uv run demos/rz.py <host> <port>
uv run demos/bbs.py bbs.fozztexx.com
```

## Development

```bash
git clone https://github.com/ehwio/yesterwind-xyzmodem
cd yesterwind-xyzmodem
uv sync --extra dev
uv run pytest
```

Tests require 100% branch coverage. See [CONTRIBUTING.md](CONTRIBUTING.md) for
the full workflow.
