# Yesterwind XYZ-modem

[![CI](https://github.com/ehwio/yesterwind-xyzmodem/actions/workflows/ci.yml/badge.svg)](https://github.com/ehwio/yesterwind-xyzmodem/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/yesterwind-xyzmodem)](https://pypi.org/project/yesterwind-xyzmodem/)
[![Python](https://img.shields.io/pypi/pyversions/yesterwind-xyzmodem)](https://pypi.org/project/yesterwind-xyzmodem/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)](https://github.com/ehwio/yesterwind-xyzmodem/actions/workflows/ci.yml)

Pure-Python async implementation of the X-, Y-, and Z-Modem file transfer
protocols.  No subprocesses, no external tools — everything is implemented
in Python 3.9+.

## Installation

```bash
pip install yesterwind-xyzmodem
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
await zmodem.send(["file1.bin", "file2.txt"])
```

### XModem / YModem

```python
from yesterwind_xyzmodem.xmodem import XModem
from yesterwind_xyzmodem.ymodem import YModem

xmodem = XModem(transport)
await xmodem.send("file.bin")
await xmodem.receive("file.bin")

ymodem = YModem(transport)
await ymodem.send(["a.bin", "b.txt"])
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

## Demos

### `rz` — download via ZModem over TCP

Equivalent to `rz --tcp-client`, works against `sz --tcp-server`:

```bash
# On the sending machine:
sz --tcp-server myfile.bin
# sz prints: connect with lrz --tcp-client "host:PORT"

# On the receiving machine:
uv run demos/rz.py <host> <PORT> [-d download_dir]
```

### `bbs` — telnet BBS client with auto ZModem receive

Connects to any telnet BBS.  ZModem transfers are detected automatically
and a retro progress panel appears, just like PCPlus or XTalk4:

```bash
uv run demos/bbs.py bbs.fozztexx.com
uv run demos/bbs.py bbs.fozztexx.com 23 -d ~/Downloads/bbs
```

Press `Ctrl+]` to disconnect.

## Development

```bash
uv sync
uv run pytest
```

Tests require 100% branch coverage.
