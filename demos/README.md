# Yesterwind BBS Demo

An async telnet BBS client with automatic ZModem receive — exactly like
PCPlus or XTalk4 from the DOS era.  Connect to any telnet BBS, navigate
menus normally, and the moment the remote side starts a ZModem transfer
a retro-style progress panel pops up automatically.

## Requirements

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) (recommended) — or `pip install rich`

The demo only needs `rich` on top of the main `yesterwind-xyzmodem`
package, which has zero external dependencies.

## Running

```bash
# Quickest — uv resolves rich and the local package automatically
uv run demos/bbs.py <host> [port]

# With a custom download directory
uv run demos/bbs.py bbs.fozztexx.com 23 -d ~/Downloads/bbs

# Or install deps once and run directly
cd demos
uv sync
uv run python bbs.py bbs.fozztexx.com
```

## Controls

| Key      | Action                  |
|----------|-------------------------|
| Any key  | Sent to BBS normally    |
| `Ctrl+]` | Disconnect cleanly      |
| `Ctrl+C` | Force quit              |

## ZModem transfers

When the BBS initiates a download via ZModem the client detects the
`**\x18B` magic bytes and takes over automatically:

1. The terminal display pauses (the BBS has stopped sending menu text).
2. A progress panel appears showing filename, size, speed, and ETA.
3. The file is saved to the download directory.
4. The progress panel closes and BBS interaction resumes.

No key presses needed — it works just like your old DOS comms program.

## Some active telnet BBSes

| Address                    | Notes                        |
|----------------------------|------------------------------|
| `bbs.fozztexx.com`         | Level 29 BBS — very active   |
| `bbs.retrobbs.org`         | Retro theme                  |
| `blackflag.acid.org`       | Classic warez BBS feel       |
| `telnet.bbs.geek.nz`       | New Zealand scene            |
| `bbs.thenetworkbb.com`     | The Network BBS              |

Most BBSes let you log in as `new` or `guest` to look around before
creating an account.  Look in the file areas for ZModem downloads.

## Architecture

```
bbs.py
├── _TelnetTransport    wraps asyncio streams → yesterwind Transport
├── _Negotiator         handles IAC WILL/DO/SB negotiation (ECHO, SGA, NAWS)
├── _ZModemPanel        Rich Live panel with spinner + progress bar
└── BBSClient
    ├── _remote_to_local   reads BBS, strips IAC, scans for ZModem magic
    ├── _do_zmodem_receive intercepts transfer → ZModem.receive() + panel
    └── _local_to_remote   loop.add_reader on raw stdin → BBS
```
