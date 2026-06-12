#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["rich>=13.7"]
# ///
"""
Yesterwind BBS Client
~~~~~~~~~~~~~~~~~~~~~
A retro-style async telnet terminal with automatic ZModem receive.
When the BBS initiates a ZModem transfer, a progress panel pops up
exactly like PCPlus or XTalk4 — no user action required.

Usage:
    uv run demos/bbs.py <host> [port] [-d download_dir]

Examples:
    uv run demos/bbs.py bbs.fozztexx.com
    uv run demos/bbs.py bbs.example.com 23 -d ~/Downloads/bbs

Controls:
    Ctrl+]   Disconnect
    Ctrl+C   Force quit
"""

from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import signal
import sys
import termios
import tty
from typing import Optional

# ── Make sure the local package is importable when run directly ──────────────
_repo_src = pathlib.Path(__file__).parent.parent / "src"
if _repo_src.is_dir():
    sys.path.insert(0, str(_repo_src))

from rich.columns import Columns
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)
from rich.table import Table
from rich.text import Text

from yesterwind_xyzmodem.callbacks import EventType, TransferProgress
from yesterwind_xyzmodem.transport import Transport
from yesterwind_xyzmodem.zmodem import ZModem

# ─────────────────────────────────────────────────────────────────────────────
# Telnet protocol constants
# ─────────────────────────────────────────────────────────────────────────────

IAC  = 0xFF  # Interpret As Command
WILL = 0xFB
WONT = 0xFC
DO   = 0xFD
DONT = 0xFE
SB   = 0xFA  # Subnegotiation Begin
SE   = 0xF0  # Subnegotiation End

OPT_ECHO    = 0x01
OPT_SGA     = 0x03  # Suppress Go Ahead
OPT_TTYPE   = 0x18  # Terminal Type
OPT_NAWS    = 0x1F  # Negotiate About Window Size

# ── ZModem session trigger: sender opens with this 4-byte sequence ───────────
ZMODEM_MAGIC = b"**\x18B"   # "**" + ZDLE + 'B' (ZHEX)

# ─────────────────────────────────────────────────────────────────────────────
# Rich console (stderr so it doesn't mix with raw terminal bytes on stdout)
# ─────────────────────────────────────────────────────────────────────────────

console = Console(stderr=True, highlight=False)


# ─────────────────────────────────────────────────────────────────────────────
# Transport bridge
# ─────────────────────────────────────────────────────────────────────────────

class _TelnetTransport(Transport):
    """
    Bridges asyncio (StreamReader, StreamWriter) to the yesterwind Transport
    interface, with an optional pre-fill byte buffer for bytes already consumed
    from the stream before the ZModem session was detected.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        prepend: bytes = b"",
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._buf = bytearray(prepend)

    async def read_byte(self) -> int:
        if self._buf:
            return self._buf.pop(0)
        data = await self._reader.readexactly(1)
        return data[0]

    async def read(self, n: int) -> bytes:
        out = bytearray()
        for _ in range(n):
            out.append(await self.read_byte())
        return bytes(out)

    async def write(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()

    async def read_with_timeout(self, n: int, timeout: float) -> bytes:
        return await asyncio.wait_for(self.read(n), timeout=timeout)

    async def read_byte_with_timeout(self, timeout: float) -> int:
        return await asyncio.wait_for(self.read_byte(), timeout=timeout)

    async def purge(self) -> None:
        self._buf.clear()
        try:
            while True:
                await asyncio.wait_for(self._reader.read(4096), timeout=0.05)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError):
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Telnet IAC negotiation
# ─────────────────────────────────────────────────────────────────────────────

def _naws_packet(cols: int, rows: int) -> bytes:
    return bytes([
        IAC, SB, OPT_NAWS,
        (cols >> 8) & 0xFF, cols & 0xFF,
        (rows >> 8) & 0xFF, rows & 0xFF,
        IAC, SE,
    ])


class _Negotiator:
    """
    Minimal telnet negotiator.
    - Accepts ECHO and SGA from server.
    - Responds to NAWS DO with our window size.
    - Rejects all other options.
    """

    def __init__(self, writer: asyncio.StreamWriter) -> None:
        self._w = writer

    async def handle(self, cmd: int, opt: int) -> None:
        if cmd == DO:
            if opt == OPT_NAWS:
                cols, rows = os.get_terminal_size()
                self._w.write(bytes([IAC, WILL, opt]))
                self._w.write(_naws_packet(cols, rows))
            else:
                self._w.write(bytes([IAC, WONT, opt]))
            await self._w.drain()
        elif cmd == WILL:
            reply = DO if opt in (OPT_ECHO, OPT_SGA) else DONT
            self._w.write(bytes([IAC, reply, opt]))
            await self._w.drain()
        # WONT / DONT require no response


# ─────────────────────────────────────────────────────────────────────────────
# ZModem progress panel  (the "pop-up box")
# ─────────────────────────────────────────────────────────────────────────────

class _ZModemPanel:
    """
    A retro-style ZModem transfer progress panel rendered with Rich.
    Mimics the kind of status box that appeared in PCPlus / XTalk4.
    """

    def __init__(self) -> None:
        self._progress = Progress(
            SpinnerColumn(style="bold yellow"),
            TextColumn("[bold cyan]{task.description}", justify="right"),
            BarColumn(bar_width=36, style="yellow", complete_style="bold green"),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=False,
            expand=False,
        )
        self._task_id: Optional[TaskID] = None
        self._live: Optional[Live] = None
        self.filename: str = ""
        self.total_bytes: int = 0

    # ── Renderable ─────────────────────────────────────────────────────────

    def _panel(self) -> Panel:
        grid = Table.grid(padding=(0, 1))
        grid.add_column(style="bold dim", justify="right")
        grid.add_column()

        grid.add_row("File:", Text(self.filename or "—", style="bold white"))
        grid.add_row("Saving to:", Text(self._dest or "—", style="dim"))
        grid.add_row("", self._progress)

        return Panel(
            grid,
            title="[bold yellow]  ⬇  ZModem Transfer  [/bold yellow]",
            border_style="yellow",
            padding=(0, 1),
            width=72,
        )

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def start(self, filename: str, total: int, dest: str) -> None:
        self.filename = filename
        self.total_bytes = total
        self._dest = dest
        self._task_id = self._progress.add_task(
            filename or "file",
            total=total if total > 0 else None,
        )
        self._live = Live(
            self._panel(),
            console=console,
            refresh_per_second=12,
            transient=False,
        )
        self._live.start()

    def update(self, bytes_done: int) -> None:
        if self._task_id is not None:
            self._progress.update(self._task_id, completed=bytes_done)
        if self._live:
            self._live.update(self._panel())

    def finish(self, ok: bool, bytes_done: int) -> None:
        if self._task_id is not None:
            self._progress.update(
                self._task_id,
                completed=bytes_done,
                description="[bold green]Complete" if ok else "[bold red]Failed",
            )
        if self._live:
            self._live.update(self._panel())
            self._live.stop()
            self._live = None

        if ok:
            console.print(
                f"\n[bold green]✓  Received [cyan]{self.filename}[/cyan]"
                f" → [dim]{self._dest}[/dim] ({bytes_done:,} bytes)[/bold green]"
            )
        else:
            console.print("\n[bold red]✗  Transfer failed or cancelled[/bold red]")


# ─────────────────────────────────────────────────────────────────────────────
# Main BBS client
# ─────────────────────────────────────────────────────────────────────────────

class BBSClient:
    """
    Async telnet BBS client.

    Transparently proxies the user's terminal to the remote BBS and
    automatically intercepts outgoing ZModem sessions.
    """

    def __init__(self, host: str, port: int, download_dir: str) -> None:
        self._host = host
        self._port = port
        self._dl_dir = pathlib.Path(download_dir).expanduser().resolve()
        self._dl_dir.mkdir(parents=True, exist_ok=True)

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._neg: Optional[_Negotiator] = None

        self._running = False
        self._in_transfer = False          # gates stdin-to-remote forwarding
        self._orig_tc: Optional[list] = None

    # ── Entry point ────────────────────────────────────────────────────────

    async def run(self) -> None:
        console.print(
            f"\n[bold]Yesterwind BBS[/bold]  [dim]connecting to "
            f"[cyan]{self._host}[/cyan]:[cyan]{self._port}[/cyan]…[/dim]"
        )
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self._host, self._port
            )
        except OSError as exc:
            console.print(f"[red]Connection failed: {exc}[/red]")
            return

        self._neg = _Negotiator(self._writer)
        self._running = True
        self._set_raw()

        console.print(
            f"[dim]Connected.  Downloads → [cyan]{self._dl_dir}[/cyan]\n"
            f"Press [bold]Ctrl+][/bold] (0x1D) to disconnect.[/dim]"
        )

        # Register SIGWINCH so the server knows when the user resizes
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGWINCH, self._on_winch)

        try:
            await asyncio.gather(
                self._remote_to_local(),
                self._local_to_remote(),
            )
        except (ConnectionResetError, asyncio.IncompleteReadError, EOFError):
            pass
        finally:
            loop.remove_signal_handler(signal.SIGWINCH)
            self._restore_terminal()
            console.print("\n[dim]Disconnected.[/dim]")
            if self._writer:
                self._writer.close()

    # ── Terminal mode ──────────────────────────────────────────────────────

    def _set_raw(self) -> None:
        fd = sys.stdin.fileno()
        if sys.stdin.isatty():
            self._orig_tc = termios.tcgetattr(fd)
            tty.setraw(fd, termios.TCSANOW)

    def _restore_terminal(self) -> None:
        if self._orig_tc is not None:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self._orig_tc)
            self._orig_tc = None

    def _on_winch(self) -> None:
        if self._writer and not self._in_transfer:
            try:
                cols, rows = os.get_terminal_size()
                self._writer.write(bytes([IAC, WILL, OPT_NAWS]))
                self._writer.write(_naws_packet(cols, rows))
            except OSError:
                pass

    # ── Remote → local ─────────────────────────────────────────────────────

    async def _remote_to_local(self) -> None:
        """
        Read from the BBS, strip/respond to IAC sequences, scan for ZModem
        magic, and forward everything else to the user's terminal.
        """
        MLEN = len(ZMODEM_MAGIC)
        magic_buf: bytearray = bytearray()   # rolling window for magic scan
        display: bytearray = bytearray()      # bytes ready to display

        while self._running:
            # ── Read a chunk ─────────────────────────────────────────────
            try:
                chunk = await asyncio.wait_for(
                    self._reader.read(4096), timeout=0.5
                )
            except asyncio.TimeoutError:
                if display:
                    sys.stdout.buffer.write(bytes(display))
                    sys.stdout.buffer.flush()
                    display.clear()
                continue
            if not chunk:
                self._running = False
                break

            i = 0
            while i < len(chunk):
                b = chunk[i]

                # ── Telnet IAC handling ───────────────────────────────────
                if b == IAC:
                    # Flush display before touching IAC so display is clean
                    if display:
                        sys.stdout.buffer.write(bytes(display))
                        sys.stdout.buffer.flush()
                        display.clear()
                    if magic_buf:
                        sys.stdout.buffer.write(bytes(magic_buf))
                        sys.stdout.buffer.flush()
                        magic_buf.clear()

                    # Need at least 2 bytes for the command
                    while len(chunk) < i + 2:
                        more = await self._reader.readexactly(1)
                        chunk = chunk + more

                    cmd = chunk[i + 1]
                    if cmd == IAC:          # escaped 0xFF literal
                        display.append(IAC)
                        i += 2
                        continue
                    if cmd in (WILL, WONT, DO, DONT):
                        while len(chunk) < i + 3:
                            more = await self._reader.readexactly(1)
                            chunk = chunk + more
                        await self._neg.handle(cmd, chunk[i + 2])
                        i += 3
                        continue
                    if cmd == SB:           # sub-negotiation: skip to IAC SE
                        j = i + 2
                        while True:
                            while len(chunk) < j + 2:
                                more = await self._reader.readexactly(1)
                                chunk = chunk + more
                            if chunk[j] == IAC and chunk[j + 1] == SE:
                                i = j + 2
                                break
                            j += 1
                        continue
                    i += 2                  # unknown 2-byte IAC — skip
                    continue

                # ── ZModem magic scanner ──────────────────────────────────
                magic_buf.append(b)

                if len(magic_buf) > MLEN:
                    # Oldest byte can no longer be part of magic — emit it
                    display.append(magic_buf.pop(0))

                if bytes(magic_buf) == ZMODEM_MAGIC:
                    # Hit! Flush everything before the magic to the terminal
                    if display:
                        sys.stdout.buffer.write(bytes(display))
                        sys.stdout.buffer.flush()
                        display.clear()
                    magic_buf.clear()

                    # Bytes from this chunk after the magic go into the
                    # ZModem receive buffer (they're the rest of ZRQINIT)
                    tail = bytes(chunk[i + 1:])
                    await self._do_zmodem_receive(tail)

                    # Restart the scan after the transfer completes
                    i = len(chunk)
                    continue

                i += 1

            # End of chunk — emit whatever was in the magic scan window
            display.extend(magic_buf)
            magic_buf.clear()

            if display:
                sys.stdout.buffer.write(bytes(display))
                sys.stdout.buffer.flush()
                display.clear()

    # ── ZModem auto-receive ────────────────────────────────────────────────

    async def _do_zmodem_receive(self, tail: bytes) -> None:
        """
        Called the instant ZModem magic is detected.
        Suspends the raw terminal, shows the retro progress panel,
        runs the ZModem session, then resumes.
        """
        self._in_transfer = True
        self._restore_terminal()          # let Rich draw normally

        # The magic bytes + any trailing bytes from the same network chunk
        # are prepended to the transport so the ZModem engine reads them first
        prepend = ZMODEM_MAGIC + tail
        transport = _TelnetTransport(self._reader, self._writer, prepend)

        panel = _ZModemPanel()
        panel_started = False
        bytes_done = 0

        def _cb(prog: TransferProgress) -> None:
            nonlocal panel_started, bytes_done
            if prog.event == EventType.FILE_START:
                dest = str(self._dl_dir / (prog.filename or "file"))
                panel.start(prog.filename or "file", prog.total_bytes, dest)
                panel_started = True
            elif prog.event in (EventType.BLOCK_RECEIVED,):
                bytes_done = prog.bytes_transferred
                if panel_started:
                    panel.update(bytes_done)
            elif prog.event == EventType.FILE_END:
                bytes_done = prog.bytes_transferred

        zmodem = ZModem(transport, callback=_cb)
        ok = False
        try:
            await zmodem.receive(str(self._dl_dir))
            ok = True
        except Exception as exc:
            console.print(f"[red]\nZModem error: {exc}[/red]")
        finally:
            if panel_started:
                panel.finish(ok, bytes_done)

        self._in_transfer = False
        self._set_raw()                   # back to raw for BBS navigation

    # ── Local → remote ─────────────────────────────────────────────────────

    async def _local_to_remote(self) -> None:
        """
        Forward keystrokes to the BBS.  Uses loop.add_reader() on raw stdin
        so every key is sent immediately without buffering.
        Ctrl+] (0x1D) triggers a clean disconnect.
        """
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[bytes] = asyncio.Queue()

        def _stdin_ready() -> None:
            try:
                data = os.read(sys.stdin.fileno(), 256)
                if data:
                    loop.call_soon_threadsafe(queue.put_nowait, data)
            except OSError:
                loop.call_soon_threadsafe(queue.put_nowait, b"")

        loop.add_reader(sys.stdin.fileno(), _stdin_ready)
        try:
            while self._running:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue

                if not data:
                    self._running = False
                    break

                if 0x1D in data:           # Ctrl+]
                    self._running = False
                    break

                if self._in_transfer:
                    continue               # drop keystrokes during ZModem

                try:
                    self._writer.write(data)
                    await self._writer.drain()
                except Exception:
                    self._running = False
                    break
        finally:
            loop.remove_reader(sys.stdin.fileno())


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="bbs.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Some active telnet BBSes to try:
  bbs.fozztexx.com        – Level 29 BBS (classic feel)
  bbs.retrobbs.org        – RetroBBS
  blackflag.acid.org      – Acid / Black Flag

Press Ctrl+] to disconnect cleanly.
""",
    )
    p.add_argument("host", help="BBS hostname or IP address")
    p.add_argument("port", nargs="?", type=int, default=23, help="TCP port (default: 23)")
    p.add_argument(
        "-d", "--download-dir",
        default="downloads",
        metavar="DIR",
        help="Directory for received files (default: ./downloads)",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()
    client = BBSClient(args.host, args.port, args.download_dir)
    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
