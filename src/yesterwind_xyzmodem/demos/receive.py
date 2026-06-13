"""
Yesterwind rz — ZModem TCP client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Connects to a ``yw-send`` / ``sz --tcp-server`` instance and receives files
via ZModem.  Functionally equivalent to ``rz --tcp-client <host>:<port>``.

Usage:
    yw-receive <host> <port> [-d download_dir]

Example:
    # On the server:
    yw-send file.bin
    # (prints something like:  Listening on 0.0.0.0:54321)

    # On the client:
    yw-receive server.host 54321
"""

from __future__ import annotations

import argparse
import asyncio
import pathlib
import sys

try:
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

    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False

from yesterwind_xyzmodem.callbacks import EventType, TransferProgress
from yesterwind_xyzmodem.transport import StreamTransport
from yesterwind_xyzmodem.zmodem import ZModem

if _HAS_RICH:
    console = Console(highlight=False)


# ─────────────────────────────────────────────────────────────────────────────
# Progress panel
# ─────────────────────────────────────────────────────────────────────────────


class _Panel:
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
        self._task_id: TaskID | None = None
        self._live: Live | None = None
        self.filename: str = ""
        self.total_bytes: int = 0
        self._dest: str = ""

    def _render(self) -> Panel:
        grid = Table.grid(padding=(0, 1))
        grid.add_column(style="bold dim", justify="right")
        grid.add_column()
        grid.add_row("File:", Text(self.filename or "—", style="bold white"))
        grid.add_row("Saving to:", Text(self._dest or "—", style="dim"))
        grid.add_row("", self._progress)
        return Panel(
            grid,
            title="[bold yellow]  ⬇  ZModem Receive  [/bold yellow]",
            border_style="yellow",
            padding=(0, 1),
            width=72,
        )

    def start(self, filename: str, total: int, dest: str) -> None:
        self.filename = filename
        self.total_bytes = total
        self._dest = dest
        self._task_id = self._progress.add_task(
            filename or "file",
            total=total if total > 0 else None,
        )
        self._live = Live(
            self._render(),
            console=console,
            refresh_per_second=12,
            transient=False,
        )
        self._live.start()

    def update(self, bytes_done: int) -> None:
        if self._task_id is not None:
            self._progress.update(self._task_id, completed=bytes_done)
        if self._live:
            self._live.update(self._render())

    def finish(self, ok: bool, bytes_done: int) -> None:
        if self._task_id is not None:
            self._progress.update(
                self._task_id,
                completed=bytes_done,
                description="[bold green]Complete" if ok else "[bold red]Failed",
            )
        if self._live:
            self._live.update(self._render())
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
# Core receive logic
# ─────────────────────────────────────────────────────────────────────────────


async def receive(host: str, port: int, download_dir: pathlib.Path) -> int:
    console.print(
        f"\n[bold]Yesterwind rz[/bold]  [dim]connecting to "
        f"[cyan]{host}[/cyan]:[cyan]{port}[/cyan]…[/dim]"
    )

    try:
        reader, writer = await asyncio.open_connection(host, port)
    except OSError as exc:
        console.print(f"[red]Connection failed: {exc}[/red]")
        return 1

    console.print(f"[dim]Connected.  Downloads → [cyan]{download_dir}[/cyan][/dim]\n")

    transport = StreamTransport(reader, writer)
    panel = _Panel()
    panel_started = False
    bytes_done = 0

    def _cb(prog: TransferProgress) -> None:
        nonlocal panel_started, bytes_done
        if prog.event == EventType.FILE_START:
            dest = str(download_dir / (prog.filename or "file"))
            panel.start(prog.filename or "file", prog.total_bytes, dest)
            panel_started = True
        elif prog.event == EventType.BLOCK_RECEIVED:
            bytes_done = prog.bytes_transferred
            if panel_started:
                panel.update(bytes_done)
        elif prog.event == EventType.FILE_END:
            bytes_done = prog.bytes_transferred

    zmodem = ZModem(transport, callback=_cb)
    ok = False
    try:
        await zmodem.receive(str(download_dir))
        ok = True
    except Exception as exc:
        console.print(f"[red]\nZModem error: {exc}[/red]")
    finally:
        if panel_started:
            panel.finish(ok, bytes_done)
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass

    return 0 if ok else 1


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="yw-receive",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("host", help="Hostname or IP of the yw-send / sz --tcp-server")
    p.add_argument("port", type=int, help="TCP port printed by yw-send")
    p.add_argument(
        "-d",
        "--download-dir",
        default="downloads",
        metavar="DIR",
        help="Directory for received files (default: ./downloads)",
    )
    return p


def main() -> None:
    if not _HAS_RICH:
        print(
            "error: yw-receive requires the 'rich' package.\n"
            "Install it with:  pip install 'yesterwind-xyzmodem[demos]'",
            file=sys.stderr,
        )
        raise SystemExit(1)

    args = _build_parser().parse_args()
    dl_dir = pathlib.Path(args.download_dir).expanduser().resolve()
    dl_dir.mkdir(parents=True, exist_ok=True)
    try:
        rc = asyncio.run(receive(args.host, args.port, dl_dir))
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted.[/dim]")
        rc = 1
    sys.exit(rc)


if __name__ == "__main__":
    main()
