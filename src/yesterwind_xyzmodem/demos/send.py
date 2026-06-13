"""
Yesterwind sz — ZModem TCP server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Listens for incoming connections and sends files via ZModem.
Functionally equivalent to ``sz --tcp-server <files...>``.

Usage:
    yw-send [options] <file> [<file> ...]

Examples:
    # Single shot — send file.bin to whoever connects, then exit:
    yw-send file.bin

    # Keep listening; serve every client that connects (in parallel):
    yw-send --serve-forever file1.bin file2.bin

    # Pick a specific port (default: OS-assigned):
    yw-send --port 12345 archive.tar.gz

On the receiving end:
    rz --tcp-client <host>:<port>
    yw-receive <host> <port>
"""

from __future__ import annotations

import argparse
import asyncio
import os
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

# Lock so concurrent client panels don't interleave their output
_print_lock = asyncio.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# Progress panel (one per client connection)
# ─────────────────────────────────────────────────────────────────────────────


class _Panel:
    def __init__(self, client_addr: str) -> None:
        self._client = client_addr
        self._progress = Progress(
            SpinnerColumn(style="bold cyan"),
            TextColumn("[bold yellow]{task.description}", justify="right"),
            BarColumn(bar_width=36, style="cyan", complete_style="bold green"),
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
        self._file_index: int = 0
        self._file_count: int = 1

    def _render(self) -> Panel:
        grid = Table.grid(padding=(0, 1))
        grid.add_column(style="bold dim", justify="right")
        grid.add_column()
        grid.add_row("Client:", Text(self._client, style="bold white"))
        grid.add_row(
            "File:",
            Text(
                f"{self.filename or '—'}  "
                f"[dim]({self._file_index + 1}/{self._file_count})[/dim]",
                style="bold white",
            ),
        )
        grid.add_row("", self._progress)
        return Panel(
            grid,
            title="[bold cyan]  ⬆  ZModem Send  [/bold cyan]",
            border_style="cyan",
            padding=(0, 1),
            width=72,
        )

    def start_session(self, file_count: int) -> None:
        self._file_count = file_count
        self._live = Live(
            self._render(),
            console=console,
            refresh_per_second=12,
            transient=False,
        )
        self._live.start()

    def start_file(self, filename: str, total: int, index: int) -> None:
        self.filename = filename
        self.total_bytes = total
        self._file_index = index
        if self._task_id is not None:
            self._progress.remove_task(self._task_id)
        self._task_id = self._progress.add_task(
            filename or "file",
            total=total if total > 0 else None,
        )
        if self._live:
            self._live.update(self._render())

    def update(self, bytes_done: int) -> None:
        if self._task_id is not None:
            self._progress.update(self._task_id, completed=bytes_done)
        if self._live:
            self._live.update(self._render())

    def finish(self, ok: bool, total_bytes: int) -> None:
        if self._task_id is not None:
            self._progress.update(
                self._task_id,
                completed=self.total_bytes or total_bytes,
                description="[bold green]Complete" if ok else "[bold red]Failed",
            )
        if self._live:
            self._live.update(self._render())
            self._live.stop()
            self._live = None
        label = "[bold green]✓  Sent[/bold green]" if ok else "[bold red]✗  Failed[/bold red]"
        console.print(
            f"\n{label} [dim]{self._file_count} file(s)[/dim]"
            f" → [bold white]{self._client}[/bold white]"
            f" [dim]({total_bytes:,} bytes total)[/dim]"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Per-client handler
# ─────────────────────────────────────────────────────────────────────────────


async def _handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    files: list[pathlib.Path],
) -> None:
    peername = writer.get_extra_info("peername")
    client_addr = f"{peername[0]}:{peername[1]}" if peername else "unknown"

    async with _print_lock:
        console.print(
            f"[bold cyan]→[/bold cyan] Client connected: [bold white]{client_addr}[/bold white]"
        )

    # Build the (filename, stream, size) list ZModem.send expects
    file_tuples = []
    handles = []
    for path in files:
        fh = open(path, "rb")  # noqa: SIM115
        handles.append(fh)
        file_tuples.append((path.name, fh, os.path.getsize(path)))

    transport = StreamTransport(reader, writer)
    panel = _Panel(client_addr)

    def _cb(prog: TransferProgress) -> None:
        if prog.event == EventType.SESSION_START:
            panel.start_session(prog.file_count)
        elif prog.event == EventType.FILE_START:
            panel.start_file(prog.filename, prog.total_bytes, prog.file_index)
        elif prog.event == EventType.BLOCK_SENT:
            panel.update(prog.bytes_transferred)

    zmodem = ZModem(transport, callback=_cb)
    ok = False
    total_sent = 0
    try:
        total_sent = await zmodem.send(file_tuples)
        ok = True
    except Exception as exc:
        async with _print_lock:
            console.print(f"[red]ZModem error ({client_addr}): {exc}[/red]")
    finally:
        for fh in handles:
            fh.close()
        panel.finish(ok, total_sent)
        # Half-close: stop sending, then drain rz's remaining output before
        # closing fully so rz doesn't get SIGPIPE on its final write.
        try:
            writer.write_eof()
        except OSError:
            pass
        try:
            while True:
                chunk = await asyncio.wait_for(reader.read(4096), timeout=1.0)
                if not chunk:
                    break
        except (OSError, asyncio.TimeoutError):
            pass
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Server logic
# ─────────────────────────────────────────────────────────────────────────────


async def serve(
    files: list[pathlib.Path],
    host: str,
    port: int,
    serve_forever: bool,
) -> int:
    pending: set[asyncio.Task] = set()

    def _client_factory(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.get_running_loop().create_task(
            _handle_client(reader, writer, files)
        )
        pending.add(task)
        task.add_done_callback(pending.discard)
        if not serve_forever:
            server.close()

    server = await asyncio.start_server(_client_factory, host, port)
    addrs = [s.getsockname() for s in server.sockets or []]
    actual_port = addrs[0][1] if addrs else port

    file_list = "  ".join(f"[cyan]{f.name}[/cyan]" for f in files)
    mode_label = (
        "[bold green]serve-forever[/bold green]" if serve_forever else "[dim]single-shot[/dim]"
    )
    console.print(
        f"\n[bold]Yesterwind sz[/bold]  {mode_label}\n"
        f"  Listening on [bold white]{host}:{actual_port}[/bold white]\n"
        f"  Files: {file_list}\n"
        f"\n[dim]Connect with:  rz --tcp-client {host}:{actual_port}[/dim]\n"
        f"[dim]           or:  yw-receive {host} {actual_port}[/dim]\n"
    )

    async with server:
        try:
            if serve_forever:
                await server.serve_forever()
            else:
                # Wait for the one connection to finish: first wait for the
                # server to stop accepting (client_factory closes it on
                # connect), then await the in-flight task directly.
                await server.start_serving()
                while server.is_serving():
                    await asyncio.sleep(0.05)
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
        except asyncio.CancelledError:
            pass

    return 0


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="yw-send",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("files", nargs="+", metavar="FILE", help="File(s) to send")
    p.add_argument(
        "--port",
        "-p",
        type=int,
        default=0,
        metavar="PORT",
        help="TCP port to listen on (default: OS-assigned)",
    )
    p.add_argument(
        "--host",
        default="0.0.0.0",
        metavar="HOST",
        help="Interface to bind to (default: 0.0.0.0)",
    )
    p.add_argument(
        "--serve-forever",
        "-S",
        action="store_true",
        help="Keep listening after the first client disconnects; handle clients in parallel",
    )
    return p


def main() -> None:
    if not _HAS_RICH:
        print(
            "error: yw-send requires the 'rich' package.\n"
            "Install it with:  pip install 'yesterwind-xyzmodem[demos]'",
            file=sys.stderr,
        )
        raise SystemExit(1)

    args = _build_parser().parse_args()

    paths: list[pathlib.Path] = []
    for name in args.files:
        p = pathlib.Path(name).expanduser().resolve()
        if not p.is_file():
            console.print(f"[red]Not a file: {p}[/red]")
            sys.exit(1)
        paths.append(p)

    try:
        rc = asyncio.run(serve(paths, args.host, args.port, args.serve_forever))
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted.[/dim]")
        rc = 0
    sys.exit(rc)


if __name__ == "__main__":
    main()
