#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["rich>=13.7"]
# ///
"""
Dev wrapper — delegates to yesterwind_xyzmodem.demos.send.

For development use:  uv run demos/sz.py <file> ...
Installed command:    yw-send <file> ...
"""

import pathlib
import sys

# Make the local src tree importable when running directly from the repo
_repo_src = pathlib.Path(__file__).parent.parent / "src"
if _repo_src.is_dir():
    sys.path.insert(0, str(_repo_src))

from yesterwind_xyzmodem.demos.send import main  # noqa: E402

if __name__ == "__main__":
    main()
