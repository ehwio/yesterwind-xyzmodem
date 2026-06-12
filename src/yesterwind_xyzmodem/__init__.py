"""
yesterwind-xyzmodem — pure Python 3 X/Y/Z-Modem file transfer protocols.

Quick start::

    from yesterwind_xyzmodem import XModem, YModem, ZModem
    from yesterwind_xyzmodem.transport import StreamTransport

    transport = StreamTransport(reader, writer)
    xm = XModem(transport)
    await xm.send(open("file.bin", "rb"))
"""

from yesterwind_xyzmodem.xmodem import XModem
from yesterwind_xyzmodem.ymodem import YModem
from yesterwind_xyzmodem.zmodem import ZModem

__all__ = ["XModem", "YModem", "ZModem"]
__version__ = "0.1.0"
