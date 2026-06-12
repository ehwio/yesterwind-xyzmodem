"""Exceptions raised by the protocol engines."""


class XYZModemError(Exception):
    """Base class for all yesterwind-xyzmodem errors."""


class TransferCancelled(XYZModemError):
    """Remote sent two CAN bytes (or user cancelled)."""


class TransferFailed(XYZModemError):
    """Retry limit exceeded or unrecoverable protocol error."""


class TransferTimeout(XYZModemError):
    """No response from remote within the configured timeout."""


class ProtocolError(XYZModemError):
    """Unexpected or malformed data from remote."""
