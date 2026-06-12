"""Control bytes shared across XModem, YModem, and ZModem."""

# ASCII control characters
SOH = 0x01   # Start of Header (128-byte block)
STX = 0x02   # Start of Text   (1024-byte block)
EOT = 0x04   # End of Transmission
ACK = 0x06   # Acknowledge
NAK = 0x15   # Negative Acknowledge
CAN = 0x18   # Cancel (two consecutive CANs abort)
SUB = 0x1A   # Substitute / CTRL-Z (padding byte)

# CRC mode initiation character ('C' = 0x43)
CRC_MODE = ord("C")

# ZModem ZDLE escape character
ZDLE = 0x18  # same byte value as CAN — context distinguishes them

# Timeouts (seconds)
DEFAULT_TIMEOUT = 10.0
DEFAULT_RETRY_LIMIT = 10
