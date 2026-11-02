"""
Common utility functions for logging, formatting, and network address validation.
"""

import ipaddress
import logging
import sys


DEFAULT_HOST = "127.0.0.1"
DEFAULT_TRACKER_PORT = 8000
DEFAULT_HEARTBEAT_INTERVAL = 5.0
DEFAULT_PEER_TIMEOUT = 30.0
BUFFER_SIZE = 65536


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Sets up a standardized console logger with timestamps and peer tags."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def validate_address(host: str, port: int) -> bool:
    """
    Validates host IP address and port boundary (1-65535).
    Manual utility wrapper for socket endpoint sanity checking.
    """
    if not (1 <= port <= 65535):
        return False
    if host.lower() == "localhost":
        return True
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def format_bytes(bytes_count: int) -> str:
    """Formats raw byte numbers into human-readable strings (KB, MB, GB)."""
    if bytes_count < 1024:
        return f"{bytes_count} B"
    elif bytes_count < 1024 * 1024:
        return f"{bytes_count / 1024:.2f} KB"
    elif bytes_count < 1024 * 1024 * 1024:
        return f"{bytes_count / (1024 * 1024):.2f} MB"
    else:
        return f"{bytes_count / (1024 * 1024 * 1024):.2f} GB"
