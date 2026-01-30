"""Logging configuration for the validator.

Configures log filters and handlers to:
- Prevent double-logging from bittensor's Python logger
- Downgrade miner-side errors (expected, not actionable) from ERROR to DEBUG
- Add colored labels to differentiate error sources
- Suppress known harmless warnings (e.g., asyncpg cancellation)
"""
from __future__ import annotations

import logging as std_logging
import warnings

from sparket.shared.log_colors import LogColors


class DendriteErrorFilter(std_logging.Filter):
    """Filter that labels and downgrades miner-side communication errors.
    
    Miner-side errors (e.g., miner offline, returning HTML, connection refused)
    are expected in normal operation and don't require validator action.
    This filter:
    1. Adds a colored [MINER] label to make the source clear
    2. Downgrades from ERROR to DEBUG to reduce log noise
    
    IMPORTANT: Only matches actual ERROR-level logs with specific error patterns.
    Does NOT match normal dendrite success logs or state dumps.
    """
    
    # Error patterns that indicate miner-side communication failures
    # These are specific error class names that appear in exception messages
    MINER_ERROR_PATTERNS = (
        "ContentTypeError",       # Miner returning HTML instead of JSON
        "ClientConnectorError",   # Miner unreachable
        "ServerDisconnectedError",  # Miner connection dropped
        "ConnectionRefusedError",  # Miner port closed
        "ClientResponseError",    # HTTP error from miner
        "ClientOSError",          # OS-level connection error
    )
    
    def filter(self, record: std_logging.LogRecord) -> bool:
        # Only process ERROR level and above
        if record.levelno < std_logging.ERROR:
            return True
        
        msg = str(getattr(record, "msg", ""))
        
        # Check if this is a miner-side error (must match specific error patterns)
        is_miner_error = any(pattern in msg for pattern in self.MINER_ERROR_PATTERNS)
        
        if is_miner_error:
            # Add colored [MINER] prefix to indicate source
            record.msg = f"{LogColors.MINER_LABEL} {record.msg}"
            # Downgrade from ERROR to DEBUG (we handle these ourselves)
            record.levelno = std_logging.DEBUG
            record.levelname = "DEBUG"
        
        return True


def configure_validator_logging() -> None:
    """Configure logging for the validator.
    
    Should be called early in validator initialization.
    """
    # Suppress verbose SQLAlchemy logging
    for sqla_logger_name in [
        "sqlalchemy",
        "sqlalchemy.engine", 
        "sqlalchemy.pool",
        "sqlalchemy.engine.Engine",
    ]:
        sqla_logger = std_logging.getLogger(sqla_logger_name)
        sqla_logger.setLevel(std_logging.CRITICAL)
        sqla_logger.disabled = True
        sqla_logger.handlers = []
        sqla_logger.propagate = False
    
    # Prevent double-logging from bittensor's standard Python logger
    # (bittensor has its own console handler, so we disable propagation
    # to prevent the root logger from also printing the same messages)
    bt_logger = std_logging.getLogger("bittensor")
    bt_logger.propagate = False
    
    # Add filter to label and downgrade miner-side errors
    bt_logger.addFilter(DendriteErrorFilter())
    
    # Suppress known harmless asyncpg warnings
    # These occur when queries are cancelled (e.g., via asyncio.wait_for timeout)
    # The "_cancel was never awaited" warning is expected and not actionable
    warnings.filterwarnings(
        "ignore",
        message=r"coroutine 'Connection\._cancel' was never awaited",
        category=RuntimeWarning,
    )
    warnings.filterwarnings(
        "ignore",
        message=r"Enable tracemalloc to get the object allocation traceback",
        category=RuntimeWarning,
    )
