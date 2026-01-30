"""Security-related database models for rate limiting and blacklisting."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from sqlalchemy import (
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class BlacklistIdentifierType(str, Enum):
    """Type of identifier in the blacklist."""
    HOTKEY = "hotkey"
    IP = "ip"


class SecurityBlacklist(Base):
    """Permanent blacklist for malicious actors.
    
    Stores hotkeys and IPs that have been permanently or temporarily banned
    due to repeated failures or detected attack patterns. Loaded into memory
    on validator startup for fast checking.
    """
    __tablename__ = "security_blacklist"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique identifier for the blacklist entry",
    )
    identifier: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="The blacklisted identifier (hotkey SS58 address or IP address)",
    )
    identifier_type: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Type of identifier: 'hotkey' or 'ip'",
    )
    reason: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Human-readable reason for blacklisting",
    )
    failure_type: Mapped[str | None] = mapped_column(
        String(32),
        comment="The primary failure type that triggered blacklisting",
    )
    failure_count: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False,
        comment="Total number of failures before blacklisting",
    )
    metadata_json: Mapped[str | None] = mapped_column(
        Text,
        comment="Optional JSON metadata (last IPs seen, failure history, etc.)",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="When the entry was created (UTC)",
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        comment="When the ban expires (NULL = permanent)",
    )

    __table_args__ = (
        UniqueConstraint(
            "identifier",
            "identifier_type",
            name="uq_security_blacklist_identifier",
        ),
        Index("ix_security_blacklist_identifier", "identifier"),
        Index(
            "ix_security_blacklist_expires",
            "expires_at",
            postgresql_where="expires_at IS NOT NULL",
        ),
        Index("ix_security_blacklist_type", "identifier_type"),
    )


__all__ = [
    "BlacklistIdentifierType",
    "SecurityBlacklist",
]
