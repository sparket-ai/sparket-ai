"""Security configuration for rate limiting and blacklisting."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Tuple


class FailureType(str, Enum):
    """Types of failures that can be tracked for cooldown/blacklist decisions."""
    
    # Token-related (medium severity)
    TOKEN_INVALID = "token_invalid"
    TOKEN_MISSING = "token_missing"
    
    # Rate limiting (low severity)
    RATE_LIMITED = "rate_limited"
    
    # Cooldown violation - submitting while in cooldown (fail2ban trigger)
    COOLDOWN_VIOLATION = "cooldown_violation"
    
    # Registration/identity (high severity)
    NOT_REGISTERED = "not_registered"
    SIGNATURE_INVALID = "signature_invalid"
    
    # Request format (high severity)
    MALFORMED_REQUEST = "malformed_request"
    
    # Security threats (critical severity)
    NONCE_REPLAY = "nonce_replay"
    SCANNER_REQUEST = "scanner_request"  # Probing/scanning (e.g., GET / with no synapse)
    
    # Generic catch-all (low severity)
    OTHER = "other"


# Failure types that count toward permanent blacklist
CRITICAL_FAILURES: Tuple[str, ...] = (
    FailureType.NOT_REGISTERED.value,
    FailureType.SIGNATURE_INVALID.value,
    FailureType.NONCE_REPLAY.value,
    FailureType.SCANNER_REQUEST.value,  # Immediate blacklist for scanners/probes
)

# Failure types that count toward cooldown
COOLDOWN_FAILURES: Tuple[str, ...] = (
    FailureType.TOKEN_INVALID.value,
    FailureType.TOKEN_MISSING.value,
    FailureType.RATE_LIMITED.value,
    FailureType.MALFORMED_REQUEST.value,
    FailureType.OTHER.value,
)


@dataclass
class CooldownConfig:
    """Configuration for cooldown behavior."""
    
    # Per-hotkey cooldown settings
    hotkey_failure_threshold: int = 10  # Failures before cooldown triggers
    hotkey_initial_cooldown_sec: int = 30  # First cooldown duration
    hotkey_max_cooldown_sec: int = 3600  # Max cooldown (1 hour)
    hotkey_backoff_multiplier: float = 2.0  # Exponential backoff factor
    
    # Per-IP aggregate settings (catches hotkey cycling)
    ip_failure_threshold: int = 50  # Total failures from IP before cooldown
    ip_failure_window_sec: int = 300  # Window for counting IP failures (5 min)
    ip_distinct_hotkey_threshold: int = 5  # Distinct failing hotkeys to flag IP
    ip_initial_cooldown_sec: int = 60  # IP cooldown duration
    ip_max_cooldown_sec: int = 3600  # Max IP cooldown
    
    # Fail2ban settings - ban miners who persistently ignore cooldowns
    # If a miner keeps submitting while in cooldown, they get banned for 24 hours
    fail2ban_violation_threshold: int = 20  # Cooldown violations before 24h ban
    fail2ban_violation_window_sec: int = 3600  # Window to count violations (1 hour)
    fail2ban_duration_sec: int = 86400  # Ban duration (24 hours)
    
    # Permanent blacklist thresholds
    # NOTE: Only CRITICAL failures (spoofing, invalid signatures, nonce replay) 
    # trigger permanent blacklist. Regular cooldowns do NOT trigger permanent bans.
    permanent_critical_threshold: int = 3  # Critical failures for permanent blacklist
    
    # Cleanup intervals
    cleanup_interval_sec: int = 300  # How often to clean stale entries
    failure_retention_sec: int = 86400  # How long to keep failure history (24h)


@dataclass
class RateLimitConfig:
    """Configuration for per-request rate limiting."""
    
    # Per-hotkey limits
    hotkey_per_second: int = 10
    hotkey_per_minute: int = 120
    
    # Per-IP limits (aggregate across all hotkeys)
    ip_per_second: int = 50
    ip_per_minute: int = 500
    
    # Global limits (all traffic)
    global_per_second: int = 200
    global_per_minute: int = 5000


@dataclass
class SecurityConfig:
    """Combined security configuration."""
    
    cooldown: CooldownConfig = field(default_factory=CooldownConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    
    # Feature flags
    enforce_registration: bool = True  # Reject unregistered hotkeys
    enforce_cooldowns: bool = True  # Apply cooldowns on failures
    enforce_blacklist: bool = True  # Check permanent blacklist


__all__ = [
    "FailureType",
    "CRITICAL_FAILURES",
    "COOLDOWN_FAILURES",
    "CooldownConfig",
    "RateLimitConfig",
    "SecurityConfig",
]
