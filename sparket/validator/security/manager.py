"""Security manager for rate limiting, cooldowns, and blacklisting.

Provides centralized security checks with:
- Dual-layer tracking (per-hotkey + per-IP aggregate)
- Exponential backoff cooldowns
- DB-backed permanent blacklist (cached in memory)
- Metagraph registration validation
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Tuple

import bittensor as bt
from sqlalchemy import text

from sparket.shared.log_colors import LogColors
from .config import (
    COOLDOWN_FAILURES,
    CRITICAL_FAILURES,
    CooldownConfig,
    FailureType,
    RateLimitConfig,
    SecurityConfig,
)
from .iptables import get_iptables_manager


@dataclass
class FailureRecord:
    """Tracks failures for a single identifier (hotkey or IP)."""
    
    failures: List[Tuple[float, str]] = field(default_factory=list)  # (timestamp, failure_type)
    cooldown_count: int = 0  # Number of cooldowns triggered
    cooldown_until: float = 0.0  # Timestamp when cooldown ends
    last_failure: float = 0.0
    cooldown_violations: List[float] = field(default_factory=list)  # Timestamps of cooldown violations
    
    def add_failure(self, failure_type: str) -> None:
        now = time.time()
        self.failures.append((now, failure_type))
        self.last_failure = now
    
    def add_cooldown_violation(self) -> None:
        """Record a cooldown violation (submitting while in cooldown)."""
        self.cooldown_violations.append(time.time())
    
    def count_cooldown_violations(self, window_sec: float) -> int:
        """Count cooldown violations in the given window."""
        cutoff = time.time() - window_sec
        return sum(1 for ts in self.cooldown_violations if ts > cutoff)
    
    def count_recent(self, window_sec: float) -> int:
        cutoff = time.time() - window_sec
        return sum(1 for ts, _ in self.failures if ts > cutoff)
    
    def count_by_type(self, failure_types: Tuple[str, ...], window_sec: float) -> int:
        cutoff = time.time() - window_sec
        return sum(1 for ts, ft in self.failures if ts > cutoff and ft in failure_types)
    
    def cleanup(self, retention_sec: float) -> None:
        cutoff = time.time() - retention_sec
        self.failures = [(ts, ft) for ts, ft in self.failures if ts > cutoff]
        self.cooldown_violations = [ts for ts in self.cooldown_violations if ts > cutoff]


@dataclass
class IPRecord(FailureRecord):
    """Extended failure record for IP tracking."""
    
    hotkeys_seen: Set[str] = field(default_factory=set)
    failing_hotkeys: Set[str] = field(default_factory=set)
    
    def add_failure_with_hotkey(self, failure_type: str, hotkey: str) -> None:
        self.add_failure(failure_type)
        self.hotkeys_seen.add(hotkey)
        self.failing_hotkeys.add(hotkey)


class SecurityManager:
    """Centralized security manager for the validator.
    
    Thread-safe implementation with:
    - In-memory tracking for hot path performance
    - Async DB operations for blacklist persistence
    - Metagraph integration for registration checks
    """
    
    def __init__(
        self,
        database: Optional[Any] = None,
        config: Optional[SecurityConfig] = None,
    ):
        self.database = database
        self.config = config or SecurityConfig()
        self._lock = Lock()
        
        # Registered hotkeys from metagraph (updated on sync)
        self._registered_hotkeys: Set[str] = set()
        
        # Per-hotkey tracking
        self._hotkey_records: Dict[str, FailureRecord] = defaultdict(FailureRecord)
        
        # Per-IP tracking
        self._ip_records: Dict[str, IPRecord] = defaultdict(IPRecord)
        
        # Blacklist (loaded from DB, cached in memory)
        # Stores {identifier: expires_at_timestamp or None for permanent}
        self._blacklist_hotkeys: Dict[str, Optional[float]] = {}
        self._blacklist_ips: Dict[str, Optional[float]] = {}
        
        # Timestamps for periodic cleanup
        self._last_cleanup = time.time()
        
        # Initialize iptables manager for network-level blocking (if root)
        try:
            iptables = get_iptables_manager()
            if iptables.initialize():
                bt.logging.info({"security_manager": "iptables_enabled"})
            else:
                bt.logging.debug({"security_manager": "iptables_disabled", "reason": "not_root_or_unavailable"})
        except Exception as e:
            bt.logging.debug({"security_manager": "iptables_init_error", "error": str(e)})
    
    # -------------------------------------------------------------------------
    # Metagraph Integration
    # -------------------------------------------------------------------------
    
    def update_registered_hotkeys(self, metagraph: Any) -> None:
        """Update the set of registered hotkeys from metagraph.
        
        Called after metagraph sync to keep registration checks current.
        """
        try:
            hotkeys = set(metagraph.hotkeys) if hasattr(metagraph, "hotkeys") else set()
            # Filter out empty/null hotkeys
            hotkeys = {hk for hk in hotkeys if hk and hk != ""}
            
            with self._lock:
                old_count = len(self._registered_hotkeys)
                self._registered_hotkeys = hotkeys
                
            bt.logging.debug({
                "security_manager": "hotkeys_updated",
                "old_count": old_count,
                "new_count": len(hotkeys),
            })
        except Exception as e:
            bt.logging.warning({
                "security_manager": "hotkey_update_failed",
                "error": str(e),
            })
    
    def is_registered(self, hotkey: str) -> bool:
        """Check if a hotkey is registered in the metagraph."""
        with self._lock:
            return hotkey in self._registered_hotkeys
    
    # -------------------------------------------------------------------------
    # Blacklist Management
    # -------------------------------------------------------------------------
    
    async def load_blacklist_from_db(self) -> None:
        """Load blacklist from database into memory.
        
        Called on validator startup. Loads both permanent and time-limited bans.
        """
        if self.database is None:
            bt.logging.warning({"security_manager": "no_database_for_blacklist"})
            return
        
        try:
            now = datetime.now(timezone.utc)
            rows = await self.database.read(
                text("""
                    SELECT identifier, identifier_type, expires_at
                    FROM security_blacklist
                    WHERE expires_at IS NULL OR expires_at > :now
                """),
                params={"now": now},
                mappings=True,
            )
            
            with self._lock:
                self._blacklist_hotkeys.clear()
                self._blacklist_ips.clear()
                
                for row in rows:
                    # Convert expires_at to timestamp (None for permanent)
                    expires_ts = None
                    if row["expires_at"] is not None:
                        expires_ts = row["expires_at"].timestamp()
                    
                    if row["identifier_type"] == "hotkey":
                        self._blacklist_hotkeys[row["identifier"]] = expires_ts
                    elif row["identifier_type"] == "ip":
                        self._blacklist_ips[row["identifier"]] = expires_ts
            
            bt.logging.info({
                "security_manager": "blacklist_loaded",
                "hotkeys": len(self._blacklist_hotkeys),
                "ips": len(self._blacklist_ips),
            })
        except Exception as e:
            bt.logging.warning({
                "security_manager": "blacklist_load_failed",
                "error": str(e),
            })
    
    async def add_to_blacklist(
        self,
        identifier: str,
        identifier_type: str,
        reason: str,
        failure_type: Optional[str] = None,
        failure_count: int = 0,
        expires_at: Optional[datetime] = None,
    ) -> None:
        """Add an identifier to the blacklist.
        
        Updates both memory cache and database.
        
        Args:
            identifier: The hotkey or IP to blacklist
            identifier_type: Either "hotkey" or "ip"
            reason: Human-readable reason for the ban
            failure_type: The type of failure that triggered this
            failure_count: Number of failures
            expires_at: When the ban expires (None for permanent)
        """
        # Convert expires_at to timestamp for memory storage
        expires_ts = expires_at.timestamp() if expires_at else None
        
        # Update memory immediately
        with self._lock:
            if identifier_type == "hotkey":
                self._blacklist_hotkeys[identifier] = expires_ts
            elif identifier_type == "ip":
                self._blacklist_ips[identifier] = expires_ts
        
        # Log with expiry info
        bt.logging.warning({
            "security_blacklist_add": {
                "identifier": identifier[:16] + "..." if len(identifier) > 16 else identifier,
                "type": identifier_type,
                "reason": reason,
                "expires": expires_at.isoformat() if expires_at else "never",
            }
        })
        
        # Block at iptables level for IPs (if running as root)
        # This provides immediate network-level blocking
        if identifier_type == "ip":
            try:
                iptables = get_iptables_manager()
                # Calculate duration from expiry (default 24h for permanent bans too)
                duration = 86400  # default 24 hours
                if expires_at is not None:
                    duration = max(1, int((expires_at - datetime.now(timezone.utc)).total_seconds()))
                iptables.block_ip(identifier, duration_sec=duration)
            except Exception as e:
                # Don't fail if iptables blocking fails - app-level blocking is primary
                bt.logging.debug({"iptables_block_error": str(e)})
        
        # Persist to database asynchronously
        if self.database is None:
            return
        
        try:
            await self.database.write(
                text("""
                    INSERT INTO security_blacklist 
                        (identifier, identifier_type, reason, failure_type, failure_count, expires_at)
                    VALUES (:identifier, :identifier_type, :reason, :failure_type, :failure_count, :expires_at)
                    ON CONFLICT (identifier, identifier_type) 
                    DO UPDATE SET
                        reason = EXCLUDED.reason,
                        failure_count = security_blacklist.failure_count + EXCLUDED.failure_count,
                        expires_at = EXCLUDED.expires_at
                """),
                params={
                    "identifier": identifier,
                    "identifier_type": identifier_type,
                    "reason": reason,
                    "failure_type": failure_type,
                    "failure_count": failure_count,
                    "expires_at": expires_at,
                },
            )
        except Exception as e:
            bt.logging.warning({
                "security_manager": "blacklist_persist_failed",
                "error": str(e),
            })
    
    def is_blacklisted(self, hotkey: Optional[str], ip: Optional[str]) -> Tuple[bool, Optional[str]]:
        """Check if hotkey or IP is blacklisted.
        
        Handles both permanent and time-limited bans.
        Expired bans are cleaned up during this check.
        
        Returns: (is_blacklisted, reason)
        """
        now = time.time()
        
        with self._lock:
            # Check hotkey blacklist
            if hotkey and hotkey in self._blacklist_hotkeys:
                expires_ts = self._blacklist_hotkeys[hotkey]
                if expires_ts is None or expires_ts > now:
                    return True, "hotkey_blacklisted"
                else:
                    # Expired, remove from memory
                    del self._blacklist_hotkeys[hotkey]
            
            # Check IP blacklist
            if ip and ip in self._blacklist_ips:
                expires_ts = self._blacklist_ips[ip]
                if expires_ts is None or expires_ts > now:
                    return True, "ip_blacklisted"
                else:
                    # Expired, remove from memory
                    del self._blacklist_ips[ip]
        
        return False, None
    
    # -------------------------------------------------------------------------
    # Cooldown Management
    # -------------------------------------------------------------------------
    
    def is_in_cooldown(self, hotkey: Optional[str], ip: Optional[str]) -> Tuple[bool, Optional[str], float]:
        """Check if hotkey or IP is in cooldown.
        
        Returns: (in_cooldown, reason, remaining_seconds)
        """
        now = time.time()
        
        with self._lock:
            # Check hotkey cooldown
            if hotkey and hotkey in self._hotkey_records:
                record = self._hotkey_records[hotkey]
                if record.cooldown_until > now:
                    remaining = record.cooldown_until - now
                    return True, "hotkey_cooldown", remaining
            
            # Check IP cooldown
            if ip and ip in self._ip_records:
                record = self._ip_records[ip]
                if record.cooldown_until > now:
                    remaining = record.cooldown_until - now
                    return True, "ip_cooldown", remaining
        
        return False, None, 0.0
    
    def _trigger_cooldown(self, record: FailureRecord, config: CooldownConfig, is_ip: bool = False) -> float:
        """Trigger cooldown for a record with exponential backoff.
        
        Returns: cooldown duration in seconds
        """
        record.cooldown_count += 1
        
        if is_ip:
            initial = config.ip_initial_cooldown_sec
            max_cooldown = config.ip_max_cooldown_sec
        else:
            initial = config.hotkey_initial_cooldown_sec
            max_cooldown = config.hotkey_max_cooldown_sec
        
        # Exponential backoff
        duration = min(
            initial * (config.hotkey_backoff_multiplier ** (record.cooldown_count - 1)),
            max_cooldown
        )
        
        record.cooldown_until = time.time() + duration
        return duration
    
    # -------------------------------------------------------------------------
    # Fail2Ban - 24h ban for persistent cooldown violators
    # -------------------------------------------------------------------------
    
    async def record_cooldown_violation(
        self,
        hotkey: Optional[str],
        ip: Optional[str],
    ) -> Optional[str]:
        """Record a cooldown violation (submitting while in cooldown).
        
        If violations exceed threshold, trigger a 24-hour ban.
        
        Returns:
            Ban reason if a ban was triggered, None otherwise
        """
        if not self.config.enforce_cooldowns:
            return None
        
        cfg = self.config.cooldown
        ban_triggered = None
        ban_identifier = None
        ban_type = None
        violation_count = 0
        
        with self._lock:
            # Record hotkey violation - use defaultdict to auto-create record
            if hotkey:
                record = self._hotkey_records[hotkey]
                record.add_cooldown_violation()
                violation_count = record.count_cooldown_violations(cfg.fail2ban_violation_window_sec)
                
                if violation_count >= cfg.fail2ban_violation_threshold:
                    ban_triggered = f"fail2ban: {violation_count} cooldown violations in {cfg.fail2ban_violation_window_sec}s"
                    ban_identifier = hotkey
                    ban_type = "hotkey"
            
            # Record IP violation (separate tracking) - use defaultdict to auto-create record
            if ip:
                ip_record = self._ip_records[ip]
                ip_record.add_cooldown_violation()
                ip_violation_count = ip_record.count_cooldown_violations(cfg.fail2ban_violation_window_sec)
                
                # IP gets banned if it exceeds threshold (catches hotkey cycling)
                if ip_violation_count >= cfg.fail2ban_violation_threshold and not ban_triggered:
                    ban_triggered = f"fail2ban: IP {ip_violation_count} cooldown violations in {cfg.fail2ban_violation_window_sec}s"
                    ban_identifier = ip
                    ban_type = "ip"
                    violation_count = ip_violation_count
        
        # Trigger 24-hour ban if threshold exceeded
        if ban_triggered and ban_identifier and ban_type:
            # Calculate expiry time (24 hours from now)
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=cfg.fail2ban_duration_sec)
            
            bt.logging.warning(
                f"{LogColors.MINER_LABEL} fail2ban_triggered: "
                f"{ban_type}={ban_identifier[:16] + '...' if len(ban_identifier) > 16 else ban_identifier}, "
                f"violations={violation_count}, "
                f"ban_duration={cfg.fail2ban_duration_sec}s (24h)"
            )
            
            await self.add_to_blacklist(
                identifier=ban_identifier,
                identifier_type=ban_type,
                reason=ban_triggered,
                failure_type=FailureType.COOLDOWN_VIOLATION.value,
                failure_count=violation_count,
                expires_at=expires_at,
            )
            
            return ban_triggered
        
        return None
    
    # -------------------------------------------------------------------------
    # Failure Recording
    # -------------------------------------------------------------------------
    
    async def record_failure(
        self,
        hotkey: Optional[str],
        ip: Optional[str],
        failure_type: str,
    ) -> None:
        """Record a failure and potentially trigger cooldown or blacklist.
        
        This is the main entry point for failure tracking.
        """
        if not self.config.enforce_cooldowns:
            return
        
        cfg = self.config.cooldown
        
        with self._lock:
            # Record per-hotkey failure
            if hotkey:
                record = self._hotkey_records[hotkey]
                record.add_failure(failure_type)
                
                # Check for cooldown trigger
                recent_count = record.count_by_type(COOLDOWN_FAILURES, 60.0)
                if recent_count >= cfg.hotkey_failure_threshold:
                    duration = self._trigger_cooldown(record, cfg, is_ip=False)
                    bt.logging.warning(
                        f"{LogColors.MINER_LABEL} cooldown_triggered: hotkey={hotkey[:16]}..., "
                        f"failures={recent_count}, duration={duration:.0f}s"
                    )
                
                # Check for permanent blacklist (critical failures)
                critical_count = record.count_by_type(CRITICAL_FAILURES, cfg.failure_retention_sec)
                total_cooldowns = record.cooldown_count
            
            # Record per-IP failure
            if ip:
                ip_record = self._ip_records[ip]
                ip_record.add_failure_with_hotkey(failure_type, hotkey or "unknown")
                
                # Check for IP cooldown trigger
                recent_ip_count = ip_record.count_recent(cfg.ip_failure_window_sec)
                distinct_failing = len(ip_record.failing_hotkeys)
                
                should_cooldown_ip = (
                    recent_ip_count >= cfg.ip_failure_threshold
                    or distinct_failing >= cfg.ip_distinct_hotkey_threshold
                )
                
                if should_cooldown_ip and ip_record.cooldown_until <= time.time():
                    duration = self._trigger_cooldown(ip_record, cfg, is_ip=True)
                    bt.logging.warning(
                        f"{LogColors.MINER_LABEL} ip_cooldown_triggered: ip={ip}, "
                        f"failures={recent_ip_count}, distinct_hotkeys={distinct_failing}, "
                        f"duration={duration:.0f}s"
                    )
        
        # Check for permanent blacklist conditions (outside lock for async DB)
        # Only permanently blacklist for CRITICAL failures (spoofing, invalid signatures, etc.)
        # Regular cooldowns (token issues, rate limits) should NOT trigger permanent bans
        if hotkey:
            should_blacklist = critical_count >= cfg.permanent_critical_threshold
            
            if should_blacklist and hotkey not in self._blacklist_hotkeys:
                reason = f"Exceeded critical failure threshold: {critical_count} critical failures"
                await self.add_to_blacklist(
                    identifier=hotkey,
                    identifier_type="hotkey",
                    reason=reason,
                    failure_type=failure_type,
                    failure_count=len(record.failures),
                )
    
    # -------------------------------------------------------------------------
    # Main Check Method
    # -------------------------------------------------------------------------
    
    def check_request(
        self,
        hotkey: Optional[str],
        ip: Optional[str],
    ) -> Tuple[bool, Optional[str]]:
        """Fast check for early request rejection.
        
        Returns: (allowed, rejection_reason)
        
        Checks in order:
        1. Permanent blacklist
        2. Registration (if enforced)
        3. Cooldown status
        
        This is the hot path - all checks are in-memory only.
        """
        # Periodic cleanup
        self._maybe_cleanup()
        
        # 1. Check permanent blacklist
        if self.config.enforce_blacklist:
            is_blocked, reason = self.is_blacklisted(hotkey, ip)
            if is_blocked:
                return False, reason
        
        # 2. Check registration
        if self.config.enforce_registration and hotkey:
            if not self.is_registered(hotkey):
                return False, "not_registered"
        
        # 3. Check cooldowns
        if self.config.enforce_cooldowns:
            in_cooldown, reason, remaining = self.is_in_cooldown(hotkey, ip)
            if in_cooldown:
                return False, f"{reason}:{remaining:.0f}s"
        
        return True, None
    
    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------
    
    def _maybe_cleanup(self) -> None:
        """Periodic cleanup of stale records and expired bans."""
        now = time.time()
        if now - self._last_cleanup < self.config.cooldown.cleanup_interval_sec:
            return
        
        with self._lock:
            self._last_cleanup = now
            retention = self.config.cooldown.failure_retention_sec
            
            # Clean hotkey records
            stale_hotkeys = []
            for hotkey, record in self._hotkey_records.items():
                record.cleanup(retention)
                if not record.failures and not record.cooldown_violations and record.cooldown_until <= now:
                    stale_hotkeys.append(hotkey)
            
            for hotkey in stale_hotkeys:
                del self._hotkey_records[hotkey]
            
            # Clean IP records
            stale_ips = []
            for ip, record in self._ip_records.items():
                record.cleanup(retention)
                if not record.failures and not record.cooldown_violations and record.cooldown_until <= now:
                    stale_ips.append(ip)
            
            for ip in stale_ips:
                del self._ip_records[ip]
            
            # Clean expired blacklist entries
            expired_bl_hotkeys = [
                hk for hk, exp in self._blacklist_hotkeys.items()
                if exp is not None and exp <= now
            ]
            for hk in expired_bl_hotkeys:
                del self._blacklist_hotkeys[hk]
            
            expired_bl_ips = [
                ip for ip, exp in self._blacklist_ips.items()
                if exp is not None and exp <= now
            ]
            for ip in expired_bl_ips:
                del self._blacklist_ips[ip]
            
            if stale_hotkeys or stale_ips or expired_bl_hotkeys or expired_bl_ips:
                bt.logging.debug({
                    "security_manager": "cleanup",
                    "stale_hotkeys": len(stale_hotkeys),
                    "stale_ips": len(stale_ips),
                    "expired_bans_hotkeys": len(expired_bl_hotkeys),
                    "expired_bans_ips": len(expired_bl_ips),
                })
    
    # -------------------------------------------------------------------------
    # Stats
    # -------------------------------------------------------------------------
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current security manager statistics."""
        with self._lock:
            return {
                "registered_hotkeys": len(self._registered_hotkeys),
                "blacklisted_hotkeys": len(self._blacklist_hotkeys),
                "blacklisted_ips": len(self._blacklist_ips),
                "tracked_hotkeys": len(self._hotkey_records),
                "tracked_ips": len(self._ip_records),
                "hotkeys_in_cooldown": sum(
                    1 for r in self._hotkey_records.values()
                    if r.cooldown_until > time.time()
                ),
                "ips_in_cooldown": sum(
                    1 for r in self._ip_records.values()
                    if r.cooldown_until > time.time()
                ),
            }


__all__ = ["SecurityManager"]
