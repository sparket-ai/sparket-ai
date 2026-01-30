"""Tests for the SecurityManager rate limiting and blacklisting system."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sparket.validator.security.config import (
    CooldownConfig,
    FailureType,
    SecurityConfig,
)
from sparket.validator.security.manager import (
    FailureRecord,
    IPRecord,
    SecurityManager,
)


class TestFailureRecord:
    """Tests for the FailureRecord dataclass."""

    def test_add_failure(self):
        """Test adding a failure to the record."""
        record = FailureRecord()
        record.add_failure("token_invalid")
        
        assert len(record.failures) == 1
        assert record.failures[0][1] == "token_invalid"
        assert record.last_failure > 0

    def test_count_recent(self):
        """Test counting recent failures within a window."""
        record = FailureRecord()
        
        # Add failures
        for _ in range(5):
            record.add_failure("token_invalid")
        
        # All should be within last 60 seconds
        assert record.count_recent(60.0) == 5
        
        # None should be older than 0 seconds window
        assert record.count_recent(0.0) == 0

    def test_count_by_type(self):
        """Test counting failures by type within a window."""
        record = FailureRecord()
        
        record.add_failure("token_invalid")
        record.add_failure("token_invalid")
        record.add_failure("rate_limited")
        record.add_failure("other")
        
        # Count only token_invalid
        count = record.count_by_type(("token_invalid",), 60.0)
        assert count == 2
        
        # Count multiple types
        count = record.count_by_type(("token_invalid", "rate_limited"), 60.0)
        assert count == 3

    def test_cleanup_removes_old_failures(self):
        """Test that cleanup removes failures older than retention period."""
        record = FailureRecord()
        
        # Add a failure with timestamp in the past
        old_timestamp = time.time() - 100
        record.failures.append((old_timestamp, "token_invalid"))
        record.failures.append((time.time(), "token_invalid"))
        
        # Cleanup with 50 second retention should remove the old one
        record.cleanup(50.0)
        
        assert len(record.failures) == 1


class TestIPRecord:
    """Tests for the IPRecord dataclass."""

    def test_add_failure_with_hotkey(self):
        """Test adding a failure with associated hotkey."""
        record = IPRecord()
        
        record.add_failure_with_hotkey("token_invalid", "hotkey1")
        record.add_failure_with_hotkey("token_invalid", "hotkey2")
        record.add_failure_with_hotkey("rate_limited", "hotkey1")
        
        assert len(record.failures) == 3
        assert "hotkey1" in record.hotkeys_seen
        assert "hotkey2" in record.hotkeys_seen
        assert len(record.failing_hotkeys) == 2


class TestSecurityManager:
    """Tests for the SecurityManager class."""

    def test_init_defaults(self):
        """Test SecurityManager initialization with defaults."""
        manager = SecurityManager()
        
        assert manager.config is not None
        assert isinstance(manager.config, SecurityConfig)
        assert len(manager._registered_hotkeys) == 0
        assert len(manager._blacklist_hotkeys) == 0
        assert len(manager._blacklist_ips) == 0

    def test_update_registered_hotkeys(self):
        """Test updating registered hotkeys from metagraph."""
        manager = SecurityManager()
        
        # Mock metagraph with hotkeys
        mock_metagraph = MagicMock()
        mock_metagraph.hotkeys = ["hotkey1", "hotkey2", "hotkey3", ""]
        
        manager.update_registered_hotkeys(mock_metagraph)
        
        # Empty hotkeys should be filtered out
        assert len(manager._registered_hotkeys) == 3
        assert "hotkey1" in manager._registered_hotkeys
        assert "" not in manager._registered_hotkeys

    def test_is_registered(self):
        """Test checking if a hotkey is registered."""
        manager = SecurityManager()
        manager._registered_hotkeys = {"hotkey1", "hotkey2"}
        
        assert manager.is_registered("hotkey1") is True
        assert manager.is_registered("hotkey3") is False

    def test_is_blacklisted_hotkey(self):
        """Test checking if a hotkey is blacklisted."""
        manager = SecurityManager()
        manager._blacklist_hotkeys = {"bad_hotkey"}
        
        is_blocked, reason = manager.is_blacklisted("bad_hotkey", None)
        assert is_blocked is True
        assert reason == "hotkey_blacklisted"
        
        is_blocked, reason = manager.is_blacklisted("good_hotkey", None)
        assert is_blocked is False
        assert reason is None

    def test_is_blacklisted_ip(self):
        """Test checking if an IP is blacklisted."""
        manager = SecurityManager()
        manager._blacklist_ips = {"192.168.1.100"}
        
        is_blocked, reason = manager.is_blacklisted(None, "192.168.1.100")
        assert is_blocked is True
        assert reason == "ip_blacklisted"
        
        is_blocked, reason = manager.is_blacklisted(None, "192.168.1.200")
        assert is_blocked is False

    def test_is_in_cooldown_no_cooldown(self):
        """Test cooldown check when not in cooldown."""
        manager = SecurityManager()
        
        in_cooldown, reason, remaining = manager.is_in_cooldown("hotkey1", "192.168.1.1")
        
        assert in_cooldown is False
        assert reason is None
        assert remaining == 0.0

    def test_is_in_cooldown_active(self):
        """Test cooldown check when actively in cooldown."""
        manager = SecurityManager()
        
        # Manually set cooldown
        record = manager._hotkey_records["hotkey1"]
        record.cooldown_until = time.time() + 60  # 60 seconds from now
        
        in_cooldown, reason, remaining = manager.is_in_cooldown("hotkey1", None)
        
        assert in_cooldown is True
        assert reason == "hotkey_cooldown"
        assert 59 <= remaining <= 60

    def test_check_request_allowed(self):
        """Test that valid requests are allowed."""
        manager = SecurityManager()
        manager._registered_hotkeys = {"hotkey1"}
        
        allowed, reason = manager.check_request("hotkey1", "192.168.1.1")
        
        assert allowed is True
        assert reason is None

    def test_check_request_blacklisted(self):
        """Test that blacklisted hotkeys are rejected."""
        manager = SecurityManager()
        manager._blacklist_hotkeys = {"bad_hotkey"}
        
        allowed, reason = manager.check_request("bad_hotkey", "192.168.1.1")
        
        assert allowed is False
        assert reason == "hotkey_blacklisted"

    def test_check_request_not_registered(self):
        """Test that unregistered hotkeys are rejected."""
        manager = SecurityManager()
        manager._registered_hotkeys = {"hotkey1"}
        
        allowed, reason = manager.check_request("unknown_hotkey", "192.168.1.1")
        
        assert allowed is False
        assert reason == "not_registered"

    def test_check_request_in_cooldown(self):
        """Test that hotkeys in cooldown are rejected."""
        manager = SecurityManager()
        manager._registered_hotkeys = {"hotkey1"}
        
        record = manager._hotkey_records["hotkey1"]
        record.cooldown_until = time.time() + 30
        
        allowed, reason = manager.check_request("hotkey1", "192.168.1.1")
        
        assert allowed is False
        assert "hotkey_cooldown" in reason

    def test_get_stats(self):
        """Test getting manager statistics."""
        manager = SecurityManager()
        manager._registered_hotkeys = {"h1", "h2", "h3"}
        manager._blacklist_hotkeys = {"bad1"}
        manager._blacklist_ips = {"ip1", "ip2"}
        
        stats = manager.get_stats()
        
        assert stats["registered_hotkeys"] == 3
        assert stats["blacklisted_hotkeys"] == 1
        assert stats["blacklisted_ips"] == 2


class TestSecurityManagerAsync:
    """Async tests for the SecurityManager class."""

    @pytest.mark.asyncio
    async def test_record_failure_triggers_cooldown(self):
        """Test that repeated failures trigger a cooldown."""
        config = SecurityConfig(
            cooldown=CooldownConfig(
                hotkey_failure_threshold=3,  # Low threshold for testing
                hotkey_initial_cooldown_sec=10,
            )
        )
        manager = SecurityManager(config=config)
        
        # Record failures
        for _ in range(5):
            await manager.record_failure("hotkey1", "192.168.1.1", "token_invalid")
        
        # Should now be in cooldown
        in_cooldown, reason, remaining = manager.is_in_cooldown("hotkey1", None)
        assert in_cooldown is True

    @pytest.mark.asyncio
    async def test_record_failure_ip_cooldown(self):
        """Test that IP-level cooldown triggers on multiple failing hotkeys."""
        config = SecurityConfig(
            cooldown=CooldownConfig(
                ip_failure_threshold=100,  # High threshold
                ip_distinct_hotkey_threshold=3,  # Trigger on 3 distinct failing hotkeys
            )
        )
        manager = SecurityManager(config=config)
        
        # Record failures from different hotkeys on same IP
        await manager.record_failure("hotkey1", "192.168.1.1", "token_invalid")
        await manager.record_failure("hotkey2", "192.168.1.1", "token_invalid")
        await manager.record_failure("hotkey3", "192.168.1.1", "token_invalid")
        
        # IP should now be in cooldown
        in_cooldown, reason, _ = manager.is_in_cooldown(None, "192.168.1.1")
        assert in_cooldown is True
        assert reason == "ip_cooldown"

    @pytest.mark.asyncio
    async def test_load_blacklist_from_db(self):
        """Test loading blacklist from database."""
        mock_db = AsyncMock()
        mock_db.read = AsyncMock(return_value=[
            {"identifier": "bad_hotkey", "identifier_type": "hotkey"},
            {"identifier": "192.168.1.100", "identifier_type": "ip"},
        ])
        
        manager = SecurityManager(database=mock_db)
        await manager.load_blacklist_from_db()
        
        assert "bad_hotkey" in manager._blacklist_hotkeys
        assert "192.168.1.100" in manager._blacklist_ips

    @pytest.mark.asyncio
    async def test_add_to_blacklist(self):
        """Test adding to blacklist updates memory and persists to DB."""
        mock_db = AsyncMock()
        mock_db.write = AsyncMock()
        
        manager = SecurityManager(database=mock_db)
        
        await manager.add_to_blacklist(
            identifier="bad_actor",
            identifier_type="hotkey",
            reason="Too many failures",
        )
        
        # Check memory updated
        assert "bad_actor" in manager._blacklist_hotkeys
        
        # Check DB write called
        mock_db.write.assert_called_once()


class TestSecurityManagerEnforcement:
    """Tests for enforcement flag behavior."""

    def test_enforcement_disabled_allows_all(self):
        """Test that disabling enforcement allows all requests."""
        config = SecurityConfig(
            enforce_registration=False,
            enforce_cooldowns=False,
            enforce_blacklist=False,
        )
        manager = SecurityManager(config=config)
        manager._blacklist_hotkeys = {"blocked_hotkey"}
        
        # Even blacklisted should be allowed
        allowed, reason = manager.check_request("blocked_hotkey", "192.168.1.1")
        assert allowed is True

    def test_registration_enforcement(self):
        """Test registration enforcement can be disabled."""
        config = SecurityConfig(enforce_registration=False)
        manager = SecurityManager(config=config)
        
        # Unknown hotkey should be allowed when enforcement disabled
        allowed, reason = manager.check_request("unknown_hotkey", "192.168.1.1")
        assert allowed is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
