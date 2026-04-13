"""Tests to verify the E2E infrastructure is working.

Run these first to ensure the localnet is properly configured
before running full scenario tests.
"""

from __future__ import annotations

import pytest

from .config import DEFAULT_CONFIG
from .harness import LocalnetHarness, ValidatorClient
from .miner_pool import MinerPool


class TestConfiguration:
    """Tests for configuration."""
    
    def test_default_config_has_miners(self):
        """Default config should have 2 miners."""
        assert len(DEFAULT_CONFIG.miners) == 2
    
    def test_miner_uids_assigned(self):
        """All miners should have UIDs."""
        for miner in DEFAULT_CONFIG.miners:
            assert miner.uid is not None
    
    def test_database_url_format(self):
        """Database URL should be properly formatted."""
        url = DEFAULT_CONFIG.database_url
        assert url.startswith("postgresql+asyncpg://")
        assert "sparket" in url


class TestValidatorConnection:
    """Tests for validator control API connection."""
    
    @pytest.mark.asyncio
    async def test_validator_health(self, harness: LocalnetHarness):
        """Validator control API should be healthy."""
        is_healthy = await harness.validator.health_check()
        # This may fail if validator isn't running - that's expected
        # The test documents what we're checking
        if not is_healthy:
            pytest.skip("Validator not running - start with pm2")
    
    @pytest.mark.asyncio
    async def test_can_get_submissions(self, harness: LocalnetHarness):
        """Should be able to query submissions."""
        is_healthy = await harness.validator.health_check()
        if not is_healthy:
            pytest.skip("Validator not running")
        
        result = await harness.validator.get_submissions()
        assert "submissions" in result or "status" in result


class TestMinerPool:
    """Tests for miner pool."""
    
    def test_pool_has_correct_size(self, harness: LocalnetHarness):
        """Pool should have 2 miners."""
        assert len(harness.miners) == 2

    def test_miners_have_wallet_names(self, harness: LocalnetHarness):
        """Each miner should have a wallet name."""
        expected_names = {"e2e-miner-2", "e2e-miner-3"}
        actual_names = {m.wallet_name for m in harness.miners}
        assert actual_names == expected_names

    @pytest.mark.asyncio
    async def test_miner_health_check(self, harness: LocalnetHarness):
        """Should be able to check miner health."""
        health = await harness.miners.health_check_all()
        assert len(health) == 2


class TestDatabaseConnection:
    """Tests for database connection."""
    
    @pytest.mark.asyncio
    async def test_database_accessible(self, harness: LocalnetHarness):
        """Should be able to connect to database."""
        from sqlalchemy import text
        
        try:
            async with harness.db.connect() as conn:
                result = await conn.execute(text("SELECT 1"))
                row = result.fetchone()
                assert row[0] == 1
        except Exception as e:
            pytest.skip(f"Database not accessible: {e}")


class TestHarnessLifecycle:
    """Tests for harness lifecycle."""
    
    @pytest.mark.asyncio
    async def test_harness_context_manager(self):
        """Harness should work as context manager."""
        async with LocalnetHarness() as harness:
            assert harness._initialized
        
        # After exit, should be torn down
        assert not harness._initialized
    
    @pytest.mark.asyncio
    async def test_health_check_all(self, harness: LocalnetHarness):
        """Should report health of all components."""
        health = await harness.health_check()
        
        # Should have entries for validator, miners, database
        assert "validator" in health
        assert "database" in health
        assert len(health) >= 4  # validator + 2 miners + database
