"""Unit tests for TTL cache."""

import asyncio
import time

import pytest

from sparket.miner.base.utils.cache import TTLCache


class TestTTLCache:
    """Tests for the TTL cache."""
    
    def test_set_and_get(self):
        """Basic set and get operations."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=60)
        
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"
    
    def test_missing_key_returns_none(self):
        """Missing key returns None."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=60)
        
        assert cache.get("nonexistent") is None
    
    def test_expiration(self):
        """Values expire after TTL."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=0.1)  # 100ms TTL
        
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"
        
        time.sleep(0.15)  # Wait for expiration
        assert cache.get("key1") is None
    
    def test_custom_ttl_per_entry(self):
        """Custom TTL can be set per entry."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=60)
        
        cache.set("short", "value", ttl=0.1)
        cache.set("long", "value", ttl=60)
        
        time.sleep(0.15)
        assert cache.get("short") is None
        assert cache.get("long") == "value"
    
    def test_invalidate(self):
        """Invalidate removes a key."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=60)
        
        cache.set("key1", "value1")
        assert cache.invalidate("key1") is True
        assert cache.get("key1") is None
        
        # Invalidating missing key returns False
        assert cache.invalidate("nonexistent") is False
    
    def test_clear(self):
        """Clear removes all entries."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=60)
        
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        
        count = cache.clear()
        assert count == 2
        assert cache.get("key1") is None
        assert cache.get("key2") is None
    
    def test_cleanup_expired(self):
        """Cleanup removes expired entries."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=0.1)
        
        cache.set("short1", "value")
        cache.set("short2", "value")
        
        time.sleep(0.15)
        
        # Add a non-expired entry
        cache.set("long", "value", ttl=60)
        
        removed = cache.cleanup_expired()
        assert removed == 2  # Two expired entries removed
        assert len(cache) == 1  # One entry remains
    
    def test_contains(self):
        """__contains__ checks for non-expired keys."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=60)
        
        cache.set("key1", "value1")
        assert "key1" in cache
        assert "nonexistent" not in cache
    
    def test_len(self):
        """__len__ returns entry count."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=60)
        
        assert len(cache) == 0
        cache.set("key1", "value1")
        assert len(cache) == 1
        cache.set("key2", "value2")
        assert len(cache) == 2

    def test_maxsize_evicts_lru(self):
        """Cache evicts least recently used items when full."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=60, maxsize=2)

        cache.set("k1", "v1")
        cache.set("k2", "v2")
        assert cache.get("k1") == "v1"  # touch k1 so k2 becomes LRU
        cache.set("k3", "v3")

        assert cache.get("k1") == "v1"
        assert cache.get("k2") is None
        assert cache.get("k3") == "v3"
    
    @pytest.mark.asyncio
    async def test_get_or_set_cached(self):
        """get_or_set returns cached value."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=60)
        
        fetch_count = 0
        
        async def fetch():
            nonlocal fetch_count
            fetch_count += 1
            return "fetched_value"
        
        # First call - should fetch
        result = await cache.get_or_set("key1", fetch)
        assert result == "fetched_value"
        assert fetch_count == 1
        
        # Second call - should use cache
        result = await cache.get_or_set("key1", fetch)
        assert result == "fetched_value"
        assert fetch_count == 1  # No additional fetch
    
    @pytest.mark.asyncio
    async def test_get_or_set_expired(self):
        """get_or_set fetches again after expiration."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=0.1)
        
        fetch_count = 0
        
        async def fetch():
            nonlocal fetch_count
            fetch_count += 1
            return f"value_{fetch_count}"
        
        result1 = await cache.get_or_set("key1", fetch)
        assert result1 == "value_1"
        
        await asyncio.sleep(0.15)  # Wait for expiration
        
        result2 = await cache.get_or_set("key1", fetch)
        assert result2 == "value_2"
        assert fetch_count == 2
    
    @pytest.mark.asyncio
    async def test_get_or_set_handles_fetch_error(self):
        """get_or_set returns None on fetch error."""
        cache: TTLCache[str] = TTLCache(ttl_seconds=60)
        
        async def failing_fetch():
            raise ValueError("Fetch failed")
        
        result = await cache.get_or_set("key1", failing_fetch)
        assert result is None








