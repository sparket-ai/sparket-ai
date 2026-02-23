"""Simple TTL cache for API responses."""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

T = TypeVar("T")


@dataclass
class CacheEntry(Generic[T]):
    """A cached value with expiration time."""
    
    value: T
    expires_at: float
    
    def is_expired(self) -> bool:
        """Check if this entry has expired."""
        return time.time() > self.expires_at


class TTLCache(Generic[T]):
    """Simple TTL (time-to-live) cache.
    
    Thread-safe for async usage. Values expire after ttl_seconds.
    
    Example:
        cache = TTLCache[dict](ttl_seconds=3600)  # 1 hour TTL
        
        # Store a value
        cache.set("key", {"data": "value"})
        
        # Retrieve (returns None if expired or missing)
        value = cache.get("key")
        
        # Async get-or-fetch pattern
        async def fetch_data():
            return await api.get_data()
        
        value = await cache.get_or_set("key", fetch_data)
    """
    
    def __init__(self, ttl_seconds: float = 3600, maxsize: int = 1024) -> None:
        """Initialize cache.
        
        Args:
            ttl_seconds: Time-to-live for cached values (default: 1 hour)
            maxsize: Maximum number of entries to retain (LRU eviction).
        """
        self.ttl_seconds = ttl_seconds
        self.maxsize = max(1, int(maxsize))
        self._cache: OrderedDict[str, CacheEntry[T]] = OrderedDict()
        self._lock = asyncio.Lock()
    
    def get(self, key: str) -> Optional[T]:
        """Get a value from cache.
        
        Args:
            key: Cache key
        
        Returns:
            Cached value if exists and not expired, None otherwise.
        """
        entry = self._cache.get(key)
        if entry is None:
            return None
        if entry.is_expired():
            # Clean up expired entry
            del self._cache[key]
            return None
        # LRU touch
        self._cache.move_to_end(key)
        return entry.value
    
    def set(self, key: str, value: T, ttl: Optional[float] = None) -> None:
        """Store a value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Optional override for TTL (seconds)
        """
        ttl_seconds = ttl if ttl is not None else self.ttl_seconds
        expires_at = time.time() + ttl_seconds
        self._cache[key] = CacheEntry(value=value, expires_at=expires_at)
        self._cache.move_to_end(key)
        while len(self._cache) > self.maxsize:
            self._cache.popitem(last=False)
    
    async def get_or_set(
        self, 
        key: str, 
        fetch_fn, 
        ttl: Optional[float] = None
    ) -> Optional[T]:
        """Get from cache or fetch and cache.
        
        Thread-safe: only one fetch will occur even with concurrent calls.
        
        Args:
            key: Cache key
            fetch_fn: Async function to fetch value if not cached
            ttl: Optional override for TTL
        
        Returns:
            Cached or freshly fetched value, or None if fetch fails.
        """
        # Fast path: check cache without lock
        value = self.get(key)
        if value is not None:
            return value
        
        # Slow path: acquire lock and fetch
        async with self._lock:
            # Double-check after acquiring lock
            value = self.get(key)
            if value is not None:
                return value
            
            # Fetch new value
            try:
                value = await fetch_fn()
                if value is not None:
                    self.set(key, value, ttl)
                return value
            except Exception:
                return None
    
    def invalidate(self, key: str) -> bool:
        """Remove a key from cache.
        
        Args:
            key: Cache key to remove
        
        Returns:
            True if key was present, False otherwise.
        """
        if key in self._cache:
            del self._cache[key]
            return True
        return False
    
    def clear(self) -> int:
        """Clear all cached values.
        
        Returns:
            Number of entries cleared.
        """
        count = len(self._cache)
        self._cache.clear()
        return count
    
    def cleanup_expired(self) -> int:
        """Remove all expired entries.
        
        Returns:
            Number of entries removed.
        """
        now = time.time()
        expired_keys = [
            key for key, entry in self._cache.items()
            if entry.expires_at < now
        ]
        for key in expired_keys:
            del self._cache[key]
        return len(expired_keys)
    
    def __len__(self) -> int:
        """Number of entries in cache (including expired)."""
        return len(self._cache)
    
    def __contains__(self, key: str) -> bool:
        """Check if key is in cache and not expired."""
        return self.get(key) is not None








