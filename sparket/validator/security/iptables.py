"""Direct iptables management for network-level IP blocking.

Provides immediate IP blocking at the firewall level when running as root.
Falls back gracefully to application-level blocking when not root.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

import bittensor as bt


def is_root() -> bool:
    """Check if running as root."""
    return os.geteuid() == 0


def iptables_available() -> bool:
    """Check if iptables command is available."""
    try:
        result = subprocess.run(
            ["iptables", "-V"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError):
        return False


@dataclass
class IPTablesManager:
    """Manages iptables rules for IP blocking.
    
    Thread-safe implementation that:
    - Adds DROP rules for banned IPs
    - Tracks rules with expiry times
    - Periodically cleans up expired rules
    - Uses a dedicated chain to avoid conflicts
    """
    
    chain_name: str = "SPARKET_BLOCK"
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _blocked_ips: Dict[str, float] = field(default_factory=dict)  # ip -> expiry_timestamp
    _initialized: bool = False
    _cleanup_thread: Optional[threading.Thread] = None
    _stop_cleanup: bool = False
    
    def __post_init__(self):
        """Initialize after dataclass creation."""
        # These need to be created fresh, not shared
        object.__setattr__(self, '_lock', threading.Lock())
        object.__setattr__(self, '_blocked_ips', {})
    
    def initialize(self) -> bool:
        """Initialize the iptables chain.
        
        Creates a dedicated chain for Sparket blocks and links it to INPUT.
        Safe to call multiple times.
        
        Returns:
            True if initialized successfully, False otherwise.
        """
        if not is_root():
            bt.logging.debug({"iptables": "not_root", "status": "skipped"})
            return False
        
        if not iptables_available():
            bt.logging.warning({"iptables": "not_available"})
            return False
        
        with self._lock:
            if self._initialized:
                return True
            
            try:
                # Create chain if it doesn't exist
                subprocess.run(
                    ["iptables", "-N", self.chain_name],
                    capture_output=True,
                    timeout=10,
                )
                
                # Check if chain is already linked to INPUT
                result = subprocess.run(
                    ["iptables", "-C", "INPUT", "-j", self.chain_name],
                    capture_output=True,
                    timeout=10,
                )
                
                if result.returncode != 0:
                    # Link chain to INPUT (at the top for early rejection)
                    subprocess.run(
                        ["iptables", "-I", "INPUT", "1", "-j", self.chain_name],
                        capture_output=True,
                        check=True,
                        timeout=10,
                    )
                
                self._initialized = True
                
                # Start cleanup thread
                self._start_cleanup_thread()
                
                bt.logging.info({
                    "iptables": "initialized",
                    "chain": self.chain_name,
                })
                return True
                
            except subprocess.CalledProcessError as e:
                bt.logging.warning({
                    "iptables": "init_failed",
                    "error": str(e),
                    "stderr": e.stderr.decode() if e.stderr else None,
                })
                return False
            except Exception as e:
                bt.logging.warning({
                    "iptables": "init_error",
                    "error": str(e),
                })
                return False
    
    def block_ip(self, ip: str, duration_sec: int = 86400) -> bool:
        """Block an IP address at the iptables level.
        
        Args:
            ip: IP address to block
            duration_sec: How long to block (default: 24 hours)
        
        Returns:
            True if blocked successfully, False otherwise.
        """
        if not self._initialized:
            if not self.initialize():
                return False
        
        with self._lock:
            # Check if already blocked
            if ip in self._blocked_ips:
                # Update expiry time
                self._blocked_ips[ip] = time.time() + duration_sec
                bt.logging.debug({"iptables_block": ip, "status": "extended"})
                return True
            
            try:
                # Add DROP rule
                result = subprocess.run(
                    ["iptables", "-A", self.chain_name, "-s", ip, "-j", "DROP"],
                    capture_output=True,
                    timeout=10,
                )
                
                if result.returncode == 0:
                    self._blocked_ips[ip] = time.time() + duration_sec
                    bt.logging.info({
                        "iptables_block": ip,
                        "duration_sec": duration_sec,
                        "status": "success",
                    })
                    return True
                else:
                    bt.logging.warning({
                        "iptables_block": ip,
                        "status": "failed",
                        "stderr": result.stderr.decode() if result.stderr else None,
                    })
                    return False
                    
            except Exception as e:
                bt.logging.warning({
                    "iptables_block": ip,
                    "error": str(e),
                })
                return False
    
    def unblock_ip(self, ip: str) -> bool:
        """Remove block for an IP address.
        
        Args:
            ip: IP address to unblock
        
        Returns:
            True if unblocked successfully, False otherwise.
        """
        if not self._initialized:
            return False
        
        with self._lock:
            if ip not in self._blocked_ips:
                return True  # Already unblocked
            
            try:
                # Remove DROP rule
                result = subprocess.run(
                    ["iptables", "-D", self.chain_name, "-s", ip, "-j", "DROP"],
                    capture_output=True,
                    timeout=10,
                )
                
                if result.returncode == 0:
                    del self._blocked_ips[ip]
                    bt.logging.info({
                        "iptables_unblock": ip,
                        "status": "success",
                    })
                    return True
                else:
                    # Rule might not exist, clean up tracking anyway
                    self._blocked_ips.pop(ip, None)
                    return True
                    
            except Exception as e:
                bt.logging.warning({
                    "iptables_unblock": ip,
                    "error": str(e),
                })
                return False
    
    def cleanup_expired(self) -> int:
        """Remove expired blocks.
        
        Returns:
            Number of blocks removed.
        """
        if not self._initialized:
            return 0
        
        now = time.time()
        expired = []
        
        with self._lock:
            for ip, expiry in list(self._blocked_ips.items()):
                if expiry <= now:
                    expired.append(ip)
        
        removed = 0
        for ip in expired:
            if self.unblock_ip(ip):
                removed += 1
        
        if removed > 0:
            bt.logging.info({"iptables_cleanup": {"removed": removed}})
        
        return removed
    
    def _start_cleanup_thread(self) -> None:
        """Start background thread for periodic cleanup."""
        if self._cleanup_thread is not None and self._cleanup_thread.is_alive():
            return
        
        self._stop_cleanup = False
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
            name="iptables-cleanup",
        )
        self._cleanup_thread.start()
    
    def _cleanup_loop(self) -> None:
        """Background loop that cleans up expired rules every 5 minutes."""
        while not self._stop_cleanup:
            try:
                time.sleep(300)  # 5 minutes
                if not self._stop_cleanup:
                    self.cleanup_expired()
            except Exception as e:
                bt.logging.warning({"iptables_cleanup_error": str(e)})
    
    def shutdown(self) -> None:
        """Shutdown the manager and clean up all rules."""
        self._stop_cleanup = True
        
        if self._cleanup_thread is not None:
            self._cleanup_thread.join(timeout=5)
        
        # Remove all blocked IPs
        with self._lock:
            for ip in list(self._blocked_ips.keys()):
                try:
                    subprocess.run(
                        ["iptables", "-D", self.chain_name, "-s", ip, "-j", "DROP"],
                        capture_output=True,
                        timeout=10,
                    )
                except Exception:
                    pass
            self._blocked_ips.clear()
        
        bt.logging.info({"iptables": "shutdown"})
    
    def get_blocked_ips(self) -> Dict[str, float]:
        """Get currently blocked IPs and their expiry times."""
        with self._lock:
            return dict(self._blocked_ips)
    
    def get_stats(self) -> dict:
        """Get statistics about blocked IPs."""
        with self._lock:
            now = time.time()
            return {
                "initialized": self._initialized,
                "blocked_count": len(self._blocked_ips),
                "blocked_ips": list(self._blocked_ips.keys()),
            }


# Global singleton instance
_iptables_manager: Optional[IPTablesManager] = None


def get_iptables_manager() -> IPTablesManager:
    """Get or create the global IPTablesManager instance."""
    global _iptables_manager
    if _iptables_manager is None:
        _iptables_manager = IPTablesManager()
    return _iptables_manager


__all__ = [
    "IPTablesManager",
    "get_iptables_manager",
    "is_root",
    "iptables_available",
]
