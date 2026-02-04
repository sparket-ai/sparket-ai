"""Background broadcaster for non-blocking connection info pushes.

Handles broadcasting to miners with true per-miner concurrency,
allowing the main validator loop to continue without blocking.
Each miner request runs independently - slow miners don't block fast ones.
"""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import bittensor as bt

from sparket.shared.log_colors import LogColors


@dataclass
class BroadcastStats:
    """Statistics from a broadcast operation."""
    
    total: int = 0
    success: int = 0
    timeout: int = 0
    error: int = 0
    unreachable: int = 0
    
    def to_dict(self) -> Dict[str, int]:
        return {
            "total": self.total,
            "success": self.success,
            "timeout": self.timeout,
            "error": self.error,
            "unreachable": self.unreachable,
        }


class ConnectionBroadcaster:
    """Handles connection info broadcasts with true per-miner concurrency.
    
    Features:
    - Each miner request runs independently (slow miners don't block fast ones)
    - Semaphore-based concurrency limiting to avoid overwhelming network
    - Individual timeouts per request
    - Stats tracking from completed broadcasts
    - Proper cleanup on shutdown
    
    Unlike batch-based approaches where we wait for the slowest miner in each
    batch, this fires off all requests concurrently (with a semaphore limit)
    and processes responses as they complete.
    """
    
    def __init__(
        self,
        wallet: Any,
        max_concurrent: int = 50,
        timeout_per_miner: float = 20.0,
    ):
        self._wallet = wallet
        self._max_concurrent = max_concurrent
        self._timeout = timeout_per_miner
        self._task: Optional[asyncio.Task] = None
        self._last_stats: Optional[BroadcastStats] = None
        self._is_broadcasting: bool = False
        # Reusable dendrite for all requests (with connection pooling)
        self._dendrite: Optional[bt.Dendrite] = None
    
    def _get_dendrite(self) -> bt.Dendrite:
        """Get or create a reusable dendrite instance."""
        if self._dendrite is None:
            self._dendrite = bt.Dendrite(wallet=self._wallet)
        return self._dendrite
    
    async def _send_single(
        self,
        axon: Any,
        synapse: Any,
        semaphore: asyncio.Semaphore,
    ) -> str:
        """Send to a single axon with semaphore-controlled concurrency.
        
        Returns: 'success', 'timeout', 'error', or 'unreachable'
        """
        async with semaphore:
            try:
                dendrite = self._get_dendrite()
                
                # Send to single axon with individual timeout
                responses = await asyncio.wait_for(
                    dendrite.forward(
                        axons=[axon],
                        synapse=copy.deepcopy(synapse),  # Copy to avoid mutation
                        timeout=self._timeout,
                        deserialize=False,
                    ),
                    timeout=self._timeout + 2.0,  # Outer timeout as safety net
                )
                
                if not responses:
                    return "unreachable"
                
                resp = responses[0]
                dendrite_info = getattr(resp, "dendrite", None)
                status_code = getattr(dendrite_info, "status_code", None) if dendrite_info else None
                status_msg = getattr(dendrite_info, "status_message", "") if dendrite_info else ""
                
                if status_code is None or status_code == 0:
                    return "unreachable"
                elif status_code == 408 or "timeout" in status_msg.lower():
                    return "timeout"
                elif status_code >= 400:
                    return "error"
                else:
                    return "success"
                    
            except asyncio.TimeoutError:
                return "timeout"
            except Exception:
                return "error"
    
    async def broadcast_async(
        self,
        axons: List[Any],
        synapse: Any,
    ) -> BroadcastStats:
        """Broadcast to all axons with true per-miner concurrency.
        
        Each miner request runs independently. A semaphore limits how many
        run at once (to avoid overwhelming the network), but slow miners
        don't block fast ones - as soon as one completes, another can start.
        """
        self._is_broadcasting = True
        stats = BroadcastStats(total=len(axons))
        
        if not axons:
            self._is_broadcasting = False
            return stats
        
        try:
            # Semaphore limits concurrent connections
            semaphore = asyncio.Semaphore(self._max_concurrent)
            
            # Create individual tasks for each miner
            tasks = [
                asyncio.create_task(
                    self._send_single(axon, synapse, semaphore),
                    name=f"broadcast_{i}",
                )
                for i, axon in enumerate(axons)
            ]
            
            # Process results as they complete (not waiting for all)
            completed = 0
            for coro in asyncio.as_completed(tasks):
                try:
                    result = await coro
                    if result == "success":
                        stats.success += 1
                    elif result == "timeout":
                        stats.timeout += 1
                    elif result == "error":
                        stats.error += 1
                    else:
                        stats.unreachable += 1
                except Exception:
                    stats.error += 1
                
                completed += 1
                
                # Log progress every 50 completions
                if completed % 50 == 0 or completed == len(axons):
                    bt.logging.debug({
                        "broadcast_progress": {
                            "completed": completed,
                            "total": len(axons),
                            "success": stats.success,
                        }
                    })
            
            # Log final summary
            success_pct = (stats.success / stats.total * 100) if stats.total > 0 else 0
            bt.logging.info(
                f"{LogColors.NETWORK_LABEL} broadcast_complete: "
                f"total={stats.total}, success={stats.success} ({success_pct:.0f}%), "
                f"timeout={stats.timeout}, error={stats.error}, "
                f"unreachable={stats.unreachable}"
            )
            
            self._last_stats = stats
            
        except asyncio.CancelledError:
            bt.logging.debug({"broadcast": "cancelled"})
            # Cancel any remaining tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
            raise
        except Exception as e:
            bt.logging.warning({"broadcast_error": str(e)})
        finally:
            self._is_broadcasting = False
        
        return stats
    
    def start_broadcast(self, axons: List[Any], synapse: Any) -> bool:
        """Fire-and-forget - starts broadcast in background.
        
        Returns True if broadcast was started, False if one is already running.
        """
        if self._is_broadcasting:
            bt.logging.debug({"broadcast": "skipped_already_running"})
            return False
        
        if self._task and not self._task.done():
            bt.logging.debug({"broadcast": "skipped_task_pending"})
            return False
        
        self._task = asyncio.create_task(
            self.broadcast_async(axons, synapse),
            name="connection_broadcast",
        )
        return True
    
    def get_last_stats(self) -> Optional[Dict[str, int]]:
        """Get stats from most recent completed broadcast."""
        if self._last_stats:
            return self._last_stats.to_dict()
        return None
    
    def is_broadcasting(self) -> bool:
        """Check if a broadcast is currently in progress."""
        return self._is_broadcasting
    
    async def close(self) -> None:
        """Close the reusable dendrite connection."""
        if self._dendrite is not None:
            try:
                await self._dendrite.aclose()
            except Exception:
                pass
            self._dendrite = None
    
    def cancel(self) -> None:
        """Cancel any running broadcast task."""
        if self._task and not self._task.done():
            self._task.cancel()
            bt.logging.debug({"broadcast": "cancel_requested"})


__all__ = ["ConnectionBroadcaster", "BroadcastStats"]
