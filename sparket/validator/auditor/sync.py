"""Ledger sync for auditor validators.

Fetches checkpoints and deltas from the primary, maintains local
accumulator state for Brier verification cross-checks, and handles
epoch changes.

State is persisted to a JSON file for crash recovery.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import bittensor as bt

from sparket.validator.ledger.models import (
    AccumulatorEntry,
    CheckpointWindow,
    DeltaWindow,
    MinerMetrics,
    RecomputeRecord,
)
from sparket.validator.ledger.store.interface import LedgerStore


@dataclass
class EpochChangeResult:
    """Outcome of processing an epoch change."""

    status: str  # "accepted", "paused", "rejected"
    reason: str = ""


@dataclass
class _RecomputeHistoryEntry:
    epoch: int
    timestamp: float
    reason_code: str
    reason_detail: str


class LedgerSync:
    """Fetches and processes ledger data from the primary.

    Maintains local accumulator state for Brier cross-verification
    and persists state for crash recovery.
    """

    def __init__(
        self,
        store: LedgerStore,
        data_dir: str,
        max_epoch_bumps_per_day: int = 1,
        max_epoch_bumps_per_week: int = 3,
    ):
        self.store = store
        self.state_path = Path(data_dir) / "auditor_state.json"
        self.max_epoch_bumps_per_day = max_epoch_bumps_per_day
        self.max_epoch_bumps_per_week = max_epoch_bumps_per_week

        # State
        self.epoch: int = 0
        self.last_delta_id: str = ""
        self.last_delta_ts: str = ""
        self.accumulator: dict[int, dict] = {}  # miner_id -> accumulator data
        self.recompute_history: list[_RecomputeHistoryEntry] = []

        # Latest fetched data
        self.latest_checkpoint: CheckpointWindow | None = None
        self.latest_deltas: list[DeltaWindow] = []

        self._load_state()
        self._prune_recompute_history()

    # -- State persistence --

    def _load_state(self) -> None:
        """Load state from disk. If missing/corrupt, start fresh."""
        if not self.state_path.exists():
            bt.logging.info({"auditor_sync": "no_state_file, starting fresh"})
            return

        try:
            with open(self.state_path) as f:
                data = json.load(f)
            self.epoch = data.get("epoch", 0)
            self.last_delta_id = data.get("last_delta_id", "")
            self.last_delta_ts = data.get("last_delta_ts", "")
            self.accumulator = data.get("accumulator", {})
            self.recompute_history = [
                _RecomputeHistoryEntry(**e)
                for e in data.get("recompute_history", [])
            ]
            bt.logging.info({"auditor_sync": "state_loaded", "epoch": self.epoch})
        except Exception as e:
            bt.logging.warning({"auditor_sync": f"state_corrupt, starting fresh: {e}"})
            self.epoch = 0

    def _save_state(self) -> None:
        """Atomically write state to disk (tmp + rename)."""
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "epoch": self.epoch,
            "last_delta_id": self.last_delta_id,
            "last_delta_ts": self.last_delta_ts,
            "accumulator": self.accumulator,
            "recompute_history": [
                {"epoch": e.epoch, "timestamp": e.timestamp,
                 "reason_code": e.reason_code, "reason_detail": e.reason_detail}
                for e in self.recompute_history
            ],
        }
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=str(self.state_path.parent), suffix=".tmp",
        )
        try:
            with os.fdopen(tmp_fd, "w") as f:
                json.dump(data, f, default=str)
            os.rename(tmp_path, str(self.state_path))
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _prune_recompute_history(self) -> None:
        """Keep recompute history bounded for long-running processes."""
        if len(self.recompute_history) <= 256:
            return
        self.recompute_history = self.recompute_history[-256:]

    # -- Sync cycle --

    async def sync_cycle(self) -> tuple[CheckpointWindow | None, list[DeltaWindow]]:
        """Run one sync cycle: fetch checkpoint + new deltas.

        Returns:
            (checkpoint, new_deltas) - the latest data for plugin dispatch.
        """
        self.latest_deltas = []

        # Fetch latest checkpoint
        cp = await self.store.get_latest_checkpoint()
        if cp is None:
            bt.logging.info({"auditor_sync": "no_checkpoint_available"})
            return None, []

        self.latest_checkpoint = cp
        cp_epoch = cp.manifest.checkpoint_epoch

        # Handle epoch change
        if cp_epoch != self.epoch:
            change_result = self._handle_epoch_change(cp)
            if change_result.status == "rejected":
                bt.logging.error({"auditor_sync": "epoch_rejected", "reason": change_result.reason})
                return cp, []
            if change_result.status == "paused":
                bt.logging.warning({"auditor_sync": "epoch_paused", "reason": change_result.reason})
                return cp, []

        # Fetch new deltas since last sync
        since = None
        if self.last_delta_ts:
            try:
                since = datetime.fromisoformat(self.last_delta_ts)
            except ValueError:
                since = None

        delta_ids = await self.store.list_deltas(self.epoch, since)

        # Filter out already-processed deltas
        new_ids = [d for d in delta_ids if d > self.last_delta_id] if self.last_delta_id else delta_ids

        for delta_id in new_ids:
            delta = await self.store.get_delta(delta_id)
            if delta is None:
                continue
            if delta.manifest.checkpoint_epoch != self.epoch:
                continue  # Stale epoch, skip

            self.latest_deltas.append(delta)
            self._apply_delta(delta)
            self.last_delta_id = delta_id
            self.last_delta_ts = delta.manifest.window_end.isoformat()

        self._save_state()

        bt.logging.info({
            "auditor_sync": {
                "epoch": self.epoch,
                "new_deltas": len(self.latest_deltas),
            }
        })

        return cp, self.latest_deltas

    def _handle_epoch_change(self, cp: CheckpointWindow) -> EpochChangeResult:
        """Process an epoch change from a new checkpoint."""
        new_epoch = cp.manifest.checkpoint_epoch
        record = cp.manifest.recompute_record

        # Validate recompute record
        if record is not None:
            if not record.reason_detail:
                return EpochChangeResult(status="rejected", reason="empty_reason_detail")
        elif new_epoch > self.epoch + 1:
            # Skipped epochs without a record is suspicious
            bt.logging.warning({"auditor_sync": "skipped_epochs", "from": self.epoch, "to": new_epoch})

        # Check rate limits
        now = time.time()
        recent_day = [
            e for e in self.recompute_history
            if now - e.timestamp < 86400
        ]
        recent_week = [
            e for e in self.recompute_history
            if now - e.timestamp < 604800
        ]

        if len(recent_day) >= self.max_epoch_bumps_per_day:
            return EpochChangeResult(status="paused", reason="RECOMPUTE_RATE_EXCEEDED_DAILY")
        if len(recent_week) >= self.max_epoch_bumps_per_week:
            return EpochChangeResult(status="paused", reason="RECOMPUTE_RATE_EXCEEDED_WEEKLY")

        # Accept: reset accumulators
        self.epoch = new_epoch
        self.accumulator = {}
        self.last_delta_id = ""
        self.last_delta_ts = ""

        # Log recompute
        if record is not None:
            self.recompute_history.append(_RecomputeHistoryEntry(
                epoch=new_epoch,
                timestamp=now,
                reason_code=record.reason_code.value if hasattr(record.reason_code, 'value') else str(record.reason_code),
                reason_detail=record.reason_detail,
            ))
            self._prune_recompute_history()
            bt.logging.warning({
                "auditor_epoch_change": {
                    "new_epoch": new_epoch,
                    "reason_code": record.reason_code,
                    "reason_detail": record.reason_detail,
                    "severity": record.severity,
                }
            })
        else:
            # Initial sync (no prior epoch)
            bt.logging.info({"auditor_epoch_change": {"new_epoch": new_epoch, "initial": True}})

        self._save_state()
        return EpochChangeResult(status="accepted")

    def _apply_delta(self, delta: DeltaWindow) -> None:
        """Apply a delta's settled scores to local accumulators.

        This is for Brier cross-verification only. The checkpoint's
        metrics are used for actual weight computation.
        """
        for sub in delta.settled_submissions:
            mid = str(sub.miner_id)
            if mid not in self.accumulator:
                self.accumulator[mid] = {"brier_ws": 0.0, "brier_wt": 0.0, "count": 0}

            acc = self.accumulator[mid]
            # Simple accumulation (decay would need age info)
            if sub.brier is not None:
                acc["brier_ws"] += sub.brier
                acc["brier_wt"] += 1.0
                acc["count"] += 1

    def get_miner_metrics(self) -> list[MinerMetrics]:
        """Extract MinerMetrics from the latest checkpoint.

        Uses the checkpoint's derived means for compute_weights().
        """
        if self.latest_checkpoint is None:
            return []

        return [
            MinerMetrics.from_accumulator(acc)
            for acc in self.latest_checkpoint.accumulators
        ]


__all__ = ["EpochChangeResult", "LedgerSync"]
