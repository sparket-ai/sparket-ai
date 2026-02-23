"""Filesystem-based LedgerStore implementation.

Writes gzip-compressed JSON to a local directory tree:
  {data_dir}/checkpoints/epoch_{N}_{date}/
  {data_dir}/deltas/epoch_{N}/{delta_id}/

Retention: keep all data within the retention window, auto-prune older.
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sparket.validator.ledger.models import CheckpointWindow, DeltaWindow


def _write_gzip_json(path: Path, data: Any) -> None:
    """Write data as gzipped JSON, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(data, default=str, sort_keys=True).encode()
    with gzip.open(path, "wb") as f:
        f.write(raw)


def _read_gzip_json(path: Path) -> Any:
    """Read gzipped JSON file."""
    with gzip.open(path, "rb") as f:
        return json.loads(f.read())


def _write_json(path: Path, data: Any) -> None:
    """Write data as plain JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, default=str, sort_keys=True)


def _read_json(path: Path) -> Any:
    """Read plain JSON file."""
    with open(path) as f:
        return json.load(f)


class FilesystemStore:
    """Local filesystem LedgerStore implementation."""

    def __init__(self, data_dir: str, retention_days: int = 7):
        self.base = Path(data_dir)
        self.checkpoints_dir = self.base / "checkpoints"
        self.deltas_dir = self.base / "deltas"
        self.retention_days = retention_days
        self.base.mkdir(parents=True, exist_ok=True)

    async def put_checkpoint(self, cp: CheckpointWindow) -> str:
        """Write a checkpoint to disk. Returns checkpoint ID."""
        epoch = cp.manifest.checkpoint_epoch
        date_str = cp.manifest.window_end.strftime("%Y%m%dT%H%M%S")
        cp_id = f"epoch_{epoch}_{date_str}"
        cp_dir = self.checkpoints_dir / cp_id

        # Write manifest as plain JSON (small, readable)
        _write_json(cp_dir / "manifest.json", cp.manifest.model_dump(mode="json"))

        # Write data sections as gzipped JSON
        _write_gzip_json(
            cp_dir / "accumulators.json.gz",
            [a.model_dump(mode="json") for a in cp.accumulators],
        )
        _write_gzip_json(
            cp_dir / "roster.json.gz",
            [r.model_dump(mode="json") for r in cp.roster],
        )
        _write_json(cp_dir / "config.json", cp.scoring_config.model_dump(mode="json"))
        if cp.chain_params:
            _write_json(cp_dir / "chain_params.json", cp.chain_params.model_dump(mode="json"))

        self._prune()
        return cp_id

    async def put_delta(self, delta: DeltaWindow) -> str:
        """Write a delta to disk. Returns delta ID."""
        epoch = delta.manifest.checkpoint_epoch
        start = delta.manifest.window_start.strftime("%Y%m%dT%H%M%S")
        end = delta.manifest.window_end.strftime("%Y%m%dT%H%M%S")
        delta_id = f"d_{start}_{end}"
        delta_dir = self.deltas_dir / f"epoch_{epoch}" / delta_id

        _write_json(delta_dir / "manifest.json", delta.manifest.model_dump(mode="json"))
        _write_gzip_json(
            delta_dir / "settled_submissions.json.gz",
            [s.model_dump(mode="json") for s in delta.settled_submissions],
        )
        _write_gzip_json(
            delta_dir / "settled_outcomes.json.gz",
            [o.model_dump(mode="json") for o in delta.settled_outcomes],
        )

        self._prune()
        return delta_id

    async def get_latest_checkpoint(self) -> CheckpointWindow | None:
        """Fetch the most recent checkpoint from disk."""
        if not self.checkpoints_dir.exists():
            return None

        dirs = sorted(self.checkpoints_dir.iterdir(), reverse=True)
        for cp_dir in dirs:
            manifest_path = cp_dir / "manifest.json"
            if manifest_path.exists():
                return self._load_checkpoint(cp_dir)
        return None

    async def list_deltas(
        self, epoch: int, since: datetime | None = None,
    ) -> list[str]:
        """List delta IDs for an epoch."""
        epoch_dir = self.deltas_dir / f"epoch_{epoch}"
        if not epoch_dir.exists():
            return []

        delta_ids = sorted(d.name for d in epoch_dir.iterdir() if d.is_dir())
        if since is not None:
            since_str = since.strftime("%Y%m%dT%H%M%S")
            delta_ids = [d for d in delta_ids if d > f"d_{since_str}"]

        return delta_ids

    async def get_delta(self, delta_id: str) -> DeltaWindow | None:
        """Fetch a specific delta by scanning epoch directories."""
        if not self.deltas_dir.exists():
            return None

        for epoch_dir in self.deltas_dir.iterdir():
            delta_dir = epoch_dir / delta_id
            if delta_dir.exists():
                return self._load_delta(delta_dir)
        return None

    def _load_checkpoint(self, cp_dir: Path) -> CheckpointWindow:
        """Load a checkpoint from a directory."""
        from sparket.validator.ledger.models import (
            AccumulatorEntry,
            ChainParamsSnapshot,
            LedgerManifest,
            MinerRosterEntry,
            ScoringConfigSnapshot,
        )

        manifest = LedgerManifest(**_read_json(cp_dir / "manifest.json"))
        accumulators = [
            AccumulatorEntry(**a)
            for a in _read_gzip_json(cp_dir / "accumulators.json.gz")
        ]
        roster = [
            MinerRosterEntry(**r)
            for r in _read_gzip_json(cp_dir / "roster.json.gz")
        ]
        scoring_config = ScoringConfigSnapshot(**_read_json(cp_dir / "config.json"))

        chain_params = None
        cp_path = cp_dir / "chain_params.json"
        if cp_path.exists():
            chain_params = ChainParamsSnapshot(**_read_json(cp_path))

        return CheckpointWindow(
            manifest=manifest,
            roster=roster,
            accumulators=accumulators,
            scoring_config=scoring_config,
            chain_params=chain_params,
        )

    def _load_delta(self, delta_dir: Path) -> DeltaWindow:
        """Load a delta from a directory."""
        from sparket.validator.ledger.models import (
            LedgerManifest,
            OutcomeEntry,
            SettledSubmissionEntry,
        )

        manifest = LedgerManifest(**_read_json(delta_dir / "manifest.json"))
        submissions = [
            SettledSubmissionEntry(**s)
            for s in _read_gzip_json(delta_dir / "settled_submissions.json.gz")
        ]
        outcomes = [
            OutcomeEntry(**o)
            for o in _read_gzip_json(delta_dir / "settled_outcomes.json.gz")
        ]

        return DeltaWindow(
            manifest=manifest,
            settled_submissions=submissions,
            settled_outcomes=outcomes,
        )

    def _prune(self) -> None:
        """Remove data older than the retention window."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        cutoff_str = cutoff.strftime("%Y%m%dT%H%M%S")

        # Prune checkpoints
        if self.checkpoints_dir.exists():
            for cp_dir in list(self.checkpoints_dir.iterdir()):
                # Extract date from dir name: epoch_N_YYYYMMDDTHHMMSS
                parts = cp_dir.name.split("_", 2)
                if len(parts) >= 3 and parts[2] < cutoff_str:
                    shutil.rmtree(cp_dir, ignore_errors=True)

        # Prune deltas
        if self.deltas_dir.exists():
            for epoch_dir in list(self.deltas_dir.iterdir()):
                if not epoch_dir.is_dir():
                    continue
                for delta_dir in list(epoch_dir.iterdir()):
                    # Delta ID: d_YYYYMMDDTHHMMSS_YYYYMMDDTHHMMSS
                    parts = delta_dir.name.split("_")
                    if len(parts) >= 2 and parts[1] < cutoff_str:
                        shutil.rmtree(delta_dir, ignore_errors=True)
                # Remove empty epoch dirs
                if epoch_dir.exists() and not any(epoch_dir.iterdir()):
                    epoch_dir.rmdir()


__all__ = ["FilesystemStore"]
