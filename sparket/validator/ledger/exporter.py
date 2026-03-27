"""Ledger exporter: produces checkpoints and deltas from primary DB state.

The exporter reads from the validator database and produces signed,
redacted ledger windows that auditors can use to verify and reproduce
weights without SportsDataIO access.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from typing import Any

import bittensor as bt
from sqlalchemy import text

from sparket.validator.config.scoring_params import get_scoring_params
from sparket.validator.scoring.determinism import get_canonical_window_bounds

from .models import (
    LEDGER_SCHEMA_VERSION,
    AccumulatorEntry,
    ChainParamsSnapshot,
    CheckpointWindow,
    DeltaWindow,
    LedgerManifest,
    MetricAccumulator,
    MinerRosterEntry,
    OutcomeEntry,
    RecomputeReasonCode,
    RecomputeRecord,
    ScoringConfigSnapshot,
    SettledSubmissionEntry,
)
from .redaction import SAFE_OUTCOME_FIELDS, SAFE_SETTLED_SUBMISSION_FIELDS, redact
from .signer import compute_section_hash, sign_manifest

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

_SELECT_ROSTER = text("""
    SELECT miner_id, uid, hotkey, active
    FROM miner
    WHERE netuid = :netuid
    ORDER BY uid
""")

_SELECT_ACCUMULATORS = text("""
    SELECT
        m.miner_id,
        m.uid,
        m.hotkey,
        mrs.n_submissions,
        mrs.brier_mean, mrs.fq_raw, mrs.pss_mean,
        mrs.es_adj, mrs.mes_mean, mrs.sos_mean, mrs.lead_ratio,
        mrs.cal_score, mrs.sharp_score,
        mrs.brier_ws, mrs.brier_wt,
        mrs.fq_ws, mrs.fq_wt,
        mrs.pss_ws, mrs.pss_wt,
        mrs.es_ws, mrs.es_wt,
        mrs.mes_ws, mrs.mes_wt,
        mrs.sos_ws, mrs.sos_wt,
        mrs.lead_ws, mrs.lead_wt,
        mrs.uniqueness_dim, mrs.marginal_dim
    FROM miner m
    JOIN miner_rolling_score mrs
        ON m.miner_id = mrs.miner_id AND m.hotkey = mrs.miner_hotkey
    WHERE m.netuid = :netuid
      AND m.active = 1
      AND mrs.as_of = :as_of
      AND mrs.window_days = :window_days
    ORDER BY m.uid
""")

_SELECT_SETTLED_SUBMISSIONS = text("""
    SELECT
        ms.miner_id,
        ms.market_id,
        ms.side,
        ms.imp_prob,
        sos.brier,
        sos.pss,
        sos.settled_at
    FROM miner_submission ms
    JOIN submission_outcome_score sos ON ms.submission_id = sos.submission_id
    WHERE sos.settled_at > :since
      AND sos.settled_at <= :until
      AND sos.brier IS NOT NULL
    ORDER BY sos.settled_at, ms.miner_id
""")

_SELECT_SETTLED_OUTCOMES = text("""
    SELECT
        o.market_id,
        e.event_id,
        o.result,
        o.score_home,
        o.score_away,
        o.settled_at
    FROM outcome o
    JOIN market mk ON o.market_id = mk.market_id
    JOIN event e ON mk.event_id = e.event_id
    WHERE o.settled_at > :since
      AND o.settled_at <= :until
    ORDER BY o.settled_at
""")

_SELECT_LEDGER_STATE = text("SELECT checkpoint_epoch FROM ledger_state WHERE id = 1")

_UPDATE_LEDGER_EPOCH = text("""
    UPDATE ledger_state
    SET checkpoint_epoch = :epoch, last_checkpoint_at = :ts
    WHERE id = 1
""")

_UPDATE_LEDGER_DELTA = text("""
    UPDATE ledger_state
    SET last_delta_at = :ts, last_delta_id = :delta_id
    WHERE id = 1
""")

_UPDATE_LEDGER_CHECKPOINT = text("""
    UPDATE ledger_state
    SET last_checkpoint_at = :ts
    WHERE id = 1
""")


def _get_code_version() -> str:
    """Get current git commit hash."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return "unknown"


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Convert DB value to float safely."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


class LedgerExporter:
    """Produces signed, redacted ledger checkpoints and deltas."""

    def __init__(self, database: Any, wallet: Any, netuid: int):
        self.database = database
        self.wallet = wallet
        self.netuid = netuid
        self.params = get_scoring_params()
        self._hotkey = wallet.hotkey.ss58_address

    async def _get_epoch(self) -> int:
        """Read current checkpoint epoch from DB."""
        rows = await self.database.read(_SELECT_LEDGER_STATE, mappings=True)
        if rows:
            return int(rows[0]["checkpoint_epoch"])
        return 1

    async def export_checkpoint(
        self,
        *,
        as_of: datetime | None = None,
        recompute_record: RecomputeRecord | None = None,
    ) -> CheckpointWindow:
        """Export a full accumulator state checkpoint.

        Args:
            as_of: Override the canonical window end (default: now floored to midnight).
            recompute_record: Attach if this is an epoch bump checkpoint.

        Returns:
            Signed CheckpointWindow ready to write to a LedgerStore.
        """
        window_days = self.params.windows.rolling_window_days
        if as_of is None:
            _, as_of = get_canonical_window_bounds(window_days)
        window_start = as_of

        epoch = await self._get_epoch()
        now = datetime.now(timezone.utc)

        # Fetch roster
        roster_rows = await self.database.read(
            _SELECT_ROSTER, params={"netuid": self.netuid}, mappings=True,
        )
        roster = [
            MinerRosterEntry(
                miner_id=r["miner_id"],
                uid=r["uid"],
                hotkey=r["hotkey"],
                active=bool(r["active"]),
            )
            for r in roster_rows
        ]

        # Fetch accumulators
        acc_rows = await self.database.read(
            _SELECT_ACCUMULATORS,
            params={"netuid": self.netuid, "as_of": as_of, "window_days": window_days},
            mappings=True,
        )

        accumulators = []
        for row in acc_rows:
            acc = AccumulatorEntry(
                miner_id=row["miner_id"],
                hotkey=row["hotkey"],
                uid=row["uid"],
                n_submissions=int(row.get("n_submissions") or 0),
                brier=MetricAccumulator(
                    ws=_safe_float(row.get("brier_ws")),
                    wt=_safe_float(row.get("brier_wt")),
                ),
                fq=MetricAccumulator(
                    ws=_safe_float(row.get("fq_ws")),
                    wt=_safe_float(row.get("fq_wt")),
                ),
                pss=MetricAccumulator(
                    ws=_safe_float(row.get("pss_ws")),
                    wt=_safe_float(row.get("pss_wt")),
                ),
                es=MetricAccumulator(
                    ws=_safe_float(row.get("es_ws")),
                    wt=_safe_float(row.get("es_wt")),
                ),
                mes=MetricAccumulator(
                    ws=_safe_float(row.get("mes_ws")),
                    wt=_safe_float(row.get("mes_wt")),
                ),
                sos=MetricAccumulator(
                    ws=_safe_float(row.get("sos_ws")),
                    wt=_safe_float(row.get("sos_wt")),
                ),
                lead=MetricAccumulator(
                    ws=_safe_float(row.get("lead_ws")),
                    wt=_safe_float(row.get("lead_wt")),
                ),
                # Derived means from DB (primary path)
                brier_mean=_safe_float(row.get("brier_mean")),
                fq_raw=_safe_float(row.get("fq_raw")),
                pss_mean=_safe_float(row.get("pss_mean")),
                es_adj=_safe_float(row.get("es_adj")),
                mes_mean=_safe_float(row.get("mes_mean"), 0.5),
                sos_score=_safe_float(row.get("sos_mean"), 0.5),
                lead_score=_safe_float(row.get("lead_ratio"), 0.5),
                cal_score=_safe_float(row.get("cal_score"), 0.5),
                sharp_score=_safe_float(row.get("sharp_score"), 0.5),
                uniqueness_dim=row.get("uniqueness_dim"),
                marginal_dim=row.get("marginal_dim"),
            )
            accumulators.append(acc)

        # Scoring config snapshot
        scoring_config = ScoringConfigSnapshot(
            params=self.params.model_dump(mode="json"),
        )

        # Build content hashes
        content_hashes = {
            "roster": compute_section_hash(roster),
            "accumulators": compute_section_hash(accumulators),
            "scoring_config": compute_section_hash(scoring_config),
        }

        manifest = LedgerManifest(
            schema_version=LEDGER_SCHEMA_VERSION,
            window_type="checkpoint",
            window_start=window_start,
            window_end=as_of,
            checkpoint_epoch=epoch,
            content_hashes=content_hashes,
            primary_hotkey=self._hotkey,
            created_at=now,
            recompute_record=recompute_record,
        )

        # Sign
        manifest.signature = sign_manifest(manifest, self.wallet)

        checkpoint = CheckpointWindow(
            manifest=manifest,
            roster=roster,
            accumulators=accumulators,
            scoring_config=scoring_config,
        )

        # Update ledger state
        await self.database.write(
            _UPDATE_LEDGER_CHECKPOINT, params={"ts": now},
        )

        bt.logging.info({
            "ledger_checkpoint": {
                "epoch": epoch,
                "miners": len(accumulators),
                "as_of": str(as_of),
            }
        })

        return checkpoint

    async def export_delta(
        self,
        since: datetime,
        until: datetime | None = None,
    ) -> DeltaWindow:
        """Export settled submission outcome scores since the given time.

        Args:
            since: Start of the delta period (exclusive).
            until: End of the delta period (inclusive, default: now).

        Returns:
            Signed DeltaWindow ready to write to a LedgerStore.
        """
        if until is None:
            until = datetime.now(timezone.utc)

        epoch = await self._get_epoch()
        now = datetime.now(timezone.utc)

        # Fetch settled submissions (only settled markets - safe from copy-trading)
        sub_rows = await self.database.read(
            _SELECT_SETTLED_SUBMISSIONS,
            params={"since": since, "until": until},
            mappings=True,
        )

        settled_submissions = [
            SettledSubmissionEntry(**redact(dict(r), SAFE_SETTLED_SUBMISSION_FIELDS))
            for r in sub_rows
        ]

        # Fetch settled outcomes (public data)
        outcome_rows = await self.database.read(
            _SELECT_SETTLED_OUTCOMES,
            params={"since": since, "until": until},
            mappings=True,
        )

        settled_outcomes = [
            OutcomeEntry(**redact(dict(r), SAFE_OUTCOME_FIELDS))
            for r in outcome_rows
        ]

        # Build content hashes
        content_hashes = {
            "settled_submissions": compute_section_hash(settled_submissions),
            "settled_outcomes": compute_section_hash(settled_outcomes),
        }

        delta_id = f"d_{since.strftime('%Y%m%dT%H%M%S')}_{until.strftime('%Y%m%dT%H%M%S')}"

        manifest = LedgerManifest(
            schema_version=LEDGER_SCHEMA_VERSION,
            window_type="delta",
            window_start=since,
            window_end=until,
            checkpoint_epoch=epoch,
            content_hashes=content_hashes,
            primary_hotkey=self._hotkey,
            created_at=now,
        )

        manifest.signature = sign_manifest(manifest, self.wallet)

        delta = DeltaWindow(
            manifest=manifest,
            settled_submissions=settled_submissions,
            settled_outcomes=settled_outcomes,
        )

        # Update ledger state
        await self.database.write(
            _UPDATE_LEDGER_DELTA, params={"ts": now, "delta_id": delta_id},
        )

        bt.logging.info({
            "ledger_delta": {
                "epoch": epoch,
                "submissions": len(settled_submissions),
                "outcomes": len(settled_outcomes),
                "since": str(since),
                "until": str(until),
            }
        })

        return delta

    async def bump_epoch(
        self,
        reason_code: str,
        reason_detail: str,
        affected_event_ids: list[int] | None = None,
        severity: str = "correction",
    ) -> CheckpointWindow:
        """Increment the checkpoint epoch and publish a new checkpoint.

        This is the recompute kill switch. Auditors will reset their
        accumulators to the new checkpoint.

        Args:
            reason_code: One of RecomputeReasonCode values.
            reason_detail: Human-readable explanation.
            affected_event_ids: Which events were impacted (empty if global).
            severity: One of 'correction', 'bugfix', 'recovery'.

        Returns:
            The new checkpoint with the recompute record attached.
        """
        old_epoch = await self._get_epoch()
        new_epoch = old_epoch + 1
        now = datetime.now(timezone.utc)

        # Validate reason code
        RecomputeReasonCode(reason_code)

        record = RecomputeRecord(
            epoch=new_epoch,
            previous_epoch=old_epoch,
            reason_code=RecomputeReasonCode(reason_code),
            reason_detail=reason_detail,
            affected_event_ids=affected_event_ids or [],
            severity=severity,
            timestamp=now,
            code_version=_get_code_version(),
        )

        # Update epoch in DB
        await self.database.write(
            _UPDATE_LEDGER_EPOCH, params={"epoch": new_epoch, "ts": now},
        )

        bt.logging.warning({
            "ledger_epoch_bump": {
                "old_epoch": old_epoch,
                "new_epoch": new_epoch,
                "reason_code": reason_code,
                "reason_detail": reason_detail,
                "severity": severity,
            }
        })

        # Export checkpoint with recompute record
        return await self.export_checkpoint(recompute_record=record)


__all__ = ["LedgerExporter"]
