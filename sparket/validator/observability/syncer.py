"""DataSyncer: pushes curated validator data to the dashboard Go API.

Lifecycle:
  - Created once at validator startup (if observability.sync_enabled)
  - push_scores() called after each scoring cycle (from main_score.py)
  - push_heartbeat() called every N steps (from validator.py loop)
  - All pushes are fire-and-forget with retry (never block validator loop)
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Optional

import bittensor as bt
import httpx
from pydantic import BaseModel
from sqlalchemy import text

from sparket.validator.observability.metrics import (
    SYNC_PUSH_DURATION,
    SYNC_PUSH_ERRORS,
    MINER_SKILL_SCORE,
    MINER_WEIGHT,
    ACTIVE_MINERS,
)
from sparket.validator.observability.schemas import (
    EventSync,
    EventsSyncPayload,
    HeartbeatPayload,
    MarketSync,
    MinerRosterItem,
    MinerScoreRow,
    OutcomeSync,
    RosterSyncPayload,
    ScoredSubmission,
    ScoringHealth,
    ScoresSyncPayload,
    SubmissionsSyncPayload,
)


def _safe_float(val: Any) -> Optional[float]:
    """Coerce Decimal/numeric to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


class DataSyncer:
    """Pushes curated validator data to the dashboard Go API."""

    def __init__(self, api_url: str, api_key: str, database: Any, timeout: int = 10):
        self._api_url = api_url.rstrip("/")
        self._api_key = api_key
        self._db = database
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self._last_sync_submission_id: int = 0
        self._last_scoring_results: dict = {}

    @property
    def enabled(self) -> bool:
        return bool(self._api_url and self._api_key)

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._api_url,
                headers={"X-API-Key": self._api_key},
                timeout=self._timeout,
            )
        return self._client

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def push_scores(self, validator_hotkey: str, scoring_results: Optional[dict] = None) -> None:
        """Called after each scoring cycle. Pushes roster + scores + submissions + events."""
        if not self.enabled:
            return
        if scoring_results:
            self._last_scoring_results = scoring_results
        try:
            await asyncio.gather(
                self._push_roster(validator_hotkey),
                self._push_rolling_scores(validator_hotkey),
                self._push_scored_submissions(validator_hotkey),
                self._push_events(validator_hotkey),
                return_exceptions=True,
            )
        except Exception as e:
            bt.logging.warning({"syncer_push_scores_error": str(e)})

    async def push_heartbeat(self, validator_hotkey: str, step: int, uptime: float) -> None:
        """Called every N validator steps. Pushes lightweight heartbeat."""
        if not self.enabled:
            return
        try:
            payload = await self._build_heartbeat(validator_hotkey, step, uptime)
            await self._push("/api/v1/admin/sync/heartbeat", payload, "heartbeat")
        except Exception as e:
            bt.logging.warning({"syncer_heartbeat_error": str(e)})

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Roster
    # ------------------------------------------------------------------

    async def _push_roster(self, validator_hotkey: str) -> None:
        rows = await self._db.read(
            text("""
                SELECT miner_id, uid, hotkey, coldkey, active, stake,
                       total_stake, incentive, emission, trust, rank
                FROM miner
            """),
            mappings=True,
        )
        miners = [
            MinerRosterItem(
                miner_id=int(r["miner_id"]),
                uid=int(r["uid"]),
                hotkey=str(r["hotkey"]),
                coldkey=str(r["coldkey"]),
                active=bool(r["active"]),
                stake=_safe_float(r["stake"]) or 0.0,
                total_stake=_safe_float(r["total_stake"]) or 0.0,
                incentive=_safe_float(r["incentive"]) or 0.0,
                emission=_safe_float(r["emission"]) or 0.0,
                trust=_safe_float(r["trust"]) or 0.0,
                rank=_safe_float(r["rank"]) or 0.0,
            )
            for r in rows
        ]

        # Update Prometheus gauge for active miners
        active_count = sum(1 for m in miners if m.active)
        ACTIVE_MINERS.set(active_count)

        payload = RosterSyncPayload(
            validator_hotkey=validator_hotkey,
            timestamp=datetime.now(timezone.utc),
            miners=miners,
        )
        await self._push("/api/v1/admin/sync/roster", payload, "roster")

    # ------------------------------------------------------------------
    # Rolling scores
    # ------------------------------------------------------------------

    async def _push_rolling_scores(self, validator_hotkey: str) -> None:
        rows = await self._db.read(
            text("""
                SELECT DISTINCT ON (miner_id) *
                FROM miner_rolling_score
                ORDER BY miner_id, as_of DESC
            """),
            mappings=True,
        )
        now = datetime.now(timezone.utc)
        scores = []
        for r in rows:
            row = MinerScoreRow(
                miner_id=int(r["miner_id"]),
                hotkey=str(r["miner_hotkey"]),
                as_of=r["as_of"],
                skill_score=_safe_float(r.get("skill_score")),
                weight=None,  # filled from weight emission if available
                forecast_dim=_safe_float(r.get("forecast_dim")),
                skill_dim=_safe_float(r.get("skill_dim")),
                econ_dim=_safe_float(r.get("econ_dim")),
                info_dim=_safe_float(r.get("info_dim")),
                fq_score=_safe_float(r.get("fq_score")),
                cal_score=_safe_float(r.get("cal_score")),
                sharp_score=_safe_float(r.get("sharp_score")),
                edge_score=_safe_float(r.get("edge_score")),
                mes_score=_safe_float(r.get("mes_score")),
                sos_score=_safe_float(r.get("sos_score")),
                lead_score=_safe_float(r.get("lead_score")),
                brier_mean=_safe_float(r.get("brier_mean")),
                pss_mean=_safe_float(r.get("pss_mean")),
                es_adj=_safe_float(r.get("es_adj")),
                es_mean=_safe_float(r.get("es_mean")),
                mes_mean=_safe_float(r.get("mes_mean")),
                fq_raw=_safe_float(r.get("fq_raw")),
                lead_ratio=_safe_float(r.get("lead_ratio")),
                n_submissions=int(r.get("n_submissions", 0)),
                n_eff=_safe_float(r.get("n_eff")),
            )
            scores.append(row)

            # Update per-miner Prometheus gauges
            if row.skill_score is not None:
                MINER_SKILL_SCORE.labels(uid=str(r.get("miner_id")), hotkey=row.hotkey).set(
                    row.skill_score
                )

        payload = ScoresSyncPayload(
            validator_hotkey=validator_hotkey,
            scoring_cycle_id=now.isoformat(),
            timestamp=now,
            scores=scores,
        )
        await self._push("/api/v1/admin/sync/scores", payload, "scores")

    # ------------------------------------------------------------------
    # Scored submissions (incremental)
    # ------------------------------------------------------------------

    async def _push_scored_submissions(self, validator_hotkey: str) -> None:
        rows = await self._db.read(
            text("""
                SELECT
                    ms.submission_id,
                    ms.miner_id,
                    ms.miner_hotkey,
                    ms.market_id,
                    ms.side,
                    ms.submitted_at,
                    ms.priced_at,
                    ms.odds_eu,
                    ms.imp_prob,
                    vc.cle,
                    vc.clv_prob,
                    vc.minutes_to_close,
                    os.brier,
                    os.logloss,
                    os.pss,
                    os.pss_brier,
                    os.pss_log,
                    os.settled_at
                FROM miner_submission ms
                JOIN submission_outcome_score os ON os.submission_id = ms.submission_id
                LEFT JOIN submission_vs_close vc ON vc.submission_id = ms.submission_id
                WHERE ms.submission_id > :last_id
                ORDER BY ms.submission_id
                LIMIT 5000
            """),
            params={"last_id": self._last_sync_submission_id},
            mappings=True,
        )

        if not rows:
            return

        submissions = [
            ScoredSubmission(
                submission_id=int(r["submission_id"]),
                miner_id=int(r["miner_id"]),
                hotkey=str(r["miner_hotkey"]),
                market_id=int(r["market_id"]),
                side=str(r["side"]),
                submitted_at=r["submitted_at"],
                priced_at=r.get("priced_at"),
                odds_eu=float(r["odds_eu"]),
                imp_prob=float(r["imp_prob"]),
                cle=_safe_float(r.get("cle")),
                clv_prob=_safe_float(r.get("clv_prob")),
                minutes_to_close=r.get("minutes_to_close"),
                brier=_safe_float(r.get("brier")),
                logloss=_safe_float(r.get("logloss")),
                pss=_safe_float(r.get("pss")),
                pss_brier=_safe_float(r.get("pss_brier")),
                pss_log=_safe_float(r.get("pss_log")),
                settled_at=r.get("settled_at"),
            )
            for r in rows
        ]

        payload = SubmissionsSyncPayload(
            validator_hotkey=validator_hotkey,
            timestamp=datetime.now(timezone.utc),
            last_sync_id=self._last_sync_submission_id,
            submissions=submissions,
        )
        await self._push("/api/v1/admin/sync/submissions", payload, "submissions")

        # Advance watermark
        self._last_sync_submission_id = max(s.submission_id for s in submissions)

    # ------------------------------------------------------------------
    # Events / markets / outcomes
    # ------------------------------------------------------------------

    async def _push_events(self, validator_hotkey: str) -> None:
        # Only sync events modified in the last 24 hours to keep payloads small
        event_rows = await self._db.read(
            text("""
                SELECT e.event_id, l.code AS league_code, s.code AS sport_code,
                       ht.name AS home_team, at.name AS away_team,
                       e.venue, e.start_time_utc, e.status
                FROM event e
                JOIN league l ON l.league_id = e.league_id
                JOIN sport s ON s.sport_id = l.sport_id
                LEFT JOIN team ht ON ht.team_id = e.home_team_id
                LEFT JOIN team at ON at.team_id = e.away_team_id
                WHERE e.start_time_utc > NOW() - INTERVAL '7 days'
            """),
            mappings=True,
        )
        events = [
            EventSync(
                event_id=int(r["event_id"]),
                league_code=str(r["league_code"]),
                sport_code=str(r["sport_code"]),
                home_team=str(r["home_team"] or "TBD"),
                away_team=str(r["away_team"] or "TBD"),
                venue=r.get("venue"),
                start_time_utc=r["start_time_utc"],
                status=str(r["status"]),
            )
            for r in event_rows
        ]

        event_ids = [e.event_id for e in events]
        markets: list[MarketSync] = []
        outcomes: list[OutcomeSync] = []

        if event_ids:
            market_rows = await self._db.read(
                text("""
                    SELECT market_id, event_id, kind, line
                    FROM market
                    WHERE event_id = ANY(:ids)
                """),
                params={"ids": event_ids},
                mappings=True,
            )
            markets = [
                MarketSync(
                    market_id=int(r["market_id"]),
                    event_id=int(r["event_id"]),
                    kind=str(r["kind"]),
                    line=_safe_float(r.get("line")),
                )
                for r in market_rows
            ]

            market_ids = [m.market_id for m in markets]
            if market_ids:
                outcome_rows = await self._db.read(
                    text("""
                        SELECT market_id, result, score_home, score_away, settled_at
                        FROM outcome
                        WHERE market_id = ANY(:ids)
                    """),
                    params={"ids": market_ids},
                    mappings=True,
                )
                outcomes = [
                    OutcomeSync(
                        market_id=int(r["market_id"]),
                        result=r.get("result"),
                        score_home=_safe_float(r.get("score_home")),
                        score_away=_safe_float(r.get("score_away")),
                        settled_at=r.get("settled_at"),
                    )
                    for r in outcome_rows
                ]

        payload = EventsSyncPayload(
            validator_hotkey=validator_hotkey,
            timestamp=datetime.now(timezone.utc),
            events=events,
            markets=markets,
            outcomes=outcomes,
        )
        await self._push("/api/v1/admin/sync/events", payload, "events")

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def _build_heartbeat(self, validator_hotkey: str, step: int, uptime: float) -> HeartbeatPayload:
        # Count miners
        miner_rows = await self._db.read(
            text("SELECT COUNT(*) AS total, SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) AS active FROM miner"),
            mappings=True,
        )
        total_miners = int(miner_rows[0]["total"]) if miner_rows else 0
        active_miners = int(miner_rows[0]["active"] or 0) if miner_rows else 0

        # Count active events/markets
        event_count_rows = await self._db.read(
            text("SELECT COUNT(*) AS cnt FROM event WHERE status IN ('scheduled', 'in_play')"),
            mappings=True,
        )
        active_events = int(event_count_rows[0]["cnt"]) if event_count_rows else 0

        market_count_rows = await self._db.read(
            text("""
                SELECT COUNT(*) AS cnt FROM market m
                JOIN event e ON e.event_id = m.event_id
                WHERE e.status IN ('scheduled', 'in_play')
            """),
            mappings=True,
        )
        active_markets = int(market_count_rows[0]["cnt"]) if market_count_rows else 0

        # Memory usage
        memory_mb: Optional[float] = None
        try:
            import psutil
            proc = psutil.Process()
            memory_mb = proc.memory_info().rss / (1024 * 1024)
        except Exception:
            pass

        # Build scoring health from last results
        lr = self._last_scoring_results
        scoring = ScoringHealth(
            last_cycle_duration_seconds=lr.get("elapsed_sec"),
            last_cycle_submissions_scored=lr.get("clv_scored", 0) + lr.get("outcome_scored", 0),
            last_cycle_errors=len(lr.get("errors", [])),
        )

        return HeartbeatPayload(
            validator_hotkey=validator_hotkey,
            timestamp=datetime.now(timezone.utc),
            uptime_seconds=uptime,
            current_step=step,
            active_miners=active_miners,
            total_miners=total_miners,
            scoring=scoring,
            active_events=active_events,
            active_markets=active_markets,
            memory_mb=memory_mb,
        )

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------

    async def _push(self, endpoint: str, payload: BaseModel, payload_type: str) -> None:
        """POST with retry (max 3 attempts, exponential backoff)."""
        client = self._get_client()
        for attempt in range(3):
            try:
                with SYNC_PUSH_DURATION.labels(payload_type=payload_type).time():
                    resp = await client.post(
                        endpoint,
                        json=payload.model_dump(mode="json"),
                    )
                    resp.raise_for_status()
                return
            except Exception as e:
                if attempt == 2:
                    SYNC_PUSH_ERRORS.labels(payload_type=payload_type).inc()
                    bt.logging.warning(
                        {"syncer_push_failed": {"endpoint": endpoint, "error": str(e)}}
                    )
                else:
                    await asyncio.sleep(2**attempt)
