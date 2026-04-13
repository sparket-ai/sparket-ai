"""Test Control API - HTTP interface for test script to control nodes.

In test mode, each node exposes a simple HTTP API that the test script
can use to:
- Seed mock data
- Trigger actions (fetch games, submit odds, etc.)
- Inspect state
- Trigger state transitions

This is much simpler than using bittensor dendrites and allows the
test script to be a pure controller without any bittensor dependencies.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any
from aiohttp import web

from .mock_provider import get_mock_provider


async def _create_db_engine():
    """Create a database engine for the control API's event loop."""
    try:
        from sqlalchemy.ext.asyncio import create_async_engine
        
        # Build URL from environment
        user = os.getenv("SPARKET_DATABASE__USER", "sparket")
        password = os.getenv("SPARKET_DATABASE__PASSWORD", "sparket")
        host = os.getenv("SPARKET_DATABASE__HOST", "127.0.0.1")
        port = os.getenv("SPARKET_DATABASE__PORT", "5435")
        name = os.getenv("SPARKET_DATABASE__NAME", "sparket_test")
        
        url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{name}"
        
        engine = create_async_engine(url, pool_size=5, max_overflow=2)
        return engine
    except Exception as e:
        print(f"[TestControlAPI] Failed to create DB engine: {e}")
        return None


class TestControlAPI:
    """HTTP control API for test mode.
    
    Runs in a background thread with its own event loop.
    Test script sends HTTP requests to control the node.
    """
    
    def __init__(
        self,
        role: str,  # "validator" or "miner"
        port: int,
        node: Any = None,  # Reference to the running node
    ) -> None:
        self.role = role
        self.port = port
        self.node = node
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._running = False
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._db_engine: Any = None  # Own DB engine for this loop
    
    def start_background(self) -> None:
        """Start the control API in a background thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        
        self._thread = threading.Thread(target=self._run_in_thread, daemon=True)
        self._thread.start()
    
    def _run_in_thread(self) -> None:
        """Run the control API in its own event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        
        try:
            self._loop.run_until_complete(self._start_async())
            self._loop.run_forever()
        finally:
            self._loop.close()
    
    async def _start_async(self) -> None:
        """Start the aiohttp server."""
        # Create own DB engine for this event loop (validator only)
        if self.role == "validator":
            self._db_engine = await _create_db_engine()
        
        self._app = web.Application()
        self._setup_routes()
        
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        
        self._site = web.TCPSite(self._runner, "127.0.0.1", self.port)
        await self._site.start()
        
        self._running = True
        db_status = "connected" if self._db_engine else "not connected"
        print(f"[TestControlAPI] {self.role} control API running on http://127.0.0.1:{self.port} (db: {db_status})")
    
    def stop(self) -> None:
        """Stop the control API server."""
        if not self._running or not self._loop:
            return
        
        async def _stop():
            if self._site:
                await self._site.stop()
            if self._runner:
                await self._runner.cleanup()
            if self._db_engine:
                await self._db_engine.dispose()
        
        try:
            future = asyncio.run_coroutine_threadsafe(_stop(), self._loop)
            future.result(timeout=5)
        except Exception:
            pass
        
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        
        if self._thread:
            self._thread.join(timeout=5)
        
        self._running = False
        print(f"[TestControlAPI] {self.role} control API stopped")
    
    # Legacy async interface for compatibility
    async def start(self) -> None:
        """Start the control API - calls start_background."""
        self.start_background()
    
    def _setup_routes(self) -> None:
        """Setup HTTP routes based on role."""
        assert self._app is not None
        
        # Common routes
        self._app.router.add_get("/health", self._handle_health)
        self._app.router.add_get("/state", self._handle_get_state)
        
        if self.role == "validator":
            self._setup_validator_routes()
        elif self.role == "miner":
            self._setup_miner_routes()
    
    def _setup_validator_routes(self) -> None:
        """Routes for validator control."""
        assert self._app is not None
        
        # Mock provider data management
        self._app.router.add_post("/mock/reset", self._handle_mock_reset)
        self._app.router.add_post("/mock/event", self._handle_add_event)
        self._app.router.add_post("/mock/market", self._handle_add_market)
        self._app.router.add_post("/mock/odds", self._handle_add_odds)
        self._app.router.add_post("/mock/outcome", self._handle_set_outcome)
        self._app.router.add_get("/mock/state", self._handle_get_mock_state)
        
        # Enhanced mock provider - sportsbooks and time-series
        self._app.router.add_post("/mock/sportsbook", self._handle_add_sportsbook)
        self._app.router.add_get("/mock/sportsbooks", self._handle_get_sportsbooks)
        self._app.router.add_post("/mock/sportsbook-odds", self._handle_add_sportsbook_odds)
        self._app.router.add_post("/mock/timeseries", self._handle_generate_timeseries)
        self._app.router.add_get("/mock/closing-odds", self._handle_get_closing_odds)
        self._app.router.add_get("/mock/consensus", self._handle_get_consensus)
        
        # Trigger actions
        self._app.router.add_post("/trigger/ingest", self._handle_trigger_ingest)
        self._app.router.add_post("/trigger/scoring", self._handle_trigger_scoring)
        self._app.router.add_post("/trigger/event-status", self._handle_set_event_status)
        
        # Database queries
        self._app.router.add_get("/db/submissions", self._handle_get_submissions)
        self._app.router.add_get("/db/scores", self._handle_get_scores)
        
        # Admin actions
        self._app.router.add_post("/admin/sync-miners", self._handle_sync_miners)
        self._app.router.add_post("/admin/wipe-db", self._handle_wipe_db)
        
        # Provider quote seeding for ground truth
        self._app.router.add_post("/mock/provider-quote", self._handle_add_provider_quote)
        self._app.router.add_post("/trigger/snapshot-pipeline", self._handle_trigger_snapshot_pipeline)
        
        # Granular scoring triggers for testing
        self._app.router.add_post("/trigger/odds-scoring", self._handle_trigger_odds_scoring)
        self._app.router.add_post("/trigger/outcome-scoring", self._handle_trigger_outcome_scoring)
        
        # Direct mock data seeding for testing
        self._app.router.add_post("/mock/ground-truth-closing", self._handle_add_ground_truth_closing)
        self._app.router.add_post("/mock/ground-truth-snapshot", self._handle_add_ground_truth_snapshot)
        self._app.router.add_post("/mock/seed-ground-truth-from-timeseries", self._handle_seed_ground_truth)
        self._app.router.add_post("/mock/settled-outcome", self._handle_add_outcome)
        self._app.router.add_post("/mock/backdate-submissions", self._handle_backdate_submissions)
        
        # Enhanced scoring inspection endpoints
        self._app.router.add_get("/db/rolling-scores", self._handle_get_rolling_scores)
        self._app.router.add_get("/db/skill-scores", self._handle_get_skill_scores)
        self._app.router.add_get("/db/weights", self._handle_get_weights)
        self._app.router.add_post("/trigger/weights", self._handle_trigger_weights)
        
        # Health and monitoring endpoints
        self._app.router.add_get("/health/memory", self._handle_get_memory)
        self._app.router.add_get("/health/jobs", self._handle_get_job_status)
    
    def _setup_miner_routes(self) -> None:
        """Routes for miner control."""
        assert self._app is not None
        
        # Trigger miner actions
        self._app.router.add_post("/action/fetch-games", self._handle_fetch_games)
        self._app.router.add_post("/action/submit-odds", self._handle_submit_odds)
        self._app.router.add_post("/action/submit-outcome", self._handle_submit_outcome)
        
        # Inspect miner state
        self._app.router.add_get("/games", self._handle_get_games)
    
    # --- Common Handlers ---
    
    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "role": self.role})
    
    async def _handle_get_state(self, request: web.Request) -> web.Response:
        state = {
            "role": self.role,
            "running": self._running,
            "node_type": type(self.node).__name__ if self.node else None,
        }
        return web.json_response(state)
    
    # --- Validator: Mock Provider Handlers ---
    
    async def _handle_mock_reset(self, request: web.Request) -> web.Response:
        provider = get_mock_provider()
        provider.reset()
        return web.json_response({"status": "ok", "message": "Mock provider reset"})
    
    async def _handle_add_event(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
            provider = get_mock_provider()
            
            start_time = datetime.fromisoformat(data["start_time"].replace("Z", "+00:00"))
            
            event = provider.add_event(
                home_team=data["home_team"],
                away_team=data["away_team"],
                start_time=start_time,
                status=data.get("status", "scheduled"),
                event_id=data.get("event_id"),
            )
            
            # Persist to database for validator to validate submissions
            db_event_id = None
            db_market_id = None
            if self._db_engine:
                try:
                    from sqlalchemy import text
                    async with self._db_engine.begin() as conn:
                        # Insert event with correct schema
                        ext_ref_json = json.dumps({"mock_event_id": event.event_id, "home": data["home_team"], "away": data["away_team"]})
                        result = await conn.execute(
                            text("""
                                INSERT INTO event (
                                    league_id, home_team_id, away_team_id, venue,
                                    start_time_utc, status, ext_ref, created_at
                                )
                                VALUES (
                                    1, NULL, NULL, NULL,
                                    :start_time, :status, CAST(:ext_ref AS jsonb), NOW()
                                )
                                RETURNING event_id
                            """),
                            {
                                "start_time": start_time,
                                "status": event.status,
                                "ext_ref": ext_ref_json,
                            }
                        )
                        row = result.fetchone()
                        if row:
                            db_event_id = row[0]
                            
                            # Create default moneyline market
                            market_result = await conn.execute(
                                text("""
                                    INSERT INTO market (
                                        event_id, kind, line, created_at
                                    )
                                    VALUES (
                                        :event_id, 'MONEYLINE', NULL, NOW()
                                    )
                                    RETURNING market_id
                                """),
                                {"event_id": db_event_id}
                            )
                            market_row = market_result.fetchone()
                            if market_row:
                                db_market_id = market_row[0]
                                # Also add to mock provider
                                provider.add_market(
                                    event_id=event.event_id,
                                    kind="moneyline",
                                    market_id=str(db_market_id)
                                )
                except Exception as db_err:
                    print(f"[TestControlAPI] DB persist error: {db_err}")
                    traceback.print_exc()
            
            event_dict = event.to_dict()
            event_dict["db_event_id"] = db_event_id
            event_dict["db_market_id"] = db_market_id
            
            return web.json_response({
                "status": "ok",
                "event": event_dict
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=400)
    
    async def _handle_add_market(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
            provider = get_mock_provider()
            
            market = provider.add_market(
                event_id=data["event_id"],
                kind=data["kind"],
                line=data.get("line"),
                market_id=data.get("market_id"),
            )
            
            return web.json_response({
                "status": "ok",
                "market": market.to_dict()
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=400)
    
    async def _handle_add_odds(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
            provider = get_mock_provider()
            
            odds = provider.add_odds(
                market_id=data["market_id"],
                side=data["side"],
                odds_eu=data["odds_eu"],
            )
            
            return web.json_response({
                "status": "ok",
                "odds": odds.to_dict()
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=400)
    
    async def _handle_set_outcome(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
            provider = get_mock_provider()
            
            provider.set_outcome(
                event_id=data["event_id"],
                result=data["result"],
                home_score=data["home_score"],
                away_score=data["away_score"],
            )
            
            return web.json_response({
                "status": "ok",
                "message": f"Outcome set for event {data['event_id']}"
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=400)
    
    async def _handle_get_mock_state(self, request: web.Request) -> web.Response:
        provider = get_mock_provider()
        return web.json_response(provider.get_state())
    
    # --- Validator: Enhanced Mock Provider Handlers ---
    
    async def _handle_add_sportsbook(self, request: web.Request) -> web.Response:
        """Add or update a sportsbook configuration."""
        try:
            data = await request.json()
            provider = get_mock_provider()
            
            book = provider.add_sportsbook(
                code=data["code"],
                name=data["name"],
                is_sharp=data.get("is_sharp", False),
                vig=data.get("vig", 0.04),
                noise=data.get("noise", 0.02),
            )
            
            return web.json_response({
                "status": "ok",
                "sportsbook": book.to_dict()
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=400)
    
    async def _handle_get_sportsbooks(self, request: web.Request) -> web.Response:
        """List all configured sportsbooks."""
        provider = get_mock_provider()
        return web.json_response({
            "sportsbooks": [book.to_dict() for book in provider.sportsbooks.values()]
        })
    
    async def _handle_add_sportsbook_odds(self, request: web.Request) -> web.Response:
        """Add odds from a specific sportsbook (home and away)."""
        try:
            data = await request.json()
            provider = get_mock_provider()
            
            timestamp = None
            if "timestamp" in data:
                timestamp = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
            
            odds_list = provider.add_sportsbook_odds(
                market_id=data["market_id"],
                sportsbook_code=data["sportsbook_code"],
                home_odds=data["home_odds"],
                away_odds=data["away_odds"],
                timestamp=timestamp,
            )
            
            return web.json_response({
                "status": "ok",
                "odds": [o.to_dict() for o in odds_list]
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=400)
    
    async def _handle_generate_timeseries(self, request: web.Request) -> web.Response:
        """Generate time-series odds for a market across sportsbooks."""
        try:
            data = await request.json()
            provider = get_mock_provider()
            
            open_time = datetime.fromisoformat(data["open_time"].replace("Z", "+00:00"))
            close_time = datetime.fromisoformat(data["close_time"].replace("Z", "+00:00"))
            
            snapshots = provider.generate_odds_series(
                market_id=data["market_id"],
                true_prob_home=data["true_prob_home"],
                open_time=open_time,
                close_time=close_time,
                interval_hours=data.get("interval_hours", 6),
                sportsbook_codes=data.get("sportsbook_codes"),
                seed=data.get("seed"),
            )
            
            return web.json_response({
                "status": "ok",
                "snapshots_count": len(snapshots),
                "sportsbooks": list(set(s.sportsbook_code for s in snapshots)),
                "time_range": {
                    "start": open_time.isoformat(),
                    "end": close_time.isoformat(),
                },
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=400)
    
    async def _handle_get_closing_odds(self, request: web.Request) -> web.Response:
        """Get closing odds for a market."""
        try:
            provider = get_mock_provider()
            
            market_id = request.query.get("market_id")
            sportsbook_code = request.query.get("sportsbook_code")
            
            if not market_id:
                return web.json_response(
                    {"status": "error", "message": "market_id required"},
                    status=400
                )
            
            closing = provider.get_closing_odds(market_id, sportsbook_code)
            
            return web.json_response({
                "status": "ok",
                "closing_odds": [o.to_dict() for o in closing]
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=400)
    
    async def _handle_get_consensus(self, request: web.Request) -> web.Response:
        """Get consensus closing odds for a market."""
        try:
            provider = get_mock_provider()
            
            market_id = request.query.get("market_id")
            side = request.query.get("side", "HOME")
            
            if not market_id:
                return web.json_response(
                    {"status": "error", "message": "market_id required"},
                    status=400
                )
            
            consensus = provider.get_consensus_closing(market_id, side)
            
            if consensus is None:
                return web.json_response({
                    "status": "ok",
                    "consensus": None,
                    "message": "No odds found for market"
                })
            
            return web.json_response({
                "status": "ok",
                "consensus": consensus
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=400)
    
    # --- Validator: Trigger Handlers ---
    
    async def _handle_trigger_ingest(self, request: web.Request) -> web.Response:
        """Trigger provider ingest (pulls from mock provider)."""
        try:
            if not self.node:
                return web.json_response({"status": "error", "message": "No node reference"}, status=500)
            
            # Mock data is written directly via control endpoints.
            
            return web.json_response({
                "status": "ok",
                "message": "Ingest triggered (mock data already in DB)"
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_trigger_scoring(self, request: web.Request) -> web.Response:
        """Trigger the scoring process."""
        try:
            if not self.node or not hasattr(self.node, "handlers"):
                return web.json_response({"status": "error", "message": "No handlers"}, status=500)
            
            await self.node.handlers.main_score_handler.run()
            
            return web.json_response({
                "status": "ok",
                "message": "Scoring completed"
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_set_event_status(self, request: web.Request) -> web.Response:
        """Update event status in database."""
        try:
            data = await request.json()
            event_id = data["event_id"]
            new_status = data["status"]
            
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No database connection"}, status=500)
            
            from sqlalchemy import text
            async with self._db_engine.begin() as conn:
                await conn.execute(
                    text("UPDATE event SET status = :status WHERE event_id = :event_id"),
                    {"event_id": event_id, "status": new_status}
                )
            
            return web.json_response({
                "status": "ok",
                "message": f"Event {event_id} status set to {new_status}"
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    # --- Validator: Database Query Handlers ---
    
    async def _handle_get_submissions(self, request: web.Request) -> web.Response:
        """Get miner submissions from database."""
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No database connection"}, status=500)
            
            from sqlalchemy import text
            async with self._db_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT submission_id, miner_id, miner_hotkey, market_id, side,
                               submitted_at, odds_eu, imp_prob
                        FROM miner_submission
                        ORDER BY submitted_at DESC
                        LIMIT 100
                    """)
                )
                rows = result.mappings().all()
            
            submissions = [
                {
                    "submission_id": r["submission_id"],
                    "miner_id": r["miner_id"],
                    "miner_hotkey": r["miner_hotkey"],
                    "market_id": r["market_id"],
                    "side": r["side"],
                    "submitted_at": r["submitted_at"].isoformat() if r["submitted_at"] else None,
                    "odds_eu": float(r["odds_eu"]) if r["odds_eu"] else None,
                    "imp_prob": float(r["imp_prob"]) if r["imp_prob"] else None,
                }
                for r in rows
            ]
            
            return web.json_response({"submissions": submissions, "count": len(submissions)})
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_get_scores(self, request: web.Request) -> web.Response:
        """Get miner scores from database."""
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No database connection"}, status=500)
            
            from sqlalchemy import text
            async with self._db_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT miner_id, miner_hotkey, as_of, window_days,
                               n_submissions, es_mean, mes_mean, sos_mean, pss_mean, composite_score
                        FROM miner_rolling_score
                        ORDER BY as_of DESC
                        LIMIT 100
                    """)
                )
                rows = result.mappings().all()
            
            def _to_float(v):
                return float(v) if v is not None else None
            
            scores = [
                {
                    "miner_id": r["miner_id"],
                    "miner_hotkey": r["miner_hotkey"],
                    "as_of": r["as_of"].isoformat() if r["as_of"] else None,
                    "window_days": r["window_days"],
                    "n_submissions": r["n_submissions"],
                    "es_mean": _to_float(r["es_mean"]),
                    "mes_mean": _to_float(r["mes_mean"]),
                    "sos_mean": _to_float(r["sos_mean"]),
                    "pss_mean": _to_float(r["pss_mean"]),
                    "composite_score": _to_float(r["composite_score"]),
                }
                for r in rows
            ]
            
            return web.json_response({"scores": scores, "count": len(scores)})
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_sync_miners(self, request: web.Request) -> web.Response:
        """Sync miners from metagraph to database.
        
        This is needed because the validator looks up miner_id from the database,
        not directly from the metagraph.
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No database connection"}, status=500)
            
            if not self.node:
                return web.json_response({"status": "error", "message": "No node"}, status=500)
            
            metagraph = getattr(self.node, "metagraph", None)
            if metagraph is None:
                return web.json_response({"status": "error", "message": "No metagraph"}, status=500)
            
            hotkeys = list(metagraph.hotkeys)
            axons = list(metagraph.axons)
            uids = list(range(len(hotkeys)))
            
            inserted = 0
            updated = 0
            
            from sqlalchemy import text
            async with self._db_engine.begin() as conn:
                for uid, hotkey, axon in zip(uids, hotkeys, axons):
                    # Skip UIDs with port 0 (not serving)
                    port = getattr(axon, "port", 0)
                    if port == 0:
                        continue
                    
                    coldkey = getattr(axon, "coldkey", hotkey)
                    netuid = getattr(metagraph, "netuid", 2)
                    
                    # Upsert miner with all required columns
                    result = await conn.execute(
                        text("""
                            INSERT INTO miner (
                                hotkey, coldkey, uid, netuid, active, stake, stake_dict, total_stake,
                                rank, emission, incentive, consensus, trust, validator_trust,
                                dividends, last_update, validator_permit, pruning_score, is_null
                            )
                            VALUES (
                                :hotkey, :coldkey, :uid, :netuid, 1, 0, '{}'::jsonb, 0,
                                0, 0, 0, 0, 0, 0,
                                0, 0, false, 0, false
                            )
                            ON CONFLICT (hotkey) DO UPDATE SET
                                uid = EXCLUDED.uid,
                                active = 1
                            RETURNING (xmax = 0) AS inserted
                        """),
                        {"hotkey": hotkey, "coldkey": coldkey, "uid": uid, "netuid": netuid}
                    )
                    row = result.fetchone()
                    if row and row[0]:
                        inserted += 1
                    else:
                        updated += 1
            
            return web.json_response({
                "status": "ok",
                "inserted": inserted,
                "updated": updated,
                "message": f"Synced {inserted + updated} miners from metagraph"
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    # --- Miner: Action Handlers ---
    
    async def _handle_fetch_games(self, request: web.Request) -> web.Response:
        """Trigger miner to fetch game data from validator."""
        try:
            if not self.node:
                return web.json_response({"status": "error", "message": "No node"}, status=500)
            
            # Get the miner's validator client and trigger sync
            if hasattr(self.node, "game_sync"):
                success = await self.node.game_sync.sync_once()
                return web.json_response({
                    "status": "ok" if success else "error",
                    "message": "Game sync completed" if success else "Game sync failed"
                })
            
            return web.json_response({
                "status": "error",
                "message": "Miner has no game_sync service"
            }, status=500)
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_submit_odds(self, request: web.Request) -> web.Response:
        """Trigger miner to submit odds to validator."""
        try:
            data = await request.json()
            
            if not self.node:
                return web.json_response({"status": "error", "message": "No node"}, status=500)
            
            if hasattr(self.node, "validator_client"):
                # Use token from request, or auto-attach from stored validator endpoint
                token = data.get("token")
                if not token:
                    endpoint = getattr(self.node, "validator_endpoint", None) or {}
                    if isinstance(endpoint, dict):
                        token = endpoint.get("token")
                payload = {
                    "submissions": data.get("submissions", []),
                    "token": token,
                }
                success = await self.node.validator_client.submit_odds(payload)
                return web.json_response({
                    "status": "ok" if success else "error",
                    "message": "Odds submitted" if success else "Odds submission failed"
                })
            
            return web.json_response({
                "status": "error",
                "message": "Miner has no validator_client"
            }, status=500)
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_submit_outcome(self, request: web.Request) -> web.Response:
        """Trigger miner to submit outcome to validator."""
        try:
            data = await request.json()
            
            if not self.node:
                return web.json_response({"status": "error", "message": "No node"}, status=500)
            
            if hasattr(self.node, "validator_client"):
                success = await self.node.validator_client.submit_outcome(data)
                return web.json_response({
                    "status": "ok" if success else "error",
                    "message": "Outcome submitted" if success else "Outcome submission failed"
                })
            
            return web.json_response({
                "status": "error",
                "message": "Miner has no validator_client"
            }, status=500)
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_get_games(self, request: web.Request) -> web.Response:
        """Get validator endpoints from miner's database.
        
        Note: Miner doesn't store events locally - it receives them on-demand
        from the validator via GAME_DATA_REQUEST synapse.
        This endpoint returns known validator endpoints instead.
        """
        try:
            # Miner uses SQLite - need to create connection in this loop
            from sqlalchemy.ext.asyncio import create_async_engine
            from sqlalchemy import text
            
            # Build SQLite path
            project_root = os.getenv("PROJECT_ROOT", os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            db_path = os.path.join(project_root, "sparket", "data", "test", "miner.db")
            
            if not os.path.exists(db_path):
                return web.json_response({
                    "validator_endpoints": [],
                    "count": 0,
                    "note": "Miner database not initialized"
                })
            
            engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
            
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT * FROM validator_endpoint ORDER BY last_seen DESC LIMIT 100"))
                rows = result.mappings().all()
            
            await engine.dispose()
            
            # Convert datetime objects to strings
            endpoints = []
            for r in rows:
                ep = dict(r)
                for k, v in ep.items():
                    if hasattr(v, 'isoformat'):
                        ep[k] = v.isoformat()
                endpoints.append(ep)
            
            return web.json_response({
                "validator_endpoints": endpoints,
                "count": len(endpoints),
                "note": "Miner stores validator endpoints, not game events"
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    # --- Validator: Admin Handlers ---
    
    async def _handle_wipe_db(self, request: web.Request) -> web.Response:
        """Wipe test database tables for fresh test run.
        
        Truncates data tables but preserves schema and reference data.
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            from sqlalchemy import text
            
            # Tables to truncate in dependency order (children first)
            tables_to_truncate = [
                "submission_vs_close",
                "submission_outcome_score",
                "miner_submission",
                "miner_rolling_score",
                "miner_market_stats",
                "ground_truth_closing",
                "ground_truth_snapshot",
                "provider_closing",
                "provider_quote",
                "outcome",
                "market",
                "event",
                "sportsbook_bias",
                "sportsbook",
            ]
            
            async with self._db_engine.begin() as conn:
                for table in tables_to_truncate:
                    try:
                        await conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                    except Exception:
                        # Table might not exist or be empty
                        pass
            
            return web.json_response({
                "status": "ok",
                "message": f"Truncated {len(tables_to_truncate)} tables"
            })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_add_provider_quote(self, request: web.Request) -> web.Response:
        """Add mock provider quote data for ground truth computation.
        
        Expected body:
        {
            "market_id": int,
            "side": "HOME" | "AWAY" | "OVER" | "UNDER",
            "odds_eu": float (decimal odds like 1.85),
            "ts": ISO timestamp (optional, defaults to now),
            "sportsbook_id": int (optional, defaults to 1)
        }
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            data = await request.json()
            from sqlalchemy import text
            
            market_id = data["market_id"]
            side = data["side"].upper()
            odds_eu = float(data["odds_eu"])
            imp_prob = 1.0 / odds_eu if odds_eu > 0 else 0.5
            ts = datetime.fromisoformat(data["ts"].replace("Z", "+00:00")) if "ts" in data else datetime.now(timezone.utc)
            provider_id = data.get("provider_id", 1)  # Default to provider_id 1
            
            async with self._db_engine.begin() as conn:
                # Ensure provider exists
                await conn.execute(text("""
                    INSERT INTO provider (provider_id, name, code)
                    VALUES (:id, 'Mock Provider', 'MOCK')
                    ON CONFLICT (provider_id) DO NOTHING
                """), {"id": provider_id})
                
                # Ensure sportsbook exists (linked to provider)
                await conn.execute(text("""
                    INSERT INTO sportsbook (sportsbook_id, provider_id, code, name, is_sharp, active, created_at)
                    VALUES (1, :provider_id, 'MOCK', 'Mock Sportsbook', true, true, NOW())
                    ON CONFLICT (sportsbook_id) DO NOTHING
                """), {"provider_id": provider_id})
                
                # Get sport_id for this market's league to create bias entry
                sport_result = await conn.execute(text("""
                    SELECT l.sport_id
                    FROM market m
                    JOIN event e ON m.event_id = e.event_id
                    JOIN league l ON e.league_id = l.league_id
                    WHERE m.market_id = :market_id
                """), {"market_id": market_id})
                sport_row = sport_result.fetchone()
                sport_id = sport_row[0] if sport_row else 1
                
                # Get market kind
                kind_result = await conn.execute(text("""
                    SELECT kind FROM market WHERE market_id = :market_id
                """), {"market_id": market_id})
                kind_row = kind_result.fetchone()
                market_kind = kind_row[0] if kind_row else 'MONEYLINE'
                
                # Ensure sportsbook_bias exists (needed for consensus computation)
                await conn.execute(text("""
                    INSERT INTO sportsbook_bias (
                        sportsbook_id, sport_id, market_kind, 
                        bias_factor, variance, sample_count, version, computed_at
                    )
                    VALUES (1, :sport_id, :market_kind, 1.0, 0.01, 100, 1, NOW())
                    ON CONFLICT (sportsbook_id, sport_id, market_kind) DO NOTHING
                """), {"sport_id": sport_id, "market_kind": market_kind})
                
                # Insert the quote
                await conn.execute(text("""
                    INSERT INTO provider_quote (provider_id, market_id, ts, side, odds_eu, imp_prob, imp_prob_norm, raw)
                    VALUES (:provider_id, :market_id, :ts, :side, :odds_eu, :imp_prob, :imp_prob_norm, :raw)
                """), {
                    "provider_id": provider_id,
                    "market_id": market_id,
                    "ts": ts,
                    "side": side,
                    "odds_eu": odds_eu,
                    "imp_prob": imp_prob,
                    "imp_prob_norm": imp_prob,  # Simplified: assume normalized
                    "raw": json.dumps({"mock": True}),
                })
            
            return web.json_response({
                "status": "ok",
                "quote": {
                    "market_id": market_id,
                    "side": side,
                    "odds_eu": odds_eu,
                    "imp_prob": imp_prob,
                    "ts": ts.isoformat(),
                }
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_trigger_snapshot_pipeline(self, request: web.Request) -> web.Response:
        """Trigger the full ground truth snapshot pipeline.
        
        This runs the SnapshotPipeline which:
        1. Computes consensus from provider quotes with bias adjustment
        2. Creates ground_truth_snapshot entries
        3. Marks closing snapshots for markets about to start
        4. Populates ground_truth_closing from closing snapshots
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            from sparket.validator.scoring.ground_truth.snapshot_pipeline import SnapshotPipeline
            import bittensor as bt
            
            # Create a DB wrapper for the pipeline that uses our engine
            class DBWrapper:
                def __init__(self, engine):
                    self._engine = engine
                
                async def read(self, query, params=None, mappings=False):
                    async with self._engine.connect() as conn:
                        result = await conn.execute(query, params or {})
                        if mappings:
                            return result.mappings().all()
                        return result.fetchall()
                
                async def write(self, query, params=None, return_rows=False, mappings=False):
                    async with self._engine.begin() as conn:
                        result = await conn.execute(query, params or {})
                        if return_rows:
                            if mappings:
                                return result.mappings().all()
                            return result.fetchall()
                        return None
            
            db = DBWrapper(self._db_engine)
            pipeline = SnapshotPipeline(db=db, logger=bt.logging)
            
            # Run the full pipeline
            snapshot_count = await pipeline.run_snapshot_cycle()
            closing_count = await pipeline.capture_closing_snapshots()
            ground_truth_count = await pipeline.populate_ground_truth_closing()
            
            return web.json_response({
                "status": "ok",
                "snapshots_created": snapshot_count,
                "closing_snapshots": closing_count,
                "ground_truth_closing": ground_truth_count,
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _handle_trigger_odds_scoring(self, request: web.Request) -> web.Response:
        """Compute CLV/CLE scores for submissions by matching with ground truth.
        
        For testing, this directly computes and inserts submission_vs_close records
        using the control API's own database connection to avoid event loop issues.
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            from datetime import datetime, timedelta, timezone
            from decimal import Decimal
            from sqlalchemy import text
            
            now = datetime.now(timezone.utc)
            since = now - timedelta(days=7)
            
            # Fetch unscored submissions with matching ground truth
            async with self._db_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT 
                            ms.submission_id,
                            ms.market_id,
                            ms.side,
                            ms.submitted_at,
                            ms.odds_eu as miner_odds,
                            ms.imp_prob as miner_prob,
                            gtc.odds_consensus as close_odds,
                            gtc.prob_consensus as close_prob,
                            e.start_time_utc
                        FROM miner_submission ms
                        JOIN ground_truth_closing gtc ON ms.market_id = gtc.market_id AND ms.side = gtc.side
                        JOIN market m ON ms.market_id = m.market_id
                        JOIN event e ON m.event_id = e.event_id
                        LEFT JOIN submission_vs_close svc ON ms.submission_id = svc.submission_id
                        WHERE svc.submission_id IS NULL
                          AND ms.submitted_at >= :since
                        ORDER BY ms.submission_id
                        LIMIT 1000
                    """),
                    {"since": since}
                )
                rows = result.mappings().all()
            
            if not rows:
                return web.json_response({"status": "ok", "submissions_scored": 0, "message": "No unscored submissions found"})
            
            scored = 0
            async with self._db_engine.begin() as conn:
                for row in rows:
                    try:
                        miner_odds = Decimal(str(row["miner_odds"]))
                        close_odds = Decimal(str(row["close_odds"]))
                        miner_prob = Decimal(str(row["miner_prob"]))
                        close_prob = Decimal(str(row["close_prob"]))
                        
                        # Compute CLV (simplified: odds difference)
                        clv_odds = float(close_odds - miner_odds)
                        clv_prob = float(miner_prob - close_prob)
                        
                        # Compute CLE (simplified: relative improvement)
                        if close_prob > 0:
                            cle = clv_prob / float(close_prob)
                        else:
                            cle = 0.0
                        
                        # Minutes to close
                        submitted_at = row["submitted_at"]
                        start_time = row["start_time_utc"]
                        if submitted_at and start_time:
                            minutes_to_close = (start_time - submitted_at).total_seconds() / 60
                        else:
                            minutes_to_close = 0
                        
                        await conn.execute(
                            text("""
                                INSERT INTO submission_vs_close (
                                    submission_id, provider_basis, close_ts, close_odds_eu,
                                    close_imp_prob, close_imp_prob_norm, clv_odds, clv_prob,
                                    cle, minutes_to_close, computed_at, ground_truth_version
                                ) VALUES (
                                    :submission_id, 'consensus', :close_ts, :close_odds_eu,
                                    :close_imp_prob, :close_imp_prob_norm, :clv_odds, :clv_prob,
                                    :cle, :minutes_to_close, :computed_at, 1
                                )
                                ON CONFLICT (submission_id) DO NOTHING
                            """),
                            {
                                "submission_id": row["submission_id"],
                                "close_ts": now,
                                "close_odds_eu": float(close_odds),
                                "close_imp_prob": float(close_prob),
                                "close_imp_prob_norm": float(close_prob),
                                "clv_odds": clv_odds,
                                "clv_prob": clv_prob,
                                "cle": cle,
                                "minutes_to_close": minutes_to_close,
                                "computed_at": now,
                            }
                        )
                        scored += 1
                    except Exception as e:
                        print(f"Error scoring submission {row['submission_id']}: {e}")
            
            return web.json_response({
                "status": "ok",
                "submissions_scored": scored,
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _handle_trigger_outcome_scoring(self, request: web.Request) -> web.Response:
        """Compute Brier/PSS scores for submissions after outcomes are known.
        
        For testing, this directly computes and inserts submission_outcome_score records
        using the control API's own database connection to avoid event loop issues.
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            from datetime import datetime, timedelta, timezone
            from decimal import Decimal
            from sqlalchemy import text
            import math
            
            now = datetime.now(timezone.utc)
            since = now - timedelta(days=7)
            
            # Fetch submissions with settled outcomes and ground truth
            async with self._db_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT 
                            ms.submission_id,
                            ms.market_id,
                            ms.side,
                            ms.imp_prob as miner_prob,
                            o.result as outcome_result,
                            o.settled_at,
                            gtc.prob_consensus as gt_prob
                        FROM miner_submission ms
                        JOIN outcome o ON ms.market_id = o.market_id
                        JOIN ground_truth_closing gtc ON ms.market_id = gtc.market_id AND ms.side = gtc.side
                        LEFT JOIN submission_outcome_score sos ON ms.submission_id = sos.submission_id
                        WHERE o.result IS NOT NULL
                          AND o.settled_at IS NOT NULL
                          AND sos.submission_id IS NULL
                          AND o.settled_at >= :since
                        ORDER BY ms.submission_id
                        LIMIT 1000
                    """),
                    {"since": since}
                )
                rows = result.mappings().all()
            
            if not rows:
                return web.json_response({"status": "ok", "submissions_scored": 0, "message": "No unsettled submissions found"})
            
            scored = 0
            async with self._db_engine.begin() as conn:
                for row in rows:
                    try:
                        miner_prob = float(row["miner_prob"])
                        gt_prob = float(row["gt_prob"])
                        outcome_result = row["outcome_result"]
                        side = row["side"]
                        
                        # Compute outcome (1 if side matches result, 0 otherwise)
                        outcome = 1.0 if side.upper() == outcome_result.upper() else 0.0
                        
                        # Brier score: (p - o)^2
                        brier = (miner_prob - outcome) ** 2
                        provider_brier = (gt_prob - outcome) ** 2
                        
                        # Log loss: -[o*log(p) + (1-o)*log(1-p)]
                        eps = 1e-10
                        p = max(eps, min(1 - eps, miner_prob))
                        gt_p = max(eps, min(1 - eps, gt_prob))
                        logloss = -(outcome * math.log(p) + (1 - outcome) * math.log(1 - p))
                        provider_logloss = -(outcome * math.log(gt_p) + (1 - outcome) * math.log(1 - gt_p))
                        
                        # PSS: 1 - (miner_score / provider_score)
                        pss_brier = 1 - (brier / max(eps, provider_brier)) if provider_brier > eps else 0
                        pss_log = 1 - (logloss / max(eps, provider_logloss)) if provider_logloss > eps else 0
                        pss = (pss_brier + pss_log) / 2
                        
                        await conn.execute(
                            text("""
                                INSERT INTO submission_outcome_score (
                                    submission_id, brier, logloss, provider_brier, provider_logloss,
                                    pss, pss_brier, pss_log, outcome_vector, settled_at
                                ) VALUES (
                                    :submission_id, :brier, :logloss, :provider_brier, :provider_logloss,
                                    :pss, :pss_brier, :pss_log, :outcome_vector, :settled_at
                                )
                                ON CONFLICT (submission_id) DO NOTHING
                            """),
                            {
                                "submission_id": row["submission_id"],
                                "brier": brier,
                                "logloss": logloss,
                                "provider_brier": provider_brier,
                                "provider_logloss": provider_logloss,
                                "pss": pss,
                                "pss_brier": pss_brier,
                                "pss_log": pss_log,
                                "outcome_vector": f"[{outcome}]",
                                "settled_at": row["settled_at"],
                            }
                        )
                        scored += 1
                    except Exception as e:
                        print(f"Error scoring submission {row['submission_id']}: {e}")
            
            return web.json_response({
                "status": "ok",
                "submissions_scored": scored,
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _handle_add_ground_truth_closing(self, request: web.Request) -> web.Response:
        """Directly add ground truth closing records for testing.
        
        Request body:
        {
            "market_id": 123,
            "sides": [
                {"side": "HOME", "prob_consensus": 0.55, "odds_consensus": 1.82},
                {"side": "AWAY", "prob_consensus": 0.45, "odds_consensus": 2.22}
            ]
        }
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            data = await request.json()
            market_id = data["market_id"]
            sides = data["sides"]
            
            from datetime import datetime, timezone
            from sqlalchemy import text
            
            now = datetime.now(timezone.utc)
            
            async with self._db_engine.begin() as conn:
                for side_data in sides:
                    await conn.execute(
                        text("""
                            INSERT INTO ground_truth_closing (
                                market_id, side, prob_consensus, odds_consensus,
                                contributing_books, computed_at, bias_version
                            ) VALUES (
                                :market_id, :side, :prob_consensus, :odds_consensus,
                                :contributing_books, :computed_at, :bias_version
                            )
                            ON CONFLICT (market_id, side) DO UPDATE SET
                                prob_consensus = EXCLUDED.prob_consensus,
                                odds_consensus = EXCLUDED.odds_consensus,
                                computed_at = EXCLUDED.computed_at
                        """),
                        {
                            "market_id": market_id,
                            "side": side_data["side"],
                            "prob_consensus": side_data["prob_consensus"],
                            "odds_consensus": side_data["odds_consensus"],
                            "contributing_books": side_data.get("contributing_books", 3),
                            "computed_at": now,
                            "bias_version": 1,
                        }
                    )
            
            return web.json_response({
                "status": "ok",
                "market_id": market_id,
                "sides_added": len(sides),
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _handle_add_ground_truth_snapshot(self, request: web.Request) -> web.Response:
        """Add a single ground truth snapshot for testing.
        
        Request body:
        {
            "market_id": 123,
            "side": "HOME",
            "snapshot_ts": "2026-01-20T12:00:00Z",
            "prob_consensus": 0.55,
            "odds_consensus": 1.82,
            "contributing_books": 3,
            "is_closing": false
        }
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            data = await request.json()
            
            from datetime import datetime, timezone
            from sqlalchemy import text
            
            snapshot_ts = datetime.fromisoformat(data["snapshot_ts"].replace("Z", "+00:00"))
            
            async with self._db_engine.begin() as conn:
                await conn.execute(
                    text("""
                        INSERT INTO ground_truth_snapshot (
                            market_id, side, snapshot_ts, prob_consensus, odds_consensus,
                            contributing_books, std_dev, bias_version, is_closing
                        ) VALUES (
                            :market_id, :side, :snapshot_ts, :prob_consensus, :odds_consensus,
                            :contributing_books, :std_dev, :bias_version, :is_closing
                        )
                        ON CONFLICT (market_id, side, snapshot_ts) DO UPDATE SET
                            prob_consensus = EXCLUDED.prob_consensus,
                            odds_consensus = EXCLUDED.odds_consensus
                    """),
                    {
                        "market_id": data["market_id"],
                        "side": data["side"],
                        "snapshot_ts": snapshot_ts,
                        "prob_consensus": data["prob_consensus"],
                        "odds_consensus": data["odds_consensus"],
                        "contributing_books": data.get("contributing_books", 3),
                        "std_dev": data.get("std_dev", 0.01),
                        "bias_version": data.get("bias_version", 1),
                        "is_closing": data.get("is_closing", False),
                    }
                )
            
            return web.json_response({
                "status": "ok",
                "market_id": data["market_id"],
                "snapshot_ts": snapshot_ts.isoformat(),
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _handle_seed_ground_truth(self, request: web.Request) -> web.Response:
        """Seed ground truth tables from mock provider time-series data.
        
        This computes consensus from the mock provider's time-series odds
        and populates both ground_truth_snapshot and ground_truth_closing tables.
        
        Request body:
        {
            "market_id": "uuid-string",  // MockProvider market_id
            "db_market_id": 123          // Database market_id
        }
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            data = await request.json()
            mock_market_id = data["market_id"]
            db_market_id = data["db_market_id"]
            
            provider = get_mock_provider()
            
            from datetime import datetime, timezone
            from sqlalchemy import text
            
            # Get all time series keys for this market
            series_keys = [k for k in provider.time_series.keys() if k.startswith(f"{mock_market_id}:")]
            
            if not series_keys:
                return web.json_response({
                    "status": "error",
                    "message": f"No time series found for market {mock_market_id}"
                }, status=404)
            
            # Group snapshots by timestamp to compute consensus
            snapshots_by_ts: dict[datetime, list] = {}
            for key in series_keys:
                for snap in provider.time_series[key]:
                    snapshots_by_ts.setdefault(snap.timestamp, []).append(snap)
            
            # Sort timestamps
            sorted_timestamps = sorted(snapshots_by_ts.keys())
            
            if not sorted_timestamps:
                return web.json_response({
                    "status": "error",
                    "message": "No snapshots in time series"
                }, status=400)
            
            snapshots_inserted = 0
            
            async with self._db_engine.begin() as conn:
                for ts in sorted_timestamps:
                    snaps = snapshots_by_ts[ts]
                    is_closing = (ts == sorted_timestamps[-1])
                    
                    # Compute consensus for HOME
                    home_probs = [s.home_prob for s in snaps]
                    home_avg = sum(home_probs) / len(home_probs)
                    home_odds = 1.0 / home_avg if home_avg > 0 else 100.0
                    
                    # Compute consensus for AWAY
                    away_probs = [s.away_prob for s in snaps]
                    away_avg = sum(away_probs) / len(away_probs)
                    away_odds = 1.0 / away_avg if away_avg > 0 else 100.0
                    
                    # Insert HOME snapshot
                    await conn.execute(
                        text("""
                            INSERT INTO ground_truth_snapshot (
                                market_id, side, snapshot_ts, prob_consensus, odds_consensus,
                                contributing_books, std_dev, bias_version, is_closing
                            ) VALUES (
                                :market_id, 'HOME', :snapshot_ts, :prob_consensus, :odds_consensus,
                                :contributing_books, :std_dev, :bias_version, :is_closing
                            )
                            ON CONFLICT (market_id, side, snapshot_ts) DO UPDATE SET
                                prob_consensus = EXCLUDED.prob_consensus,
                                odds_consensus = EXCLUDED.odds_consensus,
                                is_closing = EXCLUDED.is_closing
                        """),
                        {
                            "market_id": db_market_id,
                            "snapshot_ts": ts,
                            "prob_consensus": round(home_avg, 8),
                            "odds_consensus": round(home_odds, 4),
                            "contributing_books": len(snaps),
                            "std_dev": 0.01,
                            "bias_version": 1,
                            "is_closing": is_closing,
                        }
                    )
                    
                    # Insert AWAY snapshot
                    await conn.execute(
                        text("""
                            INSERT INTO ground_truth_snapshot (
                                market_id, side, snapshot_ts, prob_consensus, odds_consensus,
                                contributing_books, std_dev, bias_version, is_closing
                            ) VALUES (
                                :market_id, 'AWAY', :snapshot_ts, :prob_consensus, :odds_consensus,
                                :contributing_books, :std_dev, :bias_version, :is_closing
                            )
                            ON CONFLICT (market_id, side, snapshot_ts) DO UPDATE SET
                                prob_consensus = EXCLUDED.prob_consensus,
                                odds_consensus = EXCLUDED.odds_consensus,
                                is_closing = EXCLUDED.is_closing
                        """),
                        {
                            "market_id": db_market_id,
                            "snapshot_ts": ts,
                            "prob_consensus": round(away_avg, 8),
                            "odds_consensus": round(away_odds, 4),
                            "contributing_books": len(snaps),
                            "std_dev": 0.01,
                            "bias_version": 1,
                            "is_closing": is_closing,
                        }
                    )
                    
                    snapshots_inserted += 2
                    
                    # If closing, also insert ground_truth_closing
                    if is_closing:
                        await conn.execute(
                            text("""
                                INSERT INTO ground_truth_closing (
                                    market_id, side, prob_consensus, odds_consensus,
                                    contributing_books, computed_at, bias_version
                                ) VALUES (
                                    :market_id, 'HOME', :prob_consensus, :odds_consensus,
                                    :contributing_books, :computed_at, :bias_version
                                )
                                ON CONFLICT (market_id, side) DO UPDATE SET
                                    prob_consensus = EXCLUDED.prob_consensus,
                                    odds_consensus = EXCLUDED.odds_consensus
                            """),
                            {
                                "market_id": db_market_id,
                                "prob_consensus": round(home_avg, 8),
                                "odds_consensus": round(home_odds, 4),
                                "contributing_books": len(snaps),
                                "computed_at": ts,
                                "bias_version": 1,
                            }
                        )
                        await conn.execute(
                            text("""
                                INSERT INTO ground_truth_closing (
                                    market_id, side, prob_consensus, odds_consensus,
                                    contributing_books, computed_at, bias_version
                                ) VALUES (
                                    :market_id, 'AWAY', :prob_consensus, :odds_consensus,
                                    :contributing_books, :computed_at, :bias_version
                                )
                                ON CONFLICT (market_id, side) DO UPDATE SET
                                    prob_consensus = EXCLUDED.prob_consensus,
                                    odds_consensus = EXCLUDED.odds_consensus
                            """),
                            {
                                "market_id": db_market_id,
                                "prob_consensus": round(away_avg, 8),
                                "odds_consensus": round(away_odds, 4),
                                "contributing_books": len(snaps),
                                "computed_at": ts,
                                "bias_version": 1,
                            }
                        )
            
            return web.json_response({
                "status": "ok",
                "db_market_id": db_market_id,
                "snapshots_inserted": snapshots_inserted,
                "closing_lines_set": True,
                "time_range": {
                    "start": sorted_timestamps[0].isoformat(),
                    "end": sorted_timestamps[-1].isoformat(),
                },
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _handle_add_outcome(self, request: web.Request) -> web.Response:
        """Directly add outcome records for testing.
        
        Request body:
        {
            "market_id": 123,
            "result": "HOME",   # or "AWAY", "DRAW", "OVER", "UNDER"
            "score_home": 2,
            "score_away": 1
        }
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            data = await request.json()
            market_id = data["market_id"]
            result = data["result"].upper()  # Postgres enum uses uppercase (HOME/AWAY/DRAW)
            score_home = data.get("score_home", 0)
            score_away = data.get("score_away", 0)
            
            from datetime import datetime, timezone
            from sqlalchemy import text
            
            now = datetime.now(timezone.utc)
            
            async with self._db_engine.begin() as conn:
                # Also update the event status to 'finished'
                await conn.execute(
                    text("""
                        UPDATE event SET status = 'finished'
                        WHERE event_id = (SELECT event_id FROM market WHERE market_id = :market_id)
                    """),
                    {"market_id": market_id}
                )
                
                await conn.execute(
                    text("""
                        INSERT INTO outcome (
                            market_id, result, score_home, score_away, settled_at, details
                        ) VALUES (
                            :market_id, :result, :score_home, :score_away, :settled_at, :details
                        )
                        ON CONFLICT (market_id) DO UPDATE SET
                            result = EXCLUDED.result,
                            score_home = EXCLUDED.score_home,
                            score_away = EXCLUDED.score_away,
                            settled_at = EXCLUDED.settled_at
                    """),
                    {
                        "market_id": market_id,
                        "result": result,
                        "score_home": score_home,
                        "score_away": score_away,
                        "settled_at": now,
                        "details": "{}",  # Empty JSONB
                    }
                )
            
            return web.json_response({
                "status": "ok",
                "market_id": market_id,
                "result": result,
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def _handle_backdate_submissions(self, request: web.Request) -> web.Response:
        """Backdate all submissions to yesterday for testing scoring windows.
        
        The scoring window ends at today's midnight, so submissions made today
        won't be included. This endpoint shifts all submission timestamps back
        by the specified number of days (default: 1) to include them in scoring.
        
        Request body (optional):
        {
            "days_back": 1  # How many days to shift submissions back
        }
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            data = {}
            try:
                data = await request.json()
            except Exception:
                pass  # Empty body is fine
            
            days_back = data.get("days_back", 1)
            
            from sqlalchemy import text
            
            async with self._db_engine.begin() as conn:
                # Shift submission timestamps back
                result = await conn.execute(
                    text("""
                        UPDATE miner_submission
                        SET submitted_at = submitted_at - INTERVAL ':days days'
                        WHERE submitted_at > NOW() - INTERVAL '1 day'
                        RETURNING submission_id
                    """.replace(":days", str(int(days_back))))
                )
                updated_count = len(result.fetchall())
                
                # Also shift submission_vs_close timestamps
                await conn.execute(
                    text("""
                        UPDATE submission_vs_close
                        SET close_ts = close_ts - INTERVAL ':days days',
                            computed_at = computed_at - INTERVAL ':days days'
                        WHERE computed_at > NOW() - INTERVAL '1 day'
                    """.replace(":days", str(int(days_back))))
                )
                
                # Shift outcome settled_at timestamps
                await conn.execute(
                    text("""
                        UPDATE outcome
                        SET settled_at = settled_at - INTERVAL ':days days'
                        WHERE settled_at > NOW() - INTERVAL '1 day'
                    """.replace(":days", str(int(days_back))))
                )
                
                # Shift submission_outcome_score settled_at
                await conn.execute(
                    text("""
                        UPDATE submission_outcome_score
                        SET settled_at = settled_at - INTERVAL ':days days'
                        WHERE settled_at > NOW() - INTERVAL '1 day'
                    """.replace(":days", str(int(days_back))))
                )
            
            return web.json_response({
                "status": "ok",
                "submissions_backdated": updated_count,
                "days_back": days_back,
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    # --- Validator: Enhanced Scoring Inspection ---
    
    async def _handle_get_rolling_scores(self, request: web.Request) -> web.Response:
        """Get rolling aggregate scores from database."""
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            from sqlalchemy import text
            async with self._db_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT 
                            miner_id, miner_hotkey, as_of, window_days,
                            n_submissions, n_eff, 
                            fq_raw, brier_mean, pss_mean,
                            es_mean, es_std, es_adj, mes_mean,
                            cal_score, sharp_score, sos_score, lead_score
                        FROM miner_rolling_score
                        ORDER BY as_of DESC, miner_id
                        LIMIT 100
                    """)
                )
                rows = result.mappings().all()
            
            def _to_float(v):
                return float(v) if v is not None else None
            
            scores = [
                {
                    "miner_id": r["miner_id"],
                    "miner_hotkey": r["miner_hotkey"],
                    "as_of": r["as_of"].isoformat() if r["as_of"] else None,
                    "window_days": r["window_days"],
                    "n_submissions": r["n_submissions"],
                    "n_eff": _to_float(r["n_eff"]),
                    "fq_raw": _to_float(r["fq_raw"]),
                    "brier_mean": _to_float(r["brier_mean"]),
                    "pss_mean": _to_float(r["pss_mean"]),
                    "es_mean": _to_float(r["es_mean"]),
                    "es_std": _to_float(r["es_std"]),
                    "es_adj": _to_float(r["es_adj"]),
                    "mes_mean": _to_float(r["mes_mean"]),
                    "cal_score": _to_float(r["cal_score"]),
                    "sharp_score": _to_float(r["sharp_score"]),
                    "sos_score": _to_float(r["sos_score"]),
                    "lead_score": _to_float(r["lead_score"]),
                }
                for r in rows
            ]
            
            return web.json_response({"scores": scores, "count": len(scores)})
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_get_skill_scores(self, request: web.Request) -> web.Response:
        """Get final skill scores from database."""
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            from sqlalchemy import text
            async with self._db_engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT 
                            mrs.miner_id, mrs.miner_hotkey, m.uid,
                            mrs.as_of, mrs.window_days,
                            mrs.fq_score, mrs.edge_score, mrs.mes_score,
                            mrs.sos_score, mrs.lead_score,
                            mrs.forecast_dim, mrs.econ_dim, mrs.info_dim,
                            mrs.skill_score, mrs.score_version
                        FROM miner_rolling_score mrs
                        LEFT JOIN miner m ON mrs.miner_id = m.miner_id
                        WHERE mrs.skill_score IS NOT NULL
                        ORDER BY mrs.as_of DESC, mrs.skill_score DESC
                        LIMIT 100
                    """)
                )
                rows = result.mappings().all()
            
            def _to_float(v):
                return float(v) if v is not None else None
            
            scores = [
                {
                    "miner_id": r["miner_id"],
                    "miner_hotkey": r["miner_hotkey"],
                    "uid": r["uid"],
                    "as_of": r["as_of"].isoformat() if r["as_of"] else None,
                    "window_days": r["window_days"],
                    "fq_score": _to_float(r["fq_score"]),
                    "edge_score": _to_float(r["edge_score"]),
                    "mes_score": _to_float(r["mes_score"]),
                    "sos_score": _to_float(r["sos_score"]),
                    "lead_score": _to_float(r["lead_score"]),
                    "forecast_dim": _to_float(r["forecast_dim"]),
                    "econ_dim": _to_float(r["econ_dim"]),
                    "info_dim": _to_float(r["info_dim"]),
                    "skill_score": _to_float(r["skill_score"]),
                    "score_version": r["score_version"],
                }
                for r in rows
            ]
            
            return web.json_response({"scores": scores, "count": len(scores)})
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_get_weights(self, request: web.Request) -> web.Response:
        """Get computed weight array from skill scores.
        
        Returns the weight array that would be emitted to chain.
        """
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            from sqlalchemy import text
            import numpy as np
            
            # Get metagraph size from node if available
            n_neurons = 256  # Default max
            if self.node and hasattr(self.node, "metagraph"):
                n_neurons = self.node.metagraph.n
            
            async with self._db_engine.connect() as conn:
                # Get latest skill scores with UIDs
                result = await conn.execute(
                    text("""
                        SELECT m.uid, mrs.skill_score
                        FROM miner_rolling_score mrs
                        JOIN miner m ON mrs.miner_id = m.miner_id AND mrs.miner_hotkey = m.hotkey
                        WHERE mrs.skill_score IS NOT NULL
                          AND m.uid IS NOT NULL
                          AND m.active = 1
                        ORDER BY mrs.as_of DESC
                    """)
                )
                rows = result.mappings().all()
            
            # Build weight array
            weights = np.zeros(n_neurons, dtype=np.float32)
            uid_scores = {}
            
            for row in rows:
                uid = row["uid"]
                score = row["skill_score"]
                if uid is not None and 0 <= uid < n_neurons and score is not None:
                    if uid not in uid_scores:  # First (most recent) wins
                        uid_scores[uid] = float(score)
                        weights[uid] = float(score)
            
            # Normalize
            total = float(np.sum(weights))
            if total > 0:
                weights = weights / total
            
            # Build response - ensure all values are Python native types
            weight_list = [
                {"uid": int(uid), "weight": float(weights[uid]), "raw_score": float(uid_scores.get(uid, 0))}
                for uid in range(n_neurons)
                if float(weights[uid]) > 0
            ]
            
            return web.json_response({
                "weights": weight_list,
                "count": int(len(weight_list)),
                "total_weight": float(np.sum(weights)),
                "n_neurons": int(n_neurons),
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_trigger_weights(self, request: web.Request) -> web.Response:
        """Trigger weight emission from database scores."""
        try:
            if not self.node or not hasattr(self.node, "handlers"):
                return web.json_response({"status": "error", "message": "No handlers"}, status=500)
            
            # Run scoring with weight emission
            result = await self.node.handlers.main_score_handler.run(
                emit_weights=True,
                validator=self.node
            )
            
            return web.json_response({
                "status": "ok",
                "message": "Weights triggered",
                "result": result
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    # --- Validator: Health and Monitoring ---
    
    async def _handle_get_memory(self, request: web.Request) -> web.Response:
        """Get current process memory usage."""
        try:
            import resource
            import os
            import psutil
            
            # Get memory from resource module
            usage = resource.getrusage(resource.RUSAGE_SELF)
            max_rss_kb = usage.ru_maxrss  # Max resident set size in KB (Linux)
            
            # Get more detailed info from psutil if available
            try:
                process = psutil.Process(os.getpid())
                mem_info = process.memory_info()
                
                return web.json_response({
                    "status": "ok",
                    "memory": {
                        "rss_mb": mem_info.rss / (1024 * 1024),
                        "vms_mb": mem_info.vms / (1024 * 1024),
                        "max_rss_mb": max_rss_kb / 1024,
                        "percent": process.memory_percent(),
                    },
                    "pid": os.getpid(),
                })
            except ImportError:
                # psutil not available
                return web.json_response({
                    "status": "ok",
                    "memory": {
                        "max_rss_mb": max_rss_kb / 1024,
                    },
                    "pid": os.getpid(),
                })
        except Exception as e:
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def _handle_get_job_status(self, request: web.Request) -> web.Response:
        """Get status of all scoring jobs."""
        try:
            if not self._db_engine:
                return web.json_response({"status": "error", "message": "No DB engine"}, status=500)
            
            from sqlalchemy import text
            async with self._db_engine.connect() as conn:
                # Get job states
                result = await conn.execute(
                    text("""
                        SELECT 
                            job_id, status, last_checkpoint, 
                            items_total, items_processed,
                            started_at, completed_at, next_run_at,
                            error_count, last_error
                        FROM scoring_job_state
                        ORDER BY job_id
                    """)
                )
                job_rows = result.mappings().all()
                
                # Get worker heartbeats
                worker_result = await conn.execute(
                    text("""
                        SELECT 
                            worker_id, pid, hostname, started_at, last_heartbeat,
                            current_job, jobs_completed, jobs_failed, memory_mb
                        FROM scoring_worker_heartbeat
                        ORDER BY last_heartbeat DESC
                    """)
                )
                worker_rows = worker_result.mappings().all()
            
            def _ts_to_iso(ts):
                return ts.isoformat() if ts else None
            
            jobs = [
                {
                    "job_id": r["job_id"],
                    "status": r["status"],
                    "last_checkpoint": _ts_to_iso(r["last_checkpoint"]),
                    "items_total": r["items_total"],
                    "items_processed": r["items_processed"],
                    "started_at": _ts_to_iso(r["started_at"]),
                    "completed_at": _ts_to_iso(r["completed_at"]),
                    "next_run_at": _ts_to_iso(r["next_run_at"]),
                    "error_count": r["error_count"],
                    "last_error": r["last_error"],
                }
                for r in job_rows
            ]
            
            workers = [
                {
                    "worker_id": r["worker_id"],
                    "pid": r["pid"],
                    "hostname": r["hostname"],
                    "started_at": _ts_to_iso(r["started_at"]),
                    "last_heartbeat": _ts_to_iso(r["last_heartbeat"]),
                    "current_job": r["current_job"],
                    "jobs_completed": r["jobs_completed"],
                    "jobs_failed": r["jobs_failed"],
                    "memory_mb": r["memory_mb"],
                }
                for r in worker_rows
            ]
            
            return web.json_response({
                "jobs": jobs,
                "workers": workers,
                "job_count": len(jobs),
                "worker_count": len(workers),
            })
        except Exception as e:
            traceback.print_exc()
            return web.json_response({"status": "error", "message": str(e)}, status=500)
