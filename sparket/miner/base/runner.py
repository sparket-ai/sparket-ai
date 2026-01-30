"""BaseMiner - Main orchestration for the base miner.

This is the entry point for the base miner. It coordinates:
- Team stats fetching (ESPN)
- Market odds fetching (The-Odds-API, optional)
- Team strength calculation
- Odds generation and submission
- Outcome detection and submission
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import bittensor as bt

from sparket.miner.base.config import BaseMinerConfig
from sparket.miner.base.data.stats import TeamStats
from sparket.miner.base.engines.interface import OddsPrices
from sparket.miner.base.engines.naive import NaiveEngine
from sparket.miner.base.fetchers.espn import ESPNFetcher
from sparket.miner.base.model.strength import calculate_team_strength
from sparket.miner.base.model.matchup import strength_to_probability, probability_to_odds
from sparket.miner.base.model.blend import blend_odds_prices
from sparket.miner.base.utils.cache import TTLCache


class BaseMiner:
    """Self-contained base miner implementation.
    
    Generates odds using a team strength model and submits to validators.
    
    Features:
    - Fetches team stats from ESPN (free, no API key)
    - Optionally fetches market odds from The-Odds-API
    - Calculates team strength ratings
    - Blends market and model odds
    - Submits odds and outcomes to validators
    
    Usage:
        config = BaseMinerConfig.from_env()
        base_miner = BaseMiner(
            hotkey="5xxx...",
            config=config,
            validator_client=client,
            game_sync=sync,
        )
        await base_miner.start()
    """
    
    def __init__(
        self,
        hotkey: str,
        config: BaseMinerConfig,
        validator_client: Any,
        game_sync: Any,
        get_token: Optional[Callable[[], Optional[str]]] = None,
    ) -> None:
        """Initialize the base miner.
        
        Args:
            hotkey: Miner's hotkey (ss58 address)
            config: Configuration
            validator_client: Client for submitting to validators
            game_sync: GameDataSync for fetching markets
            get_token: Callback to get the current validator push token
        """
        self.hotkey = hotkey
        self.config = config
        self.validator_client = validator_client
        self.game_sync = game_sync
        self._get_token = get_token
        
        # Components
        self._espn = ESPNFetcher(cache_ttl_seconds=config.cache_ttl_seconds)
        self._naive = NaiveEngine(vig=config.vig)
        self._theodds = None  # Lazy init if API key available
        
        # State
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._stats_cache = TTLCache[TeamStats](ttl_seconds=config.stats_refresh_seconds)
        self._errors_count = 0
    
    @property
    def is_running(self) -> bool:
        """Whether the miner is currently running."""
        return self._running
    
    @property
    def errors_count(self) -> int:
        """Number of errors encountered."""
        return self._errors_count
    
    async def start(self) -> None:
        """Start the base miner background loops."""
        if self._running:
            return
        
        self._running = True
        bt.logging.info({"base_miner": "starting"})
        
        # Initialize TheOddsEngine if API key available
        if self.config.odds_api_key:
            try:
                from sparket.miner.base.engines.theodds import TheOddsEngine
                self._theodds = TheOddsEngine(
                    api_key=self.config.odds_api_key,
                    cache_ttl_seconds=self.config.cache_ttl_seconds,
                )
                bt.logging.info({"base_miner": "theodds_enabled"})
            except Exception as e:
                bt.logging.warning({"base_miner": "theodds_init_failed", "error": str(e)})
        
        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._odds_loop()),
            asyncio.create_task(self._outcome_loop()),
        ]
        
        bt.logging.info({"base_miner": "started"})
    
    async def stop(self) -> None:
        """Stop the base miner."""
        self._running = False
        
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self._tasks.clear()
        
        # Cleanup
        await self._espn.close()
        if self._theodds:
            await self._theodds.close()
        
        bt.logging.info({"base_miner": "stopped"})
    
    def start_background(self) -> None:
        """Start in background (non-blocking)."""
        asyncio.create_task(self.start())
    
    # -------------------------------------------------------------------------
    # Odds Generation
    # -------------------------------------------------------------------------
    
    async def generate_odds(self, market: Dict[str, Any]) -> Optional[OddsPrices]:
        """Generate odds for a market.
        
        Pipeline:
        1. Fetch team stats from ESPN
        2. Calculate team strengths
        3. Compute model probability (Log5)
        4. Optionally fetch market odds (The-Odds-API)
        5. Blend market and model
        
        Args:
            market: Market info dict
        
        Returns:
            OddsPrices or None if unable to generate
        """
        home_team = market.get("home_team", "")
        away_team = market.get("away_team", "")
        sport = market.get("sport", "NFL")
        kind = market.get("kind", "MONEYLINE")
        
        # 1. Get team stats
        home_stats = await self._get_team_stats(home_team, sport)
        away_stats = await self._get_team_stats(away_team, sport)
        
        if not home_stats or not away_stats:
            # Fall back to naive engine
            bt.logging.debug({
                "base_miner": "stats_unavailable",
                "home_team": home_team,
                "away_team": away_team,
            })
            return await self._naive.get_odds(market)
        
        # 2. Calculate team strengths
        home_strength = calculate_team_strength(
            home_stats, 
            at_home=True,
            weights=self.config.model_weights.to_dict(),
        )
        away_strength = calculate_team_strength(
            away_stats,
            at_home=False,
            weights=self.config.model_weights.to_dict(),
        )
        
        # 3. Compute model probability
        home_prob, away_prob = strength_to_probability(home_strength, away_strength)
        
        model_odds = OddsPrices(
            home_prob=home_prob,
            away_prob=away_prob,
            home_odds_eu=probability_to_odds(home_prob, self.config.vig),
            away_odds_eu=probability_to_odds(away_prob, self.config.vig),
        )
        
        # 4. Try to get market odds
        market_odds = None
        if self._theodds:
            try:
                market_odds = await self._theodds.get_odds(market)
            except Exception as e:
                bt.logging.debug({"base_miner": "theodds_fetch_failed", "error": str(e)})
        
        # 5. Blend
        final_odds = blend_odds_prices(
            market=market_odds,
            model=model_odds,
            market_weight=self.config.market_blend_weight,
            vig=self.config.vig,
        )
        
        bt.logging.debug({
            "base_miner": "odds_generated",
            "market_id": market.get("market_id"),
            "home_strength": round(home_strength, 3),
            "away_strength": round(away_strength, 3),
            "model_home_prob": round(home_prob, 3),
            "final_home_prob": round(final_odds.home_prob, 3),
            "has_market": market_odds is not None,
        })
        
        return final_odds
    
    async def _get_team_stats(self, team_code: str, sport: str) -> Optional[TeamStats]:
        """Get team stats with caching."""
        cache_key = f"{sport}:{team_code}"
        
        cached = self._stats_cache.get(cache_key)
        if cached is not None:
            return cached
        
        stats = await self._espn.get_team_stats(team_code, sport)
        if stats:
            self._stats_cache.set(cache_key, stats)
        
        return stats
    
    # -------------------------------------------------------------------------
    # Background Loops
    # -------------------------------------------------------------------------
    
    async def _odds_loop(self) -> None:
        """Background loop for odds submission."""
        while self._running:
            try:
                await self._run_odds_cycle()
            except Exception as e:
                self._errors_count += 1
                bt.logging.warning({"base_miner": "odds_cycle_error", "error": str(e)})
            
            await asyncio.sleep(self.config.odds_refresh_seconds)
    
    async def _run_odds_cycle(self) -> None:
        """Run one odds submission cycle with batching.
        
        Collects odds for all markets, then submits in batches to reduce
        network overhead. Batch size is configurable via config.batch_size.
        """
        # Get active markets from game sync
        markets = await self.game_sync.get_active_markets()
        
        if not markets:
            bt.logging.debug({"base_miner": "no_active_markets"})
            return
        
        # Collect all market odds first
        batch: List[Tuple[Dict[str, Any], OddsPrices]] = []
        skipped = 0
        batch_count = 0
        total_submitted = 0
        
        for market in markets:
            try:
                odds = await self.generate_odds(market)
                if odds is None:
                    skipped += 1
                    continue
                
                batch.append((market, odds))
                
                # Submit when batch is full
                if len(batch) >= self.config.batch_size:
                    payload = self._build_batch_payload(batch)
                    success = await self.validator_client.submit_odds(payload)
                    if success:
                        total_submitted += len(batch)
                    batch_count += 1
                    batch = []
                    
            except Exception as e:
                skipped += 1
                bt.logging.debug({
                    "base_miner": "market_odds_failed",
                    "market_id": market.get("market_id"),
                    "error": str(e),
                })
        
        # Submit remaining batch
        if batch:
            try:
                payload = self._build_batch_payload(batch)
                success = await self.validator_client.submit_odds(payload)
                if success:
                    total_submitted += len(batch)
                batch_count += 1
            except Exception as e:
                bt.logging.warning({
                    "base_miner": "final_batch_failed",
                    "batch_size": len(batch),
                    "error": str(e),
                })
        
        # Log cycle summary
        bt.logging.info({
            "base_miner_odds_cycle": {
                "markets": len(markets),
                "batches": batch_count,
                "submitted": total_submitted,
                "skipped": skipped,
            }
        })
    
    def _build_prices(self, market: Dict[str, Any], odds: OddsPrices) -> List[Dict[str, Any]]:
        """Build prices list for a single market submission.
        
        Args:
            market: Market info dict with 'kind' field
            odds: OddsPrices with probabilities and decimal odds
            
        Returns:
            List of price dicts with side, odds_eu, imp_prob
        """
        kind = market.get("kind", "MONEYLINE").upper()
        
        if kind in ("MONEYLINE", "SPREAD"):
            return [
                {"side": "home", "odds_eu": odds.home_odds_eu, "imp_prob": odds.home_prob},
                {"side": "away", "odds_eu": odds.away_odds_eu, "imp_prob": odds.away_prob},
            ]
        elif kind == "TOTAL":
            return [
                {"side": "over", "odds_eu": odds.over_odds_eu or odds.home_odds_eu, "imp_prob": odds.over_prob or odds.home_prob},
                {"side": "under", "odds_eu": odds.under_odds_eu or odds.away_odds_eu, "imp_prob": odds.under_prob or odds.away_prob},
            ]
        return []
    
    def _build_batch_payload(self, markets_with_odds: List[Tuple[Dict[str, Any], OddsPrices]]) -> Dict[str, Any]:
        """Build batched odds submission payload.
        
        Args:
            markets_with_odds: List of (market, odds) tuples
            
        Returns:
            Payload dict with multiple submissions
        """
        now = datetime.now(timezone.utc)
        
        submissions = []
        for market, odds in markets_with_odds:
            kind = market.get("kind", "MONEYLINE").upper()
            submissions.append({
                "market_id": int(market.get("market_id", 0)),
                "kind": kind.lower(),
                "priced_at": now.isoformat(),
                "prices": self._build_prices(market, odds),
            })
        
        payload: Dict[str, Any] = {
            "miner_hotkey": self.hotkey,
            "submissions": submissions,
        }
        
        # Include token for authentication
        if self._get_token is not None:
            token = self._get_token()
            if token:
                payload["token"] = token
        
        return payload
    
    def _build_odds_payload(self, market: Dict[str, Any], odds: OddsPrices) -> Dict[str, Any]:
        """Build odds submission payload for a single market.
        
        Convenience method that wraps _build_batch_payload for single-market submissions.
        """
        return self._build_batch_payload([(market, odds)])
    
    async def _outcome_loop(self) -> None:
        """Background loop for outcome submission."""
        while self._running:
            try:
                await self._run_outcome_cycle()
            except Exception as e:
                self._errors_count += 1
                bt.logging.warning({"base_miner": "outcome_cycle_error", "error": str(e)})
            
            await asyncio.sleep(self.config.outcome_check_seconds)
    
    async def _run_outcome_cycle(self) -> None:
        """Run one outcome submission cycle."""
        # Get potentially finished events
        events = await self._get_finished_events()
        
        if not events:
            return
        
        submitted = 0
        for event in events:
            try:
                result = await self._espn.get_result(event)
                if result is None or not result.is_final:
                    continue
                
                payload = self._build_outcome_payload(event, result)
                success = await self.validator_client.submit_outcome(payload)
                
                if success:
                    submitted += 1
                    
            except Exception as e:
                bt.logging.debug({
                    "base_miner": "outcome_submission_failed",
                    "event_id": event.get("event_id"),
                    "error": str(e),
                })
        
        if submitted > 0:
            bt.logging.info({"base_miner": "outcomes_submitted", "count": submitted})
    
    async def _get_finished_events(self) -> List[Dict[str, Any]]:
        """Get events that might be finished.
        
        Looks for events with start_time in the past.
        """
        from sparket.miner.database.repository import get_past_events
        
        try:
            if hasattr(self.game_sync, 'database'):
                return await get_past_events(self.game_sync.database)
        except Exception:
            pass
        
        return []
    
    def _build_outcome_payload(self, event: Dict[str, Any], result: Any) -> Dict[str, Any]:
        """Build outcome submission payload."""
        now = datetime.now(timezone.utc)
        
        payload: Dict[str, Any] = {
            "event_id": int(event.get("event_id", 0)),
            "miner_hotkey": self.hotkey,
            "result": result.winner,
            "score_home": result.home_score,
            "score_away": result.away_score,
            "ts_submit": now.isoformat(),
        }
        # Include token for authentication
        if self._get_token is not None:
            token = self._get_token()
            if token:
                payload["token"] = token
        return payload








