"""Configuration for the base miner."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ModelWeights:
    """Weights for the team strength model.
    
    These control how different factors contribute to
    the overall team strength rating.
    """
    season: float = 0.40       # Season win rate
    home_away: float = 0.20    # Home/away split
    recent_form: float = 0.25  # Last 5 games
    advanced: float = 0.15     # Point diff / ELO
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dict for model functions."""
        return {
            "season": self.season,
            "home_away": self.home_away,
            "recent_form": self.recent_form,
            "advanced": self.advanced,
        }


@dataclass
class BaseMinerConfig:
    """Configuration for the base miner.
    
    Can be set via environment variables with SPARKET_BASE_MINER__ prefix.
    
    Example:
        SPARKET_BASE_MINER__ENABLED=false
        SPARKET_BASE_MINER__ODDS_API_KEY=xxx
        SPARKET_BASE_MINER__ODDS_REFRESH_SECONDS=900
        SPARKET_BASE_MINER__BATCH_SIZE=50
    """
    
    # Master switch (enabled by default)
    enabled: bool = True
    
    # API keys (optional)
    odds_api_key: Optional[str] = None
    
    # Timing
    odds_refresh_seconds: int = 900      # 15 minutes
    outcome_check_seconds: int = 300     # 5 minutes
    stats_refresh_seconds: int = 3600    # 1 hour
    
    # Cache TTLs
    cache_ttl_seconds: int = 3600        # 1 hour
    
    # Batching - number of markets to batch per submission request
    batch_size: int = 50                 # Markets per batch (1-200)
    
    # Model parameters
    market_blend_weight: float = 0.60    # Trust market 60%
    vig: float = 0.045                   # Standard vig
    
    # Model weights
    model_weights: ModelWeights = field(default_factory=ModelWeights)
    
    @classmethod
    def from_env(cls) -> "BaseMinerConfig":
        """Load configuration from environment variables.
        
        Environment variable names:
        - SPARKET_BASE_MINER__ENABLED
        - SPARKET_BASE_MINER__ODDS_API_KEY
        - SPARKET_BASE_MINER__ODDS_REFRESH_SECONDS
        - SPARKET_BASE_MINER__OUTCOME_CHECK_SECONDS
        - SPARKET_BASE_MINER__STATS_REFRESH_SECONDS
        - SPARKET_BASE_MINER__CACHE_TTL_SECONDS
        - SPARKET_BASE_MINER__MARKET_BLEND_WEIGHT
        - SPARKET_BASE_MINER__VIG
        """
        def get_bool(key: str, default: bool) -> bool:
            val = os.getenv(f"SPARKET_BASE_MINER__{key}")
            if val is None:
                return default
            return val.lower() in ("true", "1", "yes")
        
        def get_int(key: str, default: int) -> int:
            val = os.getenv(f"SPARKET_BASE_MINER__{key}")
            if val is None:
                return default
            try:
                return int(val)
            except ValueError:
                return default
        
        def get_float(key: str, default: float) -> float:
            val = os.getenv(f"SPARKET_BASE_MINER__{key}")
            if val is None:
                return default
            try:
                return float(val)
            except ValueError:
                return default
        
        def get_str(key: str, default: Optional[str] = None) -> Optional[str]:
            return os.getenv(f"SPARKET_BASE_MINER__{key}", default)
        
        # Validate and clamp batch_size to 1-200 range
        batch_size = get_int("BATCH_SIZE", 50)
        batch_size = max(1, min(200, batch_size))
        
        return cls(
            enabled=get_bool("ENABLED", True),
            odds_api_key=get_str("ODDS_API_KEY"),
            odds_refresh_seconds=get_int("ODDS_REFRESH_SECONDS", 900),
            outcome_check_seconds=get_int("OUTCOME_CHECK_SECONDS", 300),
            stats_refresh_seconds=get_int("STATS_REFRESH_SECONDS", 3600),
            cache_ttl_seconds=get_int("CACHE_TTL_SECONDS", 3600),
            batch_size=batch_size,
            market_blend_weight=get_float("MARKET_BLEND_WEIGHT", 0.60),
            vig=get_float("VIG", 0.045),
        )








