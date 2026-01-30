from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import bittensor as bt

from sparket.protocol.protocol import SparketSynapse, SparketSynapseType


class ValidatorClient:
    """Client for miner-to-validator communication.
    
    Supports:
    - Pushing odds/outcome submissions to validators
    - Pulling game data (events/markets) from validators with delta sync
    """
    
    def __init__(
        self,
        *,
        wallet: Any,
        metagraph: Any,
        get_validator_endpoint: Callable[[], Optional[dict]] | None = None,
    ) -> None:
        self._wallet = wallet
        self._metagraph = metagraph
        self._get_endpoint = get_validator_endpoint
        self._dendrite = bt.Dendrite(wallet=wallet)

    def _select_validator_axons(self) -> List[Any]:
        """Select validator axons to communicate with.
        
        Filters out axons with port 0 (inactive/unregistered validators).
        """
        try:
            permits = getattr(self._metagraph, "validator_permit", [])
            axons = getattr(self._metagraph, "axons", [])
            # Filter validators with permit AND active port
            selected = [
                axons[i] for i, is_val in enumerate(permits) 
                if is_val and getattr(axons[i], "port", 0) > 0
            ]
            # Fallback: any axon with active port
            if not selected:
                selected = [ax for ax in axons if getattr(ax, "port", 0) > 0]
            return selected
        except Exception:
            return []

    def _check_response_errors(self, responses: Any, operation: str) -> bool:
        """Check response for errors and log them.
        
        Returns True if successful, False if there were errors.
        """
        if not responses:
            return True  # No response to check
        
        for i, resp in enumerate(responses if isinstance(responses, list) else [responses]):
            # Extract payload from response
            if isinstance(resp, dict):
                result = resp
            elif hasattr(resp, "payload") and isinstance(resp.payload, dict):
                result = resp.payload
            else:
                continue
            
            # Check for error response from validator
            if result.get("success") is False or "error" in result:
                error_code = result.get("error", "unknown")
                message = result.get("message", "No details")
                bt.logging.warning({
                    f"{operation}_rejected": {
                        "error": error_code,
                        "message": message,
                        "validator_index": i,
                    }
                })
                return False
            
            # Log if submission was not accepted (but no error)
            if result.get("accepted") is False:
                bt.logging.info({
                    f"{operation}_not_accepted": {
                        "message": result.get("message", "Submission not accepted"),
                        "validator_index": i,
                    }
                })
        
        return True

    async def submit_odds(self, payload: dict, *, timeout: float = 12.0) -> bool:
        """Submit odds to validators.
        
        Returns True if submission was accepted, False otherwise.
        Logs any errors received from validators.
        """
        syn = SparketSynapse(type=SparketSynapseType.ODDS_PUSH, payload=payload)
        axons = self._select_validator_axons()
        if not axons:
            bt.logging.warning({"submit_odds": "no_validators_available"})
            return False
        try:
            responses = await self._dendrite.forward(axons=axons, synapse=syn, timeout=timeout)
            return self._check_response_errors(responses, "submit_odds")
        except Exception as e:
            bt.logging.warning({"submit_odds_exception": str(e)})
            return False

    async def submit_outcome(self, payload: dict, *, timeout: float = 12.0) -> bool:
        """Submit outcome to validators.
        
        Returns True if submission was accepted, False otherwise.
        Logs any errors received from validators.
        """
        syn = SparketSynapse(type=SparketSynapseType.OUTCOME_PUSH, payload=payload)
        axons = self._select_validator_axons()
        if not axons:
            bt.logging.warning({"submit_outcome": "no_validators_available"})
            return False
        try:
            responses = await self._dendrite.forward(axons=axons, synapse=syn, timeout=timeout)
            return self._check_response_errors(responses, "submit_outcome")
        except Exception as e:
            bt.logging.warning({"submit_outcome_exception": str(e)})
            return False

    async def fetch_game_data(
        self,
        *,
        since_ts: Optional[datetime] = None,
        timeout: float = 30.0,
    ) -> Optional[Dict[str, Any]]:
        """Pull game data (events/markets) from a validator.
        
        Args:
            since_ts: If provided, only fetch events created after this timestamp (delta sync).
                     If None, fetch all upcoming events (full sync).
            timeout: Request timeout in seconds.
            
        Returns:
            Response dict with keys: events, markets, sync_ts
            Or None if request failed.
        """
        payload = {}
        if since_ts is not None:
            payload["since_ts"] = since_ts.isoformat()
        
        syn = SparketSynapse(type=SparketSynapseType.GAME_DATA_REQUEST, payload=payload)
        axons = self._select_validator_axons()
        
        if not axons:
            bt.logging.warning({"fetch_game_data": "no_validators_available"})
            return None
        
        # Query first available validator
        try:
            responses = await self._dendrite.forward(
                axons=axons[:1],  # Just query one validator
                synapse=syn,
                timeout=timeout,
            )
            
            if responses and len(responses) > 0:
                response = responses[0]
                
                # Handle both SparketSynapse and dict responses
                # Bittensor may return the deserialized payload directly as a dict
                if isinstance(response, dict):
                    result = response
                elif hasattr(response, "payload") and isinstance(response.payload, dict):
                    result = response.payload
                else:
                    bt.logging.warning({
                        "fetch_game_data": "unexpected_response_type",
                        "response_type": type(response).__name__,
                    })
                    return None
                
                # Check for errors
                if "error" in result:
                    bt.logging.warning({"fetch_game_data_error": result.get("error")})
                    return None
                
                # Response format: games (with embedded markets), retrieved_at
                games = result.get("games", [])
                bt.logging.info({
                    "fetch_game_data": {
                        "games": len(games),
                        "retrieved_at": result.get("retrieved_at"),
                    }
                })
                return result
            else:
                bt.logging.warning({"fetch_game_data": "empty_response"})
            return None
        except Exception as e:
            bt.logging.warning({"fetch_game_data_exception": str(e)})
            return None


