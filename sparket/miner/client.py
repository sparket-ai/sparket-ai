from __future__ import annotations

import copy
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import bittensor as bt

from sparket.protocol.protocol import SparketSynapse, SparketSynapseType


class ValidatorClient:
    """Client for miner-to-validator communication.
    
    Supports:
    - Pushing odds/outcome submissions to validators
    - Pulling game data (events/markets) from validators with delta sync
    - Automatic backoff when validators are not ready
    """
    
    # Backoff settings
    INITIAL_BACKOFF_SEC = 5.0
    MAX_BACKOFF_SEC = 60.0
    BACKOFF_MULTIPLIER = 2.0
    
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
        
        # Backoff state: track when to retry after "not_ready"
        self._backoff_until: float = 0.0
        self._current_backoff: float = self.INITIAL_BACKOFF_SEC

    def _resolve_endpoint_host_port(self) -> Tuple[Optional[str], Optional[int], Optional[str]]:
        """Resolve preferred validator endpoint announced via CONNECTION_INFO_PUSH."""
        if self._get_endpoint is None:
            return None, None, None
        try:
            endpoint = self._get_endpoint() or {}
        except Exception:
            return None, None, None
        if not isinstance(endpoint, dict):
            return None, None, None

        hotkey = endpoint.get("hotkey")
        host = endpoint.get("host")
        port = endpoint.get("port")
        url = endpoint.get("url")

        if isinstance(url, str) and url:
            try:
                parsed = urlparse(url)
                if parsed.hostname:
                    host = parsed.hostname
                if parsed.port:
                    port = parsed.port
            except Exception:
                pass

        if isinstance(port, str):
            try:
                port = int(port)
            except ValueError:
                port = None
        if isinstance(port, int) and port <= 0:
            port = None

        return (str(host) if host else None), port, (str(hotkey) if hotkey else None)

    def _build_preferred_axon(self, axons: List[Any]) -> Optional[Any]:
        """Build an axon override using validator-pushed endpoint data."""
        host, port, hotkey = self._resolve_endpoint_host_port()
        if not host or not port or not axons:
            return None

        base_axon = None
        if hotkey:
            try:
                hotkeys = list(getattr(self._metagraph, "hotkeys", []))
                idx = hotkeys.index(hotkey)
                metagraph_axons = list(getattr(self._metagraph, "axons", []))
                if 0 <= idx < len(metagraph_axons):
                    base_axon = metagraph_axons[idx]
            except Exception:
                base_axon = None

        if base_axon is None:
            # Fallback to any selected validator axon so we retain required fields.
            base_axon = axons[0]

        try:
            preferred = copy.deepcopy(base_axon)
            preferred.ip = host
            preferred.port = int(port)
            return preferred
        except Exception:
            return None

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
            preferred = self._build_preferred_axon(selected)
            if preferred is not None:
                preferred_hotkey = getattr(preferred, "hotkey", None)
                preferred_ip = getattr(preferred, "ip", None)
                preferred_port = getattr(preferred, "port", None)
                deduped = []
                for ax in selected:
                    same_hotkey = preferred_hotkey and getattr(ax, "hotkey", None) == preferred_hotkey
                    same_endpoint = (
                        preferred_ip
                        and preferred_port
                        and getattr(ax, "ip", None) == preferred_ip
                        and getattr(ax, "port", None) == preferred_port
                    )
                    if same_hotkey or same_endpoint:
                        continue
                    deduped.append(ax)
                selected = [preferred, *deduped]
            return selected
        except Exception:
            return []

    def _is_in_backoff(self) -> bool:
        """Check if we're currently in backoff period."""
        return time.time() < self._backoff_until
    
    def _trigger_backoff(self) -> None:
        """Trigger exponential backoff after receiving not_ready."""
        self._backoff_until = time.time() + self._current_backoff
        bt.logging.info({
            "validator_backoff": {
                "seconds": self._current_backoff,
                "until": datetime.fromtimestamp(self._backoff_until).isoformat(),
            }
        })
        # Increase backoff for next time (exponential)
        self._current_backoff = min(
            self._current_backoff * self.BACKOFF_MULTIPLIER,
            self.MAX_BACKOFF_SEC
        )
    
    def _reset_backoff(self) -> None:
        """Reset backoff after successful communication."""
        self._current_backoff = self.INITIAL_BACKOFF_SEC
        self._backoff_until = 0.0

    def _check_response_errors(self, responses: Any, operation: str) -> Tuple[bool, bool]:
        """Check response for errors and log them.
        
        Returns:
            Tuple of (success: bool, should_backoff: bool)
            - success: True if submission was accepted
            - should_backoff: True if validator returned "not_ready" or cooldown
        """
        if not responses:
            return False, False  # No response to check
        
        should_backoff = False
        accepted_any = False
        
        for i, resp in enumerate(responses if isinstance(responses, list) else [responses]):
            # Check dendrite status code FIRST (security middleware rejections)
            dendrite_info = getattr(resp, "dendrite", None)
            status_code = getattr(dendrite_info, "status_code", None) if dendrite_info else None
            status_msg = getattr(dendrite_info, "status_message", "") if dendrite_info else ""
            
            # Handle HTTP-level rejection from security middleware
            if status_code == 429:
                # Cooldown - need to back off
                bt.logging.info({
                    f"{operation}_cooldown": {
                        "status_code": status_code,
                        "message": status_msg,
                        "will_backoff": True,
                    }
                })
                should_backoff = True
                continue
            elif status_code == 403:
                # Forbidden (blacklisted or not registered)
                bt.logging.warning({
                    f"{operation}_forbidden": {
                        "status_code": status_code,
                        "message": status_msg,
                    }
                })
                continue
            elif status_code is not None and status_code >= 400:
                # Other HTTP error
                bt.logging.warning({
                    f"{operation}_http_error": {
                        "status_code": status_code,
                        "message": status_msg,
                    }
                })
                continue
            
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
                
                # Check if validator is not ready - trigger backoff
                if error_code == "not_ready":
                    bt.logging.info({
                        f"{operation}_validator_not_ready": {
                            "message": message,
                            "will_backoff": True,
                        }
                    })
                    should_backoff = True
                else:
                    bt.logging.warning({
                        f"{operation}_rejected": {
                            "error": error_code,
                            "message": message,
                            "validator_index": i,
                        }
                    })
                continue
            
            # Log if submission was not accepted (but no error)
            if result.get("accepted") is False:
                bt.logging.info({
                    f"{operation}_not_accepted": {
                        "message": result.get("message", "Submission not accepted"),
                        "validator_index": i,
                    }
                })
                continue

            accepted_any = True
        
        return accepted_any, should_backoff

    async def submit_odds(self, payload: dict, *, timeout: float = 12.0) -> bool:
        """Submit odds to validators.
        
        Returns True if submission was accepted, False otherwise.
        Respects backoff period if validator was not ready.
        """
        # Check if we're in backoff period
        if self._is_in_backoff():
            remaining = self._backoff_until - time.time()
            bt.logging.debug({
                "submit_odds_skipped": {
                    "reason": "in_backoff",
                    "remaining_seconds": round(remaining, 1),
                }
            })
            return False
        
        syn = SparketSynapse(type=SparketSynapseType.ODDS_PUSH, payload=payload)
        axons = self._select_validator_axons()
        if not axons:
            bt.logging.warning({"submit_odds": "no_validators_available"})
            return False
        try:
            responses = await self._dendrite.forward(axons=axons, synapse=syn, timeout=timeout)
            success, should_backoff = self._check_response_errors(responses, "submit_odds")
            
            if should_backoff:
                self._trigger_backoff()
            elif success:
                self._reset_backoff()
            
            return success
        except Exception as e:
            bt.logging.warning({"submit_odds_exception": str(e)})
            return False

    async def submit_outcome(self, payload: dict, *, timeout: float = 12.0) -> bool:
        """Submit outcome to validators.
        
        Returns True if submission was accepted, False otherwise.
        Respects backoff period if validator was not ready.
        """
        # Check if we're in backoff period
        if self._is_in_backoff():
            remaining = self._backoff_until - time.time()
            bt.logging.debug({
                "submit_outcome_skipped": {
                    "reason": "in_backoff",
                    "remaining_seconds": round(remaining, 1),
                }
            })
            return False
        
        syn = SparketSynapse(type=SparketSynapseType.OUTCOME_PUSH, payload=payload)
        axons = self._select_validator_axons()
        if not axons:
            bt.logging.warning({"submit_outcome": "no_validators_available"})
            return False
        try:
            responses = await self._dendrite.forward(axons=axons, synapse=syn, timeout=timeout)
            success, should_backoff = self._check_response_errors(responses, "submit_outcome")
            
            if should_backoff:
                self._trigger_backoff()
            elif success:
                self._reset_backoff()
            
            return success
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
        # Check if we're in backoff period
        if self._is_in_backoff():
            remaining = self._backoff_until - time.time()
            bt.logging.debug({
                "fetch_game_data_skipped": {
                    "reason": "in_backoff",
                    "remaining_seconds": round(remaining, 1),
                }
            })
            return None
        
        payload = {}
        if since_ts is not None:
            payload["since_ts"] = since_ts.isoformat()
        
        syn = SparketSynapse(type=SparketSynapseType.GAME_DATA_REQUEST, payload=payload)
        axons = self._select_validator_axons()
        
        if not axons:
            bt.logging.warning({"fetch_game_data": "no_validators_available"})
            return None
        
        # Try each validator until one responds successfully
        last_error: str | None = None
        for idx, axon in enumerate(axons):
            try:
                responses = await self._dendrite.forward(
                    axons=[axon],
                    synapse=syn,
                    timeout=timeout,
                )

                if not responses or len(responses) == 0:
                    last_error = "empty_response"
                    continue

                response = responses[0]

                # Check dendrite status code FIRST before processing response
                # Security middleware may reject with 429 (cooldown) or 403 (blacklist)
                dendrite_info = getattr(response, "dendrite", None)
                status_code = getattr(dendrite_info, "status_code", None) if dendrite_info else None
                status_msg = getattr(dendrite_info, "status_message", "") if dendrite_info else ""

                # Handle rejection responses (don't treat as valid game data)
                if status_code == 429:
                    bt.logging.info({
                        "fetch_game_data_cooldown": {
                            "status_code": status_code,
                            "message": status_msg,
                            "will_backoff": True,
                        }
                    })
                    self._trigger_backoff()
                    return None
                elif status_code == 403:
                    bt.logging.warning({
                        "fetch_game_data_forbidden": {
                            "status_code": status_code,
                            "message": status_msg,
                        }
                    })
                    return None
                elif status_code is not None and status_code >= 400:
                    last_error = f"http_{status_code}"
                    continue  # Try next validator

                # Handle both SparketSynapse and dict responses
                if isinstance(response, dict):
                    result = response
                elif hasattr(response, "payload") and isinstance(response.payload, dict):
                    result = response.payload
                else:
                    last_error = f"unexpected_type:{type(response).__name__}"
                    continue

                # Check for not_ready error - try next validator before triggering backoff
                error_code = result.get("error")
                if error_code == "not_ready":
                    last_error = "not_ready"
                    continue
                elif error_code:
                    last_error = error_code
                    continue

                # Success - reset backoff
                self._reset_backoff()

                games = result.get("games", [])
                bt.logging.info({
                    "fetch_game_data": {
                        "games": len(games),
                        "retrieved_at": result.get("retrieved_at"),
                        "validator_idx": idx,
                    }
                })
                return result

            except Exception as e:
                last_error = str(e)
                continue

        # All validators failed
        if last_error == "not_ready":
            self._trigger_backoff()
        bt.logging.warning({"fetch_game_data": "all_validators_failed", "last_error": last_error})
        return None


