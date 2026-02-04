from __future__ import annotations

from typing import Any, Optional

import bittensor as bt

from sparket.protocol.protocol import SparketSynapse, SparketSynapseType
from sparket.validator.utils.ratelimit import get_rate_limiter
from sparket.shared.log_colors import LogColors


def _set_error_response(synapse: SparketSynapse, error_code: str, message: str) -> None:
    """Set error response in synapse payload for miner feedback."""
    synapse.payload = {
        "success": False,
        "error": error_code,
        "message": message,
    }


def _get_miner_ip(synapse: SparketSynapse) -> Optional[str]:
    """Extract miner IP from synapse dendrite info."""
    dendrite = getattr(synapse, "dendrite", None)
    if dendrite is None:
        return None
    return getattr(dendrite, "ip", None)


async def _record_failure(validator: Any, hotkey: str, ip: Optional[str], failure_type: str) -> None:
    """Record a failure in the security manager if available."""
    security_manager = getattr(validator, "security_manager", None)
    if security_manager is None:
        return
    try:
        await security_manager.record_failure(hotkey, ip, failure_type)
    except Exception as e:
        bt.logging.debug({"security_manager_record_failure_error": str(e)})


async def route_incoming_synapse(validator: Any, synapse: SparketSynapse):
    """
    Rate limit, verify token (if required), and route incoming synapse to handler.
    
    Security layers (in order):
    1. Rate limiting (DDoS protection) - checked first for fast rejection
    2. Token verification (for push operations)
    3. Handler-level validation (market/event checks, bounds)
    
    Returns the emitted event or response data.
    Sets synapse.payload with error details on failure for miner feedback.
    Expects `validator` to have: .comms, .handlers, .step
    """
    try:
        # Guard: handlers must be initialized before we can process synapses
        if not hasattr(validator, "handlers") or validator.handlers is None:
            _set_error_response(synapse, "not_ready", "Validator still initializing, try again shortly")
            return None
        
        # Extract hotkey and IP for rate limiting and logging
        miner_hotkey = getattr(getattr(synapse, "dendrite", None), "hotkey", None) or "unknown"
        miner_ip = _get_miner_ip(synapse)
        
        # SECURITY: Rate limiting - first line of defense against DDoS
        rate_limiter = get_rate_limiter()
        allowed, reason = rate_limiter.check_and_record(miner_hotkey)
        if not allowed:
            hotkey_short = miner_hotkey[:16] + "..." if len(miner_hotkey) > 16 else miner_hotkey
            bt.logging.warning(
                f"{LogColors.MINER_LABEL} rate_limited: hotkey={hotkey_short}, reason={reason}"
            )
            # Record failure for cooldown tracking
            await _record_failure(validator, miner_hotkey, miner_ip, "rate_limited")
            _set_error_response(synapse, "rate_limited", f"Rate limited: {reason}")
            return None
        
        payload = synapse.payload if isinstance(synapse.payload, dict) else {}
        token = payload.get("token")
        comms = getattr(validator, "comms", None)
        
        # Normalize synapse_type to handle both enum and string values
        raw_type = synapse.type
        if isinstance(raw_type, SparketSynapseType):
            synapse_type = raw_type.value
        elif isinstance(raw_type, str):
            synapse_type = raw_type.lower()
        else:
            synapse_type = str(raw_type).lower() if raw_type else None
        
        # Token verification for push operations (not required for data requests)
        requires_token = synapse_type in (
            SparketSynapseType.ODDS_PUSH.value,
            SparketSynapseType.OUTCOME_PUSH.value,
        )
        
        if requires_token and comms is not None and getattr(comms, "require_token", False):
            # Ensure step is an integer (could be numpy array from state loading)
            step_val = validator.step
            step_int = int(step_val) if hasattr(step_val, '__int__') else 0
            
            if not comms.verify_token(token=token, step=step_int):
                current_epoch = step_int // comms.step_rotation
                hotkey_short = miner_hotkey[:16] + "..." if len(miner_hotkey) > 16 else miner_hotkey
                token_preview = token[:8] + "..." if token else "none"
                bt.logging.warning(
                    f"{LogColors.MINER_LABEL} token_invalid: hotkey={hotkey_short}, "
                    f"token={token_preview}, step={step_int}, epoch={current_epoch}"
                )
                # Record failure for cooldown tracking
                failure_type = "token_missing" if not token else "token_invalid"
                await _record_failure(validator, miner_hotkey, miner_ip, failure_type)
                msg = "Token missing" if not token else "Token expired or invalid"
                _set_error_response(synapse, "token_invalid", msg)
                return None

        # Route: ODDS_PUSH
        if synapse_type == SparketSynapseType.ODDS_PUSH.value:
            event = await validator.handlers.ingest_odds_handler.handle_synapse(synapse)
            if event is not None:
                await validator.handlers.odds_score_handler.score_event(event)
                # Set success response
                synapse.payload = {"success": True, "accepted": True}
            else:
                synapse.payload = {"success": True, "accepted": False, "message": "No valid submissions"}
            return event

        # Route: OUTCOME_PUSH
        if synapse_type == SparketSynapseType.OUTCOME_PUSH.value:
            event = await validator.handlers.ingest_outcome_handler.handle_synapse(synapse)
            if event is not None:
                await validator.handlers.outcome_score_handler.score_event(event)
                synapse.payload = {"success": True, "accepted": True}
            else:
                synapse.payload = {"success": True, "accepted": False, "message": "No valid outcome"}
            return event

        # Route: GAME_DATA_REQUEST (miner pulls events/markets)
        if synapse_type == SparketSynapseType.GAME_DATA_REQUEST.value:
            response = await validator.handlers.game_data_handler.handle_synapse(synapse)
            
            # Defensive logging - should always have games key
            if not response or not isinstance(response, dict):
                bt.logging.error({
                    "game_data_response_invalid": {
                        "response_type": type(response).__name__ if response else "None",
                        "hotkey": miner_hotkey[:16] + "..." if len(miner_hotkey) > 16 else miner_hotkey,
                    }
                })
                response = {"error": "internal_error", "message": "Handler returned invalid response"}
            
            # Attach response to synapse payload for return
            synapse.payload = response
            return response

        bt.logging.info({"synapse_listener": "ignored", "type": synapse_type})
        # Record as malformed request
        await _record_failure(validator, miner_hotkey, miner_ip, "malformed_request")
        _set_error_response(synapse, "unknown_type", f"Unknown synapse type: {synapse_type}")
        return None
    except Exception as e:
        bt.logging.warning(f"{LogColors.VALIDATOR_LABEL} synapse_listener_error: {e}")
        # Record as other/generic failure (best-effort, miner_hotkey may not be defined)
        try:
            hk = getattr(getattr(synapse, "dendrite", None), "hotkey", None)
            ip = _get_miner_ip(synapse)
            if hk:
                await _record_failure(validator, hk, ip, "other")
        except Exception:
            pass
        _set_error_response(synapse, "internal_error", str(e))
        return None
