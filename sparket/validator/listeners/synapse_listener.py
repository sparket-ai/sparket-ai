from __future__ import annotations

from typing import Any

import bittensor as bt

from sparket.protocol.protocol import SparketSynapse, SparketSynapseType
from sparket.validator.utils.ratelimit import get_rate_limiter


async def route_incoming_synapse(validator: Any, synapse: SparketSynapse):
    """
    Rate limit, verify token (if required), and route incoming synapse to handler.
    
    Security layers (in order):
    1. Rate limiting (DDoS protection) - checked first for fast rejection
    2. Token verification (for push operations)
    3. Handler-level validation (market/event checks, bounds)
    
    Returns the emitted event or response data.
    Expects `validator` to have: .comms, .handlers, .step
    """
    try:
        # Extract hotkey for rate limiting and logging
        miner_hotkey = getattr(getattr(synapse, "dendrite", None), "hotkey", None) or "unknown"
        
        # SECURITY: Rate limiting - first line of defense against DDoS
        rate_limiter = get_rate_limiter()
        allowed, reason = rate_limiter.check_and_record(miner_hotkey)
        if not allowed:
            bt.logging.warning({
                "synapse_listener": "rate_limited",
                "reason": reason,
                "hotkey": miner_hotkey[:16] + "..." if len(miner_hotkey) > 16 else miner_hotkey,
            })
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
            if not comms.verify_token(token=token, step=validator.step):
                current_epoch = validator.step // comms.step_rotation
                bt.logging.warning({
                    "synapse_listener": "token_invalid",
                    "miner_hotkey": miner_hotkey[:16] + "..." if len(miner_hotkey) > 16 else miner_hotkey,
                    "token_provided": bool(token),
                    "token_preview": token[:8] + "..." if token else None,
                    "validator_step": validator.step,
                    "current_epoch": current_epoch,
                    "accepted_epochs": [current_epoch, max(0, current_epoch - 1)],
                })
                return None

        # Route: ODDS_PUSH
        if synapse_type == SparketSynapseType.ODDS_PUSH.value:
            event = await validator.handlers.ingest_odds_handler.handle_synapse(synapse)
            if event is not None:
                await validator.handlers.odds_score_handler.score_event(event)
            return event

        # Route: OUTCOME_PUSH
        if synapse_type == SparketSynapseType.OUTCOME_PUSH.value:
            event = await validator.handlers.ingest_outcome_handler.handle_synapse(synapse)
            if event is not None:
                await validator.handlers.outcome_score_handler.score_event(event)
            return event

        # Route: GAME_DATA_REQUEST (miner pulls events/markets)
        if synapse_type == SparketSynapseType.GAME_DATA_REQUEST.value:
            response = await validator.handlers.game_data_handler.handle_synapse(synapse)
            # Attach response to synapse payload for return
            synapse.payload = response
            return response

        bt.logging.info({"synapse_listener": "ignored", "type": synapse_type})
        return None
    except Exception as e:
        bt.logging.warning({"synapse_listener_error": str(e)})
        return None
