from __future__ import annotations

import copy
from typing import Any, List

import bittensor as bt

from .miner_management import MinerManagementHandler


class SyncMetagraphHandler:
    """Handles metagraph synchronization and miner registration state.
    
    Critical for deregistration handling: when a miner is replaced,
    the old miner's data is preserved but marked inactive, and the
    new miner is registered fresh.
    """
    
    def __init__(self, database: Any):
        self.database = database
        self.miner_mgmt = MinerManagementHandler(database)
        self._previous_hotkeys: List[str] = []

    def should_sync(self, validator: Any) -> bool:
        """Mirror BaseNeuron.should_sync_metagraph logic."""
        try:
            return (validator.block - validator.metagraph.last_update[validator.uid]) > validator.config.neuron.epoch_length
        except Exception:
            return False

    def run(self, validator: Any, *, set_weights: bool = True) -> None:
        """
        If due, sync metagraph and optionally set weights (using the weights handler)
        to keep chain interaction centralized in handlers.
        """
        if not self.should_sync(validator):
            return
        bt.logging.info("sync_metagraph() via handler")
        
        # Save previous hotkeys for change detection
        old_hotkeys = copy.deepcopy(validator.hotkeys) if hasattr(validator, "hotkeys") else []
        
        try:
            validator.metagraph.sync(subtensor=validator.subtensor)
        except Exception as e:
            bt.logging.warning({"sync_metagraph_error": str(e)})
            return
        
        # Update validator's hotkey cache
        validator.hotkeys = copy.deepcopy(validator.metagraph.hotkeys)
        
        # Update security manager with new registered hotkeys
        if hasattr(validator, "security_manager") and validator.security_manager is not None:
            validator.security_manager.update_registered_hotkeys(validator.metagraph)
        
        # Detect hotkey changes (deregistrations)
        if old_hotkeys:
            changes = self.miner_mgmt.check_miners_registered(
                validator.metagraph, 
                old_hotkeys,
            )
            if changes:
                bt.logging.info({
                    "miner_hotkey_changes": [
                        {"uid": uid, "old": old[:16] + "...", "new": new[:16] + "..."}
                        for uid, old, new in changes
                    ]
                })

        if set_weights and hasattr(validator.handlers, "set_weights_handler"):
            try:
                validator.handlers.set_weights_handler.set_weights(validator)
            except Exception as e:
                bt.logging.warning({"set_weights_after_sync_error": str(e)})

    async def run_async(self, validator: Any, *, set_weights: bool = True) -> None:
        """Async version that also syncs miners to database.
        
        This ensures new miners can immediately submit after registration.
        """
        if not self.should_sync(validator):
            return
        
        bt.logging.info("sync_metagraph_async() via handler")
        
        # Save previous hotkeys for change detection
        old_hotkeys = copy.deepcopy(validator.hotkeys) if hasattr(validator, "hotkeys") else []
        
        try:
            validator.metagraph.sync(subtensor=validator.subtensor)
        except Exception as e:
            bt.logging.warning({"sync_metagraph_error": str(e)})
            return
        
        # Update validator's hotkey cache
        validator.hotkeys = copy.deepcopy(validator.metagraph.hotkeys)
        
        # Update security manager with new registered hotkeys
        if hasattr(validator, "security_manager") and validator.security_manager is not None:
            validator.security_manager.update_registered_hotkeys(validator.metagraph)
        
        # Sync metagraph to database (handles deregistrations)
        try:
            netuid = validator.config.netuid
            block = int(validator.block)
            
            result = await self.miner_mgmt.sync_metagraph_to_db(
                metagraph=validator.metagraph,
                block=block,
                netuid=netuid,
            )
            
            if result.get("error"):
                bt.logging.warning({"miner_db_sync_error": result["error"]})
            else:
                bt.logging.info({
                    "miner_db_sync": {
                        "inserted": result.get("inserted", 0),
                        "updated": result.get("updated", 0),
                        "deregistered": result.get("deregistered", 0),
                        "hotkey_changes": len(result.get("hotkey_changes", [])),
                    }
                })
                
                # Log each hotkey change for audit
                for change in result.get("hotkey_changes", []):
                    bt.logging.warning({
                        "miner_deregistered": {
                            "uid": change["uid"],
                            "old_hotkey": change["old_hotkey"],
                            "new_hotkey": change["new_hotkey"],
                            "old_miner_id": change["old_miner_id"],
                        }
                    })
                    
        except Exception as e:
            bt.logging.warning({"miner_db_sync_fatal": str(e)})

        if set_weights and hasattr(validator.handlers, "set_weights_handler"):
            try:
                validator.handlers.set_weights_handler.set_weights(validator)
            except Exception as e:
                bt.logging.warning({"set_weights_after_sync_error": str(e)})
