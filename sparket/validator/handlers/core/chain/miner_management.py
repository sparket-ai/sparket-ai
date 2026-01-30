"""Miner lifecycle management: registration, deregistration, hotkey changes.

When a miner is deregistered (replaced by a new miner at the same UID),
we DELETE all their data entirely. This ensures the new miner starts
completely fresh with no inherited scores or submissions.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Set, Tuple

import bittensor as bt
from sqlalchemy import text

logger = logging.getLogger(__name__)


# SQL queries for miner management
_GET_ACTIVE_MINERS = text("""
    SELECT miner_id, hotkey, uid, netuid
    FROM miner
    WHERE netuid = :netuid AND active = 1
""")

_INSERT_MINER = text("""
    INSERT INTO miner (
        hotkey, coldkey, uid, netuid, active, stake, stake_dict, total_stake,
        rank, emission, incentive, consensus, trust, validator_trust,
        dividends, last_update, validator_permit, pruning_score, is_null
    )
    VALUES (
        :hotkey, :coldkey, :uid, :netuid, 1, :stake, '{}'::jsonb, :stake,
        :rank, :emission, :incentive, :consensus, :trust, :validator_trust,
        :dividends, :last_update, :validator_permit, 0, false
    )
    ON CONFLICT (hotkey) DO UPDATE SET
        uid = EXCLUDED.uid,
        netuid = EXCLUDED.netuid,
        active = 1,
        stake = EXCLUDED.stake,
        total_stake = EXCLUDED.stake,
        rank = EXCLUDED.rank,
        emission = EXCLUDED.emission,
        incentive = EXCLUDED.incentive,
        consensus = EXCLUDED.consensus,
        trust = EXCLUDED.trust,
        validator_trust = EXCLUDED.validator_trust,
        dividends = EXCLUDED.dividends,
        last_update = EXCLUDED.last_update,
        validator_permit = EXCLUDED.validator_permit
    RETURNING miner_id, (xmax = 0) AS was_inserted
""")

# Single delete - CASCADE handles all child tables
_DELETE_MINER = text("""
    DELETE FROM miner WHERE miner_id = :miner_id
""")


class MinerManagementHandler:
    """Manages miner registration and deregistration lifecycle.
    
    When a miner is deregistered (hotkey changes at a UID), we DELETE all
    their data so the new miner starts completely fresh.
    """
    
    def __init__(self, database: Any):
        self.database = database

    async def _delete_miner_data(self, miner_id: int) -> None:
        """Delete miner and all associated data (CASCADE handles children)."""
        await self.database.write(
            _DELETE_MINER,
            params={"miner_id": miner_id},
        )

    async def sync_metagraph_to_db(
        self,
        metagraph: Any,
        block: int,
        netuid: int,
    ) -> Dict[str, Any]:
        """Sync metagraph state to miner table.
        
        Returns dict with sync statistics for logging.
        """
        inserted = 0
        updated = 0
        deleted = 0
        hotkey_changes: List[Dict[str, Any]] = []
        
        try:
            # Get current DB state for this netuid
            db_miners = await self.database.read(
                _GET_ACTIVE_MINERS,
                params={"netuid": netuid},
                mappings=True,
            )
            db_by_uid: Dict[int, Dict] = {m["uid"]: m for m in (db_miners or [])}
            db_hotkeys: Set[str] = {m["hotkey"] for m in (db_miners or [])}
            
            # Process each neuron from metagraph
            metagraph_hotkeys: Set[str] = set()
            n_neurons = len(metagraph.hotkeys) if hasattr(metagraph, "hotkeys") else 0
            
            for uid in range(n_neurons):
                hotkey = metagraph.hotkeys[uid]
                if not hotkey or hotkey == "":
                    continue
                    
                metagraph_hotkeys.add(hotkey)
                
                # Check if this UID had a different hotkey before (deregistration)
                if uid in db_by_uid:
                    old_miner = db_by_uid[uid]
                    if old_miner["hotkey"] != hotkey:
                        # DEREGISTRATION: Delete old miner entirely
                        hotkey_changes.append({
                            "uid": uid,
                            "old_hotkey": old_miner["hotkey"][:16] + "...",
                            "new_hotkey": hotkey[:16] + "...",
                            "old_miner_id": old_miner["miner_id"],
                        })
                        
                        bt.logging.info({
                            "miner_deregistered": {
                                "uid": uid,
                                "old_hotkey": old_miner["hotkey"][:16] + "...",
                                "miner_id": old_miner["miner_id"],
                            }
                        })
                        
                        await self._delete_miner_data(old_miner["miner_id"])
                        deleted += 1
                
                # Upsert the current miner
                try:
                    coldkey = str(metagraph.coldkeys[uid]) if hasattr(metagraph, "coldkeys") else hotkey
                    stake = float(metagraph.S[uid]) if hasattr(metagraph, "S") else 0.0
                    rank = float(metagraph.R[uid]) if hasattr(metagraph, "R") else 0.0
                    emission = float(metagraph.E[uid]) if hasattr(metagraph, "E") else 0.0
                    incentive = float(metagraph.I[uid]) if hasattr(metagraph, "I") else 0.0
                    consensus = float(metagraph.C[uid]) if hasattr(metagraph, "C") else 0.0
                    trust = float(metagraph.T[uid]) if hasattr(metagraph, "T") else 0.0
                    validator_trust = float(metagraph.Tv[uid]) if hasattr(metagraph, "Tv") else 0.0
                    dividends = float(metagraph.D[uid]) if hasattr(metagraph, "D") else 0.0
                    validator_permit = bool(metagraph.validator_permit[uid]) if hasattr(metagraph, "validator_permit") else False
                    
                    result = await self.database.write(
                        _INSERT_MINER,
                        params={
                            "hotkey": hotkey,
                            "coldkey": coldkey,
                            "uid": uid,
                            "netuid": netuid,
                            "stake": stake,
                            "rank": rank,
                            "emission": emission,
                            "incentive": incentive,
                            "consensus": consensus,
                            "trust": trust,
                            "validator_trust": validator_trust,
                            "dividends": dividends,
                            "last_update": block,
                            "validator_permit": validator_permit,
                        },
                        return_rows=True,
                        mappings=True,
                    )
                    
                    if result and len(result) > 0:
                        was_inserted = result[0].get("was_inserted", False)
                        if was_inserted:
                            inserted += 1
                        else:
                            updated += 1
                            
                except Exception as e:
                    bt.logging.warning({
                        "miner_sync_error": {
                            "uid": uid,
                            "hotkey": hotkey[:16] + "...",
                            "error": str(e),
                        }
                    })
            
            # Delete miners no longer in metagraph
            already_deleted = {m["miner_id"] for m in db_by_uid.values() 
                              if m["hotkey"] not in metagraph_hotkeys}
            for hotkey in db_hotkeys - metagraph_hotkeys:
                for miner in (db_miners or []):
                    if miner["hotkey"] == hotkey:
                        # Skip if already deleted via hotkey change
                        if any(c.get("old_hotkey", "").startswith(hotkey[:16]) for c in hotkey_changes):
                            continue
                        await self._delete_miner_data(miner["miner_id"])
                        deleted += 1
                        break
            
            return {
                "inserted": inserted,
                "updated": updated,
                "deleted": deleted,
                "hotkey_changes": hotkey_changes,
                "total_neurons": n_neurons,
            }
            
        except Exception as e:
            bt.logging.error({"miner_sync_fatal_error": str(e)})
            return {"error": str(e)}

    def check_miners_registered(
        self, 
        metagraph: Any,
        old_hotkeys: List[str],
    ) -> List[Tuple[int, str, str]]:
        """Compare old hotkeys to current metagraph, return changes."""
        changes = []
        new_hotkeys = metagraph.hotkeys if hasattr(metagraph, "hotkeys") else []
        
        for uid, (old, new) in enumerate(zip(old_hotkeys, new_hotkeys)):
            if old and new and old != new:
                changes.append((uid, old, new))
        
        return changes


__all__ = ["MinerManagementHandler"]