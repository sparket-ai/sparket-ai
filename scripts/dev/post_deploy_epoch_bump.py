"""Post-deploy: trigger epoch bump after v0.1.1 CLE/bias fix.

Run AFTER the validator has completed at least one scoring cycle on v0.1.1
so that rolling aggregates have been recomputed with the odds-ratio gate.

Usage:
    DATABASE_URL="postgresql+asyncpg://sparket:sparket@127.0.0.1:5435/sparket" \
      uv run python scripts/dev/post_deploy_epoch_bump.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


async def main() -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)

    os.environ.setdefault("SPARKET_DATABASE__URL", db_url)
    os.environ.setdefault("SPARKET_TEST_MODE", "true")

    import bittensor as bt

    from sparket.validator.config.config import Config as ValidatorAppConfig
    from sparket.validator.database.init import initialize as init_db
    from sparket.validator.database.dbm import DBM
    from sparket.validator.ledger.exporter import LedgerExporter
    from sparket.validator.ledger.store.filesystem import FilesystemStore

    app_config = ValidatorAppConfig()
    init_db(app_config)
    dbm = DBM.get_manager(app_config)

    wallet = bt.Wallet(
        name=os.environ.get("SPARKET_WALLET__NAME", "local-validator"),
        hotkey=os.environ.get("SPARKET_WALLET__HOTKEY", "default"),
    )
    netuid = int(os.environ.get("SPARKET_CHAIN__NETUID", "57"))
    data_dir = os.environ.get("SPARKET_LEDGER__DATA_DIR", "sparket/data/ledger")

    exporter = LedgerExporter(database=dbm, wallet=wallet, netuid=netuid)
    store = FilesystemStore(data_dir=data_dir)

    # Verify rolling aggregates have run with the new code
    from sqlalchemy import text
    rows = await dbm.read(
        text("SELECT MAX(as_of) AS latest FROM miner_rolling_score"),
        mappings=True,
    )
    latest = rows[0]["latest"] if rows else None
    print(f"Latest rolling aggregate: {latest}")

    bias_rows = await dbm.read(
        text("SELECT COUNT(*) AS n FROM sportsbook_bias WHERE sample_count > 0"),
        mappings=True,
    )
    n_bias = bias_rows[0]["n"] if bias_rows else 0
    print(f"Bias entries with samples: {n_bias}")

    if n_bias == 0:
        print("\nWARNING: BiasUpdateJob hasn't run yet (0 bias entries with samples).")
        print("Wait for a scoring cycle to complete before bumping epoch.")
        resp = input("Continue anyway? [y/N] ")
        if resp.lower() != "y":
            print("Aborted.")
            await dbm.engine.dispose()
            return

    print("\nBumping epoch...")
    checkpoint = await exporter.bump_epoch(
        reason_code="SCORING_BUG",
        reason_detail=(
            "v0.1.1: Fix CLE farming via extreme longshot odds "
            "(odds-ratio gate on ES aggregation) and activate "
            "sportsbook bias estimation"
        ),
        severity="bugfix",
    )

    cp_id = await store.put_checkpoint(checkpoint)
    print(f"Epoch bumped to {checkpoint.manifest.checkpoint_epoch}")
    print(f"Checkpoint exported: {cp_id}")
    print(f"Miners in checkpoint: {len(checkpoint.accumulators)}")

    await dbm.engine.dispose()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
