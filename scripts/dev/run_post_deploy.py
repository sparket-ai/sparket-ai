"""Post-deploy v0.1.1: run scoring cycle + epoch bump.

Executes the full scoring pipeline (including BiasUpdateJob) then
bumps the epoch so auditors reset their accumulators.

Usage:
    DATABASE_URL="postgresql+asyncpg://sparket:sparket@127.0.0.1:5435/sparket" \
      uv run python scripts/dev/run_post_deploy.py
"""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


async def main() -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)

    os.environ.setdefault("SPARKET_DATABASE__URL", db_url)

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(db_url, poolclass=NullPool)

    # ---- Step 1: Run BiasUpdateJob ----
    print("=" * 60)
    print("STEP 1: Running BiasUpdateJob")
    print("=" * 60)

    import logging
    logger = logging.getLogger("post_deploy")
    logging.basicConfig(level=logging.INFO, format="  %(message)s")

    # Minimal DB wrapper matching what ScoringJob expects
    class LiteDB:
        def __init__(self, eng):
            self._engine = eng
        async def read(self, query, params=None, *, mappings=True):
            async with AsyncSession(self._engine) as session:
                result = await session.execute(query, params or {})
                if mappings:
                    return [dict(r) for r in result.mappings().all()]
                return result.all()
        async def write(self, query, params=None, *, return_rows=False, mappings=False):
            async with AsyncSession(self._engine) as session:
                result = await session.execute(query, params or {})
                await session.commit()
                if return_rows:
                    return [dict(r) for r in result.mappings().all()] if mappings else result.all()
                return result.rowcount

    db = LiteDB(engine)

    from sparket.validator.scoring.jobs.bias_update import BiasUpdateJob
    bias_job = BiasUpdateJob(db, logger)
    t0 = time.time()
    await bias_job.run()
    print(f"  BiasUpdateJob completed in {time.time()-t0:.1f}s")

    # Check results
    async with AsyncSession(engine) as s:
        r = await s.execute(text("SELECT COUNT(*) AS n FROM sportsbook_bias WHERE sample_count > 0"))
        n_bias = dict(r.mappings().first())["n"]
        print(f"  Bias entries with samples: {n_bias}")

        r2 = await s.execute(text("""
            SELECT sportsbook_id, sport_id, market_kind::text, bias_factor, sample_count
            FROM sportsbook_bias WHERE sample_count > 0
            ORDER BY sample_count DESC LIMIT 10
        """))
        rows = r2.mappings().all()
        if rows:
            print(f"\n  Top 10 bias entries:")
            print(f"  {'Book':<8} {'Sport':<8} {'Kind':<12} {'Bias':<10} {'Samples'}")
            print(f"  {'-'*8} {'-'*8} {'-'*12} {'-'*10} {'-'*8}")
            for row in rows:
                print(f"  {row['sportsbook_id']:<8} {row['sport_id']:<8} {row['market_kind']:<12} {float(row['bias_factor']):<10.4f} {row['sample_count']}")

    # ---- Step 2: Run RollingAggregatesJob ----
    print("\n" + "=" * 60)
    print("STEP 2: Running RollingAggregatesJob (with odds-ratio gate)")
    print("=" * 60)

    from sparket.validator.scoring.jobs.rolling_aggregates import RollingAggregatesJob
    rolling_job = RollingAggregatesJob(db, logger)
    t0 = time.time()
    await rolling_job.run()
    print(f"  RollingAggregatesJob completed in {time.time()-t0:.1f}s")

    # ---- Step 3: Run SkillScoreJob ----
    print("\n" + "=" * 60)
    print("STEP 3: Running SkillScoreJob")
    print("=" * 60)

    from sparket.validator.scoring.jobs.skill_score import SkillScoreJob
    skill_job = SkillScoreJob(db, logger)
    t0 = time.time()
    await skill_job.run()
    print(f"  SkillScoreJob completed in {time.time()-t0:.1f}s")

    # ---- Step 4: Epoch bump ----
    print("\n" + "=" * 60)
    print("STEP 4: Epoch bump")
    print("=" * 60)

    import bittensor as bt
    from sparket.validator.ledger.exporter import LedgerExporter
    from sparket.validator.ledger.store.filesystem import FilesystemStore

    wallet = bt.Wallet(
        name=os.environ.get("SPARKET_WALLET__NAME", "sparket-validator"),
        hotkey=os.environ.get("SPARKET_WALLET__HOTKEY", "default"),
    )
    netuid = int(os.environ.get("SPARKET_CHAIN__NETUID", "57"))
    data_dir = os.environ.get("SPARKET_LEDGER__DATA_DIR", "sparket/data/ledger")

    exporter = LedgerExporter(database=db, wallet=wallet, netuid=netuid)
    store = FilesystemStore(data_dir=data_dir)

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
    print(f"  Epoch bumped to {checkpoint.manifest.checkpoint_epoch}")
    print(f"  Checkpoint ID: {cp_id}")
    print(f"  Miners in checkpoint: {len(checkpoint.accumulators)}")

    # ---- Step 5: Verify ----
    print("\n" + "=" * 60)
    print("STEP 5: Verification")
    print("=" * 60)

    async with AsyncSession(engine) as s:
        # Check top miners' corrected es_mean
        r = await s.execute(text("""
            SELECT miner_id, es_mean, es_adj, brier_mean, skill_score
            FROM miner_rolling_score
            WHERE as_of = (SELECT MAX(as_of) FROM miner_rolling_score)
            ORDER BY es_mean DESC
            LIMIT 5
        """))
        rows = r.mappings().all()
        if rows:
            print(f"  Top 5 miners by ES mean (should be much lower than before):")
            print(f"  {'Miner':<10} {'ES Mean':<10} {'ES Adj':<10} {'Brier':<10} {'Skill'}")
            print(f"  {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
            for row in rows:
                def f(v): return f"{float(v):.4f}" if v is not None else "N/A"
                print(f"  {row['miner_id']:<10} {f(row['es_mean']):<10} {f(row['es_adj']):<10} {f(row['brier_mean']):<10} {f(row['skill_score'])}")

    await engine.dispose()
    print("\nDone. Auditors will pick up the new checkpoint on their next sync.")


if __name__ == "__main__":
    asyncio.run(main())
