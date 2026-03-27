"""One-time backfill script for submission_vs_close CLV/CLE scoring.

Scores all unscored submissions within the rolling aggregates window (30 days)
that have matching ground_truth_closing records. Processes in batches to avoid
overwhelming the database.

Usage:
    python scripts/backfill_clv.py [--batch-size 10000] [--days 30] [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from sqlalchemy import text

from sparket.validator.database.dbm import DBM
from sparket.validator.config.config import Config
from sparket.validator.scoring.determinism import to_decimal
from sparket.validator.scoring.metrics.clv import compute_clv


_SELECT_UNSCORED_BATCH = text("""
    SELECT
        ms.submission_id,
        ms.miner_id,
        ms.market_id,
        ms.side,
        ms.submitted_at,
        ms.odds_eu,
        ms.imp_prob
    FROM miner_submission ms
    LEFT JOIN submission_vs_close svc ON ms.submission_id = svc.submission_id
    WHERE svc.submission_id IS NULL
      AND ms.submitted_at >= :since
    ORDER BY ms.submission_id
    LIMIT :limit
""")

_SELECT_GROUND_TRUTH = text("""
    SELECT
        gtc.market_id,
        gtc.side,
        gtc.prob_consensus,
        gtc.odds_consensus,
        gtc.contributing_books,
        gtc.bias_version,
        gtc.computed_at,
        e.start_time_utc
    FROM ground_truth_closing gtc
    JOIN market m ON gtc.market_id = m.market_id
    JOIN event e ON m.event_id = e.event_id
    WHERE gtc.market_id = :market_id
      AND gtc.side = :side
""")

_INSERT_SUBMISSION_VS_CLOSE = text("""
    INSERT INTO submission_vs_close (
        submission_id, provider_basis, close_ts, close_odds_eu,
        close_imp_prob, close_imp_prob_norm, clv_odds, clv_prob,
        cle, minutes_to_close, computed_at, ground_truth_version
    ) VALUES (
        :submission_id, :provider_basis, :close_ts, :close_odds_eu,
        :close_imp_prob, :close_imp_prob_norm, :clv_odds, :clv_prob,
        :cle, :minutes_to_close, :computed_at, :ground_truth_version
    )
    ON CONFLICT (submission_id) DO NOTHING
""")

# Cache ground truth lookups to avoid repeated queries for the same market/side
_gt_cache: dict[tuple[int, str], dict | None] = {}


async def fetch_ground_truth(db: DBM, market_id: int, side: str) -> dict | None:
    key = (market_id, str(side))
    if key in _gt_cache:
        return _gt_cache[key]

    rows = await db.read(
        _SELECT_GROUND_TRUTH,
        params={"market_id": market_id, "side": side},
        mappings=True,
    )
    if not rows:
        _gt_cache[key] = None
        return None

    row = rows[0]
    gt = {
        "prob_consensus": to_decimal(row["prob_consensus"], "prob_consensus"),
        "odds_consensus": to_decimal(row["odds_consensus"], "odds_consensus"),
        "contributing_books": int(row["contributing_books"] or 0),
        "bias_version": row["bias_version"],
        "computed_at": row["computed_at"],
        "start_time_utc": row["start_time_utc"],
    }
    _gt_cache[key] = gt
    return gt


async def backfill(db: DBM, since: datetime, batch_size: int, dry_run: bool) -> None:
    total_scored = 0
    total_skipped = 0
    total_errors = 0
    batch_num = 0

    while True:
        batch_num += 1
        t0 = time.time()

        rows = await db.read(
            _SELECT_UNSCORED_BATCH,
            params={"since": since, "limit": batch_size},
            mappings=True,
        )

        if not rows:
            print(f"No more unscored submissions. Done.")
            break

        scored = 0
        skipped = 0
        errors = 0
        write_params = []

        for row in rows:
            try:
                gt = await fetch_ground_truth(db, row["market_id"], row["side"])
                if gt is None or gt["contributing_books"] < 2:
                    skipped += 1
                    continue

                miner_odds = float(to_decimal(row["odds_eu"], "odds_eu"))
                miner_prob = float(to_decimal(row["imp_prob"], "imp_prob"))
                submitted_at = row["submitted_at"]

                clv_result = compute_clv(
                    miner_odds=miner_odds,
                    miner_prob=miner_prob,
                    truth_odds=float(gt["odds_consensus"]),
                    truth_prob=float(gt["prob_consensus"]),
                    submitted_ts=submitted_at.timestamp(),
                    event_start_ts=gt["start_time_utc"].timestamp(),
                )

                write_params.append({
                    "submission_id": row["submission_id"],
                    "provider_basis": "consensus",
                    "close_ts": gt["computed_at"],
                    "close_odds_eu": float(gt["odds_consensus"]),
                    "close_imp_prob": float(gt["prob_consensus"]),
                    "close_imp_prob_norm": float(gt["prob_consensus"]),
                    "clv_odds": float(clv_result.clv_odds),
                    "clv_prob": float(clv_result.clv_prob),
                    "cle": float(clv_result.cle),
                    "minutes_to_close": clv_result.minutes_to_close,
                    "computed_at": datetime.now(timezone.utc),
                    "ground_truth_version": gt["bias_version"],
                })
                scored += 1

            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Error on submission {row['submission_id']}: {e}")

        if write_params and not dry_run:
            await db.write_many(_INSERT_SUBMISSION_VS_CLOSE, write_params)

        total_scored += scored
        total_skipped += skipped
        total_errors += errors
        elapsed = time.time() - t0

        print(
            f"  Batch {batch_num}: {scored} scored, {skipped} skipped (no GT), "
            f"{errors} errors, {len(rows)} fetched, {elapsed:.1f}s"
            f"{' [DRY RUN]' if dry_run else ''}"
        )

        # If we got fewer rows than the batch size, we're done
        if len(rows) < batch_size:
            break

    print(f"\nBackfill complete: {total_scored} scored, {total_skipped} skipped, {total_errors} errors")
    print(f"GT cache size: {len(_gt_cache)} entries")


async def main():
    parser = argparse.ArgumentParser(description="Backfill CLV scores for unscored submissions")
    parser.add_argument("--batch-size", type=int, default=10000, help="Submissions per batch (default: 10000)")
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days (default: 30)")
    parser.add_argument("--dry-run", action="store_true", help="Compute but don't write to DB")
    args = parser.parse_args()

    since = datetime.now(timezone.utc) - timedelta(days=args.days)

    print(f"Backfill CLV scores")
    print(f"  Since: {since.isoformat()}")
    print(f"  Batch size: {args.batch_size}")
    print(f"  Dry run: {args.dry_run}")
    print()

    config = Config()
    db = DBM.create_worker(config)

    try:
        # Show scope
        rows = await db.read(
            text("""
                SELECT COUNT(*) AS unscored
                FROM miner_submission ms
                LEFT JOIN submission_vs_close svc ON ms.submission_id = svc.submission_id
                WHERE svc.submission_id IS NULL
                  AND ms.submitted_at >= :since
            """),
            params={"since": since},
            mappings=True,
        )
        unscored = rows[0]["unscored"] if rows else 0
        print(f"Unscored submissions in window: {unscored:,}")
        print()

        await backfill(db, since, args.batch_size, args.dry_run)
    finally:
        await db.dispose()


if __name__ == "__main__":
    asyncio.run(main())
