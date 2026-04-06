"""Diagnose inflated CLE (Closing Line Edge) for NBA total markets.

Investigates potential causes of miners appearing to beat the books
by 10-20% in NBA total markets:

1. Bias factor drift — clamping bug inflates bias_factor for ~50% probs
2. Stale closing lines — late capture uses quotes hours before tip-off
3. Sportsbook mapping — unmapped books all collapse to sportsbook_id=1
4. CLE distribution — outliers or asymmetry skewing the average

Usage:
    DATABASE_URL="postgresql+asyncpg://sparket:sparket@127.0.0.1:5435/sparket" \
      uv run python scripts/dev/diagnose_nba_totals.py
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


class LiteDB:
    """Minimal async DB wrapper — no pool, no init_db overhead."""

    def __init__(self, engine):
        self._engine = engine

    async def read(self, query, params=None, *, mappings=True):
        from sqlalchemy.ext.asyncio import AsyncSession
        async with AsyncSession(self._engine) as session:
            result = await session.execute(query, params or {})
            if mappings:
                return [dict(r) for r in result.mappings().all()]
            return result.all()

    async def dispose(self):
        await self._engine.dispose()


async def main() -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(db_url, poolclass=NullPool)
    dbm = LiteDB(engine)

    print("=" * 72)
    print("NBA TOTALS CLE DIAGNOSTIC REPORT")
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 72)

    # ------------------------------------------------------------------
    # 1. Bias factor analysis for NBA totals
    # ------------------------------------------------------------------
    print("\n--- 1. SPORTSBOOK BIAS FACTORS (NBA, market_kind='total') ---\n")

    bias_rows = await dbm.read(
        text("""
            SELECT
                sb.sportsbook_id,
                sb.sport_id,
                sb.market_kind,
                sb.bias_factor,
                sb.variance,
                sb.sample_count,
                sb.version,
                s.code AS sport_code,
                bk.name AS book_name
            FROM sportsbook_bias sb
            LEFT JOIN sport s ON s.sport_id = sb.sport_id
            LEFT JOIN sportsbook bk ON bk.sportsbook_id = sb.sportsbook_id
            WHERE sb.market_kind = 'TOTAL'
            ORDER BY sb.bias_factor DESC
        """),
        mappings=True,
    )

    if not bias_rows:
        print("  (no bias rows found for market_kind='total')")
    else:
        print(f"  {'Book':<20} {'Sport':<8} {'Bias':<10} {'Variance':<12} {'Samples':<10} {'Version'}")
        print(f"  {'-'*20} {'-'*8} {'-'*10} {'-'*12} {'-'*10} {'-'*7}")
        for r in bias_rows:
            book = r.get("book_name") or f"id={r['sportsbook_id']}"
            sport = r.get("sport_code") or f"id={r['sport_id']}"
            print(
                f"  {book:<20} {sport:<8} {float(r['bias_factor']):<10.4f} "
                f"{float(r['variance']):<12.6f} {r['sample_count']:<10} {r['version']}"
            )

        # Highlight NBA specifically
        nba_biases = [r for r in bias_rows if (r.get("sport_code") or "").lower() == "nba"]
        if nba_biases:
            avg_bias = sum(float(r["bias_factor"]) for r in nba_biases) / len(nba_biases)
            print(f"\n  ** NBA average bias_factor: {avg_bias:.4f} (should be ~1.0, >1.05 = significant) **")
        else:
            print("\n  ** No NBA-specific bias rows found. Check sport_code values. **")

    # ------------------------------------------------------------------
    # 2. Closing snapshot staleness for NBA totals
    # ------------------------------------------------------------------
    print("\n--- 2. CLOSING SNAPSHOT STALENESS (NBA totals) ---\n")

    staleness_rows = await dbm.read(
        text("""
            SELECT
                COUNT(*) AS n_markets,
                AVG(EXTRACT(EPOCH FROM (e.start_time_utc - gs.snapshot_ts)) / 3600) AS avg_hours_before_start,
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (e.start_time_utc - gs.snapshot_ts)) / 3600
                ) AS median_hours_before_start,
                MAX(EXTRACT(EPOCH FROM (e.start_time_utc - gs.snapshot_ts)) / 3600) AS max_hours_before_start,
                MIN(EXTRACT(EPOCH FROM (e.start_time_utc - gs.snapshot_ts)) / 3600) AS min_hours_before_start,
                COUNT(*) FILTER (
                    WHERE EXTRACT(EPOCH FROM (e.start_time_utc - gs.snapshot_ts)) / 3600 > 2
                ) AS stale_over_2h,
                COUNT(*) FILTER (
                    WHERE EXTRACT(EPOCH FROM (e.start_time_utc - gs.snapshot_ts)) / 3600 > 6
                ) AS stale_over_6h
            FROM ground_truth_snapshot gs
            JOIN market m ON m.market_id = gs.market_id
            JOIN event e ON e.event_id = m.event_id
            JOIN league l ON l.league_id = e.league_id
            JOIN sport s ON s.sport_id = l.sport_id
            WHERE gs.is_closing = true
              AND m.kind = 'TOTAL'
              AND s.code = 'nba'
        """),
        mappings=True,
    )

    if staleness_rows and staleness_rows[0]["n_markets"]:
        r = staleness_rows[0]
        print(f"  Total NBA total closing snapshots: {r['n_markets']}")
        print(f"  Avg hours before event start:      {_fmt(r['avg_hours_before_start'])}")
        print(f"  Median hours before start:         {_fmt(r['median_hours_before_start'])}")
        print(f"  Min hours before start:            {_fmt(r['min_hours_before_start'])}")
        print(f"  Max hours before start:            {_fmt(r['max_hours_before_start'])}")
        print(f"  Stale (>2h before start):          {r['stale_over_2h']}")
        print(f"  Very stale (>6h before start):     {r['stale_over_6h']}")
        pct_stale = 100 * r["stale_over_2h"] / r["n_markets"] if r["n_markets"] else 0
        print(f"\n  ** {pct_stale:.1f}% of closing snapshots are >2h stale (ideally <5%) **")
    else:
        print("  (no closing snapshots found for NBA totals)")

    # ------------------------------------------------------------------
    # 3. Sportsbook mapping — prevalence of fallback sportsbook_id=1
    # ------------------------------------------------------------------
    print("\n--- 3. SPORTSBOOK ID MAPPING (fallback id=1 check) ---\n")

    mapping_rows = await dbm.read(
        text("""
            SELECT
                pq.raw->>'sportsbook' AS raw_book,
                COUNT(*) AS n_quotes,
                CASE WHEN s.sportsbook_id IS NULL THEN 'UNMAPPED (→ id=1)' ELSE s.name END AS mapped_name
            FROM provider_quote pq
            JOIN market m ON m.market_id = pq.market_id
            JOIN event e ON e.event_id = m.event_id
            JOIN league l ON l.league_id = e.league_id
            JOIN sport sp ON sp.sport_id = l.sport_id
            LEFT JOIN sportsbook s ON s.provider_id = pq.provider_id
                AND s.code = pq.raw->>'sportsbook'
            WHERE m.kind = 'TOTAL'
              AND sp.code = 'nba'
              AND pq.ts > NOW() - INTERVAL '30 days'
            GROUP BY pq.raw->>'sportsbook', s.sportsbook_id, s.name
            ORDER BY n_quotes DESC
        """),
        mappings=True,
    )

    if mapping_rows:
        print(f"  {'Raw Book Code':<25} {'Mapped To':<25} {'Quotes (30d)'}")
        print(f"  {'-'*25} {'-'*25} {'-'*12}")
        unmapped_total = 0
        total_quotes = 0
        for r in mapping_rows:
            raw = r.get("raw_book") or "(null)"
            mapped = r.get("mapped_name") or "UNMAPPED (→ id=1)"
            n = r["n_quotes"]
            total_quotes += n
            if "UNMAPPED" in mapped:
                unmapped_total += n
            print(f"  {raw:<25} {mapped:<25} {n}")
        if total_quotes:
            print(f"\n  ** {100*unmapped_total/total_quotes:.1f}% of quotes unmapped (collapse to id=1) **")
    else:
        print("  (no provider quotes found)")

    # ------------------------------------------------------------------
    # 4. CLE distribution for NBA totals vs other markets
    # ------------------------------------------------------------------
    print("\n--- 4. CLE DISTRIBUTION BY SPORT + MARKET KIND ---\n")

    cle_rows = await dbm.read(
        text("""
            SELECT
                sp.code AS sport_code,
                m.kind AS market_kind,
                COUNT(*) AS n_scored,
                AVG(vc.cle) AS avg_cle,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vc.cle) AS median_cle,
                STDDEV(vc.cle) AS std_cle,
                AVG(vc.cle) FILTER (WHERE vc.cle > 0) AS avg_positive_cle,
                COUNT(*) FILTER (WHERE vc.cle > 0) AS n_positive,
                AVG(vc.cle) FILTER (WHERE vc.cle < 0) AS avg_negative_cle,
                COUNT(*) FILTER (WHERE vc.cle < 0) AS n_negative
            FROM submission_vs_close vc
            JOIN miner_submission ms ON ms.submission_id = vc.submission_id
            JOIN market m ON m.market_id = ms.market_id
            JOIN event e ON e.event_id = m.event_id
            JOIN league l ON l.league_id = e.league_id
            JOIN sport sp ON sp.sport_id = l.sport_id
            GROUP BY sp.code, m.kind
            ORDER BY AVG(vc.cle) DESC
        """),
        mappings=True,
    )

    if cle_rows:
        print(f"  {'Sport':<10} {'Kind':<12} {'N':<8} {'Avg CLE':<10} {'Med CLE':<10} {'Std':<10} {'%Pos':<8}")
        print(f"  {'-'*10} {'-'*12} {'-'*8} {'-'*10} {'-'*10} {'-'*10} {'-'*8}")
        for r in cle_rows:
            n = r["n_scored"]
            pct_pos = 100 * r["n_positive"] / n if n else 0
            print(
                f"  {r['sport_code']:<10} {r['market_kind']:<12} {n:<8} "
                f"{_fmt(r['avg_cle']):<10} {_fmt(r['median_cle']):<10} "
                f"{_fmt(r['std_cle']):<10} {pct_pos:<8.1f}"
            )
    else:
        print("  (no CLE data found)")

    # ------------------------------------------------------------------
    # 5. CLE by contributing_books count (fewer books = less reliable)
    # ------------------------------------------------------------------
    print("\n--- 5. CLE vs CONTRIBUTING BOOKS (NBA totals) ---\n")

    books_rows = await dbm.read(
        text("""
            SELECT
                gtc.contributing_books,
                COUNT(*) AS n_scored,
                AVG(vc.cle) AS avg_cle,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vc.cle) AS median_cle
            FROM submission_vs_close vc
            JOIN miner_submission ms ON ms.submission_id = vc.submission_id
            JOIN market m ON m.market_id = ms.market_id
            JOIN event e ON e.event_id = m.event_id
            JOIN league l ON l.league_id = e.league_id
            JOIN sport sp ON sp.sport_id = l.sport_id
            JOIN ground_truth_closing gtc ON gtc.market_id = ms.market_id AND gtc.side = ms.side
            WHERE m.kind = 'TOTAL'
              AND sp.code = 'nba'
            GROUP BY gtc.contributing_books
            ORDER BY gtc.contributing_books
        """),
        mappings=True,
    )

    if books_rows:
        print(f"  {'Books':<8} {'N Scored':<10} {'Avg CLE':<12} {'Median CLE'}")
        print(f"  {'-'*8} {'-'*10} {'-'*12} {'-'*10}")
        for r in books_rows:
            print(
                f"  {r['contributing_books']:<8} {r['n_scored']:<10} "
                f"{_fmt(r['avg_cle']):<12} {_fmt(r['median_cle'])}"
            )
    else:
        print("  (no data)")

    # ------------------------------------------------------------------
    # 6. Theoretical bias factor drift simulation
    # ------------------------------------------------------------------
    print("\n--- 6. THEORETICAL BIAS DRIFT (clamping bug simulation) ---\n")
    print("  Simulating EMA bias_factor convergence with clamped [0.5, 2.0] adjustment")
    print("  for a perfectly calibrated book at various probabilities:\n")

    from decimal import Decimal
    import random

    random.seed(42)
    alpha = 0.1  # default bias_learning_rate

    print(f"  {'True Prob':<12} {'Converged Bias':<18} {'Expected (correct)':<20} {'Drift'}")
    print(f"  {'-'*12} {'-'*18} {'-'*20} {'-'*10}")

    for true_prob in [0.3, 0.4, 0.5, 0.6, 0.7, 0.8]:
        bias = 1.0
        for _ in range(5000):
            hit = 1 if random.random() < true_prob else 0
            implied = hit / true_prob if true_prob > 0 else 0
            implied = max(0.5, min(implied, 2.0))  # THE BUG
            bias = (1 - alpha) * bias + alpha * implied

        drift = bias - 1.0
        print(f"  {true_prob:<12.1f} {bias:<18.4f} {'1.0000':<20} {drift:+.4f}")

    print("\n  ** Near p=0.5 (NBA totals), bias drifts +0.15 to +0.25 above true 1.0 **")
    print("  ** This inflates bias_factor, distorting consensus weights between books **")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("SUMMARY")
    print("=" * 72)
    print("""
  Check the results above for:
  1. Bias factors significantly > 1.0 for NBA total books → confirms clamping bug
  2. High % of stale closing snapshots (>2h) → stale ground truth
  3. High % of unmapped quotes → consensus distortion via id=1 fallback
  4. NBA total avg CLE much higher than other sports → confirms anomaly is localized
  5. Lower contributing_books correlating with higher CLE → thin consensus issue

  Recommended fix priority:
  - Fix bias clamping (Finding 1) — change lower clamp from 0.5 to 0.0
  - Rescoring pipeline: recalculate bias → recompute consensus → rescore CLE
  - Epoch bump to reset auditor accumulators after rescoring
    """)

    await dbm.dispose()
    print("Done.")


def _fmt(val, decimals=4):
    """Format a numeric value or None."""
    if val is None:
        return "N/A"
    return f"{float(val):.{decimals}f}"


if __name__ == "__main__":
    asyncio.run(main())
