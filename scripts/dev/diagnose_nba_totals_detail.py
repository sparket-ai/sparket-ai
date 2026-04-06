"""Deep dive into NBA total CLE: gaming mechanics and outlier analysis."""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# Standard join chain for sport filtering
_SPORT_JOIN = """
    JOIN market m ON m.market_id = ms.market_id
    JOIN event e ON e.event_id = m.event_id
    JOIN league l ON l.league_id = e.league_id
    JOIN sport sp ON sp.sport_id = l.sport_id
"""


async def main() -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(db_url, poolclass=NullPool)

    async with AsyncSession(engine) as s:

        # 1. CLE does NOT depend on outcome — verify
        print("=== 1. DO EXTREME CLE SUBMISSIONS DEPEND ON OUTCOME? ===")
        r = await s.execute(text(f"""
            SELECT
                CASE WHEN sos.pss_brier IS NOT NULL THEN 'settled' ELSE 'unscored' END AS status,
                COUNT(*) AS n,
                AVG(vc.cle) AS avg_cle
            FROM submission_vs_close vc
            JOIN miner_submission ms ON ms.submission_id = vc.submission_id
            {_SPORT_JOIN}
            LEFT JOIN submission_outcome_score sos ON sos.submission_id = ms.submission_id
            WHERE m.kind = 'TOTAL' AND sp.code = 'nba'
              AND ms.odds_eu > 3.0
            GROUP BY 1
        """))
        for row in r.mappings().all():
            print(f"  {row['status']}: n={row['n']}, avg_cle={float(row['avg_cle']):.4f}")

        # 2. Brier scores for extreme odds
        print("\n=== 2. BRIER SCORES BY ODDS BUCKET (settled NBA totals) ===")
        r = await s.execute(text(f"""
            SELECT
                CASE
                    WHEN ms.odds_eu > 5.0 THEN '>5.0'
                    WHEN ms.odds_eu > 3.0 THEN '3.0-5.0'
                    WHEN ms.odds_eu > 2.5 THEN '2.5-3.0'
                    WHEN ms.odds_eu > 2.0 THEN '2.0-2.5'
                    ELSE '<=2.0'
                END AS bucket,
                COUNT(*) AS n,
                AVG(vc.cle) AS avg_cle,
                AVG(sos.brier) AS avg_brier,
                AVG(sos.pss_brier) AS avg_pss
            FROM submission_vs_close vc
            JOIN miner_submission ms ON ms.submission_id = vc.submission_id
            JOIN submission_outcome_score sos ON sos.submission_id = ms.submission_id
            {_SPORT_JOIN}
            WHERE m.kind = 'TOTAL' AND sp.code = 'nba'
            GROUP BY 1
            ORDER BY AVG(vc.cle) DESC
        """))
        print(f"  {'Bucket':<12} {'N':<8} {'Avg CLE':<10} {'Avg Brier':<12} {'Avg PSS'}")
        print(f"  {'-'*12} {'-'*8} {'-'*10} {'-'*12} {'-'*8}")
        for row in r.mappings().all():
            pss = float(row['avg_pss']) if row['avg_pss'] is not None else 0
            print(f"  {row['bucket']:<12} {row['n']:<8} {float(row['avg_cle']):<10.4f} {float(row['avg_brier']):<12.4f} {pss:.4f}")

        # 3. Top CLE miners' overall skill scores
        print("\n=== 3. TOP CLE MINERS: OVERALL SKILL SCORE & BRIER ===")
        r = await s.execute(text(f"""
            WITH top_cle_miners AS (
                SELECT ms.miner_id, AVG(vc.cle) AS nba_total_cle
                FROM submission_vs_close vc
                JOIN miner_submission ms ON ms.submission_id = vc.submission_id
                {_SPORT_JOIN}
                WHERE m.kind = 'TOTAL' AND sp.code = 'nba'
                GROUP BY ms.miner_id
                ORDER BY AVG(vc.cle) DESC
                LIMIT 15
            )
            SELECT
                t.miner_id,
                t.nba_total_cle,
                mrs.brier_mean,
                mrs.es_mean,
                mrs.es_std,
                mrs.es_adj,
                mrs.skill_score,
                mrs.n_submissions,
                mrs.fq_raw,
                mrs.pss_mean
            FROM top_cle_miners t
            LEFT JOIN LATERAL (
                SELECT * FROM miner_rolling_score mrs2
                WHERE mrs2.miner_id = t.miner_id
                ORDER BY mrs2.as_of DESC
                LIMIT 1
            ) mrs ON true
            ORDER BY t.nba_total_cle DESC
        """))
        print(f"  {'Miner':<10} {'NBA CLE':<10} {'Brier':<8} {'ES Mean':<10} {'ES Adj':<10} {'Skill':<8} {'FQ':<8} {'PSS'}")
        print(f"  {'-'*10} {'-'*10} {'-'*8} {'-'*10} {'-'*10} {'-'*8} {'-'*8} {'-'*8}")
        for row in r.mappings().all():
            def f(v):
                return f"{float(v):.4f}" if v is not None else "N/A"
            print(f"  {row['miner_id']:<10} {f(row['nba_total_cle']):<10} {f(row['brier_mean']):<8} {f(row['es_mean']):<10} {f(row['es_adj']):<10} {f(row['skill_score']):<8} {f(row['fq_raw']):<8} {f(row['pss_mean'])}")

        # 4. Top CLE miners: fraction of extreme submissions
        print("\n=== 4. TOP CLE MINERS: EXTREME vs REASONABLE SUBMISSIONS ===")
        r = await s.execute(text(f"""
            WITH top_cle_miners AS (
                SELECT ms.miner_id
                FROM submission_vs_close vc
                JOIN miner_submission ms ON ms.submission_id = vc.submission_id
                {_SPORT_JOIN}
                WHERE m.kind = 'TOTAL' AND sp.code = 'nba'
                GROUP BY ms.miner_id
                ORDER BY AVG(vc.cle) DESC
                LIMIT 10
            )
            SELECT
                ms.miner_id,
                COUNT(*) AS total_nba_total,
                COUNT(*) FILTER (WHERE ms.odds_eu > 3.0) AS n_extreme,
                COUNT(*) FILTER (WHERE ms.odds_eu BETWEEN 1.7 AND 2.3) AS n_reasonable,
                AVG(vc.cle) FILTER (WHERE ms.odds_eu BETWEEN 1.7 AND 2.3) AS reasonable_cle,
                AVG(vc.cle) FILTER (WHERE ms.odds_eu > 3.0) AS extreme_cle
            FROM miner_submission ms
            JOIN submission_vs_close vc ON vc.submission_id = ms.submission_id
            {_SPORT_JOIN}
            WHERE ms.miner_id IN (SELECT miner_id FROM top_cle_miners)
              AND m.kind = 'TOTAL' AND sp.code = 'nba'
            GROUP BY ms.miner_id
            ORDER BY COUNT(*) FILTER (WHERE ms.odds_eu > 3.0)::float / COUNT(*) DESC
        """))
        print(f"  {'Miner':<10} {'Total':<8} {'Extreme':<10} {'%Ext':<8} {'Reas':<8} {'Reas CLE':<10} {'Ext CLE'}")
        print(f"  {'-'*10} {'-'*8} {'-'*10} {'-'*8} {'-'*8} {'-'*10} {'-'*8}")
        for row in r.mappings().all():
            pct = 100 * row['n_extreme'] / row['total_nba_total'] if row['total_nba_total'] else 0
            rc = float(row['reasonable_cle']) if row['reasonable_cle'] else 0
            ec = float(row['extreme_cle']) if row['extreme_cle'] else 0
            print(f"  {row['miner_id']:<10} {row['total_nba_total']:<8} {row['n_extreme']:<10} {pct:<8.1f} {row['n_reasonable']:<8} {rc:<10.4f} {ec:.4f}")

        # 5. Extreme submissions across all sports
        print("\n=== 5. EXTREME ODDS (>2x close) BY SPORT+KIND ===")
        r = await s.execute(text(f"""
            SELECT
                sp.code AS sport,
                m.kind::text AS kind,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE ms.odds_eu / NULLIF(vc.close_odds_eu, 0) > 2.0) AS n_extreme,
                AVG(vc.cle) AS avg_cle,
                AVG(vc.cle) FILTER (WHERE ms.odds_eu / NULLIF(vc.close_odds_eu, 0) <= 2.0) AS clean_avg_cle
            FROM submission_vs_close vc
            JOIN miner_submission ms ON ms.submission_id = vc.submission_id
            {_SPORT_JOIN}
            WHERE vc.close_odds_eu > 0
            GROUP BY sp.code, m.kind
            ORDER BY COUNT(*) FILTER (WHERE ms.odds_eu / NULLIF(vc.close_odds_eu, 0) > 2.0)::float / NULLIF(COUNT(*), 0) DESC
        """))
        print(f"  {'Sport':<8} {'Kind':<12} {'Total':<10} {'N Ext':<10} {'%Ext':<8} {'Raw Avg':<10} {'Clean Avg'}")
        print(f"  {'-'*8} {'-'*12} {'-'*10} {'-'*10} {'-'*8} {'-'*10} {'-'*10}")
        for row in r.mappings().all():
            pct = 100 * row['n_extreme'] / row['total'] if row['total'] else 0
            raw = float(row['avg_cle']) if row['avg_cle'] else 0
            clean = float(row['clean_avg_cle']) if row['clean_avg_cle'] else 0
            print(f"  {row['sport']:<8} {row['kind']:<12} {row['total']:<10} {row['n_extreme']:<10} {pct:<8.1f} {raw:<10.4f} {clean:.4f}")

    await engine.dispose()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
