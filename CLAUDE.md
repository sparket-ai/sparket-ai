# Sparket Subnet — Validator Scoring System

## Critical Rules (learned from production incidents)

1. **GT contamination**: ground_truth_closing must only contain sides valid for market kind. MONEYLINE=HOME,AWAY,DRAW. SPREAD=HOME,AWAY. TOTAL=OVER,UNDER. DRAW_NO_BET=HOME,AWAY. Any SQL touching GT closing must filter by kind.
2. **Missing metrics default to 0.5** in SkillScoreJob — this is neutral, not penalizing. Brier default 0.5 > floor 0.30 → correctly zeroes skill. But sos, lead, cal, sharp default 0.5 → free pass. Known gap.
3. **Job failures don't halt downstream**: If RollingAggregatesJob fails, SkillScoreJob still runs on stale data. Check `results["jobs_failed"]` after scoring.
4. **Sybil penalty is additive, not multiplicative**: 149-miner cluster gets uniqueness_dim ~0.37, not ~0. Known gap.
5. **Deploy before commit**: Test in prod. Only commit after validated. Scoring changes that affect miner scores deploy freely. Protocol changes (checkpoint format, synapse API) need commit + notice.
6. **Epoch bump after scoring changes**: Any change upstream of checkpoint export requires `bump_epoch()`. Auditors rate-limit: 1/day, 3/week.
7. **DB pool exhaustion**: NullPool is default for safety. Max 100 connections. Restart can spike connections; transient "too many clients" is expected.

## Architecture (3-tier)

Validator (local, PM2) → Host Server (157.173.192.23, FastAPI) → Dashboard (Vite SPA)

## Verification Gates

50 tests in `tests/validator/scoring/test_verification_gates*.py` covering 7 goal areas.
Full spec: `specs/000-system-verification/spec.md`
Run: `.venv/bin/python -m pytest tests/validator/scoring/test_verification_gates.py tests/validator/scoring/test_verification_gates_2.py -v`

## Key Paths

- Scoring pipeline: `sparket/validator/scoring/` (jobs, metrics, aggregation, ground_truth)
- Score orchestration: `sparket/validator/handlers/score/main_score.py`
- Ledger/auditor consensus: `sparket/validator/ledger/` (compute_weights.py is the critical shared path)
- Config: `sparket/validator/config/scoring_params.py`
