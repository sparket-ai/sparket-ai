# Implementation Plan: Shapley Scoring Overhaul

**Branch**: `scoring-overhaul-shapley` | **Date**: 2026-03-23 | **Spec**: `specs/scoring-overhaul-shapley/spec.md`

## Summary

Replace the additive 4-dimension SkillScore with a multiplicative Cobb-Douglas 5-pillar formula. Add two new scoring dimensions: Uniqueness (composite SOS with pairwise miner correlation + spectral clustering) and Marginal (Monte Carlo Shapley contribution via log-pool aggregation). Extend auditor pipeline with distributed correlation and Shapley computation.

## Technical Context

**Language/Version**: Python 3.10+
**Primary Dependencies**: NumPy (vectorised math), scikit-learn (spectral clustering), existing Pydantic 2+ / SQLAlchemy async / pytest-asyncio stack
**Storage**: PostgreSQL (async SQLAlchemy, Alembic migrations) — new columns on `miner_rolling_score`, new tables for correlation/Shapley
**Testing**: pytest with markers `slow`, `integration`. Backtesting against historical settled markets.
**Target Platform**: Linux VPS (4-core / 32GB RAM constraint for auditors)
**Performance Goals**: Full Shapley computation for 256 miners × 50 markets < 30 min distributed across 10 auditors. Single-market Shapley < 30s on 4 cores.
**Constraints**: All emission scoring must be deterministic. Log-pool + Shapley must produce identical results on primary and auditor given same inputs.

## Constitution Check

| Principle | Status |
|-----------|--------|
| Aggregate-First | PASS — Shapley directly measures aggregate improvement |
| Deterministic & Auditor-Verifiable | PASS — Monte Carlo uses deterministic seed per (epoch, market). Log-pool is exact arithmetic. |
| Multiplicative Complementarity | PASS — Cobb-Douglas with 5 exponents, no additive fallback |
| Test-Driven with Backtesting | PASS — synthetic Shapley tests + historical backtest required before merge |
| Existing Infrastructure Preserved | PASS — new scoring jobs plug into WorkQueue/Worker framework. Per-submission metrics unchanged. |
| No Over-Engineering | PASS — straight to Shapley, no intermediate LOO-only phase |

## Project Structure

### Source Code (new and modified files)

```text
sparket/validator/scoring/
├── jobs/
│   ├── skill_score.py                    # MODIFY — Cobb-Douglas formula, 5 pillars
│   ├── originality_lead_lag.py           # MODIFY — add SOS_crowd, SOS_cluster to output
│   ├── composite_uniqueness.py           # NEW — pairwise correlation, spectral clustering, composite SOS
│   └── shapley_contribution.py           # NEW — Monte Carlo Shapley via log-pool
├── aggregation/
│   ├── log_pool.py                       # NEW — weighted log-odds pool, LOO subtraction
│   ├── correlation.py                    # NEW — pairwise miner correlation matrix
│   └── clustering.py                     # NEW — spectral clustering, ClusterPenalty
├── metrics/
│   └── time_series.py                    # MINOR — extract compute_sos as shared utility
├── batch/
│   └── processor.py                      # MODIFY — add COMPOSITE_UNIQUENESS and SHAPLEY work types
└── worker/
    └── runner.py                         # MODIFY — dispatch new work types

sparket/validator/config/
└── scoring_params.py                     # MODIFY — add Cobb-Douglas exponents, Shapley K, correlation params

sparket/validator/ledger/
├── compute_weights.py                    # MODIFY — Cobb-Douglas formula (mirrors skill_score.py)
├── models.py                             # MODIFY — add new accumulator fields to CheckpointWindow
└── exporter.py                           # MODIFY — export new dimension scores

sparket/validator/database/schema/
├── miner.py                              # MODIFY — add columns to miner_rolling_score
└── scoring_state.py                      # MODIFY — new work types in enum

tests/validator/scoring/
├── test_cobb_douglas.py                  # NEW — unit tests for multiplicative formula
├── test_log_pool.py                      # NEW — log-pool + LOO correctness
├── test_composite_uniqueness.py          # NEW — correlation, clustering, composite SOS
├── test_shapley.py                       # NEW — Monte Carlo Shapley convergence + known scenarios
└── test_compute_weights_v2.py            # NEW — end-to-end weight computation with new formula
```

### Database Migrations

```text
alembic/versions/
└── xxx_add_shapley_scoring_columns.py    # NEW — migration for new columns + tables
```

**New columns on `miner_rolling_score`**:
- `uniqueness_dim` FLOAT — composite SOS score [0,1]
- `marginal_dim` FLOAT — normalised Shapley contribution [0,1]
- `sos_crowd` FLOAT — mean pairwise miner correlation score
- `sos_cluster` FLOAT — cluster penalty score
- `sos_composite` FLOAT — weighted blend of market/crowd/cluster
- `shapley_mean` FLOAT — time-decayed rolling Shapley value
- `shapley_ws` FLOAT — Shapley accumulator weighted sum
- `shapley_wt` FLOAT — Shapley accumulator weight total
- `cluster_id` INT NULLABLE — assigned cluster ID
- `cluster_size` INT DEFAULT 1 — size of assigned cluster

**New table `miner_pairwise_correlation`**:
- PK: (miner_a_id, miner_b_id, as_of)
- `correlation` FLOAT — Pearson correlation
- `n_common_markets` INT — number of overlapping submissions

**New table `shapley_contribution`**:
- PK: (miner_id, market_id)
- `shapley_value` FLOAT — raw Shapley for this market
- `settled_at` TIMESTAMPTZ
- `k_permutations` INT — number of permutations used

### Config Additions to `scoring_params.py`

```python
class CobbDouglasParams(BaseModel):
    accuracy_exponent: Decimal = Decimal("0.5")
    edge_exponent: Decimal = Decimal("1.0")
    timeliness_exponent: Decimal = Decimal("0.5")
    uniqueness_exponent: Decimal = Decimal("1.5")
    marginal_exponent: Decimal = Decimal("1.0")
    floor_threshold: Decimal = Decimal("0.30")  # Brier hard floor
    epsilon: Decimal = Decimal("0.01")  # Min dimension value

class CompositeSOSParams(BaseModel):
    w_market: Decimal = Decimal("0.2")
    w_crowd: Decimal = Decimal("0.5")
    w_cluster: Decimal = Decimal("0.3")
    correlation_window_days: int = 30
    min_common_markets: int = 10
    cluster_correlation_threshold: Decimal = Decimal("0.85")

class ShapleyParams(BaseModel):
    k_permutations: int = 500
    seed_prefix: int = 42  # deterministic per (epoch, market)
    min_miners_for_shapley: int = 5
    max_parallel_markets: int = 4  # parallel market processing within a batch
    batch_interval_minutes: int = 60  # run Shapley every hour
    prob_clamp_epsilon: Decimal = Decimal("0.005")  # clamp probs to [ε, 1-ε] before logit
```

## Key Design Decisions

### 1. Deterministic Shapley via Seeded RNG

Monte Carlo Shapley is inherently random (permutation sampling). For auditor reproducibility, we seed the RNG deterministically: `seed = hash(seed_prefix, epoch, market_id)`. This means the same permutations are generated on primary and auditor, producing identical Shapley values.

### 2. Log-Pool as Value Function

The Shapley value function V(S) = negative Brier score of the log-pool aggregate built from subset S. Log-pool is chosen because:
- Theoretically optimal under Bayesian independence
- Linear in log-odds space → O(1) incremental updates when adding a miner to the coalition
- Consistent with future consensus line product

### 3. Scoring Job Ordering

Current: ROLLING → CALIBRATION → ORIGINALITY → SKILL

New: ROLLING → CALIBRATION → ORIGINALITY → **COMPOSITE_UNIQUENESS** → **SHAPLEY** → SKILL

COMPOSITE_UNIQUENESS depends on ORIGINALITY (needs SOS_market). SHAPLEY depends on settled outcomes + previous epoch's weights. SKILL depends on all prior jobs.

### 4. Bootstrap / Epoch 0

On first run with no previous Cobb-Douglas weights:
- Log-pool weights for Shapley = current additive SkillScores (normalised)
- Uniqueness = existing SOS_market (SOS_crowd and SOS_cluster default to 0.5)
- Marginal = 0.5 (neutral, no Shapley data yet)
- Fixed-point: by epoch 2, Cobb-Douglas weights feed back into Shapley log-pool weights

### 5. Incremental Shapley via Settled Markets (Hourly Batches)

Shapley is computed retrospectively on settled markets only, triggered hourly as part of the scoring cycle. At 25-50 settlements/day, each hourly batch processes ~1-4 markets (~13-52 seconds of compute). Each settled market produces per-miner Shapley values that are time-decayed into the rolling `shapley_mean`. MarginalDim reflects contribution quality over the rolling window with at most ~1 hour of lag — consistent with how all other dimensions work.

## Complexity Tracking

No constitution violations requiring justification. scikit-learn is the only new dependency (for spectral clustering), which is already a transitive dependency via the existing stack.
