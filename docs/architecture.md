# Architecture: Primary + Auditor Validator Model

## Overview

Sparket uses a primary+auditor model where one primary validator ingests
paid SportsDataIO data and performs full scoring, while auditor validators
independently verify and reproduce weights from exported scoring ledger data.

**The problem:** Running a full validator requires a $600/month SportsDataIO
subscription. This concentrates trust and creates an economic barrier.

**The solution:** The primary exports license-safe scoring data (checkpoints
+ deltas) via an authenticated HTTP endpoint. Auditors fetch this data,
independently verify outcome-based scores, and set weights on chain using
a shared deterministic function.

## Data Flow

```
Primary Validator                    Auditor Validators
┌─────────────────────┐             ┌────────────────────────┐
│ SportsDataIO API    │             │                        │
│       ↓             │             │  Fetch checkpoint      │
│ Ground Truth Build  │             │  + deltas via HTTP     │
│       ↓             │             │       ↓                │
│ Score Submissions   │  ledger     │  Verify manifests      │
│       ↓             │  ──────►   │  + signatures          │
│ Rolling Aggregates  │  endpoint   │       ↓                │
│       ↓             │             │  Verify Brier scores   │
│ SkillScore + Weights│             │  independently         │
│       ↓             │             │       ↓                │
│ Set weights on chain│             │  compute_weights()     │
│       ↓             │             │       ↓                │
│ Export checkpoint   │             │  Set weights on chain  │
│ Export delta        │             │                        │
└─────────────────────┘             └────────────────────────┘
```

## Data Sensitivity Tiers

All data is classified into three tiers:

**Tier 1 - Public:** On-chain data, settled outcomes, scoring config.
Available to anyone.

**Tier 2 - Validator-Gated:** Checkpoint accumulator state, settled
submission outcome scores (post-settlement only), miner roster.
Available to authenticated validators with vpermit + 100K alpha stake.

**Tier 3 - Primary-Only:** SportsDataIO raw data, closing lines, ground
truth snapshots, unsettled miner submissions, CLV/CLE per-submission
values, sportsbook bias data. Never leaves the primary boundary.

## Scoring Ledger System

### Checkpoints (every ~6 hours, ~20KB compressed)

Full accumulator state per miner. Contains (weighted_sum, weight_sum)
pairs for each metric plus derived means. Published every scoring cycle
to keep CLV/CLE-derived metrics fresh for auditors.

### Deltas (every ~6 hours, ~50-200KB compressed)

Settled submission outcome scores since the previous window. Contains
miner implied probabilities and Brier/PSS scores for independently
verifiable audit. Only includes settled markets (zero copy-trading risk).

### Catch-up flow

New auditors download the latest checkpoint (~20KB) and recent deltas
(~1-2MB). Total catch-up: seconds, under 2MB of data.

## Authentication

Challenge-response flow with vpermit + 100K alpha stake gate:

1. Auditor sends hotkey to primary
2. Primary checks metagraph: vpermit=true, stake >= 100K alpha
3. Primary issues a random nonce
4. Auditor signs the nonce with their hotkey
5. Primary verifies signature, issues a bearer token (TTL: 1 hour)
6. All subsequent requests use the bearer token

Fail-closed: any verification failure = reject.

## Deterministic Weight Computation

The `compute_weights()` function is the critical shared code path used
by both primary and auditor:

1. Normalize metrics across miners (z-score logistic or percentile)
2. Combine into intermediate dimensions (ForecastDim, SkillDim, EconDim)
3. Map to 5 Cobb-Douglas pillars (Accuracy, Edge, Timeliness, Uniqueness, Marginal)
4. Clamp all pillars to [epsilon, 1.0]
5. Compute multiplicative Cobb-Douglas SkillScore with configured exponents
6. Apply hard accuracy floor (Brier > 0.30 → zero)
7. L1 normalize
8. Apply burn rate
9. Apply max_weight_limit + min_allowed_weights
10. Convert to uint16

Identical inputs MUST produce identical outputs.

## Standard Recompute Procedure

When the primary needs to fix bad data or bugs:

1. **Diagnose and fix** the root cause
2. **Recompute scores** via control API or CLI
3. **Publish epoch bump** with structured RecomputeRecord containing:
   - Reason code (SDIO_FEED_ERROR, SCORING_BUG, DB_CORRUPTION, etc.)
   - Human-readable detail
   - Affected event IDs
   - Severity (correction, bugfix, recovery)
   - Git commit hash of running code
4. **Auditors detect and reset** accumulators to new checkpoint
5. **Normal operations resume** with deltas under the new epoch

Auditor-enforced rate limits: max 1 bump per 24 hours, max 3 per week.

## Plugin System

The auditor runtime uses a plugin registry. New verification capabilities
are added by writing a `TaskHandler` class:

```python
class TaskHandler(Protocol):
    name: str
    version: str
    async def on_cycle(self, context: AuditorContext) -> TaskResult: ...
```

v1 ships with `WeightVerification`. Future plugins (spot-check, invariant
check, anti-cheat) plug in with zero changes to core code.

## Audit Depth (Honest Assessment)

**Independently verifiable:**
- Brier scores: recomputed from (miner probability, public outcome)
- FQ/Brier accumulation: cross-checked against checkpoint
- Hard accuracy floor: auditor can verify Brier > 0.30 → zero

**Trusted from checkpoint:**
- CLV/CLE, SOS, lead-lag: derived from paid provider data
- Uniqueness and Marginal dimensions: computed by Shapley jobs from
  pairwise correlation and leave-one-out analysis

**100% verifiable regardless:**
- All normalization, Cobb-Douglas multiplication, and weight encoding math
- Pillar clamping, exponent application, and burn rate

## Security Model

**What auditors detect:**
- Fabricated outcome scores
- Normalization/weight encoding manipulation
- Burn rate tampering
- Inconsistent Brier/FQ accumulation

**What auditors cannot detect:**
- Dishonest CLV/CLE scoring (needs closing lines)
- Biased ground truth construction (needs provider data)
- Selective submission suppression (future: Merkle commitments)

**Copy-trading risk from Tier 2 data:** Minimal. Settled miner probabilities
reveal what someone predicted for finished games, not future strategy.
Aggregated rolling metrics (es_adj, pss_mean) cannot be reverse-engineered
into individual submission timing or odds values.

## Future Roadmap

The plugin architecture enables future work without code changes to core:

- **Distributed outcome verification**: Auditors scrape ESPN/official sites
  to cross-check outcomes against the primary's records.
- **Anti-relay detection**: Anonymized submission timing analysis for
  suspiciously correlated patterns across miners.
- **Calibration auditing**: Independent calibration curves from settled
  submission probabilities + public outcomes.
- **Merkle commitment layer**: Cryptographic commitment preventing
  retroactive submission tampering, with spot-check proofs.
- **Stake-weighted attestation consensus**: Aggregate auditor attestations
  into a trust signal for the primary's scoring integrity.
- **Tiered work assignment**: Higher-stake auditors get deeper verification
  tasks, distributing computational load across the validator set.
