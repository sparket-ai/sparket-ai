# Feature Specification: Shapley Scoring Overhaul

**Feature Branch**: `scoring-overhaul-shapley`
**Created**: 2026-03-23
**Status**: Draft
**Input**: Switch from additive 4-dimension SkillScore to multiplicative 5-dimension Cobb-Douglas with Shapley contribution scoring

## User Scenarios & Testing

### User Story 1 — Cobb-Douglas SkillScore with 5 Pillars (Priority: P1)

Replace the additive `0.10*forecast + 0.10*skill + 0.50*econ + 0.30*info` formula with a fully multiplicative Cobb-Douglas product of 5 dimensions: Accuracy^0.5 × Edge^1.0 × Timeliness^0.5 × Uniqueness^1.5 × Marginal^1.0. Includes hard accuracy floor (Brier > 0.30 → zero weight). During bootstrap (before Shapley/composite SOS are available), Uniqueness defaults to existing SOS and Marginal defaults to 0.5 (neutral).

**Why this priority**: The multiplicative formula is the foundation everything else plugs into. Without it, Shapley values and composite SOS have nowhere to feed.

**Independent Test**: Run `compute_weights()` with synthetic miner metrics covering all archetypes (sharp+unique, sharp+redundant, noisy, average). Verify Pareto concentration: 95th/50th percentile ratio ≈ 18x. Verify hard floor zeroes miners above Brier threshold.

**Acceptance Scenarios**:

1. **Given** 256 miners with Beta(2,2)-distributed dimension scores, **When** Cobb-Douglas SkillScore is computed, **Then** top 10 miners capture 40–60% of total weight after L1 normalisation.
2. **Given** a miner with rolling Brier mean > 0.30, **When** SkillScore is computed, **Then** skill_score = 0 and weight = 0.
3. **Given** a miner at 80th percentile on all 5 dimensions vs. one at 80th on 3 and 50th on 2, **Then** the balanced miner scores ≥ 6x higher than the specialist.
4. **Given** bootstrap mode (no Shapley/composite SOS yet), **When** SkillScore runs, **Then** Uniqueness = existing SOS, Marginal = 0.5, and the formula still produces valid weights.

---

### User Story 2 — Composite SOS (Uniqueness Dimension) (Priority: P2)

Extend the existing SOS (market independence only) into a 3-component composite: SOS_market (existing), SOS_crowd (mean pairwise miner correlation), SOS_cluster (spectral clustering penalty). Formula: `0.2 * SOS_market + 0.5 * SOS_crowd + 0.3 * SOS_cluster`. Requires computing the N×N pairwise miner correlation matrix.

**Why this priority**: Without composite SOS, the Uniqueness pillar is blind to miner-miner copying and sybil rings — the #1 farming vector.

**Independent Test**: Inject 5 synthetic sybil miners (identical submissions + small Gaussian noise). Verify SOS_crowd ≈ 0 for the ring, SOS_cluster penalty reduces each to ~1/5th effective weight.

**Acceptance Scenarios**:

1. **Given** a miner whose submissions correlate 0.95 with another miner but only 0.3 with the market, **When** composite SOS is computed, **Then** SOS_crowd ≈ 0.05 dominates the composite and the miner scores low on Uniqueness.
2. **Given** a cluster of 5 miners with pairwise correlation > 0.9, **When** spectral clustering detects them, **Then** ClusterPenalty = 4/5 = 0.8 and SOS_cluster = 0.2 for each.
3. **Given** a genuinely independent miner (correlation < 0.3 with all others), **When** composite SOS is computed, **Then** SOS_composite ≥ 0.7.

---

### User Story 3 — Shapley Contribution Scoring (Marginal Dimension) (Priority: P3)

Implement Monte Carlo Shapley value estimation. For each settled market, sample K=500 random permutations of miners, measure the marginal change in aggregate Brier score when adding each miner to the growing log-pool coalition. Time-decay Shapley values into a rolling `shapley_mean` that feeds MarginalDim.

**Why this priority**: This is the core innovation — measuring actual marginal contribution to aggregate quality. Depends on US1 (Cobb-Douglas) and US2 (composite SOS) being in place but can be tested independently with synthetic data.

**Independent Test**: Create a 10-miner scenario with known analytical Shapley values (e.g., 5 independent informative miners + 5 pure noise miners). Verify Monte Carlo estimates converge to analytical values within tolerance. Verify noise miners get Shapley ≈ 0.

**Acceptance Scenarios**:

1. **Given** 10 miners where 5 submit independent informative predictions and 5 submit pure noise, **When** Shapley values are computed with K=500, **Then** informative miners' Shapley values are positive and noise miners' values are ≤ 0.
2. **Given** 2 identical miners (perfect copies), **When** Shapley values are computed, **Then** each receives approximately half the Shapley value of a single unique miner.
3. **Given** a miner whose removal improves the aggregate (negative contribution), **When** Shapley is computed, **Then** their Shapley value is negative and MarginalDim maps to ε (0.01).
4. **Given** N=256 miners and K=500 permutations on a settled market, **When** the computation runs, **Then** it completes in < 30 seconds on a single 4-core VPS.

---

### User Story 4 — Log-Pool Aggregate (Consensus Line Foundation) (Priority: P4)

Build a weighted logarithmic opinion pool aggregate from miner submissions for each market. This is both the aggregate used inside Shapley subset evaluations AND the foundation for the future consensus line product. Uses emission weights (previous epoch's Cobb-Douglas SkillScores) as pool weights.

**Why this priority**: Required by US3 (Shapley needs to evaluate subset aggregates via log-pool). But it also produces the raw consensus signal as a byproduct.

**Independent Test**: Given 3 miners with known probabilities and weights, verify log-pool output matches analytical expectation. Verify LOO aggregate (remove one miner) matches the cached log-odds subtraction trick.

**Acceptance Scenarios**:

1. **Given** miners with probabilities [0.6, 0.7, 0.8] and equal weights, **When** log-pool is computed, **Then** `logit(p_agg) = mean(logit(0.6), logit(0.7), logit(0.8))` and p_agg ≈ 0.703.
2. **Given** a full log-pool aggregate logit, **When** LOO for miner i is computed via `(full_logit - w_i * logit_i) / (1 - w_i)`, **Then** the result matches recomputing the pool from scratch without miner i.
3. **Given** miner probabilities at the extremes (0.01, 0.99), **When** log-pool runs, **Then** values are clamped to [ε, 1-ε] before logit to avoid ±inf.

---

### Edge Cases

- What happens when a miner has < 50 submissions (cold start)? → Bayesian shrinkage pulls dimensions toward 0.5, Cobb-Douglas product ≈ 0.044 (near median). This is the existing behaviour, preserved.
- What happens when only 1 miner submits on a market? → Shapley value = full aggregate quality (trivially). LOO = full aggregate quality. Handled naturally.
- What happens when all miners submit identical predictions? → All pairwise correlations = 1, SOS_crowd = 0, Uniqueness ≈ 0, Cobb-Douglas product collapses for everyone. All get near-zero weight. This is correct — no unique information is contributed.
- What happens during epoch 0 (no previous Cobb-Douglas weights)? → Use current additive SkillScores as initial log-pool weights. Fixed-point iteration converges within 1–2 epochs.

## Requirements

### Functional Requirements

- **FR-001**: System MUST compute Cobb-Douglas SkillScore = Accuracy^0.5 × Edge^1.0 × Timeliness^0.5 × Uniqueness^1.5 × Marginal^1.0 with configurable exponents.
- **FR-002**: System MUST zero the SkillScore for any miner with rolling Brier mean above a configurable floor threshold (default 0.30).
- **FR-003**: System MUST compute pairwise Pearson correlation between all active miner pairs over recent submissions (rolling window).
- **FR-004**: System MUST detect clusters of highly correlated miners via spectral clustering and apply ClusterPenalty = (cluster_size - 1) / cluster_size.
- **FR-005**: System MUST compute composite SOS = 0.2 × SOS_market + 0.5 × SOS_crowd + 0.3 × SOS_cluster with configurable blend weights.
- **FR-006**: System MUST compute Monte Carlo Shapley values with configurable K permutations using log-pool aggregation and Brier score as the value function.
- **FR-007**: System MUST build log-pool aggregates from miner submissions using previous epoch's emission weights.
- **FR-008**: System MUST clamp all dimension scores to [ε, 1.0] (ε = 0.01) before exponentiation.
- **FR-009**: System MUST time-decay Shapley values using the existing half-life exponential decay system.
- **FR-010**: System MUST be deterministic — identical inputs produce identical outputs on primary and auditor.
- **FR-011**: System MUST support auditor-distributed computation of correlation matrix shards and Shapley batches.
- **FR-012**: System MUST export new dimension scores (Uniqueness, Marginal) in checkpoint/delta format for auditor verification.
- **FR-013**: System MUST normalise MarginalDim (Shapley) via z-score logistic, consistent with existing Edge normalisation.

### Key Entities

- **ShapleyContribution**: Per-miner, per-market Shapley value (float). Settled retrospectively. Time-decayed into rolling `shapley_mean`.
- **MinerCorrelation**: Per-miner-pair Pearson correlation over recent submissions. Updated per scoring cycle. Feeds SOS_crowd and spectral clustering.
- **ClusterAssignment**: Per-miner cluster ID and cluster size from spectral clustering. Feeds SOS_cluster.
- **LogPoolAggregate**: Per-market, per-side weighted log-odds aggregate probability. Used as value function inside Shapley and as raw consensus output.

## Success Criteria

### Measurable Outcomes

- **SC-001**: Aggregate Brier score (log-pool with Cobb-Douglas weights) is lower than any individual miner's rolling Brier mean, measured on 30-day rolling window of settled markets.
- **SC-002**: Top 10 miners capture 40–60% of total L1-normalised weight (Pareto concentration).
- **SC-003**: Synthetic noise miners (random predictions) receive weight = 0 after hard floor + multiplicative annihilation.
- **SC-004**: Synthetic sybil ring of k miners each receives approximately 1/k of the weight a single unique miner would receive.
- **SC-005**: Shapley computation for 256 miners × 50 markets completes in < 30 minutes total when distributed across 10 auditors.
- **SC-006**: Full scoring pipeline (Rolling → Calibration → Originality → CompositeUniqueness → Shapley → SkillScore) completes within existing scoring cycle time budget.
