# Tasks: Shapley Scoring Overhaul

**Input**: `specs/scoring-overhaul-shapley/plan.md`, `specs/scoring-overhaul-shapley/spec.md`
**Prerequisites**: plan.md (complete), spec.md (complete)

## Phase 1: Setup & Schema

**Purpose**: Database schema, config, and shared infrastructure

- [ ] T001 Create Alembic migration adding new columns to `miner_rolling_score` (uniqueness_dim, marginal_dim, sos_crowd, sos_cluster, sos_composite, shapley_mean, shapley_ws, shapley_wt, cluster_id, cluster_size) in `alembic/versions/`
- [ ] T002 [P] Create Alembic migration adding `miner_pairwise_correlation` table (miner_a_id, miner_b_id, as_of, correlation, n_common_markets) in `alembic/versions/`
- [ ] T003 [P] Create Alembic migration adding `shapley_contribution` table (miner_id, market_id, shapley_value, settled_at, k_permutations) in `alembic/versions/`
- [ ] T004 Add `CobbDouglasParams`, `CompositeSOSParams`, `ShapleyParams` to `sparket/validator/config/scoring_params.py`
- [ ] T005 Add `COMPOSITE_UNIQUENESS` and `SHAPLEY` to `WorkType` enum in `sparket/validator/scoring/batch/processor.py`
- [ ] T006 Add dispatch for new work types in `sparket/validator/scoring/worker/runner.py` (`_run_work_item`)

**Checkpoint**: Schema ready, config available, worker can dispatch new job types

---

## Phase 2: Log-Pool Aggregate (US4 — Foundation for Shapley)

**Purpose**: Weighted logarithmic opinion pool — used inside Shapley and as standalone aggregate

**Goal**: Given miner submissions + weights, produce calibrated aggregate probability per market side

- [ ] T007 Write tests for log-pool in `tests/validator/scoring/test_log_pool.py`:
  - 3 miners with known probs → verify analytical log-pool output
  - LOO via cached subtraction matches brute-force recompute
  - Extreme probs (0.01, 0.99) clamped before logit
  - Edge case: single miner → aggregate = that miner's prob
  - Edge case: zero-weight miner excluded from pool

- [ ] T008 Implement `sparket/validator/scoring/aggregation/log_pool.py`:
  - `build_log_pool(probs: NDArray, weights: NDArray, epsilon: float) -> float` — weighted mean of logits, return sigmoid
  - `build_log_pool_incremental(current_logit_sum: float, current_weight_sum: float, new_logit: float, new_weight: float) -> float` — O(1) add
  - `loo_log_pool(full_logit_sum: float, full_weight_sum: float, miner_logit: float, miner_weight: float, epsilon: float) -> float` — O(1) remove
  - `evaluate_subset(miner_logits: NDArray, miner_weights: NDArray, subset_mask: NDArray, epsilon: float) -> float` — log-pool for arbitrary subset (used by Shapley)
  - All functions pure NumPy, no side effects

**Checkpoint**: Log-pool module tested and ready for Shapley integration

---

## Phase 3: Pairwise Correlation & Composite Uniqueness (US2)

**Goal**: Compute N×N miner correlation matrix, spectral clustering, and composite SOS

- [ ] T009 Write tests for correlation in `tests/validator/scoring/test_composite_uniqueness.py`:
  - 5 sybil miners (identical + noise) → pairwise corr ≈ 1.0
  - 5 independent miners → pairwise corr ≈ 0.0
  - SOS_crowd formula verified
  - Spectral clustering assigns sybils to same cluster
  - ClusterPenalty = (size-1)/size for detected clusters
  - Composite SOS blend weights verified (0.2/0.5/0.3)
  - Edge case: miner with < min_common_markets → correlation defaults to 0 (independent)

- [ ] T010 Implement `sparket/validator/scoring/aggregation/correlation.py`:
  - `compute_pairwise_correlations(submissions: dict[str, NDArray], min_common: int) -> NDArray` — N×N Pearson correlation matrix (only for market pairs both miners submitted on)
  - `compute_sos_crowd(corr_matrix: NDArray, miner_idx: int) -> float` — 1 - mean(|corr| with all other miners)
  - Pure NumPy

- [ ] T011 [P] Implement `sparket/validator/scoring/aggregation/clustering.py`:
  - `detect_clusters(corr_matrix: NDArray, threshold: float) -> list[list[int]]` — spectral clustering on affinity matrix (|corr| > threshold)
  - `compute_cluster_penalty(cluster_assignments: list[list[int]], miner_idx: int) -> float` — (size-1)/size, 0 for singleton
  - Uses scikit-learn SpectralClustering or simpler connected-components on thresholded correlation graph

- [ ] T012 Implement `sparket/validator/scoring/jobs/composite_uniqueness.py`:
  - New `CompositeUniquenessJob(ScoringJob)` with `JOB_ID = "composite_uniqueness"`
  - `execute()`:
    1. Fetch recent miner submissions (within correlation_window_days)
    2. Call `compute_pairwise_correlations()`
    3. Persist to `miner_pairwise_correlation` table
    4. Call `detect_clusters()`
    5. For each miner: compute SOS_crowd, SOS_cluster, SOS_composite
    6. Update `miner_rolling_score` with sos_crowd, sos_cluster, sos_composite, uniqueness_dim, cluster_id, cluster_size

**Checkpoint**: Composite SOS computed and stored. Uniqueness pillar feeds into Cobb-Douglas.

---

## Phase 4: Monte Carlo Shapley Contribution (US3)

**Goal**: Compute per-miner Shapley values on settled markets via permutation sampling

- [ ] T013 Write tests for Shapley in `tests/validator/scoring/test_shapley.py`:
  - 5 informative + 5 noise miners → informative get positive Shapley, noise get ≤ 0
  - 2 identical miners → each gets ~half of a single unique miner's value
  - Deterministic seed → identical results across runs
  - K=50 vs K=500 → values converge (variance decreases)
  - Single miner → Shapley = full aggregate quality (trivial)
  - Performance: 256 miners × K=500 completes < 30s

- [ ] T014 Implement `sparket/validator/scoring/jobs/shapley_contribution.py`:
  - New `ShapleyContributionJob(ScoringJob)` with `JOB_ID = "shapley"`
  - `execute()`:
    1. Fetch recently settled markets (since last Shapley run — triggered hourly via scoring cycle)
    2. For each settled market:
       a. Fetch all miner submissions (prob per side)
       b. Fetch previous epoch emission weights as log-pool weights
       c. Compute Shapley via `_monte_carlo_shapley()`
       d. Persist to `shapley_contribution` table
    3. For each miner: time-decay Shapley values into rolling `shapley_mean`
    4. Normalise via z-score logistic → `marginal_dim`
    5. Update `miner_rolling_score` with shapley_mean, shapley_ws, shapley_wt, marginal_dim
  - `_monte_carlo_shapley(miner_logits, weights, outcome, k, seed)`:
    1. `rng = np.random.default_rng(seed)`
    2. For k permutations: shuffle miners, walk permutation accumulating log-pool, record marginal Brier improvement per miner
    3. Average across permutations
    4. Return NDArray of Shapley values per miner

**Checkpoint**: Shapley values computed on settled markets. MarginalDim feeds into Cobb-Douglas.

---

## Phase 5: Cobb-Douglas SkillScore (US1)

**Goal**: Switch the final SkillScore formula from additive to multiplicative

- [ ] T015 Write tests for Cobb-Douglas in `tests/validator/scoring/test_cobb_douglas.py`:
  - All dims at 0.5 → product ≈ 0.044
  - All dims at 0.8 → product ≈ 0.362
  - All dims at 0.9 → product ≈ 0.624
  - 95th/50th ratio ≈ 18x
  - Hard floor: Brier > 0.30 → skill_score = 0
  - Specialist (80th on 3, 50th on 2) scores ≈ 6x less than balanced (80th all)
  - Dimension at ε (0.01) → product collapses (< 0.001)
  - Bootstrap mode: missing Shapley/composite SOS → defaults applied

- [ ] T016 Modify `sparket/validator/scoring/jobs/skill_score.py`:
  - Replace additive formula with Cobb-Douglas:
    ```python
    if brier_mean > params.floor_threshold:
        skill_score = 0.0
    else:
        skill_score = (
            accuracy_dim ** float(params.accuracy_exponent)
            * edge_dim ** float(params.edge_exponent)
            * timeliness_dim ** float(params.timeliness_exponent)
            * uniqueness_dim ** float(params.uniqueness_exponent)
            * marginal_dim ** float(params.marginal_exponent)
        )
    ```
  - Read uniqueness_dim from miner_rolling_score (set by CompositeUniquenessJob)
  - Read marginal_dim from miner_rolling_score (set by ShapleyContributionJob)
  - Bootstrap fallback: if uniqueness_dim is NULL → use existing sos_score; if marginal_dim is NULL → use 0.5
  - Clamp all dimensions to [ε, 1.0] before exponentiation
  - Persist new dimension columns alongside existing ones

- [ ] T017 Modify `sparket/validator/ledger/compute_weights.py`:
  - Mirror the Cobb-Douglas formula exactly (deterministic auditor path)
  - Accept new fields in `MinerMetrics` (uniqueness_dim, marginal_dim)
  - Same hard floor, same clamping, same exponents from `ScoringConfigSnapshot`
  - Update `WeightResult.dimension_scores` to include all 5 dimensions

- [ ] T018 Modify `sparket/validator/ledger/models.py` + `exporter.py`:
  - Add new accumulator fields to `AccumulatorEntry` (shapley_ws, shapley_wt, sos_crowd, sos_cluster, etc.)
  - Add new dimension fields to `CheckpointWindow` schema
  - Export new columns in `export_checkpoint()`

**Checkpoint**: Full Cobb-Douglas scoring pipeline operational. Weights computed deterministically.

---

## Phase 6: Worker Pipeline Integration

**Purpose**: Wire new jobs into the scoring worker pipeline

- [ ] T019 Modify `sparket/validator/scoring/batch/processor.py`:
  - Add work queue entries for COMPOSITE_UNIQUENESS and SHAPLEY
  - Ordering: ROLLING → CALIBRATION → ORIGINALITY → COMPOSITE_UNIQUENESS → SHAPLEY → SKILL
  - COMPOSITE_UNIQUENESS depends on ORIGINALITY completion
  - SHAPLEY depends on settled markets existing (triggered by outcome settlement)

- [ ] T020 Write end-to-end integration test in `tests/validator/scoring/test_compute_weights_v2.py`:
  - Create synthetic miner_rolling_score rows with all 5 dimensions
  - Run `compute_weights()` with Cobb-Douglas params
  - Verify output weights match expected Pareto distribution
  - Verify auditor path produces identical weights
  - Verify checkpoint export includes new fields

**Checkpoint**: Full pipeline wired and integration-tested.

---

## Phase 7: Polish & Validation

**Purpose**: Backtest, shadow mode, final verification

- [ ] T021 Backtest scoring on historical settled markets:
  - Replay historical submissions through new pipeline
  - Compare weight distributions (old vs. new)
  - Verify aggregate Brier improvement
  - Verify noise rejection (identify any miners that should be zeroed)
  - Document results in `specs/scoring-overhaul-shapley/backtest-results.md`

- [ ] T022 Shadow mode deployment:
  - Run new scoring in parallel with existing (compute both, emit old weights)
  - Monitor for 1 week: weight distribution shape, VTrust, miner ranking changes
  - Alert on any miner rank change > 50 positions
  - Document observations

- [ ] T023 [P] Update `sparket/config/sparket.example.yaml` with new config sections (cobb_douglas, composite_sos, shapley)
- [ ] T024 [P] Run full test suite: `pytest tests/ -v --cov=sparket`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Log-Pool)**: Depends on Phase 1 (config available)
- **Phase 3 (Composite Uniqueness)**: Depends on Phase 1 (schema + config)
- **Phase 4 (Shapley)**: Depends on Phase 1 + Phase 2 (needs log-pool module)
- **Phase 5 (Cobb-Douglas)**: Depends on Phase 1 + Phase 3 + Phase 4 (reads all dimensions)
- **Phase 6 (Integration)**: Depends on Phase 5 (full pipeline wired)
- **Phase 7 (Validation)**: Depends on all prior phases

### Parallel Opportunities

- Phases 2, 3, and 4 can proceed in parallel after Phase 1 completes (independent modules)
- T002 and T003 (migrations) can run in parallel with T001
- T010 and T011 (correlation + clustering) can run in parallel
- T023 and T024 (config + tests) can run in parallel

### Critical Path

Phase 1 → [Phase 2 + Phase 3 + Phase 4 in parallel] → Phase 5 → Phase 6 → Phase 7

### Future: Auditor Distributed Compute

At current scale (256 miners, 25-50 settled markets/day), all Shapley and correlation computation runs on the primary validator. Hourly batches of ~1-4 markets take ~13-52 seconds each — well within budget. Auditors verify weights via existing checkpoint pipeline (updated for Cobb-Douglas).

When miner count exceeds ~500 or market volume exceeds ~200/day, distribute correlation matrix sharding and Shapley batch estimation to auditors. The log-pool and Shapley modules are already designed for subset computation, making future distribution straightforward. See `docs/scoring_overhaul_guide.md` Section 10 for the auditor tiered workload design.
