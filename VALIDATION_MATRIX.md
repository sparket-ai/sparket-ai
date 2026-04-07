# Sparket Validator — Intent / Goal Matrix

A checklist for evaluating whether the validator, scoring pipeline, and dashboard are functioning correctly and as intended. Run these checks regularly (daily or after any code change).

---

## 1. Weight Setting

| Check | Intent | How to verify | Current status |
|-------|--------|---------------|----------------|
| Weights use real miner scores | Miners should be rewarded proportional to skill, not 100% burn | `grep "set_weights.*success" logs \| tail -1` → `n_weights` should be > 1 | Fallback to previous day if today's pipeline incomplete |
| Scores fall back on date rollover | New day shouldn't zero out weights while pipeline catches up | `grep "load_scores_fallback" logs` → should show fallback when today has no skill scores | FIXED: falls back to most recent date with scores |
| Burn rate applied correctly | 90% to burn UID, 10% distributed to miners | `grep "burn_rate_applied" logs` → check burn_rate and miner_weight_sum | Working |

## 2. Scoring Pipeline (Worker)

| Check | Intent | How to verify | Current status |
|-------|--------|---------------|----------------|
| All 6 worker stages complete | rolling → calibration → originality → composite_uniqueness → shapley → skill | `SELECT work_type, status FROM scoring_work_queue WHERE chunk_key LIKE 'YYYYMMDD%'` → all completed | FIXED: added composite_uniqueness + shapley to WORK_TYPE_ORDER |
| Skill scores are nonzero | Miners passing Brier floor should have positive skill_score | `SELECT COUNT(CASE WHEN skill_score > 0 THEN 1 END) FROM miner_rolling_score WHERE as_of = (SELECT MAX(as_of) ...)` | 23 miners on Mar 31 (correct per 0.30 Brier floor) |
| Pipeline completes daily | Each new day should have a full scoring run within hours | Check `as_of` dates in `miner_rolling_score` — should have today's date with skill scores | Pipeline takes several hours; fallback protects weight setting |

## 3. Cobb-Douglas Dimensions

| Dimension | Formula | Intent | How to verify |
|-----------|---------|--------|---------------|
| **accuracy** | `w_fq * fq_norm + w_cal * cal_norm` | Forecast quality + calibration | `forecast_dim` column; should be 0.3-0.8 range for active miners |
| **edge** | `w_edge * es_norm + w_mes * mes_norm` | Economic edge vs closing line | `econ_dim` column; requires CLV data (submission_vs_close) |
| **timeliness** | `0.5 * skill_dim + 0.5 * lead_norm` | PSS + information lead | Computed on-the-fly; depends on lead_score being populated |
| **uniqueness** | Composite SOS (pairwise correlation + clustering) | Anti-sybil, rewards independent predictions | `uniqueness_dim` column; populated by composite_uniqueness job |
| **marginal** | Shapley contribution (Monte Carlo) | Marginal value to prediction pool | `marginal_dim` column; requires shapley_contribution rows. **Currently 0.5 default** — needs outcome scoring → Shapley pipeline to complete |
| **Brier floor** | `brier_mean > 0.30 → skill_score = 0` | Hard accuracy prerequisite | 23/168 miners pass (correct). Check: `SELECT COUNT(*) FROM miner_rolling_score WHERE brier_mean <= 0.30` |

## 4. Data Pipeline

| Check | Intent | How to verify | Current status |
|-------|--------|---------------|----------------|
| Provider quotes ingesting | SportsDataIO data flowing | `SELECT COUNT(*) FROM provider_quote WHERE captured_at > now() - interval '1 hour'` | Working (ingestor PM2 process) |
| Miner submissions flowing | Miners actively submitting odds | `SELECT COUNT(*) FROM miner_submission WHERE submitted_at > now() - interval '1 hour'` → should be > 10k | Working |
| Ground truth closings produced | Closing line consensus captured at game time | `SELECT COUNT(*) FROM ground_truth_closing WHERE computed_at::date = CURRENT_DATE` | FIXED: CardinalityViolation resolved with DISTINCT ON |
| CLV scoring current | submission_vs_close populated for recent submissions | `SELECT COUNT(*) FROM submission_vs_close` → 24M+ after bulk backfill | Bulk backfill done; incremental via batch scoring |
| Outcome scoring current | submission_outcome_score for settled markets | `SELECT MAX(settled_at)::date FROM submission_outcome_score` → should be within 1-2 days | Progressing; 1 market/cycle at ~3 min cadence |
| Shapley contributions computed | Per-miner Shapley values for settled markets | `SELECT COUNT(*) FROM shapley_contribution` → should be > 0 | **BLOCKED**: 0 rows; needs outcome scoring to catch up first |

## 5. Auditor Compatibility

| Check | Intent | How to verify | Current status |
|-------|--------|---------------|----------------|
| Checkpoint data correct | Auditors receive valid MinerMetrics | `python3 -c "import gzip,json; print(len(json.load(gzip.open('checkpoints/latest/accumulators.json.gz','rt'))))"` | 168 miners in checkpoint |
| Deltas being written | Auditors get incremental outcome data | `ls sparket/data/ledger/deltas/` → should have epoch_1/ with delta files | **NOT YET**: ledger export triggered but delta submissions = 0 |
| Shared compute_weights deterministic | Primary and auditor produce same weights from same inputs | No local changes to `ledger/compute_weights.py` — verified identical | OK |
| Auditors can set weights | Auditors not skipping weight verification | Check auditor logs for `weight_verification: skip` vs `success` | Auditors currently skip (no deltas) |

## 6. Dashboard Accuracy

| Check | Intent | How to verify | Current status |
|-------|--------|---------------|----------------|
| Syncer pushes real scores | Dashboard shows current miner scores | `grep "syncer_push_ok.*scores" logs` → 202 status | FIXED: syncer now picks best date with scores |
| Dimension columns populated | accuracy, edge, timeliness, uniqueness, marginal all show values | Query dashboard DB `score_history` → dimensions should be nonzero | FIXED: mapped DB columns to Cobb-Douglas pillar names |
| Weights visible | Per-miner weight shown | `weight` field in sync payload | FIXED: set to skill_score |
| Events show correct status | finished/scheduled/in_play not all "scheduled" | `status` field in event sync includes real DB values | FIXED: query passes through real status |
| Events show submission counts | n_submissions per event nonzero | Event sync includes `n_submissions` from joined count | FIXED: added LATERAL join |
| Event final_at populated | Finished events show completion time | `final_at` from outcome.settled_at | FIXED: added to event sync |

## 7. Infrastructure Health

| Check | Intent | How to verify | Current status |
|-------|--------|---------------|----------------|
| No scoring timeouts | Batch scoring completes within timeout budget | `grep "scoring_timeout" logs` in last hour → should be 0 | Reduced to limit=1 outcome market/cycle; still borderline |
| Worker subprocess stable | No crash loop | `grep "Error in main loop" logs` in last hour → should be 0 | FIXED: DB init moved inside event loop |
| PostgreSQL shared memory | Large queries don't fail with DiskFullError | Docker shm = 4GB | FIXED: recreated container with --shm-size=4g |
| DB connections healthy | No pool exhaustion or stale connections | `SELECT count(*) FROM pg_stat_activity` → well under max_connections | 10/100 used |
| Error messages informative | No empty string errors | `grep "': ''" logs` → should be minimal | FIXED: str(e) → repr(e) for key error paths |

## 8. Known Gaps / TODOs

- [ ] **Outcome scoring throughput**: 1 market/cycle with 90k+ submissions per market is very slow. Needs bulk INSERT instead of row-by-row.
- [ ] **CLV scoring throughput**: Bulk SQL backfill done, but incremental CLV in `run_snapshots` disabled due to timeout. Needs batch optimization.
- [ ] **Marginal dimension**: Stuck at 0.5 default until Shapley pipeline has data. Blocked on outcome scoring backlog.
- [ ] **Auditor deltas**: Export triggers but finds no submissions in the time window due to backfill lag. Needs delta exporter to handle historical data.
- [ ] **Brier_mean not on dashboard**: Dashboard has no visibility into why miners are zeroed (0.30 Brier floor). Should add brier_mean + floor_threshold to sync payload.
- [ ] **Memory/infra**: Consider documenting CLAUDE.md and memory files for fresh session context.
