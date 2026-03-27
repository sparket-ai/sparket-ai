# Sparket Scoring Overhaul Constitution

## Core Principles

### I. Aggregate-First
Every scoring decision is evaluated against "does this improve the ensemble output?" — not "does this identify the best individual?" The Diversity Prediction Theorem is the mathematical foundation: crowd error = average individual error - prediction diversity. We optimise both terms.

### II. Deterministic & Auditor-Verifiable
All emission scoring computations must be deterministic given the same inputs. Auditors must be able to independently reproduce any scoring result from checkpoint + delta data. Non-deterministic components (ML models, calibrated parameters) belong in the proprietary product layer, never in emission scoring.

### III. Multiplicative Complementarity (Cobb-Douglas)
The scoring formula is fully multiplicative across dimensions. Miners must be strong across ALL pillars — dimensional substitution (being unique but inaccurate, or accurate but redundant) is structurally penalised. No additive fallbacks.

### IV. Test-Driven with Backtesting
New scoring jobs must have unit tests with synthetic data AND backtests against historical settled markets. A scoring change that degrades aggregate Brier score on historical data is rejected. Red-green-refactor applies to scoring math — write the test with expected Shapley values for a known scenario, verify it fails, then implement.

### V. Existing Infrastructure Preserved
Per-submission metrics (Brier, PSS, CLV, CLE, SOS_market, lead-lag), the rolling aggregate/decay system (10-day half-life), the checkpoint/delta auditor pipeline, and the PM2 process management all stay. New code plugs into the existing scoring job framework, it does not replace the pipeline.

### VI. No Over-Engineering
Ship the Shapley pipeline. No intermediate LOO-only phase, no feature flags for gradual rollover between old and new scoring. Shadow mode for validation, then switch. The existing additive SkillScore provides initial log-pool weights during bootstrap — that is the only transition mechanism needed.

## Constraints

- **Python 3.10+**, async-first, Pydantic 2+ models
- **NumPy** for all vectorised scoring math (no pandas in hot paths)
- **pytest-asyncio** with markers: `slow`, `integration`, `e2e`
- **4-core / 32GB RAM VPS** ceiling for auditor compute tasks
- **256 miners** max, ~50 settled markets/week baseline
- Monte Carlo Shapley: K=500 permutations default, configurable
- All new config via existing Pydantic BaseSettings + YAML + env var pattern (`SPARKET_SECTION__KEY`)

## Governance

This constitution governs all code on the `scoring-overhaul-shapley` branch. Amendments require updating this document with rationale.

**Version**: 1.0 | **Ratified**: 2026-03-23
