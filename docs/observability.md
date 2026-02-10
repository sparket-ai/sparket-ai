# Observability Stack

Architecture overview for the Sparket observability service. This system gives
miners detailed visibility into their scores and performance via a web dashboard,
and provides operational monitoring of validator nodes.

## System Overview

Three tiers, three repos:

| Tier | Repo | Runtime | Purpose |
|------|------|---------|---------|
| Validator Node | `sparket-subnet` | Python (this repo) | Prometheus instrumentation + DataSyncer |
| VPS | `sparket-api` (new) | Go + PostgreSQL + Prometheus | API server, data store, metrics scraper |
| Website | `sparket-dashboard` (new) | Next.js on Vercel | Miner-facing dashboard |

```
                       Validator Node
                ┌─────────────────────────┐
                │  ValidatorLoop          │
                │    ├── ScoringPipeline  │
                │    │     └── jobs/      │
                │    ├── SynapseListener  │
                │    └── SDIOIngestor     │
                │                         │
                │  Prometheus /metrics ◄───┼──── scrape ─────┐
                │  DataSyncer ────────────┼──── HTTPS ──┐    │
                │  Local PostgreSQL       │             │    │
                └─────────────────────────┘             │    │
                                                        │    │
                       VPS                              │    │
                ┌───────────────────────────────────────┼────┼─┐
                │  Nginx (TLS, rate limiting)           │    │ │
                │    ├── Go API Server ◄────────────────┘    │ │
                │    │     ├── Admin sync endpoints          │ │
                │    │     ├── Public query endpoints         │ │
                │    │     ├── Auth endpoints                 │ │
                │    │     └── In-memory cache                │ │
                │    │                                        │ │
                │  Cloud PostgreSQL                           │ │
                │    ├── miner, miner_score                   │ │
                │    ├── submission_score                     │ │
                │    ├── event, market, outcome               │ │
                │    ├── user_account, hotkey_claim           │ │
                │    └── leaderboard_mv, fleet_stats_mv       │ │
                │                                             │ │
                │  Prometheus ◄───────────────────────────────┘ │
                └───────────────┬───────────────────────────────┘
                                │
                       Vercel   │
                ┌───────────────┼───────────────┐
                │  Next.js Dashboard            │
                │    ├── Leaderboard            │
                │    ├── Miner detail pages     │
                │    ├── Score history charts   │
                │    ├── Fleet statistics       │
                │    ├── Validator health       │
                │    └── User settings          │
                └───────────────────────────────┘
```

## Data Flow

### Scoring Data (every ~5 minutes)

```
Validator scoring cycle completes
  └── DataSyncer.push_scores()
        ├── POST /admin/sync/roster    → upsert miner table
        ├── POST /admin/sync/scores    → append miner_score rows
        ├── POST /admin/sync/submissions → append scored submissions (incremental)
        └── POST /admin/sync/events    → upsert events/markets/outcomes
                                          │
                                Go API admin handler
                                  ├── write to PostgreSQL
                                  ├── REFRESH MATERIALIZED VIEW CONCURRENTLY
                                  └── invalidate in-memory cache
```

### Heartbeat (every ~60 seconds)

```
Validator loop (every N steps)
  └── DataSyncer.push_heartbeat()
        └── POST /admin/sync/heartbeat → insert validator_heartbeat row
```

### Live Metrics (every 15 seconds)

```
Validator /metrics endpoint
  └── Prometheus on VPS scrapes
        └── Dashboard queries Prometheus HTTP API (PromQL)
```

### Dashboard Query (on user request)

```
User browser
  └── Next.js on Vercel (ISR / client-side fetch)
        └── GET /api/v1/leaderboard (or other endpoint)
              │
              ├── Layer 1: Vercel edge / browser cache (Cache-Control)
              ├── Layer 2: Nginx proxy_cache
              ├── Layer 3: Go in-memory cache
              ├── Layer 4: Materialized view / PostgreSQL
              └── Response returned at first cache hit
```

## Validator-Side Components (this repo)

All observability code lives in `sparket/validator/observability/`:

```
sparket/validator/observability/
    __init__.py    # re-exports REGISTRY and DataSyncer
    metrics.py     # Prometheus metric definitions (28 instruments)
    schemas.py     # Pydantic models for sync payloads (14 models)
    syncer.py      # DataSyncer class (HTTPS push to Go API)
```

### Prometheus Metrics

Defined in `metrics.py`, instrumented across the validator codebase:

| Category | Instruments | Instrumented In |
|----------|-------------|-----------------|
| Validator Loop | step_duration, step_total, uptime | `validator.py` main loop |
| Scoring Pipeline | cycle_duration, job_duration, errors, submissions_scored | `main_score.py` |
| Miner Fleet | skill_score (per miner), weight, active_miners | `syncer.py` (after scoring) |
| Submissions | received, accepted, rejected, processing_time | `synapse_listener.py` |
| Provider/Ingest | requests, errors, latency, active events/markets | `sportsdata_ingestor.py` |
| Database | pool_size, pool_checked_out | `dbm.py` |
| Chain Ops | weight_set_duration, metagraph_sync_duration | `validator.py` |
| Worker Health | memory_mb, jobs_completed | `validator.py` |
| DataSyncer | push_duration, push_errors | `syncer.py` |

The `/metrics` endpoint is served by the existing ledger HTTP server
(`sparket/validator/ledger/store/http_server.py`) on port 8200. No auth
required -- this is operational data only.

### DataSyncer

Defined in `syncer.py`. Created at validator startup if
`observability.sync_enabled = true`. Pushes curated data to the Go API.

**Key design decisions:**
- **Fire-and-forget** -- never blocks the validator loop
- **Retry with backoff** -- 3 attempts, exponential backoff (1s, 2s)
- **Incremental submissions** -- watermark-based (`_last_sync_submission_id`)
- **Parallel pushes** -- roster, scores, submissions, events sent concurrently

### Sync Payloads

Defined in `schemas.py` as Pydantic models. These are the canonical interface
contracts between the validator and the Go API:

| Payload | Endpoint | Frequency | Typical Size |
|---------|----------|-----------|--------------|
| `RosterSyncPayload` | POST /admin/sync/roster | Every scoring cycle | ~256 miners, ~30KB |
| `ScoresSyncPayload` | POST /admin/sync/scores | Every scoring cycle | ~256 rows, ~80KB |
| `SubmissionsSyncPayload` | POST /admin/sync/submissions | Every scoring cycle | 0-5000 rows, ~500KB max |
| `EventsSyncPayload` | POST /admin/sync/events | Every scoring cycle | Variable, ~50KB |
| `HeartbeatPayload` | POST /admin/sync/heartbeat | Every ~60s | ~1KB |

### Configuration

Added to `sparket/config/core.py` as `ObservabilitySettings`:

```yaml
# sparket.yaml
observability:
  enabled: false              # master switch
  prometheus_enabled: true    # expose /metrics endpoint
  sync_enabled: false         # push data to Go API
  sync_api_url: ""            # e.g., https://api.sparket.io
  sync_api_key: ""            # admin API key
  sync_timeout_seconds: 10
  heartbeat_interval_steps: 5 # every ~60s at 12s/step
```

Environment variables follow the existing `SPARKET_` prefix pattern:

```bash
SPARKET_OBSERVABILITY__ENABLED=true
SPARKET_OBSERVABILITY__SYNC_ENABLED=true
SPARKET_OBSERVABILITY__SYNC_API_URL=https://api.sparket.io
SPARKET_OBSERVABILITY__SYNC_API_KEY=your-secret-key
```

**Everything is off by default.** No impact on existing validators unless
explicitly configured.

## VPS-Side Components (separate repo)

See [dashboard_api.md](dashboard_api.md) for full details on:
- Cloud PostgreSQL schema (9 synced tables, 3 dashboard-only tables)
- Go API endpoints (public, authenticated, admin)
- Caching strategy (4 layers)
- Auth system with hotkey verification
- VPS infrastructure

## Data Access Policy

| Access Level | What's Visible | What's Hidden |
|-------------|----------------|---------------|
| **Public** | Aggregated rolling scores, leaderboard, fleet stats, events/outcomes, validator health | Per-submission data, raw odds |
| **Authenticated (hotkey owner)** | Own scored submissions, own pending submissions, saved settings | Other miners' submissions |
| **Never exposed** | Provider quotes, ground truth, sportsbook bias, security data, job state | -- |

Per-submission data (Brier, PSS, CLE, odds) is private to the hotkey owner.
Brier scores can reveal probability estimates via `brier = (p - outcome)^2`,
so submission-level data is never made public.

## Caching Architecture

Public data changes every ~5 minutes (scoring cycle). Four cache layers ensure
most requests never reach PostgreSQL:

| Layer | Location | Invalidation | Purpose |
|-------|----------|--------------|---------|
| 1. Browser / Vercel edge | Client + CDN | `Cache-Control` headers (2-5 min TTL) | Offload API entirely |
| 2. Nginx proxy_cache | VPS | Time-based (120s) | Defense in depth |
| 3. Go in-memory cache | Go API | Explicit (on admin sync) | Primary app-level cache |
| 4. Materialized views | PostgreSQL | `REFRESH CONCURRENTLY` (on sync) | Expensive aggregation |

In steady state, ~99% of leaderboard and fleet stats requests resolve at
layer 1 or 2 without hitting the Go API at all.

See [dashboard_api.md](dashboard_api.md) for implementation details on each
cache layer, including materialized view SQL and Nginx config.

## Dependencies

Validator-side (this repo):
- `prometheus_client` -- Prometheus Python client (metrics + exposition)
- `httpx` -- async HTTP client for DataSyncer (already a project dependency)

VPS-side (separate repo):
- Go standard library + `pgx` (PostgreSQL driver)
- PostgreSQL 16+
- Prometheus
- Nginx
