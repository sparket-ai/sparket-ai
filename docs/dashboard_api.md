# Dashboard API Server (sparket-api)

Design specification for the Go API server that powers the miner-facing dashboard.
Runs on a VPS alongside PostgreSQL and Prometheus.

## Architecture

```
Validator Node                    VPS                          Vercel
┌───────────┐    HTTPS POST     ┌──────────────┐             ┌───────────┐
│ DataSyncer ├──────────────────►│  Go API      │◄────────────┤ Next.js   │
└───────────┘                   │  (admin sync) │  REST API   │ Dashboard │
                                └──────┬───────┘             └───────────┘
┌───────────┐    scrape /metrics ┌─────┴──────┐
│ /metrics  ├───────────────────►│ Prometheus │
└───────────┘                   └────────────┘
                                ┌────────────┐
                                │ PostgreSQL │
                                └────────────┘
```

## Cloud PostgreSQL Schema

All IDs come from the validator (no auto-increment on synced tables).

### `miner`

| Column | Type | Notes |
|--------|------|-------|
| miner_id | BIGINT PK | from validator |
| uid | INT NOT NULL | bittensor neuron UID |
| hotkey | VARCHAR UNIQUE NOT NULL | ss58 hotkey |
| coldkey | VARCHAR NOT NULL | ss58 coldkey |
| active | BOOLEAN NOT NULL | from metagraph |
| stake | NUMERIC | hotkey stake |
| total_stake | NUMERIC | total including delegations |
| incentive | NUMERIC | from metagraph |
| emission | NUMERIC | emission rate |
| trust | NUMERIC | trust value |
| rank | NUMERIC | rank metric |
| updated_at | TIMESTAMPTZ NOT NULL | last sync time |

### `miner_score` (time-series, append-only)

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | auto-increment |
| miner_id | BIGINT FK(miner) NOT NULL | |
| hotkey | VARCHAR NOT NULL | |
| recorded_at | TIMESTAMPTZ NOT NULL | scoring cycle timestamp |
| skill_score | NUMERIC | final Cobb-Douglas composite [0,1] |
| weight | NUMERIC | chain weight |
| forecast_dim | NUMERIC | legacy: 0.6*FQ + 0.4*CAL |
| skill_dim | NUMERIC | legacy: PSS_norm |
| econ_dim | NUMERIC | legacy: 0.7*EDGE + 0.3*MES |
| info_dim | NUMERIC | legacy: 0.6*SOS + 0.4*LEAD |
| accuracy_dim | NUMERIC | CD pillar: forecast accuracy [0,1] |
| edge_dim | NUMERIC | CD pillar: economic edge [0,1] |
| timeliness_dim | NUMERIC | CD pillar: lead-lag + skill blend [0,1] |
| uniqueness_dim | NUMERIC | CD pillar: submission originality [0,1] |
| marginal_dim | NUMERIC | CD pillar: Shapley marginal contribution [0,1] |
| sos_crowd | NUMERIC | anti-sybil: crowd-based SOS |
| sos_cluster | NUMERIC | anti-sybil: cluster-based SOS |
| sos_composite | NUMERIC | anti-sybil: blended SOS |
| shapley_mean | NUMERIC | avg Shapley contribution value |
| cluster_id | INT | sybil cluster assignment (NULL = independent) |
| cluster_size | INT DEFAULT 1 | miners in assigned cluster |
| fq_score | NUMERIC | forecast quality [0,1] |
| cal_score | NUMERIC | calibration [0,1] |
| sharp_score | NUMERIC | sharpness [0,1] |
| edge_score | NUMERIC | economic edge [0,1] |
| mes_score | NUMERIC | market efficiency [0,1] |
| sos_score | NUMERIC | originality [0,1] |
| lead_score | NUMERIC | lead ratio [0,1] |
| brier_mean | NUMERIC | raw avg Brier (>0.30 = floor) |
| pss_mean | NUMERIC | raw avg PSS |
| es_adj | NUMERIC | risk-adjusted CLE |
| es_mean | NUMERIC | raw CLE mean |
| mes_mean | NUMERIC | raw MES mean |
| fq_raw | NUMERIC | raw forecast quality |
| lead_ratio | NUMERIC | raw lead ratio |
| n_submissions | INT NOT NULL | submission count |
| n_eff | NUMERIC | effective sample size |

Indexes: `(miner_id, recorded_at)`, `(hotkey, recorded_at)`, `(recorded_at)`, `(skill_score DESC)`

> **v0.1.0 note:** `skill_score` is now computed via Cobb-Douglas multiplicative formula across
> the 5 pillar dimensions (accuracy, edge, timeliness, uniqueness, marginal). The legacy
> `forecast_dim`/`skill_dim`/`econ_dim`/`info_dim` columns are still populated for backward
> compatibility but are no longer used in weight computation. Dashboard should transition to
> displaying the 5 CD pillar dimensions.

### `submission_score` (append-only, access-controlled)

| Column | Type | Notes |
|--------|------|-------|
| submission_id | BIGINT PK | from validator |
| miner_id | BIGINT FK(miner) NOT NULL | |
| hotkey | VARCHAR NOT NULL | |
| market_id | BIGINT FK(market) NOT NULL | |
| side | VARCHAR NOT NULL | home/away/draw/over/under |
| submitted_at | TIMESTAMPTZ NOT NULL | |
| priced_at | TIMESTAMPTZ | miner-declared |
| odds_eu | NUMERIC NOT NULL | decimal odds |
| imp_prob | NUMERIC NOT NULL | 1/odds |
| cle | NUMERIC | closed-line efficiency |
| clv_prob | NUMERIC | CLV probability space |
| minutes_to_close | INT | minutes before event start |
| brier | NUMERIC | Brier score |
| logloss | NUMERIC | log loss |
| pss | NUMERIC | blended PSS |
| pss_brier | NUMERIC | PSS via Brier |
| pss_log | NUMERIC | PSS via log loss |
| settled_at | TIMESTAMPTZ | market settlement time |

Indexes: `(hotkey, settled_at)`, `(market_id)`, `(settled_at)`

**Access control:** Only accessible to authenticated users who have verified ownership of the hotkey.

### `event`

| Column | Type | Notes |
|--------|------|-------|
| event_id | BIGINT PK | from validator |
| league_code | VARCHAR NOT NULL | e.g., "NBA" |
| sport_code | VARCHAR NOT NULL | e.g., "nba" |
| home_team | VARCHAR NOT NULL | |
| away_team | VARCHAR NOT NULL | |
| venue | VARCHAR | |
| start_time_utc | TIMESTAMPTZ NOT NULL | |
| status | VARCHAR NOT NULL | scheduled/in_play/finished/void |

### `market`

| Column | Type | Notes |
|--------|------|-------|
| market_id | BIGINT PK | from validator |
| event_id | BIGINT FK(event) NOT NULL | |
| kind | VARCHAR NOT NULL | moneyline/spread/total/draw_no_bet |
| line | NUMERIC | NULL for moneyline |

### `outcome`

| Column | Type | Notes |
|--------|------|-------|
| market_id | BIGINT PK FK(market) | one outcome per market |
| result | VARCHAR | home/away/draw/over/under/void/push |
| score_home | NUMERIC | |
| score_away | NUMERIC | |
| settled_at | TIMESTAMPTZ | |

### `consensus_snapshot` (time-series, append-only)

Consensus probability timeline per market/side. Powers the line movement chart.
Synced from the validator's ground truth snapshot data.

| Column | Type | Notes |
|--------|------|-------|
| market_id | BIGINT FK(market) | composite PK part |
| side | VARCHAR NOT NULL | composite PK part (home/away/over/under) |
| ts | TIMESTAMPTZ NOT NULL | composite PK part (snapshot timestamp) |
| prob_consensus | NUMERIC NOT NULL | bias-weighted consensus probability |
| odds_consensus | NUMERIC NOT NULL | 1 / prob_consensus |
| contributing_books | INT NOT NULL | number of sportsbooks in consensus |

Primary key: `(market_id, side, ts)`

Indexes: `(market_id, side, ts)`, `(ts)`

Retention: keep for 30 days after event settlement, then prune.

### `validator_heartbeat`

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| validator_hotkey | VARCHAR NOT NULL | |
| timestamp | TIMESTAMPTZ NOT NULL | |
| uptime_seconds | NUMERIC | |
| current_phase | VARCHAR | |
| active_miners | INT | |
| scoring_health | JSONB | |
| submission_rates | JSONB | |
| provider_health | JSONB | |
| memory_mb | NUMERIC | |
| db_pool_active | INT | |

Retention: keep latest 24h, roll up to hourly/daily.

### `user_account` (dashboard-only)

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| username | VARCHAR UNIQUE NOT NULL | |
| email | VARCHAR UNIQUE | optional |
| password_hash | VARCHAR NOT NULL | bcrypt |
| created_at | TIMESTAMPTZ NOT NULL DEFAULT now() | |
| last_login_at | TIMESTAMPTZ | |

### `user_settings` (dashboard-only)

| Column | Type | Notes |
|--------|------|-------|
| user_id | BIGINT PK FK(user_account) | |
| settings | JSONB NOT NULL DEFAULT '{}' | filters, layout, theme |
| updated_at | TIMESTAMPTZ NOT NULL DEFAULT now() | |

### `hotkey_claim` (dashboard-only)

| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| user_id | BIGINT FK(user_account) NOT NULL | |
| hotkey | VARCHAR NOT NULL | |
| challenge | VARCHAR | random nonce |
| challenge_expires_at | TIMESTAMPTZ | 5-minute expiry |
| verified | BOOLEAN DEFAULT false | |
| verified_at | TIMESTAMPTZ | |

Unique: `(user_id, hotkey)`. One user can claim multiple hotkeys.

---

## API Endpoints

### Public (no auth)

#### `GET /api/v1/leaderboard`

Paginated miner leaderboard from latest scoring cycle.

```
Query params:
  sort_by: string = "skill_score"
  order: "asc" | "desc" = "desc"
  page: int = 1
  per_page: int = 50 (max 200)
  hotkeys: string[]       # optional filter
  min_submissions: int    # optional filter

Response: {
  data: MinerScoreRow[],
  meta: { page, per_page, total, sort_by, order, as_of }
}
```

#### `GET /api/v1/miners/{hotkey}/scores`

```
Query params:
  since: datetime
  until: datetime
  granularity: "raw" | "hourly" | "daily" = "raw"

Response: {
  hotkey, uid,
  history: MinerScoreRow[],
  meta: { count, earliest, latest }
}
```

#### `GET /api/v1/miners/{hotkey}/summary`

```
Response: {
  hotkey, uid, active,
  rank: int,
  total_miners: int,
  latest: MinerScoreRow,
  percentiles: { skill_score, econ_dim, ... }
}
```

#### `GET /api/v1/fleet/stats`

```
Response: {
  active_miners, total_miners,
  mean_skill_score, median_skill_score,
  p25_skill_score, p75_skill_score,
  total_submissions_24h,
  active_events, active_markets,
  last_scoring_cycle, validator_status
}
```

#### `GET /api/v1/events`

```
Query params: status[], sport, league, since, until, page, per_page
Response: { data: EventWithMarkets[], meta: { page, per_page, total } }
```

#### `GET /api/v1/events/{event_id}/consensus`

Ground truth consensus probability timeline for each market/side in an event.
Powers the "line movement vs. miner submissions" chart.

```
Query params:
  market_id: int          # optional, filter to one market
  side: string            # optional, filter to one side (home/away/over/under)

Response: {
  event_id: int,
  event: { home_team, away_team, league_code, sport_code, start_time_utc, status },
  markets: [
    {
      market_id: int,
      kind: string,              # moneyline / spread / total
      line: float | null,
      sides: [
        {
          side: string,          # home / away / over / under
          closing_prob: float,   # final consensus probability
          closing_odds: float,   # final consensus odds
          timeline: [            # ordered by ts ASC
            {
              ts: datetime,      # snapshot timestamp
              prob_consensus: float,
              odds_consensus: float,
              contributing_books: int
            },
            ...
          ]
        },
        ...
      ]
    },
    ...
  ]
}
```

Cacheable for settled events (`Cache-Control: public, max-age=86400`).
For live/scheduled events, shorter TTL (`max-age=300`).

#### `GET /api/v1/health`

```
Response: { status, last_sync_at, last_heartbeat_at, db_connected }
```

### Authenticated (JWT, verified hotkey)

#### `GET /api/v1/me/submissions`

Own scored submissions only (hotkey must be verified by user).

```
Query params: hotkey, market_id, sport, league, since, until, sort_by, order, page, per_page
Response: { data: ScoredSubmission[], meta: { page, per_page, total, hotkey } }
```

#### `GET /api/v1/me/submissions/stats`

Pre-aggregated submission statistics grouped by dimensions. Avoids transferring
thousands of raw submissions for heatmaps, calibration curves, and breakdowns.

```
Query params:
  hotkey: string              # required, must be a verified hotkey
  group_by: string[]          # one or more of: sport, league, kind, side, time_bucket
  time_bucket: string         # "hour" | "day" | "week" | "month" (default: "day")
  since: datetime             # optional
  until: datetime             # optional

Response: {
  hotkey: string,
  groups: [
    {
      # Grouping keys (present based on group_by selection)
      sport_code: string | null,
      league_code: string | null,
      kind: string | null,        # moneyline / spread / total
      side: string | null,
      time_bucket: string | null, # ISO date/datetime of bucket start

      # Aggregates
      n_submissions: int,
      n_settled: int,             # submissions with outcomes
      mean_cle: float | null,
      mean_clv_prob: float | null,
      mean_brier: float | null,
      mean_pss: float | null,
      mean_logloss: float | null,

      # Timing
      mean_minutes_to_close: float | null,

      # Calibration buckets (only when group_by includes "calibration")
      calibration: [              # null unless requested
        {
          prob_bucket: float,     # bucket midpoint (0.05, 0.15, ..., 0.95)
          predicted_mean: float,  # mean imp_prob in bucket
          outcome_rate: float,    # fraction that actually occurred
          count: int
        },
        ...
      ] | null
    },
    ...
  ],
  meta: { total_groups, total_submissions }
}
```

**Example queries:**

- Heatmap: `?group_by=sport,kind` -- CLE by sport x market type
- Edge decay: `?group_by=time_bucket&time_bucket=hour` -- CLE over time
- Calibration: `?group_by=calibration&league=NBA` -- calibration curve for NBA
- Sport breakdown: `?group_by=sport` -- aggregate per sport

#### `GET /api/v1/me/settings` / `PUT /api/v1/me/settings`

```
GET  Response: { settings: JSONB }
PUT  Request:  { settings: JSONB }
```

### Auth

#### `POST /api/v1/auth/register`

```
Request:  { username, password, email? }
Response: { user_id, username }
```

#### `POST /api/v1/auth/login`

```
Request:  { username, password }
Response: { token, expires_at }
```

#### `POST /api/v1/auth/claim-hotkey`

```
Request:  { hotkey }  (JWT required)
Response: { challenge, expires_at }
```

#### `POST /api/v1/auth/verify-hotkey`

```
Request:  { hotkey, signature }  (sr25519 signature of challenge)
Response: { verified, hotkey }
```

### Admin (API key auth via `X-API-Key` header)

#### `POST /api/v1/admin/sync/roster`

Body: `RosterSyncPayload` -- upsert miners, set `updated_at`.

#### `POST /api/v1/admin/sync/scores`

Body: `ScoresSyncPayload` -- insert into `miner_score`, dedupe on `(miner_id, recorded_at)`.

#### `POST /api/v1/admin/sync/submissions`

Body: `SubmissionsSyncPayload` -- insert into `submission_score`, dedupe on `submission_id`.

#### `POST /api/v1/admin/sync/events`

Body: `EventsSyncPayload` -- upsert events, markets, outcomes.

#### `POST /api/v1/admin/sync/consensus`

Body: `ConsensusSyncPayload` -- append consensus timeline snapshots. Synced
separately from events because the payload is larger and only needed for
markets with active submissions.

```
Request body: {
  validator_hotkey: string,
  timestamp: datetime,
  snapshots: [
    {
      market_id: int,
      side: string,
      ts: datetime,
      prob_consensus: float,
      odds_consensus: float,
      contributing_books: int
    },
    ...
  ]
}
```

Action: Append to `consensus_snapshot` table. Deduplicate on
`(market_id, side, ts)`.

#### `POST /api/v1/admin/sync/heartbeat`

Body: `HeartbeatPayload` -- insert into `validator_heartbeat`, prune > 24h.

All admin responses: `{ accepted: bool, message?: string }`

---

## Caching Strategy

Public dashboard data only changes every scoring cycle (~5 min). Four cache layers
work together so that the vast majority of requests never touch PostgreSQL.

### Request Flow

```
Browser / Vercel Edge          Nginx proxy_cache          Go In-Memory Cache          Materialized Views          PostgreSQL
        |                            |                            |                            |                      |
        |--- GET /leaderboard ------>|                            |                            |                      |
        |   Cache-Control hit? ----->| yes -> return cached       |                            |                      |
        |                    no ---->|--- forward --------------->|                            |                      |
        |                            |   in-memory hit? --------->| yes -> return cached       |                      |
        |                            |                    no ---->|--- SELECT from ----------->|                      |
        |                            |                            |   leaderboard_mv           |--- (precomputed) --->|
        |                            |                            |<-- result + cache it        |                      |
        |<-- response + headers -----|<-- response ---------------|                            |                      |
        |                            |                            |                            |                      |
        |                            |    (on POST sync/scores)   |                            |                      |
        |                            |                            |--- invalidate cache ------->|                      |
        |                            |                            |--- REFRESH MV CONCURRENTLY->|                      |
```

### Layer 1: Go API In-Memory Cache

Highest impact, simplest to implement. The Go API holds pre-built response
bodies in a map keyed by endpoint + query params. The cache is **invalidated
explicitly** when admin sync endpoints receive new data -- no TTL guessing.

| Endpoint | Cache Key Pattern | Invalidated By | Hit Rate |
|----------|-------------------|----------------|----------|
| `GET /leaderboard` | `leaderboard:{sort}:{order}:{page}` | sync/scores | ~99% |
| `GET /fleet/stats` | `fleet_stats` | sync/scores | ~99% |
| `GET /miners/{hk}/summary` | `summary:{hotkey}` | sync/scores | ~95% |
| `GET /miners/{hk}/scores` | `scores:{hotkey}:{since}:{until}` | sync/scores | ~90% |
| `GET /events` | `events:{status}:{sport}:{page}` | sync/events | ~95% |
| `GET /health` | `health` | sync/heartbeat | ~80% |

**Not cached:** `GET /me/submissions` (per-user, authenticated, lower volume),
all `POST` endpoints.

Implementation: `sync.RWMutex` + `map[string]*CacheEntry` with a version
counter bumped on each sync. The full leaderboard for 256 miners is ~50KB,
so caching hundreds of query variations barely touches memory.

### Layer 2: HTTP Cache-Control Headers

Set on responses so the Next.js dashboard and Vercel's edge network can cache
client-side without hitting the API at all.

| Endpoint Type | Header | Effect |
|---------------|--------|--------|
| Public (leaderboard, stats) | `Cache-Control: public, max-age=120, stale-while-revalidate=300` | Edge + browser cache 2 min, serve stale up to 5 min |
| Miner history (long range) | `Cache-Control: public, max-age=3600` | Historical data changes slowly |
| Authenticated (submissions) | `Cache-Control: private, no-store` | Never cached publicly |
| Health | `Cache-Control: public, max-age=30` | Fresh for 30s |

On the Next.js side, `fetch()` with `next: { revalidate: 120 }` aligns with
these headers.

### Layer 3: PostgreSQL Materialized Views

Pre-compute expensive aggregations so hot queries read from indexed views
instead of scanning the `miner_score` time-series table.

**Fleet stats view:**

```sql
CREATE MATERIALIZED VIEW fleet_stats_mv AS
SELECT
    COUNT(*) FILTER (WHERE m.active) AS active_miners,
    COUNT(*) AS total_miners,
    AVG(ms.skill_score) AS mean_skill_score,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ms.skill_score) AS median_skill_score,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ms.skill_score) AS p25_skill_score,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ms.skill_score) AS p75_skill_score
FROM miner m
JOIN LATERAL (
    SELECT * FROM miner_score
    WHERE miner_id = m.miner_id
    ORDER BY recorded_at DESC LIMIT 1
) ms ON true;
```

**Leaderboard view (with pre-computed ranks):**

```sql
CREATE MATERIALIZED VIEW leaderboard_mv AS
SELECT
    ms.*,
    m.uid, m.active, m.stake, m.total_stake,
    RANK() OVER (ORDER BY ms.skill_score DESC NULLS LAST) AS rank
FROM miner m
JOIN LATERAL (
    SELECT * FROM miner_score
    WHERE miner_id = m.miner_id
    ORDER BY recorded_at DESC LIMIT 1
) ms ON true
WHERE m.active = true;

CREATE INDEX ix_leaderboard_mv_rank ON leaderboard_mv (rank);
CREATE INDEX ix_leaderboard_mv_skill ON leaderboard_mv (skill_score DESC);
CREATE INDEX ix_leaderboard_mv_hotkey ON leaderboard_mv (hotkey);
```

Both views are refreshed by the admin `sync/scores` handler after inserting
new rows. `REFRESH MATERIALIZED VIEW CONCURRENTLY` ensures reads are not
blocked during refresh.

### Layer 4: Nginx proxy_cache

Defense-in-depth on the VPS. Caches Go API responses at the reverse proxy
level, useful if the Go API restarts or during load spikes.

```nginx
proxy_cache_path /tmp/sparket_cache levels=1:2
                 keys_zone=api:10m max_size=100m inactive=10m;

location /api/v1/ {
    proxy_pass http://127.0.0.1:8080;
    proxy_cache api;
    proxy_cache_valid 200 120s;
    proxy_cache_bypass $http_authorization;  # skip for authenticated requests
    add_header X-Cache-Status $upstream_cache_status;
}
```

### Implementation Priority

1. **Go in-memory cache + sync invalidation** -- build first, biggest impact
2. **Cache-Control headers** -- trivial to add, large win with Vercel edge
3. **Materialized views** -- add when `miner_score` exceeds ~100K rows
4. **Nginx proxy_cache** -- add if Go API gets hammered despite layers 1-2

---

## Data Access Policy

**Public (no auth):**
- Miner leaderboard (aggregated rolling scores, ranks)
- Score history per miner over time
- Fleet statistics (mean, median, percentiles)
- Events, markets, outcomes
- Validator health

**Authenticated (verified hotkey owner):**
- Own scored submission detail (per-submission Brier, PSS, CLE)
- Saved dashboard settings

**Never exposed:**
- Other miners' per-submission data
- Provider quotes / ground truth internals
- Sportsbook bias data
- Security blacklist
- Scoring job state / work queues

---

## Hotkey Verification Flow

1. User logs in with username/password, gets JWT.
2. `POST /auth/claim-hotkey` with hotkey, API returns random nonce (5-minute expiry).
3. User signs nonce with their bittensor sr25519 key (miner CLI or browser extension).
4. `POST /auth/verify-hotkey` with hotkey + signature, API verifies against hotkey pubkey.
5. On success, `hotkey_claim.verified = true`. User can now query their submissions.

---

## VPS Infrastructure

Single VPS (e.g., Hetzner CX22 ~$5/month):

- **PostgreSQL 16+** -- dashboard data (~100MB initially)
- **Go API** -- compiled binary, ~20MB RAM, systemd service
- **Prometheus** -- scrapes validator `/metrics`, 2-week retention
- **Nginx** -- reverse proxy, TLS (Let's Encrypt), rate limiting

```
Ports:
  443  -> Nginx -> Go API (HTTPS)
  9090 -> Prometheus (internal only)
```
