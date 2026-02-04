# Miner Guide

This guide covers setup and usage for a Sparket miner. Miners generate
odds and outcomes and submit them to validators for scoring.

## What a miner does
A miner:
- Connects to the subnet and exposes an axon.
- Produces odds for active markets.
- Submits outcomes after events are settled.
- Builds a track record that feeds SkillScore and emissions.

## Miner-Validator Communication Flow

Understanding the communication flow is essential for building a competitive miner.

### 1. Connection Info Push (Validator → Miner)

When your miner starts, it registers its axon on the chain. Validators
periodically scan the metagraph and push connection info to active miners:

```
Validator                          Miner
    |                                |
    |---CONNECTION_INFO_PUSH-------->|
    |   {                            |
    |     "validator_hotkey": "...", |
    |     "endpoint": "https://...", |
    |     "push_token": "...",       |
    |   }                            |
    |                                |
    |                                | (stores endpoint + token)
```

The miner stores this endpoint and uses it for all subsequent requests.
The `push_token` rotates and must be included in submissions for validators
that require it (`api.require_push_token: true`).

### 2. Game Data Request (Miner → Validator)

Once the miner has the validator endpoint, it can request available games:

```
Miner                              Validator
    |                                |
    |---GAME_DATA_REQUEST----------->|
    |                                |
    |<--games + markets--------------|
```

The response includes:
- **Games**: event_id, team names, venue, start_time_utc, league/sport codes
- **Markets**: market_id, kind (moneyline/spread/total), line value
- **accepts_odds**: whether the 7-day submission window is open

**Important**: The validator does NOT provide current odds or probabilities.
Miners must generate their own predictions from their own data sources.

### 3. Odds Submission (Miner → Validator)

With game and market info, miners submit their price predictions:

```
Miner                              Validator
    |                                |
    |---ODDS_PUSH------------------->|
    |   {                            |
    |     "submissions": [{          |
    |       "market_id": 123,        |
    |       "kind": "moneyline",     |
    |       "priced_at": "...",      |
    |       "prices": [              |
    |         {                      |
    |           "side": "home",      |
    |           "odds_eu": 1.91,     |
    |           "imp_prob": 0.524    |
    |         },                     |
    |         {                      |
    |           "side": "away",      |
    |           "odds_eu": 2.10,     |
    |           "imp_prob": 0.476    |
    |         }                      |
    |       ]                        |
    |     }]                         |
    |   }                            |
    |                                |
    |<--ack--------------------------|
```

**Submission fields:**
- `odds_eu`: Decimal odds (e.g., 1.91 = -110 American, 2.00 = even money)
- `imp_prob`: Implied probability (0 to 1, before vig normalization)
- `side`: The outcome being priced (home/away for moneyline/spread, over/under for totals)

The validator:
1. Validates the submission (rate limits, schema, timing)
2. Records it with a timestamp
3. Later scores it against ground truth (closing lines, outcomes)

### 4. Outcome Submission (Miner → Validator)

After an event settles, miners can submit the observed outcome:

```
Miner                              Validator
    |                                |
    |---OUTCOME_PUSH---------------->|
    |   {                            |
    |     "event_id": 456,           |
    |     "result": "home",          |
    |     "score_home": 3,           |
    |     "score_away": 2,           |
    |   }                            |
    |                                |
    |<--ack--------------------------|
```

### Time Windows

| Window | Duration | Description |
|--------|----------|-------------|
| Game visibility | 14 days ahead | Games appear in GAME_DATA_REQUEST |
| Odds acceptance | 7 days before start | `accepts_odds: true` in response |
| Scoring window | Until game start | Earlier = better for lead-lag scoring |

### Flow Summary

```
1. Miner starts, registers axon on chain
2. Validator discovers miner, pushes connection info
3. Miner requests game data (games + markets, NO odds)
4. Miner generates probabilities from its own data/models
5. Miner submits odds on cadence
6. After settlement, miner submits outcomes
7. Validator scores submissions → SkillScore → emissions
```

## Rate Limiting, Cooldowns & Blacklists

Validators enforce rate limits to ensure fair access and prevent abuse. Understanding
these limits is critical for building a reliable miner.

### Rate Limit Architecture

The validator uses a **three-tier** protection system:

| Tier | Scope | Trigger | Consequence |
|------|-------|---------|-------------|
| **Rate Limit** | Per-request | Too many requests/second | Immediate 429 rejection |
| **Cooldown** | Per-hotkey + Per-IP | Repeated failures | Temporary block (30s → 1h exponential) |
| **Blacklist** | Permanent or 24h | Critical violations or persistent abuse | 403 rejection |

### Rate Limits

Requests are rate-limited at multiple levels:

| Limit Type | Threshold | Window |
|------------|-----------|--------|
| Per-hotkey | 10 req/sec, 120 req/min | Rolling |
| Per-IP | 50 req/sec, 500 req/min | Rolling |
| Global | 200 req/sec, 5000 req/min | Rolling |

**Best practice**: Submit no more than **1 request per second** per hotkey.

### Cooldown Mechanics

When you exceed thresholds or make invalid requests repeatedly, you enter a **cooldown**:

1. **First cooldown**: 30 seconds
2. **Each subsequent**: 2× previous (exponential backoff)
3. **Maximum cooldown**: 1 hour

Cooldowns apply separately to:
- Your **hotkey** (your miner identity)
- Your **IP address** (aggregate across all hotkeys)

**What triggers cooldown:**
- Invalid/missing push tokens
- Malformed requests
- Excessive rate limit hits
- Submitting while already in cooldown

### Fail2Ban (24-Hour Ban)

If you **persistently ignore cooldowns** (keep submitting while blocked), you trigger
an automatic 24-hour ban:

| Threshold | Window | Ban Duration |
|-----------|--------|--------------|
| 20 cooldown violations | 1 hour | 24 hours |

**What counts as a violation**: Any request rejected with `ip_cooldown` or `hotkey_cooldown`.

**Avoid this by**: Implementing proper backoff in your miner. When you receive a 429
response with `Retry-After` header, **wait that many seconds** before retrying.

### Permanent Blacklist

Critical security violations result in permanent blacklist:
- Invalid signatures (spoofing attempts)
- Nonce replay attacks
- Scanner/probe requests (e.g., GET / with no bittensor headers)
- Repeated unregistered hotkey submissions

### Response Headers

When rate-limited, the validator returns helpful headers:

```
HTTP/1.1 429 Too Many Requests
Retry-After: 45
Content-Type: application/json

{
  "success": false,
  "error": "ip_cooldown",
  "retry_after": 45,
  "cooldown_type": "ip_cooldown",
  "cooldown_remaining_seconds": 45,
  "message": "You are in cooldown. Try again in 45 seconds"
}
```

**Always check `Retry-After`** and implement exponential backoff.

### Recommended Submission Cadence

The scoring system rewards **consistent, well-timed submissions** over high frequency.
Quality matters more than quantity.

#### By League Type

| League Type | Recommended Interval | Rationale |
|-------------|---------------------|-----------|
| **NFL/NBA/MLB** | Every 15-30 minutes | Markets move slowly, low game volume |
| **Soccer (EPL, etc)** | Every 10-15 minutes | More games, moderate line movement |
| **High-volume days** | Every 5-10 minutes | Multiple concurrent games |

#### Batching Strategy

**Do batch** submissions by league or time window:

```python
# Good: Batch all NFL markets together
nfl_markets = get_markets_by_league("NFL")
submit_odds_batch(nfl_markets)  # Single request with multiple markets

# Good: Batch markets starting within same hour
upcoming = get_markets_starting_within(hours=1)
submit_odds_batch(upcoming)
```

**Don't** submit one market at a time:

```python
# Bad: One request per market (wastes rate limit budget)
for market in markets:
    submit_odds(market)  # 100 markets = 100 requests
```

#### Recommended Cycle

```
Every 15 minutes (900 seconds):
  1. Fetch active markets from validator
  2. Generate odds for all markets
  3. Batch into groups of 50-100 markets
  4. Submit each batch (1-2 requests total)
  5. Log results, sleep until next cycle
```

The base miner uses this pattern by default with `SPARKET_BASE_MINER__BATCH_SIZE=50`.

### Handling Rate Limit Responses

Implement proper backoff in your submission client:

```python
import time
from typing import Optional

class SubmissionClient:
    def __init__(self):
        self.backoff_until: float = 0
    
    def submit(self, payload: dict) -> bool:
        # Check if we're in backoff
        now = time.time()
        if now < self.backoff_until:
            remaining = self.backoff_until - now
            print(f"In backoff, waiting {remaining:.0f}s")
            return False
        
        response = self._send_request(payload)
        
        if response.status_code == 429:
            # Rate limited - extract retry delay
            retry_after = int(response.headers.get("Retry-After", 60))
            self.backoff_until = now + retry_after
            print(f"Rate limited, backing off for {retry_after}s")
            return False
        
        if response.status_code == 403:
            # Blacklisted - serious problem
            print("BLACKLISTED - check logs for reason")
            return False
        
        # Success - reset backoff
        self.backoff_until = 0
        return True
```

### Monitoring Your Rate Limit Status

Watch for these log patterns:

**Healthy submission:**
```json
{"base_miner_odds_cycle": {"markets": 87, "batches": 2, "submitted": 87, "skipped": 0}}
```

**Rate limited (needs backoff adjustment):**
```json
{"submit_odds_rejected": {"error": "ip_cooldown", "retry_after": 45}}
```

**Approaching danger zone:**
```json
{"validator_backoff": {"seconds": 30, "reason": "repeated_rejections"}}
```

**Blacklisted (serious):**
```json
{"submit_odds_rejected": {"error": "ip_blacklisted", "message": "Your IP has been blacklisted"}}
```

### Summary: Do's and Don'ts

| Do | Don't |
|----|-------|
| Batch markets into single requests | Submit one market per request |
| Respect `Retry-After` headers | Retry immediately after rejection |
| Use 15-minute cycles for most leagues | Submit every few seconds |
| Implement exponential backoff | Ignore 429 responses |
| Monitor submission success rate | Flood the validator hoping some get through |

## Before you begin


You will need:
- A Linux server or VPS (Ubuntu 22.04 recommended).
- Python 3.10+ and the Bittensor CLI.
- A funded coldkey and a registered hotkey.
- A public IP address.
- An open inbound TCP port for your miner axon (default 8094).

The axon port must be reachable from the internet. A common setup issue
is a firewall or cloud security group blocking inbound traffic.

## Install the miner software
Step 1: clone the repository:
```
git clone https://github.com/sparketlabs/sparket-subnet.git
cd sparket-subnet
```

Step 2: install uv (the Python toolchain) and make sure Python 3.10 is available:
```
curl -LsSf https://astral.sh/uv/install.sh | sh
uv python install 3.10
```

Step 3: install all dependencies:
```
uv sync --dev
```

If this is your first time using uv, it will also create a `.venv/` virtual
environment in the repository.

## Wallet and registration
Miners need a coldkey (funds) and a hotkey (miner identity). If you
already have a wallet, you can skip creation and only register.

Create a wallet:
```
btcli wallet create --wallet.name miner-wallet --wallet.hotkey default
```

Register your hotkey on the subnet:
```
btcli subnet register \
  --wallet.name miner-wallet \
  --wallet.hotkey default \
  --netuid 2 \
  --subtensor.chain_endpoint ws://your-subtensor:9945
```

## Configuration
### 1) Environment file (recommended)
Copy the example and edit it with your values:
```
cp sparket/config/env.example .env
```

At minimum, set:
- `SPARKET_ROLE=miner`
- `SPARKET_AXON__HOST=0.0.0.0` (listen on all interfaces)
- `SPARKET_AXON__PORT=8094` (your public axon port)

If your server is behind NAT or a load balancer, also set:
- `SPARKET_AXON__EXTERNAL_IP=<public-ip>`
- `SPARKET_AXON__EXTERNAL_PORT=<public-port>`

### 2) Miner YAML
The repo ships a default miner YAML at `sparket/config/miner.yaml`.
For your own settings, copy it and point to the new path:
```
cp sparket/config/miner.yaml sparket/config/miner.local.yaml
```

Then set:
```
export SPARKET_MINER_CONFIG_FILE="$(pwd)/sparket/config/miner.local.yaml"
```

Key miner settings:
```
miner:
  markets: [123, 456]
  events: ["event-id-1", "event-id-2"]
  cadence:
    odds_seconds: 60
    outcomes_seconds: 120
  rate:
    global_per_minute: 60
    per_market_per_minute: 6
  retry:
    max_attempts: 3
  idempotency:
    bucket_seconds: 60
  allow_connection_info_from_unpermitted_validators: false
  endpoint_override:
    url: null
    host: null
    port: null
```

Use `endpoint_override` if you want to pin the miner to a specific
validator endpoint instead of accepting the announced one.

## Open your axon port (critical)
Validators must be able to reach your miner on the axon port. If this
port is blocked, your miner will not receive traffic and will not score.

### On Ubuntu with UFW
Allow inbound TCP on the axon port:
```
sudo ufw allow 8094/tcp
sudo ufw status
```

### On cloud providers
Most VPS providers use a security group or firewall rule. Add an inbound
rule for TCP port 8094 to your instance.

### Test the port
From another machine, check the port:
```
nc -vz <public-ip> 8094
```
If this fails, the port is still blocked or the miner is not listening.

## Run the miner
Activate the virtual environment first:
```
source .venv/bin/activate
```

Then start the miner:
```
python sparket/entrypoints/miner.py
```

### PM2 (optional)
```
pm2 start ecosystem.miner.config.js
pm2 logs miner-local
pm2 save
```

Logs live in `sparket/logs/pm2`.

## Verify it is reachable
Look for logs showing the axon is started. You can also confirm the port
is listening on the server:
```
ss -lntp | grep 8094
```

If your miner is running but validators cannot connect, re-check:
- The firewall rule or cloud security group
- The public IP and port
- Any NAT or port forwarding rules

## How submissions are produced
The miner uses `MinerService` to submit odds and outcomes on a cadence.
It reads market IDs and event IDs from `miner.markets` and `miner.events`.
Replace that pipeline with your own model or data source if desired.

## Base miner (reference implementation)
The repository includes a base miner that uses free or low-cost sources and
simple heuristics to generate odds. It is intentionally lightweight and
serves as a reference implementation, not a competitive strategy. Expect it
to perform poorly in the scoring system compared with miners that ingest
better data and model the market more accurately.

The base miner is **enabled by default** and will start automatically when
you run the miner. If you want to replace it with your own service, disable it:
```
export SPARKET_BASE_MINER__ENABLED=false
```

Additional base miner settings live in `sparket/miner/base/config.py`.

## Building a competitive miner
Competitive submissions require better data and stronger models than the
reference miner. The highest leverage improvements are:

### 1) Data quality and coverage
Data quality is the primary edge. The scoring system rewards early, accurate,
and original probabilities, so higher-resolution inputs matter.
- **Finer granularity**: line history, player availability, injuries, travel,
  lineup changes, weather, venue effects, and market microstructure.
- **Faster ingestion**: lower latency to new information improves time-to-close
  advantage and lead-lag scores.
- **Coverage depth**: more leagues and market types increase sample size and
  stability of rolling metrics.

Instructional tip: start by logging raw data with timestamps and source IDs.
This makes debugging and calibration much easier when performance drops.

### 2) Classical probability modeling
Solid statistical baselines often outperform naive heuristics and are easier
to debug and calibrate.
- **Elo‑style ratings** with home‑field and rest adjustments.
- **Poisson or bivariate Poisson** for scoreline‑driven markets.
- **Logistic regression / GLMs** for win probabilities with structured covariates.
- **Bayesian updating** to incorporate late news while preserving calibration.

### 3) Machine learning approaches
ML can capture nonlinear effects and interactions, but must remain calibrated.
- **Gradient‑boosted trees** for structured tabular features (injuries, rest, travel).
- **Sequence models** for time‑ordered signals (line moves, recent form).
- **Ensembling** multiple model families to reduce variance and improve stability.
- **Calibration layers** (isotonic regression, Platt scaling) to keep probabilities honest.

Instructional tip: evaluate models with proper scoring rules (Brier, log-loss)
and calibration plots, not just accuracy.

### 4) Market-aware adjustments
The system compares you to closing lines, so understand market structure.
- **De‑vigging** and implied probability normalization.
- **Consensus vs sharp books** weighting.
- **Outlier and stale line filtering** to avoid spurious signals.

### 5) Operational reliability
Consistency matters for rolling metrics and shrinkage.
- Avoid gaps in submission cadence.
- Backfill missed markets only when you have reliable data.
- Track submission timing relative to event start.

## Resources and references
- Proper scoring rules: https://en.wikipedia.org/wiki/Proper_scoring_rule
- Calibration overview (scikit-learn): https://scikit-learn.org/stable/modules/calibration.html
- Elo rating system: https://en.wikipedia.org/wiki/Elo_rating_system
- Poisson models for scores: https://en.wikipedia.org/wiki/Poisson_distribution
- Forecasting fundamentals: https://otexts.com/fpp3/

## Replacing or extending the miner
If you are building a competitive miner, plan to replace the base miner
with your own service. A simple, safe path is:
1. Disable the base miner (`SPARKET_BASE_MINER__ENABLED=false`).
2. Implement a new service that:
   - Collects your data feeds
   - Produces calibrated probabilities
   - Submits odds/outcomes on a cadence
3. Wire your service into the miner entrypoint or run it as a separate process.

Starting points in this repo:
- `sparket/miner/service.py`: the default cadence-based submission loop
- `sparket/miner/base/runner.py`: the base miner reference implementation
- `sparket/miner/utils/payloads.py`: submission payload builders
- `sparket/miner/client.py`: validator submission client

See `docs/miner_custom_service_example.md` for a minimal end-to-end example.

The key outcome is calibrated probabilities that beat the market early.

## First Run Verification

After starting your miner, verify these steps in order:

### Step 1: Registration Confirmed
Look for a log line showing your UID:
```
Running neuron on subnet: X with uid Y
```
If missing, check your registration with `btcli wallet overview`.

### Step 2: Validator Connection Received
Look for:
```json
{"miner_validator_endpoint_updated": {"token_received": true}}
```
This means a validator has pushed connection info and your miner has the auth token.

### Step 3: Game Data Synced
Look for:
```json
{"fetch_game_data": {"games": N}}
```
Where N > 0. If N is 0, the validator may still be initializing or there may be no upcoming games.

### Step 4: Odds Submitted
Look for the new batched submission log:
```json
{"base_miner_odds_cycle": {"markets": 50, "batches": 1, "submitted": 50, "skipped": 0}}
```
The `submitted` count should be > 0.

### Step 5: No Persistent Errors
Check recent logs for recurring errors:
```bash
pm2 logs miner-local --lines 100 | grep -i error
```
Occasional warnings are normal; persistent errors need attention.

## Log Indicators

Understanding log messages helps diagnose issues quickly.

### Success Indicators
```json
{"miner_validator_endpoint_updated": {"token_received": true}}
{"fetch_game_data": {"games": 45}}
{"base_miner_odds_cycle": {"markets": 87, "batches": 2, "submitted": 87, "skipped": 0}}
{"base_miner": "odds_generated", "market_id": 123, "has_market": true}
```

### Warning Indicators (may need attention)
```json
{"validator_backoff": {"seconds": 10}}
{"base_miner": "stats_unavailable", "home_team": "NYY"}
{"submit_odds_skipped": {"reason": "in_backoff", "remaining_seconds": 5.2}}
```

### Failure Indicators (need immediate attention)
```json
{"submit_odds_rejected": {"error": "token_invalid", "message": "Token expired"}}
{"fetch_game_data": "no_validators_available"}
{"base_miner": "odds_cycle_error", "error": "Connection refused"}
```

## Troubleshooting

### Connection Issues

**Symptom:** Validator cannot reach miner, no connection info received.

**Checks:**
```bash
# Verify miner is listening
ss -lntp | grep 8094

# Test port from outside
nc -vz <your-public-ip> 8094

# Check UFW status
sudo ufw status

# Check cloud security group allows TCP 8094 inbound
```

**Fixes:**
- Open the port: `sudo ufw allow 8094/tcp`
- Add inbound rule in cloud provider console
- If behind NAT, configure port forwarding and set `SPARKET_AXON__EXTERNAL_IP`

### Token Validation Failures

**Symptom:** `{"submit_odds_rejected": {"error": "token_invalid"}}`

**Causes:**
1. **Clock skew** - Token epochs are time-based
2. **Validator not pushing** - No CONNECTION_INFO_PUSH received
3. **Stale token** - Token rotates every ~10 validator steps

**Checks:**
```bash
# Check system time is synced
timedatectl status

# Look for connection push in logs
pm2 logs miner-local --lines 500 | grep "token_received"
```

**Fixes:**
- Sync system clock: `sudo timedatectl set-ntp true`
- Wait for validator to push new token (automatic)
- Restart miner if stuck: `pm2 restart miner-local`

### No Game Data

**Symptom:** `{"fetch_game_data": {"games": 0}}` or `"no_validators_available"`

**Causes:**
1. Validator still initializing (normal on first start)
2. Network connectivity issues
3. Miner not registered or deregistered

**Checks:**
```bash
# Verify registration
btcli wallet overview --wallet.name miner-wallet

# Check metagraph for validators
btcli subnet metagraph --netuid 2
```

**Fixes:**
- Wait for validator to finish initializing (can take 5-10 minutes)
- Check network connectivity to subtensor endpoint
- Re-register if deregistered

### Submission Rejections

**Symptom:** Low `submitted` count, high `skipped` count in cycle logs

**Common causes:**
1. **Rate limited** - Too many submissions per market per day (cap: 200)
2. **Invalid markets** - Market not in acceptance window
3. **Odds generation failed** - Missing team stats or model errors

**Checks:**
```bash
# Look for specific rejection reasons
pm2 logs miner-local --lines 200 | grep -E "rejected|rate_limit|invalid"
```

**Fixes:**
- Reduce submission frequency if rate limited
- Check that markets are within 7-day acceptance window
- Enable debug logging to see odds generation details

### Database Errors

**Symptom:** SQLite lock errors, disk space issues

**Checks:**
```bash
# Check database integrity
sqlite3 ~/.sparket/miner.db "PRAGMA integrity_check;"

# Check disk space
df -h

# Check database size
ls -lh ~/.sparket/miner.db
```

**Fixes:**
- Stop miner before database maintenance
- If corrupted: backup and recreate database
- Free disk space if full

### Diagnostic Commands

```bash
# View recent logs
pm2 logs miner-local --lines 100

# Follow logs in real-time
pm2 logs miner-local

# Check miner process status
pm2 status

# Restart miner
pm2 restart miner-local

# View miner memory/CPU usage
pm2 monit

# Check listening ports
ss -lntp | grep 8094

# Test validator connectivity
nc -vz <validator-ip> 8093

# Check system time sync
timedatectl status

# View environment variables
env | grep SPARKET
```

## Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARKET_ROLE` | - | Set to `miner` for miner mode |
| `SPARKET_WALLET__NAME` | `default` | Wallet name |
| `SPARKET_WALLET__HOTKEY` | `default` | Hotkey name |
| `SPARKET_AXON__HOST` | `0.0.0.0` | Axon bind address |
| `SPARKET_AXON__PORT` | `8094` | Axon port |
| `SPARKET_AXON__EXTERNAL_IP` | (auto) | Public IP (for NAT) |
| `SPARKET_AXON__EXTERNAL_PORT` | (same as port) | Public port (for NAT) |

### Base Miner Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARKET_BASE_MINER__ENABLED` | `true` | Enable base miner |
| `SPARKET_BASE_MINER__BATCH_SIZE` | `50` | Markets per submission batch (1-200) |
| `SPARKET_BASE_MINER__ODDS_REFRESH_SECONDS` | `900` | Seconds between odds cycles |
| `SPARKET_BASE_MINER__OUTCOME_CHECK_SECONDS` | `300` | Seconds between outcome checks |
| `SPARKET_BASE_MINER__STATS_REFRESH_SECONDS` | `3600` | Team stats cache TTL |
| `SPARKET_BASE_MINER__CACHE_TTL_SECONDS` | `3600` | General cache TTL |
| `SPARKET_BASE_MINER__MARKET_BLEND_WEIGHT` | `0.60` | Weight for market odds vs model |
| `SPARKET_BASE_MINER__VIG` | `0.045` | Vigorish for odds calculation |
| `SPARKET_BASE_MINER__ODDS_API_KEY` | (none) | The-Odds-API key (optional) |

### Control API Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARKET_MINER_API_ENABLED` | `false` | Enable local control API |
| `SPARKET_MINER_API_PORT` | `8198` | Control API port |

## Control API

The miner exposes an optional local HTTP API for monitoring and control.

**Enable:**
```bash
export SPARKET_MINER_API_ENABLED=true
export SPARKET_MINER_API_PORT=8198
```

**Endpoints:**
- `GET /health` - Basic health check (returns 200 OK)
- `GET /status` - Miner status and statistics

**Example:**
```bash
curl http://localhost:8198/health
curl http://localhost:8198/status
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         MINER NODE                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │    Axon      │    │  BaseMiner   │    │   GameDataSync   │  │
│  │  (port 8094) │    │   (runner)   │    │                  │  │
│  │              │    │              │    │  - fetch games   │  │
│  │  receives:   │    │  - generate  │    │  - sync markets  │  │
│  │  CONNECTION_ │    │    odds      │    │  - local DB      │  │
│  │  INFO_PUSH   │    │  - batch     │    │                  │  │
│  │              │    │    submit    │    │                  │  │
│  └──────┬───────┘    └──────┬───────┘    └────────┬─────────┘  │
│         │                   │                     │             │
│         │    stores token   │   uses markets      │             │
│         └───────────────────┼─────────────────────┘             │
│                             │                                   │
│  ┌──────────────────────────┴─────────────────────────────────┐ │
│  │                    ValidatorClient                         │ │
│  │                                                            │ │
│  │  submit_odds(payload)    submit_outcome(payload)           │ │
│  │        │                        │                          │ │
│  │        │  ODDS_PUSH (batched)   │  OUTCOME_PUSH            │ │
│  │        └────────────────────────┴──────────────────────────┼─┼─► Validator
│  │                                                            │ │
│  │  Backoff handling:                                         │ │
│  │  - not_ready → wait 5s, 10s, 20s... (max 60s)             │ │
│  │  - success → reset backoff                                 │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Submission Flow

```
Every odds_refresh_seconds (default: 900s / 15 min):

1. GameDataSync.get_active_markets()
   └─► Returns list of markets within acceptance window

2. For each market:
   └─► BaseMiner.generate_odds(market)
       ├─► Fetch team stats (ESPN, cached)
       ├─► Calculate team strengths
       ├─► Compute model probabilities (Log5)
       ├─► Fetch market odds (The-Odds-API, if configured)
       └─► Blend model + market → final odds

3. Batch markets (default: 50 per batch)
   └─► _build_batch_payload([(market1, odds1), (market2, odds2), ...])

4. Submit batch
   └─► ValidatorClient.submit_odds(payload)
       ├─► Check backoff (skip if in backoff period)
       ├─► Send ODDS_PUSH synapse to validator
       └─► Handle response (trigger backoff on not_ready)

5. Log cycle summary
   └─► {"base_miner_odds_cycle": {"markets": N, "batches": B, "submitted": S}}
```

## Upgrades
```
git pull
uv sync --dev
pm2 restart miner-local
```
