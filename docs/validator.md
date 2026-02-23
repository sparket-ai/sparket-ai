# Validator Guide

This guide gets a Sparket validator from zero to production. It covers
requirements, setup, configuration, and day-two operations.

## Validator modes

Sparket validators run in one of two modes:

- **Primary mode**: Full scoring engine with SportsDataIO subscription.
  Ingests provider data, scores miners, exports scoring ledger for auditors.
  The Sparket team runs the primary.
- **Auditor mode**: Lightweight validator that fetches scoring data from the
  primary, independently verifies outcome-based scores, and sets weights
  on chain. No SportsDataIO subscription, no database needed.

Most validators should run in **auditor mode**. See the
[Auditor Mode](#auditor-mode) section below.

## What the primary validator does

The primary validator is the scoring engine of the subnet. It:
- Ingests provider data and builds ground truth snapshots.
- Accepts miner submissions and scores them.
- Aggregates scores into rolling metrics and final SkillScore.
- Emits weights back to the chain.
- Exports scoring ledger (checkpoints + deltas) for auditor validators.

## Requirements (primary mode)

### Hardware
- 4 to 8 CPU cores
- 32 GB RAM
- 500 GB to 1 TB SSD
- Reliable 100 Mbps uplink

### Wallet and chain access
- Bittensor CLI installed
- A coldkey and hotkey created and registered
- Access to the subtensor endpoint for your target netuid

### Data provider subscription
Primary validators must ingest provider data to build ground truth.
We use SportsDataIO for this subnet.

You will need a SportsDataIO plan that includes:
- Odds (line history and closing lines)
- Schedules
- Final scores / outcomes

Cost is roughly $600 per month for the plan we run.

## Install

### Prerequisites
From a clean Ubuntu/Debian host, install system dependencies:
```bash
# Update packages
sudo apt update && sudo apt upgrade -y

# Install build essentials and git
sudo apt install -y build-essential git curl

# Install Node.js (required for pm2)
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs

# Install pm2 globally
sudo npm install -g pm2

# Install Docker (for managed Postgres)
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
# Log out and back in for docker group to take effect
```

### Clone and setup
```bash
git clone https://github.com/sparketlabs/sparket-subnet.git
cd sparket-subnet
```

Install uv and Python, then sync dependencies:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc  # or restart shell to get uv in PATH
uv python install 3.10
uv sync --dev
```

## Configuration (primary mode)
### 1) Environment file
Copy the example env file and edit it:
```
cp sparket/config/env.example .env
```

Required fields:
- `SPARKET_ROLE=validator`
- `SDIO_API_KEY=...`
- Database settings or `DATABASE_URL`
- Axon host and port

### 2) YAML config
Copy the example validator config:
```
cp sparket/config/sparket.example.yaml sparket/config/sparket.yaml
```

Update these sections:
```
role: validator

wallet:
  name: your-wallet
  hotkey: your-hotkey

subtensor:
  chain_endpoint: ws://your-subtensor:9945

chain:
  netuid: 2

database:
  host: 127.0.0.1
  port: 5435
  user: sparket
  name: sparket
  docker:
    enabled: true
```

Validator worker settings live under `validator`:
```
validator:
  scoring_worker_enabled: true
  scoring_worker_count: 2
  scoring_worker_fallback: true
```

### 3) Optional proxy URL
If you front your validator with a proxy or tunnel, set:
- `.env`: `SPARKET_API__PROXY_URL=https://your-proxy.example.com/axon`
- `sparket/config/sparket.yaml`:
```
api:
  proxy_url: https://your-proxy.example.com/axon
```

## First run (optional but recommended)
Running once in the foreground helps verify your setup before handing off to pm2:
```bash
uv run python sparket/entrypoints/validator.py
```

This will:
- Start Postgres via Docker if enabled
- Create the database if missing
- Run migrations and seed reference data

Stop it with Ctrl+C once you see the validator loop running and no errors.

> **Note:** This step is optional. PM2 runs the same entrypoint and will
> perform the bootstrap automatically. However, running interactively first
> makes it easier to spot configuration errors.

## Run in production
### PM2 (recommended)
```bash
# Start the validator
pm2 start ecosystem.config.js --only validator-local

# Watch logs to verify startup
pm2 logs validator-local

# Save process list so pm2 restarts on reboot
pm2 save

# Enable pm2 startup on boot
pm2 startup
# Follow the printed command (sudo env PATH=... pm2 startup ...)
```

Logs live in `sparket/logs/pm2`.

### Useful pm2 commands
```bash
pm2 status              # Check process status
pm2 restart validator-local
pm2 stop validator-local
pm2 delete validator-local  # Remove from pm2
```

### Systemd (optional)
`scripts/ops/setup_validator.sh` writes a systemd unit at
`scripts/systemd/sparket-validator.service`. Copy it to `/etc/systemd/system`
and enable it if you prefer systemd.

## Multi-worker scoring
The validator can offload scoring to worker processes. Increase
`validator.scoring_worker_count` to match your CPU. If workers become
unhealthy, `scoring_worker_fallback` keeps scoring in the main process.

## Ledger configuration

To enable the scoring ledger for auditor validators, add these to your `.env`:

```
SPARKET_LEDGER__ENABLED=true
SPARKET_LEDGER__HTTP_PORT=8200
SPARKET_LEDGER__MIN_STAKE_THRESHOLD=100000
SPARKET_LEDGER__DATA_DIR=sparket/data/ledger
```

The primary exports checkpoints (full accumulator state) and deltas
(settled submission scores) each scoring cycle. Auditors authenticate
via challenge-response and must have `validator_permit=true` with at
least 100K alpha stake.

For recompute procedures and technical details, see the
[Architecture Guide](architecture.md).

## Upgrades
```bash
cd sparket-subnet
git pull
uv sync --dev
pm2 restart validator-local
pm2 logs validator-local  # verify clean restart
```

## Troubleshooting (primary mode)
- Missing provider data: check `SDIO_API_KEY` and your SportsDataIO plan.
- DB connection errors: verify host, port, and credentials.
- Miners cannot reach you: set `SPARKET_AXON__PORT` and open the port on the host.
- Ledger endpoint not serving: verify `SPARKET_LEDGER__ENABLED=true` and port 8200 is open.

---

## Auditor Mode

Auditor mode is for non-primary validators who want to set weights
without a SportsDataIO subscription. The auditor fetches scoring data
from the primary, independently verifies outcome-based scores, and
sets weights on chain.

### What the auditor does

- Fetches scoring checkpoints and deltas from the primary validator.
- Independently verifies Brier scores from (miner probability + public outcome).
- Recomputes weights deterministically using the same shared function as the primary.
- Sets weights on chain.

### Requirements (auditor mode)

**Hardware:**
- 2 to 4 CPU cores
- 8 GB RAM
- 50 GB SSD
- Reliable network access to the primary validator

**Wallet:**
- Registered validator hotkey with `validator_permit`
- At least 100,000 alpha stake

**What you do NOT need:**
- SportsDataIO subscription
- PostgreSQL database
- Docker

### Install (auditor mode)

Same codebase as the primary. Follow the [Install](#install) section above.
You can skip the Docker installation since auditors don't need Postgres.

### Before you start

Verify your hotkey meets the eligibility requirements:

```bash
# Check validator permit and stake
btcli wallet overview --wallet.name <name> --wallet.hotkey <hotkey> --netuid 57

# You need:
#   validator_permit: True
#   stake (alpha): >= 100,000
```

If your hotkey does not have `validator_permit`, you need more stake or a
higher-ranked position. Contact the Sparket team if unsure.

Test connectivity to the primary validator:

```bash
curl -s http://62.146.174.57:8200/ledger/auth/challenge \
  -X POST -H "Content-Type: application/json" -d '{}'
# Any JSON response (even an error) means the server is reachable.
# "Connection refused" means the URL or port is wrong.
```

### Configuration (auditor mode)

Create a `.env` file with:
```
# Wallet (required)
SPARKET_WALLET__NAME=your-validator-wallet
SPARKET_WALLET__HOTKEY=your-validator-hotkey

# Chain (required)
SPARKET_SUBTENSOR__NETWORK=finney
SPARKET_CHAIN__NETUID=57

# Auditor role (required)
SPARKET_ROLE=auditor
SPARKET_AUDITOR__PRIMARY_HOTKEY=5GxzfbdPxQ9HQkejbR32JnGE86PrAqxBdxiuYrwvpnfT9jq6
SPARKET_AUDITOR__PRIMARY_URL=http://62.146.174.57:8200

# Auditor tuning (optional)
SPARKET_AUDITOR__POLL_INTERVAL_SECONDS=900
SPARKET_AUDITOR__WEIGHT_TOLERANCE=0.001
SPARKET_AUDITOR__DATA_DIR=sparket/data/auditor
```

Required:
- `SPARKET_WALLET__NAME`: Your validator wallet name.
- `SPARKET_WALLET__HOTKEY`: Your validator hotkey name.
- `SPARKET_SUBTENSOR__NETWORK`: Chain network (`finney` for mainnet, `test` for testnet).
- `SPARKET_CHAIN__NETUID`: Subnet netuid (57 for Sparket mainnet).
- `SPARKET_AUDITOR__PRIMARY_HOTKEY`: The SS58 address of the primary validator's hotkey
  (`5GxzfbdPxQ9HQkejbR32JnGE86PrAqxBdxiuYrwvpnfT9jq6`).
- `SPARKET_AUDITOR__PRIMARY_URL`: HTTP endpoint where the primary serves ledger data
  (`http://62.146.174.57:8200`).

Optional:
- `SPARKET_AUDITOR__POLL_INTERVAL_SECONDS`: How often to check for new data (default: 900 / 15 minutes).
- `SPARKET_AUDITOR__WEIGHT_TOLERANCE`: Cosine similarity threshold (default: 0.001).
- `SPARKET_AUDITOR__DATA_DIR`: Where to store local state (default: sparket/data/auditor).
  Created automatically if it does not exist.

### Run (auditor mode)

```bash
pm2 start ecosystem.config.js --only auditor-local
pm2 logs auditor-local
pm2 save
```

Or foreground for testing (requires a `.env` file in the project root with
the auditor configuration above, since `--auditor.*` flags are read from
env vars):
```bash
uv run python sparket/entrypoints/auditor.py \
  --wallet.name your-wallet \
  --wallet.hotkey your-hotkey \
  --subtensor.network finney \
  --netuid 57
```

If you see `SPARKET_AUDITOR__PRIMARY_HOTKEY is required` and the process
exits, your `.env` file is missing or not in the current directory. Make
sure you run the command from the project root where `.env` lives.

You should see logs showing (auditor now defaults to TRACE verbosity):
1. `auditor: starting` and `auditor_config` with your settings
2. `auditor_cycle: starting` every poll interval
3. `auditor_sync` with checkpoint and delta fetch results
4. `auditor_plugin_result` with weight verification status
5. `auditor_runtime: sleeping` with `next_poll_in_seconds`

Cadence note: auditor is poll-interval based (default 900s / 15 minutes), not
epoch-based. A quiet period between cycles is expected. If you need tighter
alignment with primary export cadence, lower
`SPARKET_AUDITOR__POLL_INTERVAL_SECONDS` (for example, 750-900 seconds).

If you need less verbosity, pass `--logging.info` (or `--logging.debug`
for moderate verbosity).

### What gets verified

**Independently verifiable (~20% of score weight):**
- Brier scores: recomputed from miner probabilities + public outcomes
- FQ accumulation: tracked incrementally, cross-checked against checkpoint

**Consistency-checked:**
- PSS scores: if PSS > 0, miner Brier must be < provider Brier

**Trusted from checkpoint (~80% of score weight):**
- CLV/CLE metrics (es_adj, mes_mean): derived from SportsDataIO closing lines
- SOS and lead-lag: derived from provider time series
- Ground truth construction

**Fully verifiable (100% of math):**
- Normalization across miners (z-score logistic or percentile)
- Dimension combining (ForecastDim, SkillDim, EconDim, InfoDim)
- Weight encoding (L1 normalize, burn rate, max weight limit, uint16)

This catches fabricated outcome scores, normalization manipulation, and
burn rate tampering. It does not catch a dishonest primary biasing
CLV/CLE scoring, which remains a trust assumption inherent to the paid
data model.

### Epoch bumps / recomputes

The primary may occasionally need to recompute scores (bad data, bugs,
DB recovery). When this happens:

1. The primary publishes a new checkpoint with an incremented epoch and
   a structured recompute record (reason code, detail, affected events).
2. Your auditor automatically detects the epoch change, logs the reason,
   and resets its accumulators to the new checkpoint.
3. Normal delta flow resumes.

**When to investigate:**
- If you see more than 1 epoch bump in 24 hours, the auditor will pause
  weight-setting and log a `RECOMPUTE_RATE_EXCEEDED` warning. Check the
  primary's stated reason before overriding.
- If scores suddenly change dramatically without a clear reason, consider
  pausing your auditor and contacting the subnet team.

### Troubleshooting (auditor mode)

- **Auth failure (403)**: Check that your hotkey has `validator_permit=true`
  and at least 100K alpha stake.
- **Connection refused**: Verify the primary URL and port. The default
  ledger port is 8200.
- **Checkpoint fetch timeout**: The primary may be restarting or under heavy
  load. The auditor retries with backoff automatically.
- **Weight mismatch**: If the auditor logs a mismatch but you trust the
  primary, check the cosine similarity value. Small differences
  (< 0.001) are normal due to timing.
