# Deploying the Auditor Branch to Production

Quick guide for getting the `auditor` branch running on the production
validator. Covers migration, configuration, and first-run verification.

## Prerequisites

- Production validator currently running on the `test` branch
- SSH access to the production machine
- PostgreSQL accessible (same credentials as current setup)

## 1. Pull the branch

```bash
cd sparket-subnet
pm2 stop validator-local
git fetch origin
git checkout auditor
git pull origin auditor
uv sync --dev
```

## 2. Run database migrations

Three new migrations need to apply on top of the existing schema:

```bash
DATABASE_URL="postgresql+asyncpg://<user>:<pass>@<host>:<port>/<dbname>" \
  uv run alembic -c sparket/validator/database/alembic.ini upgrade head
```

This adds:
- `security_blacklist` table (for rate-limit/ban tracking)
- Accumulator columns on `miner_rolling_score` (for ledger export)
- `ledger_state` table (singleton row, auto-seeded)
- `skill_dim` column on `miner_rolling_score`

Verify with:
```bash
DATABASE_URL="..." uv run alembic -c sparket/validator/database/alembic.ini current
# Should show: b7c8d9e0f1a2 (head)
```

## 3. Configure environment

Add these to your `.env` file (or export them):

### Required (new in this branch)

```bash
# Enable the ledger HTTP endpoint for auditor validators
SPARKET_LEDGER__ENABLED=true
SPARKET_LEDGER__HTTP_PORT=8200
SPARKET_LEDGER__DATA_DIR=sparket/data/ledger

# Minimum alpha stake for auditor authentication (production value)
SPARKET_LEDGER__MIN_STAKE_THRESHOLD=100000
```

### Existing (verify these are set)

```bash
# These should already be in your .env from the test branch
SDIO_API_KEY=<your key>
SPARKET_DATABASE__USER=<user>
SPARKET_DATABASE__PASSWORD=<pass>
SPARKET_DATABASE__HOST=<host>
SPARKET_DATABASE__PORT=<port>
SPARKET_DATABASE__NAME=<dbname>
```

### NOT needed in production

```bash
# Do NOT set these in production:
# SPARKET_TEST_MODE=true     (disables auth checks - e2e only)
# SPARKET_LEDGER__MIN_STAKE_THRESHOLD=0  (e2e only)
```

## 4. Open port 8200

Auditor validators connect to the ledger HTTP endpoint on port 8200.

```bash
# UFW
sudo ufw allow 8200/tcp

# Or cloud security group: allow TCP 8200 inbound
```

If you want to delay enabling auditor access, simply omit
`SPARKET_LEDGER__ENABLED=true` from your `.env`. The validator
runs fine without it -- the ledger server just won't start.

## 5. Start the validator

```bash
pm2 start ecosystem.config.js --only validator-local
pm2 logs validator-local
```

## 6. First-run verification

### Immediate (within 60 seconds)

Watch logs for these:

```
ledger_http: started, port: 8200        # Ledger server up
sdio_background: started                # SDIO ingest running
sportsbook_registered: code: FanDuel    # New! Books being registered
```

### After first SDIO cycle (~1-2 minutes)

```
sdio_outcomes_recorded: count: N        # Outcomes for finished events
sdio_lifecycle_sync: outcomes_recorded   # Status + outcome sync
```

### After first scoring cycle (~5 minutes)

```
main_score: end                         # Scoring pipeline completed
ledger_checkpoint: epoch: 1, miners: N  # Checkpoint exported
ledger_export: checkpoint: True         # Ledger data written to disk
```

### Diagnostic SQL (after ~10 minutes)

```sql
-- Sportsbooks registered?
SELECT count(*) FROM sportsbook;  -- Expected: ~10

-- Outcomes created for finished events?
SELECT count(*) FROM outcome;  -- Expected: >> 3 (was stuck at 3)

-- Ground truth flowing?
SELECT count(*), ROUND(AVG(contributing_books)::numeric, 1) as avg_books
FROM ground_truth_closing;  -- Expected: >> 15, avg_books >= 2

-- CLV scoring resumed?
SELECT count(*) as total, max(computed_at) as latest
FROM submission_vs_close;  -- Expected: >> 966, recent timestamp

-- PSS computing?
SELECT count(*) FILTER (WHERE pss IS NOT NULL) as has_pss
FROM submission_outcome_score;  -- Expected: > 0

-- Ledger state healthy?
SELECT * FROM ledger_state;  -- epoch=1, last_checkpoint_at recent

-- skill_dim populated?
SELECT uid, skill_dim, skill_score
FROM miner_rolling_score mrs
JOIN miner m ON mrs.miner_id = m.miner_id
WHERE as_of = (SELECT max(as_of) FROM miner_rolling_score)
ORDER BY skill_score DESC NULLS LAST;
```

## 7. What changed in this branch (summary)

### Scoring pipeline fixes
- **Sportsbook auto-registration**: SDIO ingestor now registers sportsbook
  codes on first encounter (was empty, blocking ground truth consensus)
- **Snapshot JOIN fix**: Ground truth quotes now match by sportsbook code
  (was matching all books to every quote via provider_id only)
- **Outcome ingestion**: Outcomes recorded for already-finished events
  (was only recording on status transition, missing 99.9% of events)
- **skill_dim column**: SkillDim (PSS_norm) now persisted alongside the
  other three dimension columns

### Auditor system (new)
- Ledger HTTP server on port 8200 (checkpoint + delta distribution)
- Challenge-response auth with vpermit + stake gate
- Auditor entrypoint (`sparket/entrypoints/auditor.py`)
- Plugin-based verification (WeightVerification v1)
- PM2 config for auditor-local in `ecosystem.config.js`

### Security
- SecurityManager for rate limiting, cooldowns, and blacklisting
- iptables integration for network-level blocking (if root)
- Fail2ban for persistent cooldown violators

### Dependencies
- `bittensor>=10.0.0` (was >=9.11.1)
- `aiohttp>=3.13.3` (new, for ledger HTTP server)

## 8. Rollback

If something goes sideways:

```bash
pm2 stop validator-local
git checkout test
pm2 start ecosystem.config.js --only validator-local
```

The new DB columns and tables are additive (nullable columns, new tables).
The old code ignores them, so no downgrade migration is needed for a
quick rollback. Run `alembic downgrade` only if you need to permanently
revert.
