#!/usr/bin/env bash
# Sparket Nightly Improvement Run (Ralph Wiggum — bash loop)
#
# Each iteration: fresh claude session → reads state from files → does ONE
# task → logs results → exits. Shell loop restarts with fresh context.
#
# State persists in files, not context. Key files:
#   specs/nightly/state.md    — cross-night persistent state (leaderboard equivalent)
#   specs/nightly/runs/DATE.md — tonight's iteration log
#   CLAUDE.md                  — auto-loaded rules (Tier 1, ~30 lines)
#   PostToolUse hook           — gate reminders on code edit (Tier 2)
#   specs/000-system-verification/ — deep docs loaded on demand (Tier 3)

set -euo pipefail

PROJDIR="/root/sparket-ai"
LOGDIR="$PROJDIR/specs/nightly/runs"
DATE=$(date -u +"%Y-%m-%d")
LOGFILE="${LOGDIR}/${DATE}-cron.log"
RUNLOG="${LOGDIR}/${DATE}.md"
SUMMARY="${LOGDIR}/${DATE}-summary.yaml"
PIDFILE="/tmp/sparket_nightly.pid"
STOPFILE="/tmp/sparket_nightly_stop"
BRANCH="nightly/${DATE}"

mkdir -p "$LOGDIR"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOGFILE"; }

# ── Prevent double runs ──────────────────────────────────────
if [[ -f "$PIDFILE" ]]; then
    OLDPID=$(cat "$PIDFILE")
    if kill -0 "$OLDPID" 2>/dev/null; then
        log "Another nightly run is active (PID $OLDPID). Exiting."
        exit 0
    fi
    rm -f "$PIDFILE"
fi
echo $$ > "$PIDFILE"
trap 'rm -f "$PIDFILE"' EXIT

# ── Remove stale stop file from previous nights ──────────────
rm -f "$STOPFILE"

# ── Window check ─────────────────────────────────────────────
in_window() {
    [[ -f "$STOPFILE" ]] && return 1
    local hour_utc=$((10#$(date -u +%H)))  # 10# forces base-10 (avoids octal 08/09 bug)
    # 10PM MST (UTC-7) = 05 UTC → 6AM MST = 13 UTC
    [[ "$hour_utc" -ge 5 && "$hour_utc" -lt 13 ]]
}

if ! in_window; then
    log "Outside authorized window. Aborting."
    exit 0
fi

# ── Health check ─────────────────────────────────────────────
if ! pm2 describe validator-local >/dev/null 2>&1; then
    log "Validator not running. Aborting."
    exit 1
fi

cd "$PROJDIR"

# ── Create nightly branch ────────────────────────────────────
CURRENT_BRANCH=$(git branch --show-current)
if [[ "$CURRENT_BRANCH" != "$BRANCH" ]]; then
    # Stash any uncommitted work first
    if ! git diff --quiet HEAD 2>/dev/null; then
        git stash push -m "pre-nightly-$(date -u +%H%M%S)" 2>> "$LOGFILE" || true
        log "Stashed uncommitted work"
    fi
    git checkout -B "$BRANCH" main 2>> "$LOGFILE"
    log "Created and checked out branch: $BRANCH"
fi

# ── Initialize run log ───────────────────────────────────────
if [[ ! -f "$RUNLOG" ]]; then
    cat > "$RUNLOG" <<EOF
# Nightly Run — $DATE

**Window**: 10PM-6AM MST (05:00-13:00 UTC)
**Started**: $(date -u +%Y-%m-%dT%H:%M:%SZ)
**Branch**: $BRANCH

## Iterations

EOF
fi

# ── Initialize summary log ───────────────────────────────────
if [[ ! -f "$SUMMARY" ]]; then
    echo "# Nightly summary $DATE" > "$SUMMARY"
    echo "iterations: []" >> "$SUMMARY"
fi

# ── The Loop ─────────────────────────────────────────────────
ITERATION=1
MAX_ITERATIONS=80
COOLDOWN=30

log "=== Nightly loop starting (max_iter=$MAX_ITERATIONS, branch=$BRANCH) ==="

while in_window && [[ $ITERATION -le $MAX_ITERATIONS ]]; do
    ITER_START=$(date -u +%s)
    log "--- Iteration $ITERATION ---"

    ITER_LOG="${LOGDIR}/${DATE}-iter-${ITERATION}.log"

    # ── Build prompt ──────────────────────────────────────────
    # CLAUDE.md auto-loads (Tier 1). This prompt is the per-iteration instruction.
    PROMPT="$(cat <<PROMPT_EOF
# Sparket Nightly — Iteration $ITERATION ($DATE)

You have ONE iteration. Do ONE focused task. Be surgical.

## Orient (fast)

1. Read today's run log: \`specs/nightly/runs/${DATE}.md\` — what was already done tonight.
2. Read persistent state: \`specs/nightly/state.md\` — verified improvements, killed ideas, constraints.
3. Check time: \`date -u\`. If past 12:45 UTC, write a summary to the run log and exit.
4. \`pm2 list\` — confirm validator healthy.

## Pick ONE task

Priority (skip anything in today's run log or in "Killed" state):
1. Fix FAIL/PARTIAL verification gates
2. Outcome scoring coverage improvements
3. Missing test coverage for verification gates
4. Known issues: aggregate skip logging (G5.1), job dependency enforcement
5. Code quality in scoring path (dead code, unclear names, missing types)
6. New verification gates for uncovered failure modes

## Execute

- Make the change on branch \`$BRANCH\` (you are already on it).
- Run tests: \`.venv/bin/python -m pytest tests/validator/scoring/ -x -q --tb=short\`
- Classify: A (safe self-deploy+merge), B (deploy+observe, don't merge), C (propose only)
- **Category A**: Full suite green → \`pm2 restart validator-local\` → wait 60s → check \`pm2 list\` and auditor logs → if healthy, commit on this branch. Note: to merge to main, cherry-pick the commit.
- **Category B**: Deploy → observe one scoring cycle → log findings → DO NOT merge. If anything looks wrong, \`git checkout -- <files>\` and \`pm2 restart validator-local\`.
- **Category C**: Write proposal to \`specs/nightly/proposals/${DATE}-<title>.md\`. Do not deploy.
- If tests fail after your change: revert immediately, log what happened, move on.

## Log (MANDATORY before exit)

Append to \`specs/nightly/runs/${DATE}.md\`:

\`\`\`
### Iteration $ITERATION — HH:MM UTC
**Task**: [what you did]
**Category**: A/B/C
**Files**: [changed files]
**Tests**: [result]
**Deployed**: yes/no
**Committed**: yes/no (commit hash if yes)
**Result**: [observation]
\`\`\`

Update \`specs/nightly/state.md\` if needed (add to Verified/Pending/Killed).

## Safety (NON-NEGOTIABLE)

- You are on branch \`$BRANCH\`, NOT main. Production validator runs from working tree.
- NEVER change compute_weights(), checkpoint format, synapse API, LEDGER_SCHEMA_VERSION
- ALWAYS run tests before any deploy
- REVERT immediately if anything breaks: \`git checkout -- <file> && pm2 restart validator-local\`
- Check auditor sync after any restart
- This is a PRODUCTION Bittensor validator (netuid 57, mainnet)

Exit after completing ONE task and logging it.
PROMPT_EOF
    )"

    # ── Run claude (fresh session) ────────────────────────────
    /root/.local/bin/claude \
        --print \
        --dangerously-skip-permissions \
        --max-turns 30 \
        "$PROMPT" \
        > "$ITER_LOG" 2>&1 || true

    ITER_END=$(date -u +%s)
    DURATION=$(( ITER_END - ITER_START ))

    # ── Append structured summary ─────────────────────────────
    cat >> "$SUMMARY" <<EOF
- iteration: $ITERATION
  started: "$(date -u -d @$ITER_START +%Y-%m-%dT%H:%M:%SZ)"
  duration_s: $DURATION
  log: "${DATE}-iter-${ITERATION}.log"
EOF

    log "Iteration $ITERATION complete (${DURATION}s)"

    ITERATION=$((ITERATION + 1))

    # ── Cooldown ──────────────────────────────────────────────
    if in_window && [[ $ITERATION -le $MAX_ITERATIONS ]]; then
        log "Cooldown ${COOLDOWN}s"
        sleep "$COOLDOWN"
    fi
done

# ── End reason ────────────────────────────────────────────────
if [[ -f "$STOPFILE" ]]; then
    log "Stopped by stop file ($STOPFILE)"
    rm -f "$STOPFILE"
elif ! in_window; then
    log "Window closed"
else
    log "Max iterations ($MAX_ITERATIONS) reached"
fi

# ── Switch back to main ──────────────────────────────────────
# Don't delete the branch — human will review and merge/discard
git checkout main 2>> "$LOGFILE" || true
log "Switched back to main. Nightly branch $BRANCH preserved for review."

log "=== Nightly run complete ($((ITERATION - 1)) iterations) ==="
