# Dashboard View Ideas

Candidate views for the miner-facing dashboard. Organised by category, with
data sources and implementation notes for each. Views marked **(auth)** require
hotkey verification.

---

## 1. Line Movement vs. Miner Submissions

> *"Did I beat the market, or did I follow it?"*

### Single-Event View

Time-series chart for one event/market showing:

- **Provider consensus line** (ground truth) as a smooth curve over time
- **Miner submission dots** plotted at `(submitted_at, imp_prob)` for each
  submission the miner made on that market
- **Closing line** marked as a horizontal dashed line at event start
- **Shaded lead/lag regions** -- green where the miner moved before the
  consensus, red where they followed after

The x-axis is time (hours/days before event start), y-axis is implied
probability. Each submission dot is coloured by its CLE score (green = positive
edge, red = negative).

Hovering a dot shows: `imp_prob`, `odds_eu`, `cle`, `clv_prob`,
`minutes_to_close`, and the matched consensus probability at that moment.

**Data:** `submission_score` joined with `market` and `event`. The consensus
line comes from the validator's ground truth snapshots -- we'd add a
`consensus_timeline` field to the events sync payload (array of
`{ts, prob_consensus}` points per market side).

### Composite View

Same concept but aggregated across markets. Three zoom levels:

| Level | Aggregation | X-Axis | Y-Axis |
|-------|-------------|--------|--------|
| Single event | Per-market submissions | Time to close | Implied probability |
| League/sport | All submissions in league | Minutes to close (bucketed) | Mean CLE per bucket |
| Overall | All submissions | Minutes to close (bucketed) | Mean CLE per bucket |

The composite version becomes a **timing curve**: "on average, how much edge
does this miner have at T minutes before close?" A miner with genuine
information advantage will show positive CLE that increases as you move further
from close.

---

## 2. Skill Score Breakdown (Radar / Sunburst)

> *"Where are my strengths and weaknesses?"*

Interactive radar chart showing the four dimensions and their sub-scores for a
single miner:

```
            Forecast
           (FQ + CAL)
              ╱╲
             ╱  ╲
     Info   ╱    ╲   Skill
  (SOS+LEAD)      (PSS)
             ╲  ╱
              ╲╱
            Economic
          (EDGE + MES)
```

Each axis extends from 0 to 1. The inner polygon is the miner's current scores,
the outer polygon is the fleet median (or a selectable comparison miner).

Clicking a dimension expands to show the sub-scores:
- Forecast: `fq_score` + `cal_score`
- Economic: `edge_score` + `mes_score`
- Info: `sos_score` + `lead_score`
- Skill: `pss_mean` (normalised)

A small bar chart below shows the **weight contribution** of each dimension to
the final skill_score (10% / 10% / 50% / 30%).

**Data:** `MinerScoreRow` from `/miners/{hotkey}/summary`.

---

## 3. Score Evolution Timeline

> *"Am I getting better or worse?"*

Multi-line time-series chart showing a miner's scores over time. Default view
shows `skill_score` with a shaded confidence band based on `n_eff` (wider band
= less data = more uncertainty).

Toggle layers for each dimension (`forecast_dim`, `skill_dim`, `econ_dim`,
`info_dim`) and sub-scores. Each line is colour-coded.

Key moments are annotated on the x-axis:
- Scoring cycle boundaries (vertical dotted lines)
- Large score changes (auto-detected, flagged with delta)
- Chain weight changes

A secondary y-axis can show `n_submissions` as a bar chart to correlate
activity volume with score movements.

**Data:** `/miners/{hotkey}/scores` with `granularity=raw` for detailed view,
`granularity=daily` for long-range view.

---

## 4. Fleet Distribution Explorer

> *"Where do I sit among all miners?"*

Interactive distribution charts for any score metric:

- **Histogram** of all active miners' `skill_score` values with the current
  miner highlighted as a vertical line
- **Percentile readout**: "You are in the 73rd percentile"
- **Dropdown to switch metrics**: skill_score, any dimension, any sub-score,
  brier_mean, n_submissions, etc.

Below the histogram, a **strip plot** shows every miner as a dot on a number
line, colour-coded by a secondary metric (e.g. dots coloured by `n_eff` so you
can see which high-scorers have reliable sample sizes).

**Data:** `/leaderboard` (full, unpaginated or large page) + `/fleet/stats` for
percentile context.

---

## 5. Market Type Performance Heatmap

> *"Am I better at moneyline or spreads? NBA or NFL?"*

Grid heatmap where:
- **Rows** = market types (moneyline, spread, total)
- **Columns** = sports/leagues (NBA, NFL, MLB, EPL, ...)
- **Cell colour** = average CLE (or Brier, or PSS) for that combination
- **Cell size** = number of submissions (bigger = more data)

A diverging colour scale (red-white-green) centred on zero makes it immediately
obvious where the miner has edge vs. where they're losing.

Clicking a cell drills into the submission list for that sport + market type
combination.

**(auth)** -- requires per-submission data.

**Data:** `/me/submissions` grouped by `kind` + `league_code`.

---

## 6. Calibration Curve

> *"When I say 60%, does it happen 60% of the time?"*

Classic calibration plot:
- X-axis: miner's predicted probability (bucketed into 10-20 bins)
- Y-axis: actual outcome frequency in that bucket
- Perfect calibration = diagonal line
- Overconfidence = curve below the diagonal
- Underconfidence = curve above

A secondary panel below shows the **sharpness histogram** -- the distribution
of the miner's predicted probabilities. A sharp forecaster has predictions
clustered near 0 and 1 (confident). A blunt forecaster clusters near 0.5.

Filters:
- By sport/league
- By market type
- By time range
- Side-by-side comparison with fleet average calibration

**(auth)** -- requires per-submission probabilities.

**Data:** `/me/submissions` -- bin `imp_prob` values and cross-reference with
outcomes from `/events`.

---

## 7. Edge Decay Curve

> *"How far ahead of the market am I?"*

Shows how a miner's CLE decays as a function of time-to-close:

- X-axis: `minutes_to_close` (bucketed: 24h+, 12-24h, 6-12h, 2-6h, 1-2h, <1h)
- Y-axis: mean CLE per bucket
- Bars coloured green (positive edge) or red (negative)

A miner with real information advantage shows strong positive CLE at early
times that gradually converges toward zero as the market absorbs the
information. A copy-trader shows near-zero CLE at all times or slightly
negative (late to the party).

Overlay the **fleet median** edge decay as a reference line.

Filters: by sport, league, market type.

**(auth)** -- requires `cle` and `minutes_to_close` per submission.

**Data:** `/me/submissions` bucketed by `minutes_to_close`.

---

## 8. Leaderboard with Spark Charts

> *"Who's hot, who's not?"*

Enhanced leaderboard table where each row includes:

| Rank | Miner | Skill Score | 7d Trend | Econ | Info | Forecast | Subs | Weight |
|------|-------|-------------|----------|------|------|----------|------|--------|
| 1 | 5Abc... | 0.847 | [sparkline] | 0.91 | 0.82 | 0.73 | 1204 | 0.034 |
| 2 | 5Def... | 0.821 | [sparkline] | 0.88 | 0.79 | 0.81 | 987 | 0.031 |

The **7d Trend** column contains a tiny inline sparkline chart of
`skill_score` over the last 7 days. Green if trending up, red if down.

Columns are sortable. Clicking a row opens the miner's detail page.

Optional filters:
- Active miners only
- Minimum submission count
- Hotkey search / multi-select for comparison

**Data:** `/leaderboard` + `/miners/{hotkey}/scores?since=-7d` for sparklines.

---

## 9. Head-to-Head Comparison

> *"How do I stack up against miner X?"*

Side-by-side comparison of two (or more) miners:

- Dual radar charts (dimension scores)
- Overlaid score evolution timelines
- Bar chart of each sub-score with both miners' values
- Table of raw metrics

Useful for miners who want to understand what a top-performing miner is doing
differently (without seeing their actual submissions -- only aggregated scores).

**Data:** `/miners/{hotkey}/scores` and `/miners/{hotkey}/summary` for each
selected miner.

---

## 10. Submission Activity Feed (auth)

> *"What did I submit recently and how did it score?"*

Scrollable feed of recent submissions, most recent first. Each card shows:

```
┌─────────────────────────────────────────────────────┐
│  NBA  Moneyline  |  Lakers vs Celtics              │
│  Submitted: 2h ago  |  Side: home  |  Odds: 1.85   │
│                                                     │
│  CLE: +0.034 ✓   Brier: 0.18   PSS: +0.12         │
│  ───────────────────────────────────────────────     │
│  Submitted 4.2h before close  |  Settled: Lakers W  │
└─────────────────────────────────────────────────────┘
```

Cards are colour-coded by CLE (green edge, red negative). Clicking expands to
show the line movement chart (view 1) for that specific event.

Filters: sport, league, market type, date range, settled/pending.

**(auth)** -- requires per-submission data.

**Data:** `/me/submissions` with event/market context from `/events`.

---

## 11. Weight & Earnings Tracker

> *"What's my incentive trajectory?"*

Dual-axis time-series:
- Left axis: `weight` over time (from `MinerScoreRow.weight`)
- Right axis: estimated TAO earnings per epoch (derived from weight share)
- Annotations for weight changes with delta values

Below, a **weight decomposition** stacked area chart showing how much each
dimension contributes:
- Economic (50% weight): `econ_dim * 0.50`
- Information (30%): `info_dim * 0.30`
- Forecast (10%): `forecast_dim * 0.10`
- Skill (10%): `skill_dim * 0.10`

This makes it tangible: "my weight dropped because my economic edge decreased,
which accounts for 50% of my score."

**Data:** `/miners/{hotkey}/scores` for weight and dimension history.

---

## 12. Validator Health Dashboard

> *"Is the validator healthy? Is my data being scored?"*

Operational dashboard (public, no auth needed):

- **Status indicator**: healthy / degraded / down (from heartbeat)
- **Last scoring cycle**: timestamp + duration + submissions scored
- **Submission throughput**: live-updating counter (5m / 15m / 1h rates)
- **Active events & markets**: counts with sport breakdown
- **Provider health**: SportsDataIO API status + latency
- **Validator uptime**: time since last restart

If Prometheus is queryable from the dashboard, add sparklines for:
- Scoring cycle duration over last 24h
- Submission accept/reject rates
- Provider API latency

**Data:** `/health` + heartbeat data. Prometheus HTTP API for sparklines.

---

## 13. Sport / League Breakdown (Treemap)

> *"Where is my submission volume concentrated?"*

Interactive treemap where:
- Top level: sports (NBA, NFL, MLB, ...)
- Second level: market types within each sport
- Tile size: number of submissions
- Tile colour: average CLE (diverging scale)

Quickly reveals: "I submit mostly on NBA moneylines and I have positive edge
there, but my NFL spread submissions are dragging me down."

**(auth)** -- requires per-submission data.

**Data:** `/me/submissions` grouped by `sport_code` + `kind`.

---

## Implementation Notes

### New API Endpoints Needed

Most views work with existing endpoints. Two additions would unlock the richest
visualisations:

1. **Consensus timeline per market** -- extend `EventsSyncPayload` to include
   ground truth snapshot timeseries for each market. This powers the line
   movement chart (view 1).

2. **Submission aggregation endpoint** -- `GET /api/v1/me/submissions/stats`
   that returns pre-aggregated stats grouped by dimensions (sport, league,
   market type, time bucket). This avoids transferring thousands of raw
   submissions to compute heatmaps and calibration curves client-side.

### Frontend Libraries

- **Charts**: Recharts or Tremor (React) for standard charts
- **Radar**: Recharts radar chart or custom D3
- **Heatmap**: D3 or Nivo
- **Treemap**: Recharts treemap or D3
- **Sparklines**: Tremor SparkChart or inline SVG
- **Calibration**: Custom with Recharts scatter + reference line

### Data Freshness per View

| View | Refresh Cadence | Cache Strategy |
|------|-----------------|----------------|
| Leaderboard | Every scoring cycle (~5m) | ISR revalidate=120 |
| Score evolution | Every scoring cycle | ISR revalidate=120, stale-while-revalidate |
| Fleet distribution | Every scoring cycle | ISR revalidate=120 |
| Validator health | Every heartbeat (~60s) | Client-side polling, 30s interval |
| Submission feed | On demand (user navigates) | No cache (private, per-user) |
| Calibration curve | On demand | Client-side cache 5m (private) |
| Line movement | On demand | Cache-Control: public, max-age=3600 (settled events don't change) |
