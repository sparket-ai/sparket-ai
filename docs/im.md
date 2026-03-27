# Incentive Mechanism (IM)

This document describes the incentive mechanism for the Sparket subnet.
It is written from a game theoretic perspective: we reward original,
sharp, and early odds that hold up against a strong market benchmark.

## Design goal
We want a system where the best response is to publish honest, sharp,
original probabilities early. Copy trading late should earn less, and
overconfident noise should be punished.

## Scoring pipeline overview
1. Ingest provider quotes and build ground truth closing lines.
2. Score each miner submission against closing lines (CLV, CLE, MES).
3. Score each submission against outcomes (Brier, LogLoss, PSS).
4. Aggregate miner metrics with time decay and shrinkage.
5. Compute calibration and sharpness.
6. Compute originality and lead-lag.
7. Normalize and combine into SkillScore and weights.

All windows and weights are deterministic and configured in
`sparket/validator/config/scoring_params.py`.

## Ground truth and closing lines
Validators ingest provider odds and create a consensus closing line
per market and side. This is the benchmark for both economic edge
and outcome skill scoring.

## Outcome submissions
Miners also submit outcomes for finished events. Validators record the
settled outcome and use it to compute outcome-based scores
(Brier, log-loss, and PSS).

## Per-submission economic metrics (odds vs close)
Let:
- `O_miner` be the miner decimal odds
- `p_miner` be the miner implied probability
- `O_close` be the closing decimal odds
- `p_close` be the closing implied probability

We compute:
- `CLV_odds = (O_miner - O_close) / O_close`
- `CLV_prob = (p_close - p_miner) / p_close`
- `CLE = O_miner * p_close - 1`
- `MES = 1 - min(1, |CLV_prob|)`

Interpretation:
- Positive CLV and CLE mean the miner beat the closing line.
- MES rewards staying close to the efficient market while still
  finding edge.

## Per-submission outcome metrics (proper scoring rules)
Let `p_k` be the miner probability for side k and `y_k` be the outcome.

We compute:
- Brier: `sum_k (p_k - y_k)^2`
- LogLoss: `-log(p_k*)` where k* is the realized outcome
- PSS: `1 - (miner_score / truth_score)`

PSS compares the miner to the closing line as a baseline. PSS > 0
means the miner beats the market.

## Time-to-close bonus (anti-copy incentive)
Scores are adjusted by time to event start:
- Early correct picks get full credit.
- Early wrong picks are clipped (uncertainty is expected).
- Late correct picks get reduced credit (copy risk).
- Late wrong picks get full penalty.

This is a logarithmic time factor with asymmetric treatment that
discourages last-minute copying.

## Rolling aggregates
Submissions are aggregated per miner with time decay:
- Recent submissions carry more weight.
- Effective sample size is tracked.
- Metrics are shrunk toward population means for low-sample miners.

Key aggregates:
- `brier_mean` and `fq_raw`
  - `FQ = 1 - 2 * brier_mean`
- `pss_mean` (time-adjusted PSS)
- `es_mean`, `es_std`, `es_adj`
  - `es_adj = es_mean / es_std` (Sharpe-like)
- `mes_mean`

## Calibration and sharpness
Calibration fits a logit regression:
```
logit(observed) = a + b * logit(predicted)
```
Calibration score:
```
CAL = 1 / (1 + |b - 1| + |a|)
```

Sharpness measures variance of predicted probabilities:
```
Sharp = min(1, var / target_var)
```

Bin edges are deterministically jittered per window to prevent
gaming the calibration bins.

## Originality and lead-lag
We compare miner probability time series to provider time series.

Source of Signal (SOS):
```
SOS = 1 - |correlation|
```

Lead ratio counts how often the miner moved before the market
on significant moves within a lead window.

High SOS and high lead ratio reward independent, early signals.

## SkillScore (Cobb-Douglas 5-pillar model)
SkillScore uses a multiplicative Cobb-Douglas formula across 5 pillar
dimensions. Unlike an additive model, weakness in any single pillar
significantly drags the total score — balanced miners are rewarded.

### Intermediate dimensions
Normalized metrics are first combined into intermediate dimensions:

```
ForecastDim = w_fq * FQ_norm + w_cal * CAL
SkillDim    = PSS_norm
EconDim     = w_edge * ES_norm + w_mes * MES
```

Default sub-dimension weights (from `sparket/validator/config/scoring_params.py`):
- ForecastDim: `w_fq = 0.60`, `w_cal = 0.40`
- EconDim: `w_edge = 0.70`, `w_mes = 0.30`

### Cobb-Douglas pillar mapping
The intermediate dimensions are mapped to 5 pillars:

| Pillar | Derivation | What it measures |
|--------|------------|------------------|
| **Accuracy** | ForecastDim | Forecast quality + calibration |
| **Edge** | EconDim | Economic edge vs closing line |
| **Timeliness** | 0.5 * SkillDim + 0.5 * LEAD | Relative skill + early signal |
| **Uniqueness** | Composite uniqueness (or SOS fallback) | Independence from the crowd |
| **Marginal** | Shapley contribution (or 0.5 fallback) | Leave-one-out value added |

All pillar values are clamped to [epsilon, 1.0] where epsilon = 0.01.

**Uniqueness** is computed via pairwise correlation analysis, cluster
detection, and sub-window overlap scoring. It penalizes miners that
submit near-identical predictions to others (sybil rings, copy-trading
clusters). During bootstrap (before Shapley jobs run), the simple SOS
originality score `1 - |correlation|` is used as a fallback.

**Marginal** is the miner's leave-one-out Shapley contribution to the
crowd forecast. It answers: "how much would the crowd's accuracy drop if
this miner were removed?" During bootstrap, a neutral default of 0.5 is
used.

### Final SkillScore
```
SkillScore = Accuracy^0.5 * Edge^1.0 * Timeliness^0.5 * Uniqueness^1.5 * Marginal^1.0
```

Default exponents:
- `accuracy_exponent  = 0.5` — sub-linear: necessary but diminishing returns
- `edge_exponent      = 1.0` — linear: core economic value signal
- `timeliness_exponent = 0.5` — sub-linear: necessary but diminishing returns
- `uniqueness_exponent = 1.5` — super-linear: strong anti-sybil incentive
- `marginal_exponent  = 1.0` — linear: core crowd contribution signal

The multiplicative structure means:
- A miner scoring 0 on any pillar gets 0 total (after clamping to epsilon)
- A miner who is strong on 4 pillars but weak on 1 scores much lower than
  a balanced miner across all 5
- The super-linear uniqueness exponent (1.5) means originality is the
  single most impactful pillar

### Hard accuracy floor
```
SkillScore = 0  if  brier_mean > 0.30
```
Miners with poor absolute accuracy are zeroed out regardless of their
other pillar scores. This prevents fundamentally miscalibrated miners
from earning weight through other dimensions.

### Component definitions
- **FQ_norm**: forecast quality from `FQ = 1 - 2 * brier_mean`, mapped to [0, 1].
- **CAL**: calibration score from the logit regression fit.
- **PSS_norm**: normalized, time-adjusted PSS vs market baseline.
- **ES_norm**: normalized economic edge from `ES_adj` (CLE mean/std).
- **MES**: market efficiency score.
- **SOS**: originality score `1 - |correlation|`.
- **LEAD**: lead ratio (how often the miner moves first).

### Normalization
- FQ is mapped from [-1, 1] to [0, 1].
- PSS and ES are normalized via z-score logistic when enough miners
  exist, otherwise percentile normalization is used.
- CAL, MES, SOS, LEAD are clipped to [0, 1].

### Weight encoding
After SkillScore:
1. L1 normalize across all miners
2. Apply burn rate (default 90% to burn UID)
3. Enforce `max_weight_limit` and `min_allowed_weights`
4. Convert to uint16 for chain submission

## How to excel (game theoretic view)
The multiplicative Cobb-Douglas structure changes optimal strategy
compared to a linear additive model. You cannot compensate for a weak
pillar by excelling at another — every dimension matters.

Dominant strategies:
- **Be balanced.** The multiplicative formula punishes any pillar near zero.
  A miner with 0.8 across all 5 pillars far outscores one with 1.0 on
  4 pillars and 0.2 on the fifth.
- **Be original.** Uniqueness has the highest exponent (1.5), making it
  the single most impactful dimension. Independent signals are rewarded
  super-linearly.
- **Be early.** Timeliness rewards early correct signals. Late
  copy-trading reduces credit via the time bonus and hurts the lead
  ratio component.
- **Be calibrated.** Miners with Brier > 0.30 are hard-floored to zero
  weight, regardless of other pillar scores.
- **Add marginal value.** Submitting predictions that merely duplicate
  what the crowd already knows earns low Shapley contribution. The
  highest-value submissions are those that improve the crowd forecast
  when added.

Losing strategies under Cobb-Douglas:

If you only mirror the closing line:
- Low uniqueness (SOS near 0) → near-zero total score (exponent 1.5)
- Low marginal contribution (Shapley near 0)
- Less timeliness credit

If you run sybil copies:
- Cluster detection penalizes correlated submissions
- All copies share a low uniqueness score
- Marginal contribution near zero (redundant information)

If you are noisy or overconfident:
- Poor calibration → low accuracy pillar
- If Brier > 0.30 → hard floor, zero weight entirely

The best response is to publish honest, original probabilities early,
with evidence-backed deviations from the market that improve crowd
accuracy.
