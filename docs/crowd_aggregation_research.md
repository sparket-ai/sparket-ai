# Crowd Aggregation Under Low Sharp Participation

**Sparket Subnet — Research Note**
**February 2026**

---

## 1. The Problem

Sparket's scoring pipeline (§ scoring\_equations.md) is optimised to identify and
reward sharp bettors — miners who beat the closing line, predict outcomes
accurately, lead the market, and maintain originality. The pipeline works.

The existential risk is upstream of scoring: **sharps may never participate**.
Professional sports bettors operate in liquid, real-money markets. Unless
subnet incentives substantially exceed their opportunity cost, they have
little reason to divert attention here. The realistic base case is a miner
population composed largely of semi-automated systems wrapping public odds
APIs, hobbyist modellers, and generic ML pipelines — individually mediocre,
but potentially numerous and diverse.

The question: **can the aggregate of many mediocre inputs produce a composite
signal that is better than any individual input, and competitive with market
closing lines?**

---

## 2. Theoretical Foundations

### 2.1 The Diversity Prediction Theorem

The core mathematical identity that makes crowd aggregation non-trivial
(Page, 2007):

$$\text{Crowd Error}^2 = \overline{\text{Individual Error}^2} - \text{Prediction Diversity}$$

where Prediction Diversity is the average squared deviation of each
individual forecast from the crowd mean. This is an **algebraic identity**,
not an approximation — the crowd *always* outperforms the average individual
by exactly the amount of diversity present. More diverse inputs → larger gap
between crowd and average individual.

**Implication for Sparket**: The existing `SOS` (originality) score, defined as
$SOS = 1 - |\rho|$ where $\rho = \text{corr}(p_{\text{miner}}, p_{\text{market}})$,
already incentivises diversity. This is not merely a detection heuristic for
sharps — it is the **mathematical prerequisite** for crowd aggregation to
produce value. Any composite odds product requires that miner errors be at
least partially independent.

> **Source**: Page, S. E. (2007). *The Difference: How the Power of Diversity
> Creates Better Groups, Firms, Schools, and Societies*. Princeton University
> Press.

### 2.2 Condorcet's Jury Theorem and Extensions

Condorcet (1785) proved that if each voter is independently correct with
probability $p > 0.5$, the majority vote converges to certainty as the number
of voters grows. Modern extensions (Ladha, 1992; Berend & Paroush, 1998)
relax the independence assumption and show that aggregation still helps when
correlations are moderate and average accuracy exceeds chance.

For probabilistic forecasts (continuous $p \in (0,1)$ rather than binary
votes), the analogous result is that the mean of $N$ forecasters with average
Brier score $B$ and average pairwise correlation $\bar\rho$ achieves:

$$\text{Brier}_{\text{crowd}} \approx \frac{B}{N}(1 + (N-1)\bar\rho)$$

When $\bar\rho < 1$, the crowd Brier score is strictly less than the average
individual Brier score. When $\bar\rho = 0$ (full independence), it shrinks
toward zero as $N$ grows.

> **Source**: Clemen, R. T. (1989). Combining forecasts: A review and
> annotated bibliography. *International Journal of Forecasting*, 5(4),
> 559–583.

### 2.3 The Good Judgment Project: Proof of Concept

The IARPA-funded Good Judgment Project (GJP) demonstrated that properly
aggregated non-expert forecasters consistently outperformed professional
intelligence analysts with access to classified information (Tetlock &
Gardner, 2015). Key aggregation innovations:

1. **Extremised mean**: After computing the arithmetic mean $\bar p$, apply a
   power transform to correct for information dilution:

$$p_{\text{ext}} = \frac{\bar{p}^{\,\alpha}}{\bar{p}^{\,\alpha} + (1 - \bar{p})^{\alpha}}, \quad \alpha > 1$$

The parameter $\alpha$ is calibrated on historical data. GJP found
$\alpha \approx 2.5$ for their forecaster pool.

2. **Performance-weighted aggregation**: Weight forecasters by recency-adjusted
   track record, analogous to our time-decayed rolling scores.

3. **Team structures**: Grouping forecasters into deliberative teams further
   improved accuracy.

The GJP result is directly relevant: it proves that **a structured aggregation
of non-expert probabilistic forecasts can beat domain experts**, provided the
aggregation method accounts for information dilution and forecaster diversity.

> **Source**: Tetlock, P. E. & Gardner, D. (2015). *Superforecasting: The Art
> and Science of Prediction*. Crown Publishing.
>
> **Source**: Satopää, V. A., et al. (2014). Combining multiple probability
> predictions using a simple logit model. *International Journal of
> Forecasting*, 30(2), 344–356.

### 2.4 Logarithmic Opinion Pools

The geometric mean of odds (equivalently, the arithmetic mean of log-odds) is
the theoretically optimal aggregation under certain Bayesian independence
assumptions (Genest & Zidek, 1986):

$$\text{logit}(p_{\text{agg}}) = \sum_{i=1}^{N} w_i \cdot \text{logit}(p_i), \quad \sum w_i = 1$$

where $\text{logit}(p) = \ln(p / (1-p))$.

This is a natural fit for odds aggregation — it operates in the space
bookmakers already use (log-odds ≈ American odds) and inherently extremises
relative to the linear pool. It also admits per-forecaster weights $w_i$,
which can be derived from rolling skill scores.

> **Source**: Genest, C. & Zidek, J. V. (1986). Combining probability
> distributions: A critique and an annotated bibliography. *Statistical
> Science*, 1(1), 114–135.

### 2.5 The Surprisingly Popular Algorithm

Prelec, Seung & McCoy (2017) introduced a mechanism for extracting truth from
crowds even when the **majority is wrong**. The protocol asks each participant
for (a) their answer and (b) their prediction of the distribution of answers.
The answer that is **more popular than predicted** (surprisingly popular)
tends to be correct.

Intuitively: informed participants know something others don't. They predict
the crowd will disagree with them. When their answer turns out to be more
common than they expected, it reveals genuine private information.

This requires a protocol extension (miners would submit both their probability
and a meta-prediction of the crowd's probability), but it is the most
theoretically principled method for extracting signal when individual accuracy
is low.

> **Source**: Prelec, D., Seung, H. S. & McCoy, J. (2017). A solution to the
> single-question crowd wisdom problem. *Nature*, 541, 532–535.

---

## 3. Proposed Aggregation Methods for Sparket

The following methods are ordered by implementation complexity. All operate on
the existing data — miner submissions of $(\text{market\_id}, \text{side},
\text{imp\_prob})$ already stored in the `miner_submission` table — unless
noted otherwise.

### 3.1 Extremised Log-Opinion Pool (No Protocol Change)

For each market and side, compute:

$$\text{logit}(p_{\text{agg}}) = \sum_{i} w_i \cdot \text{logit}(p_i)$$

where weights $w_i$ are proportional to each miner's rolling SkillScore
(already computed). Then extremise:

$$p_{\text{final}} = \frac{p_{\text{agg}}^{\,\alpha}}{p_{\text{agg}}^{\,\alpha} + (1 - p_{\text{agg}})^{\alpha}}$$

Calibrate $\alpha$ by minimising Brier score on the historical corpus of
settled markets. This can be done with a one-dimensional grid search over
$\alpha \in [1.0, 4.0]$.

**Effort**: Low. A single SQL query to pull submissions + a few lines of NumPy.
Could be prototyped in a day and run as a periodic batch job alongside
`MainScoreHandler`.

### 3.2 Learn-to-Aggregate Meta-Model (No Protocol Change)

Train a lightweight model (logistic regression or gradient-boosted tree) that
maps **distributional features** of miner submissions to an adjusted
probability:

| Feature | Description |
|---|---|
| `mean_prob` | Arithmetic mean of miner imp\_prob values |
| `median_prob` | Median |
| `std_prob` | Standard deviation (proxy for crowd disagreement) |
| `skew_prob` | Skewness of submission distribution |
| `n_submissions` | Number of miners who priced this market/side |
| `skill_weighted_mean` | Mean weighted by SkillScore |
| `spread_vs_market` | Crowd mean − current market consensus |
| `minutes_to_start` | Time until event start |
| `league` | Categorical: sport/league |
| `market_kind` | Categorical: moneyline / spread / total |

**Target**: Binary outcome (1 if this side won, 0 otherwise) for settled
markets. Optimise log-loss or Brier score.

This model can learn **systematic biases** specific to the miner population
(e.g., "the crowd is consistently 2 points too generous on NBA home
spreads") and correct for them automatically.

**Effort**: Medium. Requires a training pipeline and periodic retraining, but
no protocol changes and no new data collection.

### 3.3 Temporal Crowd Signal Extraction (No Protocol Change)

Track the **crowd centroid** over time for each market:

$$\bar{p}(t) = \frac{\sum_{i: t_i \in [t-\Delta, t]} w_i \cdot p_i}{\sum_{i: t_i \in [t-\Delta, t]} w_i}$$

Compute the rate of crowd movement $\Delta\bar{p}/\Delta t$. When the miner
crowd moves directionally **before** the market line moves, this is a leading
signal even if no individual miner is sharp. The existing `Lead` metric
already captures this per-miner; extending it to the aggregate crowd centroid
produces a usable trading signal.

**Effort**: Low-Medium. Requires windowed aggregation queries on
`miner_submission` joined with `ground_truth_snapshot` timestamps.

### 3.4 Surprisingly Popular Extension (Protocol Change Required)

Extend `MarketSubmission` to include:

```python
class MarketSubmission:
    ...
    meta_prob: Optional[float]  # "What do you think the average miner will submit?"
```

Compute the surprisingly popular adjustment:

$$p_{\text{SP}} = p_{\text{mean}} + \beta \cdot (p_{\text{mean}} - \bar{m})$$

where $\bar{m}$ is the mean of meta-predictions and $\beta$ is a learned
scaling parameter. Answers that are more common than miners predicted them to
be receive a boost.

**Effort**: High. Requires synapse schema change, miner-side implementation,
and careful incentive design to prevent gaming of the meta-prediction field.
Should be considered a Phase 2 initiative after validating simpler methods.

---

## 4. When Aggregation Fails

Intellectual honesty requires stating the conditions under which **none of
these methods will help**:

1. **Perfect correlation**: If all miners wrap the same odds API with no
   independent modelling, $\bar\rho \approx 1$ and the crowd reduces to a
   single noisy copy of the market. The Diversity Prediction Theorem gives
   zero lift.

2. **Systematic bias with no diversity**: If all miners are biased in the same
   direction (e.g., all overweight home teams), aggregation preserves the
   bias. The learn-to-aggregate model can correct for *known* biases but
   cannot create information that isn't present.

3. **Adversarial manipulation**: If miners collude to skew the composite,
   aggregation can be gamed. Robust aggregation (trimmed means, influence
   function analysis) mitigates this partially.

The critical design lever is **incentivising genuine diversity**. The existing
`SOS` originality score is a good start. Additional measures could include:

- Rewarding miners whose predictions have **low pairwise correlation** with
  other miners (not just low correlation with the market)
- Penalising exact duplication of publicly available odds
- Offering bonus incentives for novel model architectures or alternative data
  sources

---

## 5. Recommended Implementation Roadmap

| Phase | Method | Timeline | Expected Impact |
|---|---|---|---|
| **1** | Extremised log-opinion pool | 1–2 weeks | Baseline composite; measurable vs closing line |
| **2** | Learn-to-aggregate meta-model | 2–4 weeks | Bias correction; sport-specific adjustments |
| **3** | Temporal crowd signal extraction | 2–3 weeks | Leading indicator product; complements Phase 1–2 |
| **4** | Surprisingly Popular extension | 6–8 weeks | Maximum theoretical lift; requires protocol work |

At each phase, the composite output should be backtested against historical
closing lines using Brier score and CLV as primary evaluation metrics. The
composite line becomes a **network product** — the "Sparket Consensus Line" —
whose value is derived from the distributed intelligence of the miner
population rather than any single participant.

---

## 6. Conclusion

The mathematical and empirical evidence suggests that crowd aggregation under
low sharp participation is not only possible but has been demonstrated in
analogous domains (geopolitical forecasting, ensemble weather prediction,
recommendation systems). The key requirements are:

1. **Diversity of inputs** — already incentivised via the SOS originality score
2. **Proper aggregation** — extremised log-opinion pools and learned meta-models
3. **Sufficient volume** — enough miners submitting to drive $\bar\rho < 1$
4. **Feedback loop** — settled outcomes enable continuous calibration

The subnet's existing infrastructure (per-miner rolling scores, ground truth
snapshots, settled outcomes) provides all the raw materials needed. The gap is
an aggregation layer that treats the miner population as an **ensemble** and
produces a composite output that is greater than the sum of its parts.

---

## References

- Berend, D. & Paroush, J. (1998). When is Condorcet's Jury Theorem valid? *Social Choice and Welfare*, 15, 481–488.
- Clemen, R. T. (1989). Combining forecasts: A review and annotated bibliography. *International Journal of Forecasting*, 5(4), 559–583.
- Genest, C. & Zidek, J. V. (1986). Combining probability distributions: A critique and an annotated bibliography. *Statistical Science*, 1(1), 114–135.
- Ladha, K. K. (1992). The Condorcet Jury Theorem, free speech, and correlated votes. *American Journal of Political Science*, 36(3), 617–634.
- Page, S. E. (2007). *The Difference: How the Power of Diversity Creates Better Groups, Firms, Schools, and Societies*. Princeton University Press.
- Prelec, D., Seung, H. S. & McCoy, J. (2017). A solution to the single-question crowd wisdom problem. *Nature*, 541, 532–535.
- Satopää, V. A., et al. (2014). Combining multiple probability predictions using a simple logit model. *International Journal of Forecasting*, 30(2), 344–356.
- Surowiecki, J. (2004). *The Wisdom of Crowds*. Doubleday.
- Tetlock, P. E. & Gardner, D. (2015). *Superforecasting: The Art and Science of Prediction*. Crown Publishing.
