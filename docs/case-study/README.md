# Case study: canary a faster pipeline step with a feature flag

A worked example on real Airflow with a real feature-flag backend (Unleash), start to finish. It
shows the everyday version of what this provider is for: roll a risky pipeline change out gradually,
measure it, and keep the escape hatch.

## The problem

A data team runs a nightly `revenue_rollup` ETL that aggregates raw orders into daily revenue per
region. Traffic grew, the job is slow, and it keeps missing its SLA. An engineer rewrote the
aggregation to be much faster (`rollup_v2`). The numbers look right in a notebook, but this feeds
finance dashboards: shipping a wrong total to every region at once is not an option, and "revert" today
means a code change, a review, and a redeploy while the pipeline is already late.

## The idea

Put `v2` behind a feature flag and ramp it across regions instead of flipping it everywhere.

- Start at 0%: every region stays on the trusted `v1`.
- Raise the dial: a few regions run `v2`, the rest stay on `v1`.
- At each step, measure two things: the runtime, and whether `v2`'s revenue matches `v1` to the cent.
- If a region's numbers ever diverge, set the flag back to 0%. No redeploy.

The flag lives in Unleash. Airflow reads it through this provider, so the rollout is a dial in a UI,
not an edit to the DAG.

## Setup

**The flag** is a normal Unleash feature with a gradual-rollout strategy, stickiness on the region key
so a region stays in its cohort as you ramp.

![The feature flag in Unleash](img/unleash-flag.png)

![The gradual-rollout strategy at 50%](img/unleash-rollout.png)

**The pipeline** ([`pipeline.py`](../../system_tests/case_study/pipeline.py)) generates a realistic
orders dataset (480k rows across 12 regions and 30 days) and has both rollups. They return identical
numbers; `v2` just does far fewer passes:

```python
def rollup_v1(rows):   # original: rescans the shard per (region, date) group
    keys = {(r["region"], r["order_date"]) for r in rows}
    return {(region, date): _revenue(rows, region, date) for region, date in keys}

def rollup_v2(rows):   # rewrite: one pass, dict accumulation
    out = defaultdict(float)
    for r in rows:
        ...
    return dict(out)
```

**The wiring** is the gate: each region reads the flag, runs the matching rollup, and records the
outcome for measurement.

```python
from openfeature_airflow.gate import flag_enabled
from openfeature_airflow.measure import track_outcome

use_v2 = flag_enabled("revenue_rollup.use_fast_agg", region, region=region)
result = (rollup_v2 if use_v2 else rollup_v1)(shard)
track_outcome("rollup_ms", region, value=elapsed_ms, variant="v2" if use_v2 else "v1", region=region)
```

## Running the ramp

[`run_case_study.py`](../../system_tests/case_study/run_case_study.py) turns the Unleash dial from 0%
to 100%, and at each step evaluates every region through the provider, runs its real shard, and checks
the guardrail (`v2 == v1`).

![Ramping the rollout and measuring each step](img/run.png)

The result at every step: `v2` runs about **89% faster** (v1 ~77 ms per region, v2 ~9 ms), and the
revenue is **identical to the cent**. Because the rollout percentage is a per-region probability, the
enabled set grows in steps as you raise the dial (0, then 4, then 10, then all 12 regions), and a
region never leaves the cohort once it is in it.

If `v2` had produced a wrong total for any region, the guardrail would have caught it and the fix is
one dial back to 0% in the Unleash UI, with no code change and no redeploy.

## Reproduce it

```bash
# 1. bring up Unleash and create the flag
docker compose -f system_tests/docker-compose.unleash.yml up -d
python system_tests/case_study/setup_unleash_flag.py 50   # creates the flag at a 50% rollout

# 2. run the ramp
pip install airflow-provider-openfeature UnleashClient
PYTHONPATH="src:system_tests/case_study" python system_tests/case_study/run_case_study.py
```

## What it generalizes to

The step here is an aggregation rewrite, but the shape is the same for any risky pipeline change: a new
model version, a different join strategy, a library upgrade, a move to a new pool or executor. Ramp it
across a cohort, measure the outcome, keep the numbers honest with a guardrail, and revert with a flag
instead of a deploy. Swap Unleash for flagd, GrowthBook, Statsig, LaunchDarkly, or an in-house engine
without touching the DAG, since it all goes through OpenFeature.
