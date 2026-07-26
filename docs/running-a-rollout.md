# Running a rollout

You have a risky platform change, like a new executor or a different worker pool, and you want to move a
few pipelines onto it at a time instead of flipping every DAG at once. This walks through one rollout
end to end, and how to read it so you know when it is safe to widen.

New here? Do the [getting started](getting-started.md) walkthrough first.

## The loop

A rollout is five steps you repeat:

1. **Put the change behind a flag.** The policy reads the placement flags (`airflow.task.pool`,
   `airflow.task.queue`, `airflow.task.executor`, `airflow.task.priority_weight`) with no DAG edit. For
   any other operator setting, register your own dimension — see [extending](extending.md).
2. **Turn on the policy** in `[openfeature]` config with `enable_policy = True`. Add
   `enable_exposure_listener = True` if you want to measure.
3. **Ramp in steps.** Start with a few DAGs or a small percentage in your backend, not 100%. flagd,
   GrowthBook, Unleash, Statsig, and LaunchDarkly all express this as a targeting rule or a percentage.
4. **Watch each step** before widening.
5. **Widen or revert.** Widening raises the percentage; reverting empties the flag. Neither is a deploy.

## What to watch

Ramp 0 → 10% → 50% → 100%, and check three things at each step.

- **The split landed where you set it.** Read the pool, queue, or executor back off the tasks
  ([getting started](getting-started.md) step 5 shows how). If you set 25% and 60% of DAGs moved, the
  flag rule is wrong. Fix that before reading anything else. `openfeature_airflow.analysis.srm_check`
  runs this as a sample-ratio-mismatch test and tells you when the observed split is off.
- **The guardrail holds.** Pair the outcome with a correctness check that does not depend on the win you
  are after. In the [revenue-rollup case study](case-study/README.md) the faster rewrite is accepted only if its
  totals match the original to the cent. If the guardrail breaks, revert; the speed number does not matter.
- **The outcome moved the right way.** Record it with [`track_outcome`](measurement.md) and compare the
  treated DAGs to the rest.

## Doing it well

A few things keep a rollout honest, all true of the provider as it ships.

- **Keep the DAG as the unit.** Place by DAG, so a DAG is wholly in or wholly out. If one task in a DAG
  lands on the new executor and another does not, the two interfere and the result is unreadable.
- **The split is deterministic and sticky.** A DAG stays in the same group as you raise the percentage,
  so every step compares the same DAGs. The 25% group is a subset of the 50% group, not a fresh draw,
  and reverting then re-ramping puts the same DAGs back.
- **Measure runs that ran, with the right statistic.** A DAG whose flag resolved but whose task never
  started (a sensor timed out, an upstream failed) is not evidence. For infrastructure metrics like task
  duration and queued latency, watch p95 or p99, not the mean; one stuck task drags the mean around and
  hides the real change. See [which metric to measure](measurement.md#which-metric-to-measure).
- **Keep revert in the backend.** An incident should be a flag edit, not a rollback deploy.

## Watch the shared pool

One trap is specific to a shared cluster. If the treated DAGs and the untreated DAGs pull workers from
the same pool, a large treatment arm can starve the rest, which makes the change look better than it is.
Ramp gradually, and watch the untreated group's numbers too. If both groups move together, the pool is
the cause, not your change. For a population this small, `openfeature_airflow.switchback` sidesteps the
trap: assign whole time windows to treatment or control, so the shared pool affects both arms equally
within each window and the comparison is across time instead of across DAGs.

## Worked examples

- [Canary a faster pipeline step](case-study/README.md): the revenue-rollup ramp end to end, with a guardrail.
- [Use cases](use-cases.md): the 2→3 migration, the KubernetesExecutor canary, A/B a model, and more,
  each with the flag it uses.

## What is coming

Switchback assignment (`openfeature_airflow.switchback`) and a sample-ratio-mismatch check
(`openfeature_airflow.analysis`) ship today. Readouts you can watch continuously without inflating false
positives (always-valid inference) and variance reduction (CUPED) are planned.
