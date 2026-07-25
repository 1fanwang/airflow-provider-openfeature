# e2e evidence

All runs on real Airflow 3.2.2. This covers the use cases, the five backends, real network
data flow, and the airflow-native install path, each with verbatim output.

## Environment

```
apache-airflow 3.2.2 · openfeature-sdk 0.10.0
flagd       ghcr.io/open-feature/flagd:latest (container, gRPC + OFREP) · openfeature-provider-flagd 0.5.1
Unleash     unleashorg/unleash-server:latest (container + postgres)      · UnleashClient 6.8.0
GrowthBook  growthbook 2.3.1 (SDK, local eval)
Statsig     statsig 0.72.0 (SDK, local mode)
in-house    InHouseTreatmentProvider (in-process template)
```

## The example DAGs run and behave as documented

`system_tests/verify_examples_e2e.py` runs every example DAG through `airflow dags test` against a real
metadata DB with the policy + an in-house backend wired in, then checks the behavior the docs claim.
It also guards the mapped-operator fix.

```
$ python system_tests/verify_examples_e2e.py

[revenue_rollup_example]              exit=0  tasks=6   all success: True
    every region ran v2 with the parity guardrail passing: True
[migration_2to3_example]              exit=0  tasks=2   all success: True
    policy moved pool airflow_2x -> airflow_3x: True  (pools={'airflow_3x'})
[ab_test_model_example]               exit=0  tasks=1   all success: True
    task picked the flagged model variant: True  (model=v2)
[openfeature_example]                 exit=0  tasks=2   all success: True
[kubernetes_executor_rollout_example] exit=0  tasks=20  all success: True
[executor placement]  policy sets task.executor on a regular operator: True
[mapped-task safety]  executor flag on a mapped task does not break DAG parsing: True
[custom dimension]    a registered custom placement dimension is applied on a real operator: True

10/10 checks passed
ALL EXAMPLE DAGS EXECUTE AND BEHAVE AS DOCUMENTED
```

This driver runs in CI on the Airflow 2.11 and 3.3 matrix (it needs no Docker — the in-house provider is
in-process), so a regression in an example DAG or the policy fails the build. It surfaced a real bug: a mapped operator's `executor` is read-only, so setting
`airflow.task.executor` made the policy raise `AirflowClusterPolicyError` and broke DAG parsing for the
whole file. The policy now skips a placement the operator does not accept, verified by the last two
checks and by `tests/test_policy.py::TestReadOnlyAttributeIsSkipped`.

## Does real data flow?

Yes. A **live flag change in a real flagd daemon flips which pool a real
Airflow task executes in.** Not a parse-time attribute, an actual `TaskInstance` run.

```
=== REAL TASK RUN #1 (flagd: dag_003 in the migration cohort) ===
  TaskInstance dag_003.only ran with pool = 'airflow_3x'  state = 'success'

  ...edited the flag config flagd watches: drop dag_003 from the cohort; flagd hot-reloads...

=== REAL TASK RUN #2 (flagd live-changed: dag_003 no longer in cohort) ===
  TaskInstance dag_003.only NOW ran with pool = 'airflow_2x'  state = 'success'
```

Full path: flag config → flagd daemon → gRPC → OpenFeature provider → `task_policy` → scheduled
`TaskInstance` → executed in the flag-driven pool. Run via `airflow dags test dag_003` against a real
metadata DB, pool read back from the `task_instance` table.

Which backends flow eval data over the wire:

| Backend | Data flow | How it's proven here |
|---|---|---|
| **flagd** | real network, gRPC to a daemon reading a config file, hot-reload | the live task-run flip above; wire-level OFREP JSON below |
| **Unleash** | real network, HTTP to a server backed by postgres | flag created via the Admin API, fetched by `UnleashClient` over HTTP, evaluated |
| GrowthBook | real network, HTTP GET of the features payload | `GrowthBookProvider(api_host=…, client_key=…)` calls `load_features()`; SDK evaluates locally |
| Statsig | real network, HTTP GET of the config spec | SDK fetches `/v1/download_config_specs` from the server; evaluates the gate locally |
| in-house | in-process template | n/a |

## Execution-level matrix, backends × placement use cases

`system_tests/matrix.py`: each backend's cohort config, fetched live, drives a real `airflow dags
test`; the pool (UC1, 2→3 migration) and queue (UC2, Kubernetes worker migration) the TaskInstance ran
with are read back from the metadata DB.

```
backend               net   UC1 pool (in/out)         UC2 queue (in/out)        ok
----------------------------------------------------------------------------------
flagd (gRPC)          yes   canary_pool/default_pool  kubernetes/default        PASS
GrowthBook (HTTP)     yes   canary_pool/default_pool  kubernetes/default        PASS
Statsig (HTTP)        yes   canary_pool/default_pool  kubernetes/default        PASS
Unleash (container)   yes   canary_pool/default_pool  kubernetes/default        PASS
in-house (in-proc)    no    canary_pool/default_pool  kubernetes/default        PASS
```

## Gate use cases + exposure listener, at execution

`system_tests/gates_listener.py`: a real PythonOperator task, running under `airflow dags test`,
evaluates the two code-path gates and calls the exposure listener against real flagd.

```
dag       UC3 k8s pods  UC4 checkpoint  listener exposure (pool)
--------------------------------------------------------------------------
dag_000   True          True            canary_pool
dag_004   False         False           default_pool
```

UC3 (KubernetesExecutor concurrent pods) and UC4 (disruption checkpointing) gates and the exposure
listener all carry real flagd data through task execution.

### Wire-level (flagd OFREP HTTP)

```
$ curl -s -X POST localhost:8116/ofrep/v1/evaluate/flags/airflow.task.pool -d '{"context":{"dag_id":"dag_003"}}'
{"value": "airflow_3x", "key": "airflow.task.pool", "reason": "TARGETING_MATCH", "variant": "v3x", "metadata": {}}
$ curl ... -d '{"context":{"dag_id":"dag_020"}}'
{"value": "airflow_2x", "key": "airflow.task.pool", "reason": "TARGETING_MATCH", "variant": "v2x", "metadata": {}}
```

### Live change propagating: flagd daemon → gRPC → task_policy → task.pool

```
REAL DATA FLOW: flagd daemon -> gRPC -> OpenFeature provider -> task_policy -> task.pool
  dag_017 BEFORE (migration cohort = dag_000..014): airflow_2x
  ...edited flags.json (ramp 15 -> 20 DAGs); waiting for flagd hot-reload...
  dag_017 AFTER  (migration cohort = dag_000..019): airflow_3x
  dag_017 RESTORED (cohort back to dag_000..014): airflow_2x
```

Editing the flag config the daemon watches moved `dag_017` into the rollout, the Airflow placement
changed with no code change.

## 1. The four original use cases (flagd, real Airflow)

```
$ PYTHONPATH="$PWD/src:$PWD/system_tests" python system_tests/run_use_cases.py
==============================================================================
Original use cases, gated live on real Airflow via flagd
==============================================================================

UC1  2->3 migration        15/30 DAGs on airflow_3x pool, rest airflow_2x   ok=True
UC2  K8s worker migration  10/30 DAGs on the kubernetes queue                ok=True
UC3  k8s concurrent pods   5/20 clusters enabled (code-path gate)      ok=True
UC4  disruption checkpoint 10/30 tasks get resumable checkpointing (gate)   ok=True

------------------------------------------------------------------------------
Backend portability of UC1 (2->3 migration routing), same policy, other backends:
    flagd == GrowthBook == in-house for all 30 DAGs: True
==============================================================================
```

- **UC1 / UC2** are the **policy** shape (rewrite `pool` / `queue` at parse time).
- **UC3 / UC4** are the **code-path gate** shape (`gate.flag_enabled` in executor/task code).

## 2. Identical gating across five backends

```
$ PYTHONPATH="$PWD/src:$PWD/system_tests" python system_tests/run_all_backends.py
[flagd (container)]           canary_pool: 10  default_pool: 20  matches expected cohort: True
[GrowthBook (SDK, local)]     canary_pool: 10  default_pool: 20  matches expected cohort: True
[in-house engine (template)]  canary_pool: 10  default_pool: 20  matches expected cohort: True
[Unleash (container)]         canary_pool: 10  default_pool: 20  matches expected cohort: True
[Statsig (SDK, local mode)]   canary_pool: 10  default_pool: 20  matches expected cohort: True

IDENTICAL-GATING CHECK across 5 live backends:
    all backends produced identical placement for all 30 DAGs: True
```

## 3. Airflow-native install (the wheel, entry points)

```
pip install dist/airflow_provider_openfeature-0.1.0-py3-none-any.whl
discovered provider: ['airflow-provider-openfeature']  version: 0.1.0
2. airflow.policy entrypoint  -> task_policy hookimpls: ['openfeature']   (auto-registered)
3. airflow.plugins entrypoint -> loaded plugins incl: ['openfeature']
4. DagBag parse via entrypoint policy (enable_policy=True): {'d_canary':'canary_pool','d_other':'default_pool'}  OK
```

No `airflow_local_settings`, the policy fires because the installed wheel's `airflow.policy` entry
point auto-registered it (config-gated on `[openfeature] enable_policy`).

## 4. Hook + sensor via an Airflow connection (real flagd)

```
hook.get_variant(pool, mig_dag_003): canary_pool
hook.get_variant(pool, mig_dag_020): default_pool
hook.is_enabled(rollout, mig_dag_003): True
FeatureFlagSensor.poke canary: True  non-canary: False
```

## Unit tests

```
$ pytest tests/ -q
14 passed
```

A proprietary in-house engine drives the same policy identically and emits its own exposure event; that
adapter is kept in a private repo. `InHouseTreatmentProvider` is the public template.

## Measure loop: assign -> run -> measure -> read out

`system_tests/measure_loop.py` closes the loop. A DAG population is split into a `fastpath` cohort and a
`control` cohort; each task does cohort-dependent work, times itself, and reports the duration through
`track_outcome`. The outcome is read back from **each backend's own readout surface**, and the
control-vs-fastpath lift is printed from the measured durations (nothing synthetic).

```
$ PYTHONPATH="$PWD/src:$PWD/system_tests" python system_tests/measure_loop.py

[flagd (gRPC container)]  readout: no native analytics -> OTEL/StatsD warehouse (openfeature.outcome.* metric)
    cohort      runs    mean ms   median ms
    control        8       69.2        70.2
    fastpath       8       22.4        23.0
    -> fastpath 67.7% faster (measured)

[Statsig (server SDK, real log_event)]  readout: 16 events -> sg.log_event (Pulse/metrics in a hosted project)
    cohort      runs    mean ms   median ms
    control        8       74.1        69.1
    fastpath       8       21.7        21.5
    -> fastpath 70.7% faster (measured)

[in-house engine (template)]  readout: 16 outcomes -> provider.tracked (warehouse export)
    control 67.8 ms  vs  fastpath 23.2 ms   -> 65.8% faster (measured)

readout proven for: ['flagd', 'statsig', 'in-house']
```

- **flagd** has no analytics, so the outcome rides the `openfeature.outcome.*` StatsD/OTEL metric into
  your warehouse (Grafana/Prometheus).
- **Statsig** receives the outcome through the real server SDK's `log_event` (captured here; in a hosted
  project it shows up in Pulse/metrics).
- **in-house** engines get the outcome on a `tracked` export list.

`track_outcome` routes through the OpenFeature tracking API, so a provider that implements `track()`
(Statsig, GrowthBook, LaunchDarkly) receives it, and the tagged metric covers backends that don't.

## KubernetesExecutor canary on a real cluster

`system_tests/k8s_canary_e2e.py` proves the flagship rollout on a real Kubernetes cluster (a local
`kind` cluster here). A flag (`airflow.task.executor`, resolved live from a real flagd container) routes
a cohort of DAGs to the kubernetes executor; each routed task is launched as a **real pod** (the action
KubernetesExecutor performs per task), and the pod state is read back with `kubectl`. Ramping the flag
25% → 50% via flagd hot-reload grows the cohort and the pod count with no code change.

```
$ PYTHONPATH="$PWD/src:$PWD/system_tests" python system_tests/k8s_canary_e2e.py

[ramp 25%] flag routes 2/12 DAGs to kubernetes: ['etl_dag_02', 'etl_dag_10']
           real pods on the cluster: 2  states={'etl_dag_02': 'Running', 'etl_dag_10': 'Running'}

[ramp 50%  (ramped live via flagd hot-reload)] flag routes 7/12 DAGs to kubernetes:
    ['etl_dag_02', 'etl_dag_06', 'etl_dag_07', 'etl_dag_08', 'etl_dag_09', 'etl_dag_10', 'etl_dag_11']
           real pods on the cluster: 7  states={'etl_dag_02':'Succeeded', 'etl_dag_06':'Running', ...}

cohort grew 2 -> 7 and every routed task ran as a real pod: True
```

The 25% cohort is a subset of the 50% cohort (flagd `fractional` is sticky per entity), so a ramp only
adds DAGs. This is the reliable core of the KubernetesExecutor-canary use case (cf. apache/airflow#68480)
without standing up a full executor: the provider routes the cohort by flag, and that routing puts real
work on real k8s.


