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
