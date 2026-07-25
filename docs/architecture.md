# Architecture

Everything goes through the [OpenFeature](https://openfeature.dev) evaluation API.
This package adds three Airflow surfaces on top of it; the backend decides who is in which cohort, so
swapping flagd for GrowthBook, Unleash, or an in-house engine changes no Airflow code.

## Components

```mermaid
flowchart TB
    subgraph airflow["Airflow"]
        author["DAG / task code"]
        parse["parse & schedule<br/>(task_policy)"]
        done["task finished"]
    end
    subgraph pkg["openfeature_airflow"]
        hook["hook / sensor / gate"]
        policy["placement policy"]
        listener["exposure listener"]
    end
    api[["OpenFeature<br/>evaluation API"]]
    subgraph backends["Any OpenFeature backend"]
        flagd["flagd"]
        gb["GrowthBook"]
        unleash["Unleash"]
        inhouse["in-house engine"]
    end
    measure["warehouse /<br/>experiment platform"]

    author --> hook --> api
    parse --> policy --> api
    done --> listener --> api
    api --> flagd & gb & unleash & inhouse
    listener --> measure
```

Two independent capabilities:

- **Placement policy** (`policy.py`). A cluster policy consults a flag
  and overrides a task's `pool` / `queue` / `executor` for its cohort. A rollout is a backend config
  change, not a DAG edit.
- **In-DAG evaluation** (`hooks/`, `sensors/`, `gate.py`), read a flag for a deterministic entity
  inside a task, and record the assignment through the exposure listener for measurement.

Both are no-ops until turned on in config, so installing the package changes nothing.

## Progressive delivery: how a ramp reaches a task

```mermaid
sequenceDiagram
    participant Eng as Platform engineer
    participant BE as Flag backend
    participant Sched as Airflow parse/schedule
    participant Pol as placement policy
    participant OF as OpenFeature API
    participant TI as TaskInstance

    Eng->>BE: set airflow.task.pool cohort = dag_00x (or 10% ramp)
    Sched->>Pol: task_policy(task)
    Pol->>OF: get_string(airflow.task.pool, entity=dag_id:task_id, {dag_id})
    OF->>BE: resolve(entity, context)
    BE-->>OF: "canary_pool" (in cohort) or default
    OF-->>Pol: value
    Pol->>TI: task.pool = canary_pool
    Note over TI: task runs in the flag-driven pool
```

The entity is `dag_id:task_id`, bucketed deterministically, so the same task lands in the same cohort
every parse until the backend config changes. Widen or revert by editing the flag; no redeploy.

## In-DAG evaluation and exposure

```mermaid
sequenceDiagram
    participant Task
    participant Gate as hook / gate
    participant OF as OpenFeature API
    participant BE as Backend
    participant Lis as exposure listener
    participant WH as warehouse / experiment platform

    Task->>Gate: flag_enabled("my.experiment", entity)
    Gate->>OF: get_boolean(...)
    OF->>BE: resolve(entity)
    BE-->>OF: true (variant A)
    OF-->>Task: true
    Task->>Task: run variant A
    Lis->>WH: exposure(entity, flag, variant)
```

Exposure is what makes an experiment measurable: the listener records which cohort each run landed in,
so an analysis join lives in whatever platform you already use. On Airflow 2.x the listener fires
worker-side; on 3.x, call `emit_exposure` from the task or policy, since task-instance listeners moved
to the Task SDK.

## Airflow version notes

- Policy hooks (`airflow.policies.hookimpl`, the `airflow.policy` entry point) exist on 2.6+ and 3.x.
- `pool` and `queue` overrides work on 2.x and 3.x. Per-task `executor` needs 2.10+/3.x.
- The package depends on `apache-airflow-providers-common-compat` so the hook and sensor import the
  right base classes across versions.
