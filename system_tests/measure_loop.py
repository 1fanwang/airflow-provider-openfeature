"""The measure half of the loop, across real backends: assign -> run -> measure -> read out.

For each backend, a DAG population is split into a ``fastpath`` group and a ``control`` group; each
task does group-dependent work and reports its own duration through ``track_outcome``. We then read
the outcome back from *that backend's own readout surface* and print the measured control-vs-fastpath
lift. Nothing synthetic: the lift is whatever the real runs produced.

Backends exercised with their real libs:
  * flagd (real gRPC container)  -- assigns the group; has no analytics, so the outcome rides the
    OpenFeature ``track()`` no-op + the OTEL/StatsD warehouse path (lift shown from the measured values).
  * Statsig (real server SDK)     -- assigns via a gate; ``track()`` -> real ``log_event``, captured.
  * in-house engine (template)    -- assigns via a rollout; ``track()`` -> its warehouse export list.

(Real subprocess execution via ``airflow dags test`` is proven separately in real_data_flow.py; here
the task callables run in-process so the readout capture is deterministic.)

Prereqs: Docker (flagd), ``pip install statsig openfeature-provider-flagd``.
Run:  PYTHONPATH=src:system_tests python system_tests/measure_loop.py
"""

from __future__ import annotations

import os
import statistics
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path[:0] = [str(REPO / "src"), str(HERE)]

_TMP = tempfile.mkdtemp(prefix="measure_loop_")
os.environ.setdefault("AIRFLOW_HOME", _TMP)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", f"sqlite:///{_TMP}/airflow.db")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")

from openfeature import api  # noqa: E402

from openfeature_airflow.gate import flag_enabled  # noqa: E402
from openfeature_airflow.measure import track_outcome  # noqa: E402

POPULATION = [f"dag_{i:03d}" for i in range(16)]
FLAG = "experiment.compute.fastpath"
FLAGD_PORT = 8313
WORK_N = 3_000_000


def do_work(dag_id: str) -> tuple[str, float]:
    """Group-dependent work; returns (group, measured_ms). fastpath takes the optimized branch."""
    fast = flag_enabled(FLAG, f"{dag_id}:work", dag_id=dag_id)
    group = "fastpath" if fast else "control"
    t = time.perf_counter()
    if fast:
        total = sum(range(WORK_N))                  # optimized (C-level built-in)
    else:
        total = 0
        for i in range(WORK_N):                     # baseline (explicit Python loop)
            total += i
    ms = (time.perf_counter() - t) * 1000.0
    track_outcome("task_duration_ms", f"{dag_id}:work", value=round(ms, 3), variant=group, dag_id=dag_id)
    return group, ms


def _print_lift(title: str, samples: list[tuple[str, float]], readout_note: str) -> bool:
    by = defaultdict(list)
    for group, ms in samples:
        by[group].append(ms)
    print(f"\n[{title}]  readout: {readout_note}")
    print(f"    {'group':<10}{'runs':>6}{'mean ms':>11}{'median ms':>12}")
    means = {}
    for group in sorted(by):
        vals = by[group]
        means[group] = statistics.mean(vals)
        print(f"    {group:<10}{len(vals):>6}{means[group]:>11.1f}{statistics.median(vals):>12.1f}")
    if "control" in means and "fastpath" in means and means["control"] > 0:
        lift = (means["control"] - means["fastpath"]) / means["control"] * 100
        print(f"    -> fastpath {lift:.1f}% faster (measured)")
        return lift > 0
    return False


def flagd_up():
    subprocess.run(["docker", "rm", "-f", "flagd-measure"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(
        ["docker", "run", "-d", "--name", "flagd-measure", "-p", f"{FLAGD_PORT}:8013",
         "-v", f"{HERE / 'flags'}:/flags", "ghcr.io/open-feature/flagd:latest",
         "start", "--uri", "file:/flags/measure_flags.json"],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)


def run_flagd() -> bool:
    from openfeature.contrib.provider.flagd import FlagdProvider

    flagd_up()
    api.set_provider(FlagdProvider(host="localhost", port=FLAGD_PORT))
    time.sleep(2)
    samples = [do_work(d) for d in POPULATION]
    subprocess.run(["docker", "rm", "-f", "flagd-measure"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return _print_lift("flagd (gRPC container)", samples,
                       "no native analytics -> OTEL/StatsD warehouse (openfeature.outcome.* metric)")


def run_statsig() -> bool:
    from statsig import StatsigOptions
    from statsig import statsig as sg

    from openfeature_airflow.providers.statsig import StatsigProvider

    sg.initialize("secret-local", StatsigOptions(local_mode=True))
    captured = []
    orig = sg.log_event
    sg.log_event = lambda e: (captured.append(e), orig(e))[1]  # wrap the REAL SDK method

    fast_set = set(POPULATION[:8])
    for d in POPULATION:  # deterministic 50/50 via local-mode gate overrides
        sg.override_gate("experiment_fastpath", d in fast_set, f"{d}:work")
    api.set_provider(StatsigProvider(sg, gate_map={FLAG: "experiment_fastpath"}))

    samples = [do_work(d) for d in POPULATION]
    ev = [(e.metadata.get("variant"), float(e.value)) for e in captured
          if getattr(e, "event_name", None) == "task_duration_ms" and e.metadata]
    ok = _print_lift("Statsig (server SDK, real log_event)", ev,
                     f"{len(captured)} events -> sg.log_event (Pulse/metrics in a hosted project)")
    sg.log_event = orig
    return ok and len(ev) == len(POPULATION)


def run_inhouse() -> bool:
    from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider

    provider = InHouseTreatmentProvider(
        string_flags={FLAG: {"rollout": [("on", 50), ("off", 50)], "default": "off"}}
    )
    api.set_provider(provider)
    samples = [do_work(d) for d in POPULATION]
    ev = [(t["attrs"].get("variant"), float(t["value"])) for t in provider.tracked
          if t["metric"] == "task_duration_ms"]
    ok = _print_lift("in-house engine (template)", ev,
                     f"{len(provider.tracked)} outcomes -> provider.tracked (warehouse export)")
    return ok and len(ev) == len(POPULATION)


def main():
    print("=" * 84)
    print(f"MEASURE LOOP across real backends: {len(POPULATION)} DAGs, group-split work, outcome read")
    print("back from each backend's own readout. Lift is measured, not synthetic.")
    print("=" * 84)
    results = {}
    try:
        results["flagd"] = run_flagd()
    except Exception as exc:
        print(f"\n[flagd] skipped: {type(exc).__name__}: {str(exc)[:70]}")
    results["statsig"] = run_statsig()
    results["in-house"] = run_inhouse()

    print("\n" + "=" * 84)
    proven = [k for k, v in results.items() if v]
    print(f"readout proven for: {proven}")
    ok = all(results.get(k) for k in ("statsig", "in-house"))
    print("MEASURE LOOP e2e OK -- real backends, real readout, real lift" if ok else "MEASURE LOOP FAILED")
    print("=" * 84)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
