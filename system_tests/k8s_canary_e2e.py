"""Flag-driven KubernetesExecutor canary, proven on a real cluster (kind).

A flag (`airflow.task.executor`, resolved live from a real flagd container) decides which DAGs run on
the kubernetes executor. For the routed cohort we launch a **real pod on a real Kubernetes cluster** --
the same action KubernetesExecutor performs per task -- and read the pod state back with kubectl. Then
we ramp the flag (25% -> 50%) and watch the cohort, and the pod count, grow with no code change.

This is the reliable core of the KubernetesExecutor-canary use case (cf. apache/airflow#68480) without
standing up a full executor: the provider's job is to route the cohort by flag; here that routing puts
real work on real k8s.

Prereqs: Docker + a reachable cluster (a local `kind` cluster is ideal). Set the namespace with
``K8S_NS`` (default ``of-e2e``). Run:  PYTHONPATH=src:system_tests python system_tests/k8s_canary_e2e.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path[:0] = [str(REPO / "src"), str(HERE)]

POPULATION = [f"etl_dag_{i:02d}" for i in range(12)]
FLAG = "airflow.task.executor"
FLAGD_PORT = 8413
NS = os.environ.get("K8S_NS", "of-e2e")
FLAGS_FILE = HERE / "flags" / "k8s_flags.json"


def set_ramp(pct: int):
    """Rewrite the flagd flag file to route ``pct`` % of DAGs to kubernetes; flagd hot-reloads it."""
    cfg = {
        "flags": {
            FLAG: {
                "state": "ENABLED",
                "variants": {"kubernetes": "kubernetes", "local": "local"},
                "defaultVariant": "local",
                "targeting": {"fractional": [{"var": "dag_id"}, ["kubernetes", pct], ["local", 100 - pct]]},
            }
        }
    }
    FLAGS_FILE.write_text(json.dumps(cfg, indent=2))
    time.sleep(3)  # let flagd pick up the change


def flagd_up():
    subprocess.run(["docker", "rm", "-f", "flagd-k8s"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(
        ["docker", "run", "-d", "--name", "flagd-k8s", "-p", f"{FLAGD_PORT}:8013",
         "-v", f"{HERE / 'flags'}:/flags", "ghcr.io/open-feature/flagd:latest",
         "start", "--uri", "file:/flags/k8s_flags.json"],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)


def routed_cohort() -> list[str]:
    from openfeature_airflow.gate import variant

    return [d for d in POPULATION if variant(FLAG, d, "local", dag_id=d) == "kubernetes"]


def launch_pod(dag_id: str):
    """Launch the task as a real pod, the way KubernetesExecutor would. Idempotent."""
    manifest = {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": f"of-canary-{dag_id.replace('_', '-')}", "namespace": NS,
                     "labels": {"app": "openfeature-canary", "dag_id": dag_id}},
        "spec": {"restartPolicy": "Never",
                 "containers": [{"name": "task", "image": "busybox:1.36",
                                 "command": ["sh", "-c", f"echo running {dag_id} on kubernetes; sleep 2"]}]},
    }
    subprocess.run(["kubectl", "apply", "-f", "-"], input=json.dumps(manifest).encode(),
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def pod_states() -> dict:
    out = subprocess.run(
        ["kubectl", "get", "pods", "-n", NS, "-l", "app=openfeature-canary",
         "-o", "jsonpath={range .items[*]}{.metadata.labels.dag_id}={.status.phase} {end}"],
        capture_output=True, text=True)
    states = {}
    for tok in out.stdout.split():
        if "=" in tok:
            k, v = tok.split("=", 1)
            states[k] = v
    return states


def show(ramp_label: str):
    cohort = routed_cohort()
    print(f"\n[ramp {ramp_label}] flag routes {len(cohort)}/{len(POPULATION)} DAGs to kubernetes: {sorted(cohort)}")
    for d in cohort:
        launch_pod(d)
    # wait for the launched pods to reach a terminal/running phase
    for _ in range(30):
        st = {k: v for k, v in pod_states().items() if k in cohort}
        if st and all(v in ("Running", "Succeeded") for v in st.values()) and len(st) == len(cohort):
            break
        time.sleep(2)
    st = pod_states()
    running = {k: v for k, v in st.items() if v in ("Running", "Succeeded")}
    print(f"           real pods on the cluster: {len(running)}  states={dict(sorted(st.items()))}")
    return cohort, running


def main():
    # sanity: cluster reachable?
    if subprocess.run(["kubectl", "get", "ns", NS], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
        subprocess.run(["kubectl", "create", "namespace", NS], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    from openfeature import api
    from openfeature.contrib.provider.flagd import FlagdProvider

    print("=" * 84)
    print("KubernetesExecutor canary on a REAL cluster: a flag routes the cohort, routed tasks become")
    print("real pods. Ramp the flag, watch the cohort and the pods grow. No code change.")
    print("=" * 84)

    set_ramp(25)
    flagd_up()
    api.set_provider(FlagdProvider(host="localhost", port=FLAGD_PORT))
    time.sleep(2)

    c25, p25 = show("25%")
    set_ramp(50)
    c50, p50 = show("50%  (ramped live via flagd hot-reload)")

    ok = set(c25).issubset(set(c50)) and len(c50) > len(c25) and len(p50) == len(c50)
    print("\n" + "=" * 84)
    print(f"cohort grew {len(c25)} -> {len(c50)} and every routed task ran as a real pod: {ok}")
    print("KUBERNETES CANARY e2e OK" if ok else "KUBERNETES CANARY e2e FAILED")
    print("=" * 84)

    # teardown
    subprocess.run(["kubectl", "delete", "pods", "-n", NS, "-l", "app=openfeature-canary", "--wait=false"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["docker", "rm", "-f", "flagd-k8s"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    set_ramp(25)  # restore the committed flag file
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
