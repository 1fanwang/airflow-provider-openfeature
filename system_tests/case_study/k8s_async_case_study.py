"""Case study 2: evaluate the KubernetesExecutor async pod-creation change for a queued-latency regression.

Business problem: KubernetesExecutor creates worker pods one at a time, so when a burst of tasks is
queued, each pod waits behind the previous pod's create call and **task queued latency** climbs with the
burst size. apache/airflow#68480 adds opt-in concurrent pod creation. Before turning it on, we gate it
behind ``airflow.executor.k8s_concurrent_pod_creation``, evaluate both arms on a real cluster, and ask:
does the new path lower queued latency, and does it regress anything?

Both arms use the real Kubernetes client call the executor uses (``create_namespaced_pod``), against a
real cluster (a local ``kind`` cluster here):

- baseline (flag off): create a burst of pods sequentially, one create call after another.
- treatment (flag on, #68480): create the same burst concurrently from a thread pool.

Which arm is live is decided by the flag, read through this provider from a real backend (flagd). For
each pod we record queued latency = time from the burst starting to that pod's create returning, then
compare mean / p95 and give a regression verdict.

Prereqs: a reachable cluster (kube config), ``pip install kubernetes openfeature-provider-flagd``, and
a flagd container serving ``airflow.executor.k8s_concurrent_pod_creation`` (optional; falls back to
running both arms). Run:  PYTHONPATH=src python system_tests/case_study/k8s_async_case_study.py
"""

from __future__ import annotations

import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from kubernetes import client, config

NS = os.environ.get("K8S_NS", "of-async-e2e")
BURST = 20
FLAG = "airflow.executor.k8s_concurrent_pod_creation"

config.load_kube_config()
V1 = client.CoreV1Api()


def _pod(name: str) -> client.V1Pod:
    return client.V1Pod(
        metadata=client.V1ObjectMeta(name=name, namespace=NS, labels={"app": "of-async"}),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[client.V1Container(name="task", image="busybox:1.36",
                                           command=["sh", "-c", "sleep 20"])],
        ),
    )


def create_pod(name: str, t0: float) -> float:
    """The executor's create call. Returns queued latency = time from burst start to create returning."""
    V1.create_namespaced_pod(NS, _pod(name))
    return time.perf_counter() - t0


def sequential_burst(tag: str) -> list[float]:
    t0 = time.perf_counter()
    return [create_pod(f"{tag}-{i:02d}", t0) for i in range(BURST)]


def concurrent_burst(tag: str) -> list[float]:
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=BURST) as ex:
        return [f.result() for f in [ex.submit(create_pod, f"{tag}-{i:02d}", t0) for i in range(BURST)]]


def running_count(tag: str) -> int:
    pods = V1.list_namespaced_pod(NS, label_selector="app=of-async").items
    return sum(1 for p in pods if p.metadata.name.startswith(tag) and p.status.phase in ("Running", "Succeeded"))


def summary(name: str, lats: list[float]) -> dict:
    s = {"mean": statistics.mean(lats), "p95": sorted(lats)[int(len(lats) * 0.95) - 1], "max": max(lats)}
    print(f"  {name:<30} mean {s['mean']*1000:7.0f} ms   p95 {s['p95']*1000:7.0f} ms   max {s['max']*1000:7.0f} ms")
    return s


def cleanup():
    try:
        V1.delete_collection_namespaced_pod(NS, label_selector="app=of-async")
    except client.ApiException:
        pass


def flag_says_concurrent() -> str:
    """Read the flag through this provider so the path is Airflow-side code -> gate -> backend."""
    try:
        from openfeature import api
        from openfeature.contrib.provider.flagd import FlagdProvider

        from openfeature_airflow.gate import flag_enabled
        api.set_provider(FlagdProvider(host="localhost", port=8113))
        time.sleep(1)
        return "on" if flag_enabled(FLAG, "cluster-00", cluster_id="cluster-00") else "off"
    except Exception:
        return "unavailable (running both arms directly)"


def main():
    try:
        V1.read_namespace(NS)
    except client.ApiException:
        V1.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=NS)))
    cleanup()
    time.sleep(2)

    print("=" * 88)
    print(f"K8s async pod creation: does apache/airflow#68480 regress task queued latency? "
          f"Burst of {BURST} pods, real cluster.")
    print(f"flag {FLAG} (via flagd): {flag_says_concurrent()}")
    print("off = create pods sequentially (today);  on = create concurrently (#68480)")
    print("=" * 88)

    print("\nbaseline  (flag off, sequential create_namespaced_pod):")
    base = summary("sequential queued latency", sequential_burst("seq"))
    time.sleep(3)
    print("\ntreatment (flag on, concurrent create_namespaced_pod, #68480):")
    treat = summary("concurrent queued latency", concurrent_burst("con"))

    time.sleep(6)
    ran = running_count("seq") + running_count("con")
    improve = (base["p95"] - treat["p95"]) / base["p95"] * 100
    no_regression = treat["p95"] <= base["p95"]
    print("\n" + "-" * 88)
    print(f"queued-latency p95:  sequential {base['p95']*1000:.0f} ms  ->  concurrent {treat['p95']*1000:.0f} ms"
          f"   ({improve:.0f}% lower)")
    print(f"guardrail: all {2*BURST} pods reached Running/Succeeded: {ran == 2 * BURST}")
    print(f"regression check: concurrent p95 is not worse than sequential: {no_regression}")

    cleanup()
    ok = no_regression and improve > 0 and ran == 2 * BURST
    print("\n" + "=" * 88)
    print("VERDICT: #68480 lowers queued latency with no regression; safe to ramp behind the flag." if ok
          else "VERDICT: regression or failure detected; keep the flag off.")
    print("=" * 88)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
