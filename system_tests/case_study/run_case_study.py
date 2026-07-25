"""Case study runner: canary the v2 revenue rollup across regions, ramped from a real Unleash flag.

At each ramp level we set the flag's rollout % in Unleash, evaluate every region through the
OpenFeature ``UnleashProvider``, run the matching rollup on that region's real shard, and record the
runtime and a revenue-parity guardrail (v2 must equal v1 to the cent). The output is the evaluation:
which regions are on v2, the runtime drop, and that the numbers never changed.

Prereqs: Unleash up (docker-compose.unleash.yml) with the feature created (see the case study doc),
``pip install UnleashClient``, and the orders dataset (auto-generated). Run:

    PYTHONPATH=src:system_tests/case_study python system_tests/case_study/run_case_study.py
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path[:0] = [str(REPO / "src"), str(HERE)]

from openfeature import api  # noqa: E402

from openfeature_airflow.gate import flag_enabled  # noqa: E402
from openfeature_airflow.measure import track_outcome  # noqa: E402
from pipeline import REGIONS, generate_orders_csv, load_region, rollup_v1, rollup_v2  # noqa: E402

UNLEASH = "http://localhost:4242"
ADMIN = "*:*.unleash-insecure-admin-api-token"
CLIENT_TOKEN = "default:development.unleash-insecure-api-token"
FLAG = "revenue_rollup.use_fast_agg"
DATA = "/tmp/case_study/orders.csv"


def _req(method: str, path: str, body: dict | None = None, token: str = ADMIN) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(UNLEASH + path, data=data, method=method,
                                 headers={"Authorization": token, "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        raw = r.read().decode()
        return json.loads(raw) if raw else {}


def strategy_id() -> str:
    feat = _req("GET", f"/api/admin/projects/default/features/{FLAG}")
    for env in feat["environments"]:
        if env["name"] == "development":
            for s in env["strategies"]:
                if s["name"] == "flexibleRollout":
                    return s["id"]
    raise RuntimeError("flexibleRollout strategy not found")


def set_rollout(sid: str, pct: int):
    _req("PUT", f"/api/admin/projects/default/features/{FLAG}/environments/development/strategies/{sid}",
         {"name": "flexibleRollout", "parameters": {"rollout": str(pct), "stickiness": "default", "groupId": "revenue_rollup"}})
    time.sleep(2)  # let the change propagate to the client fetch


def make_client():
    from UnleashClient import UnleashClient

    from openfeature_airflow.providers.unleash import UnleashProvider

    c = UnleashClient(url=f"{UNLEASH}/api", app_name="revenue-etl",
                      custom_headers={"Authorization": CLIENT_TOKEN},
                      refresh_interval=1, disable_metrics=True, disable_registration=True)
    c.initialize_client()
    time.sleep(2)
    api.set_provider(UnleashProvider(c))
    return c


def run_stage(pct: int, sid: str) -> dict:
    set_rollout(sid, pct)
    time.sleep(2)  # the client polls every 1s; let it pick up the new rollout
    rows_by_impl = {"v2 (fast)": [], "v1 (slow)": []}
    on_v2, parity_ok = [], True
    for region in REGIONS:
        shard = load_region(DATA, region)
        use_v2 = flag_enabled(FLAG, region, region=region)
        impl = "v2 (fast)" if use_v2 else "v1 (slow)"
        t = time.perf_counter()
        result = (rollup_v2 if use_v2 else rollup_v1)(shard)
        secs = time.perf_counter() - t
        rows_by_impl[impl].append(secs * 1000)
        if use_v2:
            on_v2.append(region)
            parity_ok &= (rollup_v1(shard) == result)  # guardrail: v2 must match v1 exactly
        track_outcome("rollup_ms", region, value=round(secs * 1000, 2),
                      variant=("v2" if use_v2 else "v1"), region=region)
    means = {k: (statistics.mean(v) if v else 0.0) for k, v in rows_by_impl.items()}
    return {"pct": pct, "on_v2": on_v2, "means": means,
            "n": {k: len(v) for k, v in rows_by_impl.items()}, "parity_ok": parity_ok}


def main():
    if not os.path.exists(DATA):
        print("generating orders dataset ...")
        generate_orders_csv(DATA, rows_per_region=40000, days=30, seed=7)

    sid = strategy_id()
    client = make_client()  # one client for the whole ramp; it polls Unleash every second
    print("=" * 84)
    print("CASE STUDY: canary the v2 revenue rollup across 12 regions, ramped from a real Unleash flag.")
    print("Each region runs its real shard; we measure runtime and check v2 == v1 to the cent.")
    print("=" * 84)

    stages = [run_stage(p, sid) for p in (0, 25, 50, 75, 100)]
    baseline = stages[0]["means"]["v1 (slow)"]
    for s in stages:
        v2n, v1n = s["n"]["v2 (fast)"], s["n"]["v1 (slow)"]
        print(f"\n[rollout {s['pct']:>3}%]  v2 regions: {v2n:>2}   v1 regions: {v1n:>2}   parity(v2==v1): {s['parity_ok']}")
        if v2n:
            print(f"    v2 (fast) mean {s['means']['v2 (fast)']:.1f} ms   vs   v1 baseline {baseline:.1f} ms"
                  f"   -> {(baseline - s['means']['v2 (fast)']) / baseline * 100:.0f}% faster")
        print(f"    on v2: {sorted(s['on_v2'])}")

    final = stages[-1]
    ok = final["n"]["v2 (fast)"] == len(REGIONS) and all(s["parity_ok"] for s in stages)
    Path("/tmp/case_study/results.json").write_text(json.dumps(stages, indent=2))
    client.destroy()
    print("\n" + "=" * 84)
    print(f"ramped 0 -> 100%; every region ended on v2; revenue identical at every step: {ok}")
    print("CASE STUDY e2e OK" if ok else "CASE STUDY FAILED")
    print("=" * 84)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
