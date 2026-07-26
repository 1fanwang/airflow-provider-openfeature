"""Real eval data flowing from a live backend into REAL Airflow task execution.

For each backend, the subset config is fetched **over the network** (flagd gRPC, GrowthBook HTTP
features, Statsig HTTP config-spec) and drives which pool a real ``airflow dags test`` TaskInstance
actually runs in -- the pool is read back from the metadata DB after the run, not asserted at parse.

A DAG in the canary subset (dag_000) must run in ``canary_pool``; one outside it (dag_004) in
``default_pool`` -- for every backend, from live network data, with no code change between backends.

Prereqs: Docker (for flagd), and the provider importable (``pip install -e ..`` or the wheel).
GrowthBook + Statsig HTTP servers are started in-process. Run:  python real_data_flow.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
POPULATION = [f"dag_{i:03d}" for i in range(5)]
CANARY = {"dag_000", "dag_001"}
FLAG = "airflow.task.pool"
CANARY_POOL, DEFAULT_POOL = "canary_pool", "default_pool"
FLAGD_PORT = 8213
GB_PORT, STATSIG_PORT = 4690, 4790
AIRFLOW_HOME = "/tmp/rdf"


# ----- live network sources for GrowthBook + Statsig (flagd is a container) -----

def _serve(port, handler_cls):
    srv = HTTPServer(("127.0.0.1", port), handler_cls)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def growthbook_server():
    features = {FLAG: {"defaultValue": DEFAULT_POOL,
                       "rules": [{"condition": {"dag_id": {"$in": list(CANARY)}}, "force": CANARY_POOL}]}}
    payload = json.dumps({"status": 200, "features": features}).encode()

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.send_header("Content-Length", str(len(payload))); self.end_headers()
            self.wfile.write(payload)
        def log_message(self, *a): pass
    return _serve(GB_PORT, H)


def statsig_server():
    # Statsig server SDK GETs /v1/download_config_specs/<key>.json; unit_id/any matches the raw user_id
    # (entity = "<dag_id>:<task_id>"), so target the canary tasks by "<dag>:only".
    spec = {
        "has_updates": True, "time": int(time.time() * 1000),
        "feature_gates": [{
            "name": "airflow_task_pool", "type": "feature_gate", "salt": "s", "enabled": True,
            "defaultValue": False,
            "rules": [{"name": "canary", "id": "canary", "salt": "r", "passPercentage": 100,
                       "returnValue": True,
                       "conditions": [{"type": "unit_id", "idType": "userID", "operator": "any",
                                       "targetValue": [f"{d}:only" for d in CANARY],
                                       "field": None, "additionalValues": {}, "isDeviceBased": False}]}],
        }],
        "dynamic_configs": [], "layer_configs": [], "layers": {}, "id_lists": {},
        "sdk_keys_to_app_ids": {}, "hashed_sdk_keys_to_app_ids": {},
    }
    body = json.dumps(spec).encode()

    class H(BaseHTTPRequestHandler):
        def _w(self, b):
            self.send_response(200); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
        def do_GET(self):
            self._w(body if "download_config_specs" in self.path else b'{"has_updates":false}')
        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0) or 0)); self._w(b"{}")
        def log_message(self, *a): pass
    return _serve(STATSIG_PORT, H)


# ----- the per-backend airflow_local_settings the tested subprocess loads -----

LOCAL_SETTINGS = f'''
import os, time
from openfeature import api
from openfeature_airflow.policy import apply_placement

b = os.environ["OF_BACKEND"]
if b == "flagd":
    from openfeature.contrib.provider.flagd import FlagdProvider
    api.set_provider(FlagdProvider(host="localhost", port={FLAGD_PORT})); time.sleep(2)
elif b == "growthbook":
    from openfeature_airflow.providers.growthbook import GrowthBookProvider
    api.set_provider(GrowthBookProvider(api_host="http://127.0.0.1:{GB_PORT}", client_key="sdk-local"))
elif b == "statsig":
    from statsig import statsig as sg, StatsigOptions
    from openfeature_airflow.providers.statsig import StatsigProvider
    url = "http://127.0.0.1:{STATSIG_PORT}/v1/"
    sg.initialize("secret-local", StatsigOptions(api=url, api_for_download_config_specs=url, local_mode=False, init_timeout=5))
    time.sleep(1)
    api.set_provider(StatsigProvider(sg, gate_map={{"{FLAG}": "airflow_task_pool"}}, enabled_values={{"{FLAG}": "{CANARY_POOL}"}}))

def task_policy(task):
    apply_placement(task)
'''


def setup_airflow_home():
    home = Path(AIRFLOW_HOME)
    if home.exists():
        import shutil; shutil.rmtree(home)
    (home / "dags").mkdir(parents=True)
    (home / "config").mkdir()
    (home / "config" / "airflow_local_settings.py").write_text(LOCAL_SETTINGS)
    for d in POPULATION:
        (home / "dags" / f"{d}.py").write_text(
            "from airflow import DAG\n"
            "from airflow.providers.standard.operators.empty import EmptyOperator\n"
            "import datetime\n"
            f"with DAG(dag_id='{d}', schedule=None, start_date=datetime.datetime(2024,1,1), catchup=False) as dag:\n"
            f"    EmptyOperator(task_id='only', pool='{DEFAULT_POOL}')\n")


def base_env():
    e = dict(os.environ)
    e.update(
        AIRFLOW_HOME=AIRFLOW_HOME,
        PYTHONPATH=f"{REPO/'src'}",
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__DAGS_FOLDER=f"{AIRFLOW_HOME}/dags",
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{AIRFLOW_HOME}/airflow.db",
        AIRFLOW__LOGGING__LOGGING_LEVEL="ERROR",
    )
    return e


def run_task(dag_id, backend):
    env = base_env(); env["OF_BACKEND"] = backend
    subprocess.run(["airflow", "dags", "test", dag_id], env=env, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def pool_of(dag_id):
    from airflow.models.taskinstance import TaskInstance
    from airflow.settings import Session
    s = Session()
    ti = (s.query(TaskInstance).filter(TaskInstance.dag_id == dag_id)
          .order_by(TaskInstance.start_date.desc()).first())
    s.close()
    return (ti.pool, ti.state) if ti else (None, None)


def flagd_up():
    subprocess.run(["docker", "rm", "-f", "flagd-rdf"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(
        ["docker", "run", "-d", "--name", "flagd-rdf", "-p", f"{FLAGD_PORT}:8013",
         "-v", f"{HERE/'flags'}:/flags", "ghcr.io/open-feature/flagd:latest",
         "start", "--uri", "file:/flags/real_data_flow_flags.json"],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)


def main():
    os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME
    setup_airflow_home()
    env = base_env(); env["OF_BACKEND"] = "flagd"
    subprocess.run(["airflow", "db", "migrate"], env=env, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    growthbook_server(); statsig_server()
    try:
        flagd_up()
    except subprocess.CalledProcessError:
        print("flagd container failed to start -- is Docker running?"); sys.exit(1)

    backends = {
        "flagd (gRPC container)": "flagd",
        "GrowthBook (HTTP features fetch)": "growthbook",
        "Statsig (HTTP config-spec fetch)": "statsig",
    }
    canary_dag, default_dag = "dag_000", "dag_004"
    print("=" * 84)
    print("REAL eval data -> REAL task execution. Pool read from the metadata DB after `airflow dags test`.")
    print(f"canary subset = {sorted(CANARY)}  ->  {CANARY_POOL};   everyone else -> {DEFAULT_POOL}")
    print("=" * 84)
    print(f"{'backend (network source)':<36} {canary_dag+' (in subset)':<22} {default_dag+' (out)':<20} ok")
    print("-" * 84)

    all_ok = True
    for label, key in backends.items():
        run_task(canary_dag, key)
        cp, cs = pool_of(canary_dag)
        run_task(default_dag, key)
        dp, ds = pool_of(default_dag)
        ok = (cp == CANARY_POOL and cs == "success" and dp == DEFAULT_POOL and ds == "success")
        all_ok &= ok
        print(f"{label:<36} {cp+' ('+str(cs)+')':<22} {dp+' ('+str(ds)+')':<20} {'PASS' if ok else 'FAIL'}")

    subprocess.run(["docker", "rm", "-f", "flagd-rdf"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("-" * 84)
    print("ALL BACKENDS: real network data drove a real task's execution pool"
          if all_ok else "FAILURE: a backend did not drive execution as expected")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
