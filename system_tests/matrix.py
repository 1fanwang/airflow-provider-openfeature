"""Execution-level matrix: backends x placement use cases, real data flow into real task runs.

For each backend, a cohort config from a live source (flagd gRPC, GrowthBook HTTP, Statsig HTTP, or an
in-process in-house engine) drives BOTH placement use cases at once through the policy:

    UC1  airflow.task.pool  -> canary_pool   (2->3 migration routing)
    UC2  airflow.task.queue -> kubernetes    (Kubernetes worker migration)

A real ``airflow dags test`` runs each DAG; the pool AND queue the TaskInstance actually ran with are
read back from the metadata DB. An in-cohort DAG must get (canary_pool, kubernetes); an out-of-cohort
DAG must stay (default_pool, default). Prints a backend x use-case matrix.

Prereqs: Docker (flagd), provider importable. GrowthBook + Statsig HTTP servers start in-process.
Run:  python system_tests/matrix.py
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
COHORT = {"dag_000", "dag_001"}
IN_DAG, OUT_DAG = "dag_000", "dag_004"
FLAG_POOL, FLAG_QUEUE = "airflow.task.pool", "airflow.task.queue"
CANARY_POOL, DEFAULT_POOL = "canary_pool", "default_pool"
K8S_QUEUE, DEFAULT_QUEUE = "kubernetes", "default"
FLAGD_PORT, GB_PORT, STATSIG_PORT = 8313, 4691, 4791
AIRFLOW_HOME = "/tmp/ofmatrix"


def _serve(port, handler):
    srv = HTTPServer(("127.0.0.1", port), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def growthbook_server():
    feats = {
        FLAG_POOL: {"defaultValue": DEFAULT_POOL, "rules": [{"condition": {"dag_id": {"$in": list(COHORT)}}, "force": CANARY_POOL}]},
        FLAG_QUEUE: {"defaultValue": DEFAULT_QUEUE, "rules": [{"condition": {"dag_id": {"$in": list(COHORT)}}, "force": K8S_QUEUE}]},
    }
    body = json.dumps({"status": 200, "features": feats}).encode()

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
        def log_message(self, *a): pass
    _serve(GB_PORT, H)


def statsig_server():
    def gate(name):
        return {"name": name, "type": "feature_gate", "salt": "s", "enabled": True, "defaultValue": False,
                "rules": [{"name": "c", "id": "c", "salt": "r", "passPercentage": 100, "returnValue": True,
                           "conditions": [{"type": "unit_id", "idType": "userID", "operator": "any",
                                           "targetValue": [f"{d}:only" for d in COHORT],
                                           "field": None, "additionalValues": {}, "isDeviceBased": False}]}]}
    spec = {"has_updates": True, "time": int(time.time() * 1000),
            "feature_gates": [gate("airflow_task_pool"), gate("airflow_task_queue")],
            "dynamic_configs": [], "layer_configs": [], "layers": {}, "id_lists": {},
            "sdk_keys_to_app_ids": {}, "hashed_sdk_keys_to_app_ids": {}}
    body = json.dumps(spec).encode()

    class H(BaseHTTPRequestHandler):
        def _w(self, b):
            self.send_response(200); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
        def do_GET(self):
            self._w(body if "download_config_specs" in self.path else b'{"has_updates":false}')
        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0) or 0)); self._w(b"{}")
        def log_message(self, *a): pass
    _serve(STATSIG_PORT, H)


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
    api.set_provider(GrowthBookProvider(api_host="http://127.0.0.1:{GB_PORT}", client_key="sdk"))
elif b == "statsig":
    from statsig import statsig as sg, StatsigOptions
    from openfeature_airflow.providers.statsig import StatsigProvider
    url = "http://127.0.0.1:{STATSIG_PORT}/v1/"
    sg.initialize("secret-local", StatsigOptions(api=url, api_for_download_config_specs=url, local_mode=False, init_timeout=5)); time.sleep(1)
    api.set_provider(StatsigProvider(sg,
        gate_map={{"{FLAG_POOL}": "airflow_task_pool", "{FLAG_QUEUE}": "airflow_task_queue"}},
        enabled_values={{"{FLAG_POOL}": "{CANARY_POOL}", "{FLAG_QUEUE}": "{K8S_QUEUE}"}}))
elif b == "inhouse":
    from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider
    cohort = {sorted(COHORT)!r}
    api.set_provider(InHouseTreatmentProvider(string_flags={{
        "{FLAG_POOL}": {{"segments": [{{"attribute": "dag_id", "in": cohort, "variant": "{CANARY_POOL}"}}], "default": "{DEFAULT_POOL}"}},
        "{FLAG_QUEUE}": {{"segments": [{{"attribute": "dag_id", "in": cohort, "variant": "{K8S_QUEUE}"}}], "default": "{DEFAULT_QUEUE}"}}}}))
elif b == "unleash":
    from UnleashClient import UnleashClient
    from openfeature_airflow.providers.unleash import UnleashProvider
    c = UnleashClient(url="http://localhost:4242/api", app_name="matrix",
                      custom_headers={{"Authorization": "default:development.unleash-insecure-api-token"}},
                      refresh_interval=1, disable_metrics=True, disable_registration=True)
    c.initialize_client(); time.sleep(3)
    api.set_provider(UnleashProvider(c, enabled_values={{"{FLAG_POOL}": "{CANARY_POOL}", "{FLAG_QUEUE}": "{K8S_QUEUE}"}}))

def task_policy(task):
    apply_placement(task)
'''


def setup():
    import shutil
    home = Path(AIRFLOW_HOME)
    if home.exists():
        shutil.rmtree(home)
    (home / "dags").mkdir(parents=True)
    (home / "config").mkdir()
    (home / "config" / "airflow_local_settings.py").write_text(LOCAL_SETTINGS)
    for d in POPULATION:
        (home / "dags" / f"{d}.py").write_text(
            "from airflow import DAG\n"
            "from airflow.providers.standard.operators.empty import EmptyOperator\n"
            "import datetime\n"
            f"with DAG(dag_id='{d}', schedule=None, start_date=datetime.datetime(2024,1,1), catchup=False) as dag:\n"
            f"    EmptyOperator(task_id='only', pool='{DEFAULT_POOL}', queue='{DEFAULT_QUEUE}')\n")


def env(backend):
    e = dict(os.environ)
    e.update(AIRFLOW_HOME=AIRFLOW_HOME, PYTHONPATH=str(REPO / "src"), OF_BACKEND=backend,
             AIRFLOW__CORE__LOAD_EXAMPLES="False", AIRFLOW__CORE__DAGS_FOLDER=f"{AIRFLOW_HOME}/dags",
             AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{AIRFLOW_HOME}/airflow.db",
             AIRFLOW__LOGGING__LOGGING_LEVEL="ERROR")
    return e


def run(dag_id, backend):
    subprocess.run(["airflow", "dags", "test", dag_id], env=env(backend), check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def placement(dag_id):
    from airflow.models.taskinstance import TaskInstance
    from airflow.settings import Session
    s = Session()
    ti = s.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).order_by(TaskInstance.start_date.desc()).first()
    s.close()
    return (ti.pool, ti.queue, ti.state) if ti else (None, None, None)


def flagd_up():
    subprocess.run(["docker", "rm", "-f", "flagd-matrix"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["docker", "run", "-d", "--name", "flagd-matrix", "-p", f"{FLAGD_PORT}:8013",
                    "-v", f"{HERE / 'flags'}:/flags", "ghcr.io/open-feature/flagd:latest",
                    "start", "--uri", "file:/flags/matrix_flags.json"], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)


def main():
    os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME
    setup()
    subprocess.run(["airflow", "db", "migrate"], env=env("flagd"), check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    growthbook_server(); statsig_server()
    try:
        flagd_up()
    except subprocess.CalledProcessError:
        print("flagd failed to start (Docker?)"); sys.exit(1)

    backends = [("flagd (gRPC)", "flagd", True), ("GrowthBook (HTTP)", "growthbook", True),
                ("Statsig (HTTP)", "statsig", True), ("Unleash (container)", "unleash", True),
                ("in-house (in-proc)", "inhouse", False)]

    print("=" * 82)
    print("EXECUTION-LEVEL MATRIX, real data -> real `airflow dags test` -> pool+queue from the DB")
    print(f"cohort {sorted(COHORT)}: UC1 pool->{CANARY_POOL}, UC2 queue->{K8S_QUEUE};  others -> defaults")
    print("=" * 82)
    print(f"{'backend':<22}{'net':<6}{'UC1 pool (in/out)':<26}{'UC2 queue (in/out)':<26}ok")
    print("-" * 82)

    all_ok = True
    for label, key, real_net in backends:
        run(IN_DAG, key); pin, qin, sin = placement(IN_DAG)
        run(OUT_DAG, key); pout, qout, sout = placement(OUT_DAG)
        uc1 = pin == CANARY_POOL and pout == DEFAULT_POOL
        uc2 = qin == K8S_QUEUE and qout == DEFAULT_QUEUE
        ok = uc1 and uc2 and sin == "success" and sout == "success"
        all_ok &= ok
        print(f"{label:<22}{('yes' if real_net else 'no'):<6}{pin+'/'+pout:<26}{qin+'/'+qout:<26}{'PASS' if ok else 'FAIL'}")

    subprocess.run(["docker", "rm", "-f", "flagd-matrix"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("-" * 82)
    print("all backends: real eval -> real task pool+queue (flagd/GrowthBook/Statsig/Unleash over the network)"
          if all_ok else "FAILURE: a cell did not place as expected")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
