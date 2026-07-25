"""Create the case-study flag in Unleash: revenue_rollup.use_fast_agg with a gradual-rollout strategy.

Idempotent. Run after ``docker compose -f system_tests/docker-compose.unleash.yml up -d``:

    python system_tests/case_study/setup_unleash_flag.py [rollout_pct]
"""

from __future__ import annotations

import json
import sys
import urllib.request

BASE = "http://localhost:4242"
ADMIN = "*:*.unleash-insecure-admin-api-token"
FLAG = "revenue_rollup.use_fast_agg"


def req(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method,
                               headers={"Authorization": ADMIN, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        if e.code in (409, 400):  # already exists
            return {}
        raise


def main():
    pct = sys.argv[1] if len(sys.argv) > 1 else "50"
    req("POST", "/api/admin/projects/default/features",
        {"name": FLAG, "type": "experiment",
         "description": "Canary the v2 (fast) revenue rollup across regions"})
    params = {"rollout": str(pct), "stickiness": "default", "groupId": "revenue_rollup"}
    # add or update the flexibleRollout strategy
    feat = req("GET", f"/api/admin/projects/default/features/{FLAG}")
    sid = None
    for env in feat.get("environments", []):
        if env["name"] == "development":
            for s in env["strategies"]:
                if s["name"] == "flexibleRollout":
                    sid = s["id"]
    if sid:
        req("PUT", f"/api/admin/projects/default/features/{FLAG}/environments/development/strategies/{sid}",
            {"name": "flexibleRollout", "parameters": params})
    else:
        req("POST", f"/api/admin/projects/default/features/{FLAG}/environments/development/strategies",
            {"name": "flexibleRollout", "parameters": params})
    req("POST", f"/api/admin/projects/default/features/{FLAG}/environments/development/on")
    print(f"{FLAG}: gradual rollout at {pct}% in development")


if __name__ == "__main__":
    main()
