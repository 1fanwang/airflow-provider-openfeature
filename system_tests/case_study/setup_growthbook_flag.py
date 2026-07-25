"""Create the case-study flag in GrowthBook: revenue_rollup.use_fast_agg with a 50%-by-region rollout.

Registers the first user + org if needed, then creates the boolean feature with a percentage-rollout
rule sampled by region. Idempotent. Run after the GrowthBook stack is up:

    docker compose -f system_tests/case_study/docker-compose.growthbook.yml up -d
    python system_tests/case_study/setup_growthbook_flag.py [rollout_pct]
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

API = "http://localhost:3100"
EMAIL, PW, NAME = "casestudy@example.com", "CaseStudyDemo123!", "Case Study"
FLAG = "revenue_rollup.use_fast_agg"


def call(method, path, body=None, token=None, org=None):
    h = {"Content-Type": "application/json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    if org:
        h["X-Organization"] = org
    req = urllib.request.Request(API + path, data=(json.dumps(body).encode() if body is not None else None),
                                 method=method, headers=h)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def main():
    pct = float(sys.argv[1]) / 100 if len(sys.argv) > 1 else 0.5

    call("POST", "/auth/register", {"email": EMAIL, "name": NAME, "password": PW})  # no-op if exists
    _, login = call("POST", "/auth/login", {"email": EMAIL, "password": PW})
    token = login.get("token")
    _, user = call("GET", "/user", token=token)
    orgs = user.get("organizations", [])
    if not orgs:
        call("POST", "/organization", {"company": "Case Study Co"}, token=token)
        _, login = call("POST", "/auth/login", {"email": EMAIL, "password": PW})
        token = login.get("token")
        _, user = call("GET", "/user", token=token)
        orgs = user.get("organizations", [])
    org = orgs[0]["id"]

    feat = {
        "id": FLAG, "description": "Canary the v2 (fast) revenue rollup across regions",
        "valueType": "boolean", "defaultValue": "false", "tags": ["case-study"], "project": "",
        "environmentSettings": {
            "production": {"enabled": True, "rules": [
                {"type": "rollout", "description": "gradual rollout", "value": "true",
                 "coverage": pct, "hashAttribute": "region", "enabled": True}]}},
    }
    status, resp = call("POST", "/feature", feat, token=token, org=org)
    if status not in (200, 201):
        print(f"note: {resp.get('message', resp)}")  # likely already exists
    print(f"{FLAG}: {int(pct*100)}% rollout by region in GrowthBook org {org}")


if __name__ == "__main__":
    main()
