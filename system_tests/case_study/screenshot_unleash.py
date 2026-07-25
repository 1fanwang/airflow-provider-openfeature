"""Screenshot the Unleash UI for the case study (real ecosystem backend, real gradual-rollout view).

Logs into the local Unleash with the default admin, then captures the feature-flag detail page (the
flag + its gradual-rollout strategy) and the project's flag list. Run after the flag exists:

    /tmp/boot32/bin/python system_tests/case_study/screenshot_unleash.py
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parents[2] / "docs" / "case-study" / "img"
BASE = "http://localhost:4242"
FLAG = "revenue_rollup.use_fast_agg"
ADMIN = "*:*.unleash-insecure-admin-api-token"


def strategy_id() -> str | None:
    req = urllib.request.Request(f"{BASE}/api/admin/projects/default/features/{FLAG}",
                                 headers={"Authorization": ADMIN})
    with urllib.request.urlopen(req) as r:
        feat = json.loads(r.read().decode())
    for env in feat["environments"]:
        if env["name"] == "development":
            for s in env["strategies"]:
                if s["name"] == "flexibleRollout":
                    return s["id"]
    return None
USER, PW = "casestudy@example.com", "CaseStudyDemo123!"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.goto(BASE, wait_until="networkidle")

        # log in: wait for the form to render, then fill + submit
        try:
            page.wait_for_selector('input[type="password"]', timeout=8000)
            page.locator('input[type="text"], input[type="email"]').first.fill(USER)
            page.locator('input[type="password"]').first.fill(PW)
            page.get_by_role("button", name="Sign in").click()
            page.wait_for_url(lambda u: "login" not in u, timeout=8000)
            page.wait_for_load_state("networkidle")
        except Exception as exc:
            print(f"login step: {type(exc).__name__}: {exc}")

        # flag detail page (overview)
        page.goto(f"{BASE}/projects/default/features/{FLAG}", wait_until="networkidle")
        page.wait_for_timeout(2500)
        # expand the development environment card so the gradual-rollout strategy shows
        try:
            page.get_by_text("development", exact=True).first.click()
            page.wait_for_timeout(1500)
        except Exception as exc:
            print(f"expand step: {type(exc).__name__}")
        page.screenshot(path=str(OUT / "unleash-flag.png"), full_page=True)
        print(f"wrote {OUT / 'unleash-flag.png'}")

        # the gradual-rollout strategy edit view (shows the % dial)
        sid = strategy_id()
        if sid:
            page.goto(f"{BASE}/projects/default/features/{FLAG}/strategies/edit"
                      f"?environmentId=development&strategyId={sid}", wait_until="networkidle")
            page.wait_for_timeout(2500)
            page.screenshot(path=str(OUT / "unleash-rollout.png"), full_page=True)
            print(f"wrote {OUT / 'unleash-rollout.png'}")

        # project flag list
        page.goto(f"{BASE}/projects/default", wait_until="networkidle")
        page.wait_for_timeout(2000)
        page.screenshot(path=str(OUT / "unleash-project.png"), full_page=True)
        print(f"wrote {OUT / 'unleash-project.png'}")

        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
