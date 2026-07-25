"""Screenshot the GrowthBook UI for the case study (a second real ecosystem backend).

Logs into the local GrowthBook and captures the feature-flag page for revenue_rollup.use_fast_agg,
including its percentage-rollout rule. Run after the feature exists:

    /tmp/boot32/bin/python system_tests/case_study/screenshot_growthbook.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parents[2] / "docs" / "case-study" / "img"
BASE = "http://localhost:3000"
FLAG = "revenue_rollup.use_fast_agg"
USER, PW = "casestudy@example.com", "CaseStudyDemo123!"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.goto(BASE, wait_until="domcontentloaded")
        try:
            page.wait_for_selector('input[type="password"]', timeout=8000)
            page.locator('input[type="email"], input[name="email"], input[type="text"]').first.fill(USER)
            page.locator('input[type="password"]').first.fill(PW)
            page.get_by_role("button", name="Log in").click()
            page.wait_for_timeout(3000)
            page.wait_for_timeout(2500)
        except Exception as exc:
            print(f"login step: {type(exc).__name__}: {exc}")

        page.goto(f"{BASE}/features/{FLAG}", wait_until="domcontentloaded")
        page.wait_for_timeout(3500)
        page.screenshot(path=str(OUT / "growthbook-flag.png"), full_page=True)
        print(f"wrote {OUT / 'growthbook-flag.png'}")

        page.goto(f"{BASE}/features", wait_until="domcontentloaded")
        page.wait_for_timeout(2500)
        page.screenshot(path=str(OUT / "growthbook-features.png"), full_page=True)
        print(f"wrote {OUT / 'growthbook-features.png'}")
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
