#!/usr/bin/env python
"""Render a live e2e demo to an animated GIF for the README.

Runs the real demo against the live backends, captures its actual output, and reveals it line by
line as a terminal GIF (real data; the reveal pacing is synthesized). Needs `agg` on PATH.

    PYTHONPATH=src:system_tests /path/to/python system_tests/make_demo_gif.py

GIF lands in docs/demo-all-backends.gif.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)
_DOCS = os.path.join(_REPO, "docs")

_DROP = re.compile(r"(fork_posix|WARNING|Deprecat|category=|setup plugin|INFO -|UserWarning|warnings\.warn)")
RESET = "\033[0m"


def _color(line: str) -> str:
    line = re.sub(r"\b(True|PASS|OK|success)\b", "\033[1;32m\\1" + RESET, line)
    line = re.sub(r"\b(False|FAIL)\b", "\033[1;31m\\1" + RESET, line)
    line = re.sub(r"(\[[^\]]+\])", "\033[1;36m\\1" + RESET, line)
    line = re.sub(r"(canary_pool|kubernetes|airflow_3x)", "\033[33m\\1" + RESET, line)
    if set(line.strip()) == {"="}:
        line = "\033[2;36m" + line + RESET
    return line


def _run(script: str) -> list[str]:
    env = dict(os.environ, PYTHONPATH=f"{_REPO}/src:{_HERE}", PYTHONUNBUFFERED="1")
    out = subprocess.run([sys.executable, os.path.join(_HERE, script)], capture_output=True, text=True, env=env, cwd=_REPO)
    return [ln for ln in (out.stdout + out.stderr).splitlines() if not _DROP.search(ln)]


def main() -> int:
    lines = _run("run_all_backends.py")
    cast = os.path.join(_DOCS, "demo-all-backends.cast")
    gif = os.path.join(_DOCS, "demo-all-backends.gif")

    header = {"version": 2, "width": 92, "height": len(lines) + 4, "timestamp": int(time.time()),
              "env": {"TERM": "xterm-256color", "SHELL": "/bin/bash"}}
    events = []
    t = 0.0
    events.append([t, "o", "\033[1;37m$ python system_tests/run_all_backends.py\033[0m\r\n"])
    t += 0.8
    for ln in lines:
        events.append([t, "o", _color(ln) + "\r\n"])
        # dwell longer on backend verdicts and the final result, quick on separators
        t += 0.7 if ("matches expected cohort" in ln or "identical placement" in ln or "GATE IDENTICALLY" in ln) else 0.16
    t += 1.5  # hold the final frame

    with open(cast, "w") as f:
        f.write(json.dumps(header) + "\n")
        for e in events:
            f.write(json.dumps(e) + "\n")

    subprocess.run(["agg", "--font-size", "20", "--line-height", "1.3", "--theme", "asciinema",
                    "--speed", "1.0", cast, gif], check=True)
    os.remove(cast)
    print(f"wrote {gif}  ({os.path.getsize(gif)//1024} KB, {len(lines)} lines)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
