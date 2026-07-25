#!/usr/bin/env python
"""Render the live e2e demos to terminal SVGs for the README.

Runs the real demo scripts as subprocesses against the live backends and renders their
*actual* output to styled SVGs (no hand-written text). Regenerate after changing a demo:

    # backends up first (see system_tests/E2E.md):
    #   flagd :8013 -> flags/flags.json ; flagd :8113 -> flags/use_case_flags.json
    #   docker compose -f system_tests/docker-compose.unleash.yml up -d && bash system_tests/setup_unleash.sh
    PYTHONPATH=src:system_tests /path/to/python system_tests/make_demo_svgs.py

Needs `rich`. SVGs land in docs/.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

from rich.console import Console
from rich.text import Text

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)
_DOCS = os.path.join(_REPO, "docs")

_DROP = re.compile(r"(fork_posix|ev_poll_posix|FD from fork parent|WARNING|Deprecat|category=|setup plugin|INFO -|UserWarning|warnings\.warn|already have \d+ instance|FutureWarning|parser\.py|log_filename_template|instanceId:|please double check|instantiated\.)")
_GOOD = re.compile(r"\b(True|PASS|OK|success)\b")
_BAD = re.compile(r"\b(False|FAIL|error)\b")


def _run(cmd: list[str]) -> list[str]:
    env = dict(os.environ, PYTHONPATH=f"{_REPO}/src:{_HERE}")
    out = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=_REPO)
    lines = (out.stdout + out.stderr).splitlines()
    return [ln for ln in lines if not _DROP.search(ln)]


def _style(line: str) -> Text:
    if set(line.strip()) == {"="}:
        return Text(line, style="dim cyan")
    t = Text(line)
    t.highlight_regex(_GOOD, "bold green")
    t.highlight_regex(_BAD, "bold red")
    t.highlight_regex(r"\[[^\]]+\]", "bold cyan")  # [backend name]
    t.highlight_regex(r"canary_pool|kubernetes|airflow_3x", "yellow")
    t.highlight_regex(r"fastpath|faster", "yellow")
    return t


def render(name: str, prompt: str, cmd: list[str], title: str) -> None:
    lines = _run(cmd)
    con = Console(record=True, width=98, file=open(os.devnull, "w"))
    con.print(Text(f"$ {prompt}", style="bold white"))
    for ln in lines:
        con.print(_style(ln))
    path = os.path.join(_DOCS, name)
    con.save_svg(path, title=title)
    print(f"wrote {path}  ({len(lines)} lines)")


def main() -> int:
    render(
        "demo-all-backends.svg",
        "python system_tests/run_all_backends.py",
        [sys.executable, os.path.join(_HERE, "run_all_backends.py")],
        "airflow-provider-openfeature · one policy, five feature-flag backends",
    )
    render(
        "demo-use-cases.svg",
        "python system_tests/run_use_cases.py",
        [sys.executable, os.path.join(_HERE, "run_use_cases.py")],
        "airflow-provider-openfeature · progressive delivery on real Airflow 3.2.2",
    )
    render(
        "demo-measure-loop.svg",
        "python system_tests/measure_loop.py",
        [sys.executable, os.path.join(_HERE, "measure_loop.py")],
        "airflow-provider-openfeature · assign → run → measure → read out, real backends",
    )
    render(
        "demo-k8s-canary.svg",
        "python system_tests/k8s_canary_e2e.py",
        [sys.executable, os.path.join(_HERE, "k8s_canary_e2e.py")],
        "airflow-provider-openfeature · KubernetesExecutor canary on a real cluster",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
