"""An optional Airflow 3.x UI panel that explains the placement policy.

The policy moves a task's pool, queue, or executor from a feature flag with no DAG change. That is
powerful and invisible: an operator sees a task in ``canary_pool`` and cannot tell which flag put it
there or what else is affected. This panel closes that gap. It reads the real tasks in the instance,
evaluates the placement flags through the same OpenFeature client the policy uses, and shows which
tasks are being moved right now and to what. Backend-agnostic.

Off by default. Enable with ``AIRFLOW__OPENFEATURE__ENABLE_UI=True``. The wiring is Airflow 3.x only
(``fastapi_apps`` + ``external_views``); on Airflow 2.x it is a no-op. FastAPI is imported lazily so
this module imports cleanly wherever FastAPI is absent.
"""
from __future__ import annotations

from typing import Any

# The placement dimensions the policy overrides, with the value that means "not moved".
_DIMENSIONS = [
    ("pool", "airflow.task.pool", "default_pool"),
    ("queue", "airflow.task.queue", "default"),
    ("executor", "airflow.task.executor", ""),
]


def _enabled() -> bool:  # pragma: no cover - reads Airflow config at runtime
    from airflow.configuration import conf

    return conf.getboolean("openfeature", "enable_ui", fallback=False)


def _provider_name() -> str:
    from openfeature import api

    try:
        return api.get_provider_metadata().name
    except Exception:
        return "unknown"


def _real_tasks(limit: int = 500) -> list[tuple[str, str]]:
    """Distinct (dag_id, task_id) present in this Airflow instance."""
    try:
        from airflow.models.taskinstance import TaskInstance
        from airflow.utils.session import create_session

        with create_session() as session:
            rows = session.query(TaskInstance.dag_id, TaskInstance.task_id).distinct().limit(limit).all()
        return [(r[0], r[1]) for r in rows]
    except Exception:
        return []


def _state() -> dict[str, Any]:
    from openfeature import api
    from openfeature.evaluation_context import EvaluationContext

    client = api.get_client()
    tasks = _real_tasks()
    moved: list[dict] = []
    per_dim: dict[str, int] = {}
    for dag_id, task_id in tasks:
        ctx = EvaluationContext(targeting_key=f"{dag_id}:{task_id}")
        for dim, flag, default in _DIMENSIONS:
            value = client.get_string_value(flag, default, ctx)
            if value and value != default:
                moved.append({"dag_id": dag_id, "task_id": task_id, "dimension": dim, "flag": flag, "value": value})
                per_dim[dim] = per_dim.get(dim, 0) + 1
    return {
        "provider": _provider_name(),
        "total_tasks": len(tasks),
        "moved_count": len(moved),
        "per_dimension": per_dim,
        "moved": sorted(moved, key=lambda m: (m["dimension"], m["dag_id"], m["task_id"])),
    }


def _build_app():  # pragma: no cover - FastAPI wiring, exercised in a real Airflow 3.x runtime
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse

    app = FastAPI()

    @app.get("/state")
    def state() -> JSONResponse:  # noqa: F821
        return JSONResponse(_state())

    @app.get("/", response_class=HTMLResponse)
    def panel() -> HTMLResponse:  # noqa: F821
        return HTMLResponse(_PANEL_HTML)

    return app


def fastapi_apps() -> list[dict]:
    """Plugin hook: mount the panel API when enabled and FastAPI is available (Airflow 3.x)."""
    if not _enabled():
        return []
    try:
        return [{"name": "OpenFeature", "app": _build_app(), "url_prefix": "/openfeature"}]
    except Exception:
        return []


def external_views() -> list[dict]:
    """Plugin hook: add the navbar tab when enabled."""
    if not _enabled():
        return []
    return [{"name": "OpenFeature", "href": "/openfeature/", "destination": "nav", "icon": "fa-flag"}]


_PANEL_HTML = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><title>OpenFeature</title>
<style>
 body{margin:0;background:#0d1117;color:#e6edf3;font:14px -apple-system,Segoe UI,Roboto,sans-serif}
 .w{max-width:900px;margin:0 auto;padding:22px 18px}
 h1{font-size:20px;margin:0 0 4px}.sub{color:#8b949e;margin:0 0 18px;max-width:640px}
 .pill{display:inline-block;border-radius:999px;padding:2px 10px;font-size:12px;margin-right:6px;
       background:#1f6feb22;color:#58a6ff;border:1px solid #1f6feb55}
 .cards{display:flex;gap:12px;margin:14px 0 18px;flex-wrap:wrap}
 .card{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:14px 18px;min-width:150px}
 .card .n{font-size:30px;font-weight:700;line-height:1}.card .l{color:#8b949e;font-size:12px;margin-top:4px}
 table{width:100%;border-collapse:collapse;background:#161b22;border:1px solid #30363d;border-radius:12px;overflow:hidden}
 th,td{text-align:left;padding:9px 14px;border-bottom:1px solid #21262d;font-size:13px}
 th{color:#8b949e;font-weight:600;text-transform:uppercase;letter-spacing:.03em;font-size:11px}
 tr:last-child td{border-bottom:none}
 code{background:#0d1117;border:1px solid #30363d;border-radius:5px;padding:1px 6px}
 .val{color:#3fb950;font-weight:600}
 .empty{color:#8b949e;padding:18px 4px}
 .live{display:inline-block;width:8px;height:8px;border-radius:50%;background:#2ea043;margin-right:6px;
       animation:p 1.6s infinite}@keyframes p{0%,100%{opacity:1}50%{opacity:.3}}
</style></head><body><div class=w>
 <h1><span class=live></span>OpenFeature &mdash; what's moving your tasks</h1>
 <p class=sub>The placement policy overrides a task's pool, queue, or executor from a feature flag with no
   DAG change. This reads your real tasks, evaluates the flags through the OpenFeature client, and shows
   which tasks are being moved right now.</p>
 <div><span class=pill id=prov>backend: -</span></div>
 <div class=cards>
   <div class=card><div class=n id=moved>-</div><div class=l>tasks moved by a flag</div></div>
   <div class=card><div class=n id=total>-</div><div class=l>tasks evaluated</div></div>
   <div class=card><div class=n id=dims>-</div><div class=l>dimensions active</div></div>
 </div>
 <table><thead><tr><th>DAG</th><th>Task</th><th>Dimension</th><th>Now</th><th>Flag</th></tr></thead>
   <tbody id=rows></tbody></table>
 <div class=empty id=empty style=display:none>No task is currently moved by a flag. Ramp a flag in your backend and this fills in.</div>
</div><script>
function esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML;}
async function tick(){try{const r=await fetch('state',{cache:'no-store'});if(!r.ok)return;const s=await r.json();
 document.getElementById('prov').textContent='backend: '+s.provider;
 document.getElementById('moved').textContent=s.moved_count;
 document.getElementById('total').textContent=s.total_tasks;
 document.getElementById('dims').textContent=Object.keys(s.per_dimension||{}).length;
 const tb=document.getElementById('rows');tb.innerHTML='';
 (s.moved||[]).forEach(m=>{const tr=document.createElement('tr');
   tr.innerHTML='<td><code>'+esc(m.dag_id)+'</code></td><td>'+esc(m.task_id)+'</td><td>'+esc(m.dimension)+
     '</td><td class=val>'+esc(m.value)+'</td><td><code>'+esc(m.flag)+'</code></td>';tb.appendChild(tr);});
 document.getElementById('empty').style.display=(s.moved||[]).length?'none':'block';
}catch(e){}}
tick();setInterval(tick,2000);
</script></body></html>"""
