# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Airy self-healing MCP server (demo).

Three write-capable tools on top of the Airflow REST API — ``diagnose_dag``,
``fix_dag_code`` and ``rerun_dag`` — so an LLM can find a broken Dag, patch its
source, and re-run it.  Deliberately goes beyond AIP-91 phase 1 (read-only) to
show where the value ends up.

Runs as a second MCP sidecar next to the read-only ``astro-airflow-mcp``.

Env:
    AIRFLOW_API_URL      Airflow API base (default http://localhost:8080)
    AIRFLOW_USERNAME     simple-auth-manager user (default admin)
    AIRFLOW_PASSWORD     simple-auth-manager password (default admin)
    AIRY_MCP_DAGS_DIR    bundle root; also the write jail (default /files/dags)
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from fastmcp import FastMCP

API_URL = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080").rstrip("/")
USERNAME = os.environ.get("AIRFLOW_USERNAME", "admin")
PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")
DAGS_DIR = Path(os.environ.get("AIRY_MCP_DAGS_DIR", "/files/dags"))

REPARSE_TIMEOUT_S = 45.0
LOG_TAIL_LINES = 40
LOG_TAIL_CHARS = 4000

mcp: FastMCP = FastMCP("airy-selfheal")

_token: str | None = None


class DagFileError(ValueError):
    """Raised when a Dag file cannot be located or safely written."""


def _login() -> str:
    resp = httpx.post(f"{API_URL}/auth/token", json={"username": USERNAME, "password": PASSWORD}, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def _api(method: str, path: str, **kwargs: Any) -> Any:
    """Call the Airflow REST API, logging in (or re-logging in) as needed."""
    global _token
    if _token is None:
        _token = _login()
    url = f"{API_URL}/api/v2{path}"
    resp = httpx.request(method, url, headers={"Authorization": f"Bearer {_token}"}, timeout=60, **kwargs)
    if resp.status_code == 401:
        _token = _login()
        resp = httpx.request(method, url, headers={"Authorization": f"Bearer {_token}"}, timeout=60, **kwargs)
    resp.raise_for_status()
    return resp.json() if resp.content else None


def _dag_url(dag_id: str, suffix: str = "") -> str:
    """Build a /dags/... path. Ids are model-supplied, so they are always escaped."""
    return f"/dags/{quote(dag_id, safe='')}{suffix}"


def _dag_path(dag_id: str, dag: dict[str, Any] | None = None) -> Path:
    """Resolve a Dag's source file, jailed to ``DAGS_DIR``.

    The path never comes from the caller — only the ``dag_id`` does — and the
    result is re-checked against the bundle root, so no traversal is possible.
    """
    relative = (dag or _api("GET", _dag_url(dag_id))).get("relative_fileloc")
    if not relative:
        raise DagFileError(f"Dag {dag_id!r} has no file location")
    path = (DAGS_DIR / relative).resolve()
    if not path.is_relative_to(DAGS_DIR.resolve()) or path.suffix != ".py":
        raise DagFileError(f"{path} is outside the editable Dag bundle")
    if not path.is_file():
        raise DagFileError(f"{path} does not exist")
    return path


def _latest_version(dag_id: str) -> int | None:
    versions = _api(
        "GET", _dag_url(dag_id, "/dagVersions"), params={"order_by": "-version_number", "limit": 1}
    )["dag_versions"]
    return versions[0]["version_number"] if versions else None


def _force_reparse(dag_id: str, file_token: str, previous_version: int | None) -> str:
    """Ask the Dag processor to re-read the file *now*, and wait for it to land.

    Deliberately the opposite of disabling the processor: ``/files/dags`` is a
    local folder bundle, so the file we just wrote *is* the source of truth and
    nothing overwrites it — the only real problem is the bundle-refresh delay,
    which would be ~30 s of dead air on stage.  (A *git* bundle would clobber
    the edit on refresh; that is the case that needs a writable "MCP bundle" —
    see dev/airy_mcp/README.md.)
    """
    try:
        _api("PUT", f"/parseDagFile/{quote(file_token, safe='')}")
    except httpx.HTTPStatusError as e:
        # 409 = a reparse for this file is already queued, which is what we want.
        if e.response.status_code != 409:
            raise
    deadline = time.monotonic() + REPARSE_TIMEOUT_S
    while time.monotonic() < deadline:
        time.sleep(0.5)
        current = _latest_version(dag_id)
        if current != previous_version:
            return f"reparsed — Dag version {previous_version} → {current}"
    return f"reparse requested, but the Dag version did not change within {REPARSE_TIMEOUT_S:g}s"


def _tail(content: Any) -> str:
    """Last few log lines, whatever shape the API returned them in."""
    if isinstance(content, list):
        lines = content[-LOG_TAIL_LINES:]
        text = "\n".join(line if isinstance(line, str) else json.dumps(line) for line in lines)
    else:
        text = str(content)
    return text[-LOG_TAIL_CHARS:]


def diagnose_dag(dag_id: str) -> dict[str, Any]:
    """
    Find out why the latest run of a Dag failed.

    Returns the failed task, the tail of its log (including the traceback) and
    the full Dag source, which together are enough to work out the fix.
    """
    runs = _api("GET", _dag_url(dag_id, "/dagRuns"), params={"order_by": "-run_after", "limit": 5})[
        "dag_runs"
    ]
    if not runs:
        return {"dag_id": dag_id, "diagnosis": "this Dag has never run"}
    run = next((r for r in runs if r["state"] == "failed"), runs[0])
    run_path = f"/dagRuns/{quote(run['dag_run_id'], safe='')}"

    tis = _api("GET", _dag_url(dag_id, f"{run_path}/taskInstances"), params={"state": "failed"})[
        "task_instances"
    ]

    result: dict[str, Any] = {
        "dag_id": dag_id,
        "dag_run_id": run["dag_run_id"],
        "run_state": run["state"],
    }
    # Diagnosis is read-only, so a Dag outside the writable bundle is still worth
    # reporting on — just without its source.
    try:
        path = _dag_path(dag_id)
        result["source_file"] = str(path)
        result["source"] = path.read_text(errors="replace")
    except (DagFileError, OSError) as e:
        result["source"] = f"unavailable: {e}"

    if not tis:
        result["diagnosis"] = f"latest run is {run['state']}; no failed task instances"
        return result

    ti = tis[0]
    log = _api(
        "GET",
        _dag_url(
            dag_id,
            f"{run_path}/taskInstances/{quote(ti['task_id'], safe='')}/logs/{ti['try_number']}",
        ),
    )
    result["failed_task_id"] = ti["task_id"]
    result["log_tail"] = _tail(log.get("content") if isinstance(log, dict) else log)
    return result


def fix_dag_code(dag_id: str, old: str, new: str) -> dict[str, Any]:
    """
    Patch a Dag's source file by replacing ``old`` with ``new``, then make
    Airflow pick the change up immediately.

    ``old`` must appear exactly once in the file. Returns a unified diff.
    """
    dag = _api("GET", _dag_url(dag_id))
    path = _dag_path(dag_id, dag)
    source = path.read_text()
    version_before_write = _latest_version(dag_id)

    occurrences = source.count(old)
    if occurrences != 1:
        return {
            "applied": False,
            "error": f"{old!r} appears {occurrences} times in {path.name}; it must appear exactly once",
        }

    patched = source.replace(old, new)
    if patched == source:
        return {"applied": False, "error": "the replacement is identical to the original"}
    try:
        compile(patched, str(path), "exec")
    except SyntaxError as e:
        return {"applied": False, "error": f"the patched file would not compile: {e}"}

    backup = path.with_suffix(".py.airy-bak")
    if not backup.exists():
        backup.write_text(source)
    path.write_text(patched)

    diff = "".join(
        difflib.unified_diff(
            source.splitlines(keepends=True),
            patched.splitlines(keepends=True),
            f"a/{path.name}",
            f"b/{path.name}",
        )
    )
    # The write is the commit point: never report a failure that leaves the caller
    # unsure whether the file on disk changed.
    try:
        reparse = _force_reparse(dag_id, dag["file_token"], version_before_write)
    except Exception as e:  # the write already landed; never raise past it
        reparse = f"file patched, but the reparse request failed: {e}"
    return {"applied": True, "file": str(path), "diff": diff, "reparse": reparse}


def revert_dag_code(dag_id: str) -> dict[str, Any]:
    """Restore a Dag's source from the backup taken before the first fix."""
    dag = _api("GET", _dag_url(dag_id))
    path = _dag_path(dag_id, dag)
    backup = path.with_suffix(".py.airy-bak")
    if not backup.exists():
        return {"reverted": False, "error": f"no backup for {path.name}"}
    version_before_write = _latest_version(dag_id)
    path.write_text(backup.read_text())
    backup.unlink()
    try:
        reparse = _force_reparse(dag_id, dag["file_token"], version_before_write)
    except Exception as e:  # the restore already landed; never raise past it
        reparse = f"file restored, but the reparse request failed: {e}"
    return {"reverted": True, "file": str(path), "reparse": reparse}


def rerun_dag(dag_id: str) -> dict[str, Any]:
    """Unpause the Dag if needed and trigger a fresh run on the latest code."""
    if _api("GET", _dag_url(dag_id))["is_paused"]:
        _api("PATCH", _dag_url(dag_id), json={"is_paused": False})
    run = _api("POST", _dag_url(dag_id, "/dagRuns"), json={"logical_date": None, "conf": {}})
    return {"dag_id": dag_id, "dag_run_id": run["dag_run_id"], "state": run["state"]}


def _run_version(run: dict[str, Any]) -> int | None:
    """The Dag version a run executed with — the last entry is the one in effect."""
    versions = run.get("dag_versions") or []
    return versions[-1].get("version_number") if versions else None


def compare_dag_runs(dag_id: str, run_a: str, run_b: str) -> dict[str, Any]:
    """
    Compare two runs of a Dag: per-task duration changes, conf differences,
    and — when the runs executed different Dag versions — the source diff.

    Answers "was it my change?" after a run that used to work starts failing.
    """
    summaries: dict[str, dict[str, Any]] = {}
    durations: dict[str, dict[str, float | None]] = {}
    for label, run_id in (("run_a", run_a), ("run_b", run_b)):
        run_path = f"/dagRuns/{quote(run_id, safe='')}"
        run = _api("GET", _dag_url(dag_id, run_path))
        tis = _api("GET", _dag_url(dag_id, f"{run_path}/taskInstances"))["task_instances"]
        summaries[label] = {
            "dag_run_id": run_id,
            "state": run.get("state"),
            "duration": run.get("duration"),
            "version": _run_version(run),
            "conf": run.get("conf") or {},
        }
        durations[label] = {ti["task_id"]: ti.get("duration") for ti in tis}

    task_durations = []
    for task_id in sorted(set(durations["run_a"]) | set(durations["run_b"])):
        a, b = durations["run_a"].get(task_id), durations["run_b"].get(task_id)
        task_durations.append(
            {
                "task_id": task_id,
                "run_a": a,
                "run_b": b,
                "delta": round(b - a, 3) if a is not None and b is not None else None,
            }
        )

    conf_a, conf_b = summaries["run_a"].pop("conf"), summaries["run_b"].pop("conf")
    conf_changes = {
        key: {"run_a": conf_a.get(key), "run_b": conf_b.get(key)}
        for key in sorted(set(conf_a) | set(conf_b))
        if conf_a.get(key) != conf_b.get(key)
    }

    ver_a, ver_b = summaries["run_a"]["version"], summaries["run_b"]["version"]
    source_diff = None
    if ver_a is not None and ver_b is not None and ver_a != ver_b:
        sources = {
            ver: _api("GET", f"/dagSources/{quote(dag_id, safe='')}", params={"version_number": ver})[
                "content"
            ]
            for ver in (ver_a, ver_b)
        }
        source_diff = "".join(
            difflib.unified_diff(
                sources[ver_a].splitlines(keepends=True),
                sources[ver_b].splitlines(keepends=True),
                f"{dag_id} v{ver_a}",
                f"{dag_id} v{ver_b}",
            )
        )

    return {
        "dag_id": dag_id,
        "run_a": summaries["run_a"],
        "run_b": summaries["run_b"],
        "task_durations": task_durations,
        "conf_changes": conf_changes,
        "source_diff": source_diff,
    }


# Registered here rather than with @mcp.tool so the module keeps exporting plain
# functions — directly callable from tests.
for _tool in (diagnose_dag, compare_dag_runs, fix_dag_code, revert_dag_code, rerun_dag):
    mcp.tool(_tool)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    # Loopback only: the transport is unauthenticated and fix_dag_code writes
    # Python that Airflow then executes.  The plugin dials localhost.
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()
    mcp.run(transport="http", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
