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

Write-capable tools on top of the Airflow REST API — ``diagnose_dag`` finds
what is wrong, ``plan_dag_code_changes``/``apply_dag_code_changes`` repair the
source as one atomic change, ``plan_task_instance_clear``/``apply_task_instance_clear``
re-run an instance that already exists, and ``rerun_dag`` starts a fresh run.
Deliberately goes beyond AIP-91 phase 1 (read-only) to show where the value ends up.

Every mutation is planned first: the planning tool is read-only and hands back a
single-use token, and the writing tool refuses without it.  That is what makes the
approval card show the user the change they are actually approving.

Runs as a second MCP sidecar next to the read-only ``astro-airflow-mcp``.

Env:
    AIRFLOW_API_URL      Airflow API base (default http://localhost:8080)
    AIRFLOW_USERNAME     simple-auth-manager user (default admin)
    AIRFLOW_PASSWORD     simple-auth-manager password (default admin)
    AIRY_MCP_DAGS_DIR    bundle root; also the write jail (default /files/dags)
"""

from __future__ import annotations

import argparse
import ast
import difflib
import fcntl
import json
import os
import re
import secrets
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import md5
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
FAILURE_SCAN_LIMIT = 50
LOG_TAIL_LINES = 40
LOG_TAIL_CHARS = 4000
MAX_BACKFILL_RUNS = int(os.environ.get("AIRY_MCP_MAX_BACKFILL_RUNS", "50"))
# One diagnosis now carries every failed task's log, so it needs a ceiling the
# per-log tail does not give: a fan-out of 200 failed mapped instances would
# otherwise return 800 KB and blow the model's context on its way through.
DIAGNOSIS_LOG_BUDGET_CHARS = 12000
# ``taskInstances`` pages, and a silently short list would let a diagnosis miss a
# failure or a clear compare the wrong task set. Asking for a bigger page does
# not help — ``[api] maximum_page_limit`` (100) clamps it — so the pages are
# followed, up to a ceiling, and whatever is still missing is reported.
TASK_INSTANCE_PAGE = 100
TASK_INSTANCE_SCAN_LIMIT = 500

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


def _parsed_source(dag_id: str, source_digest: str | None = None) -> str:
    """The parsed source, proved to be the bytes this call was authorized against.

    Matched by content hash rather than version number: Airflow rewrites the
    latest version's source in place when it changes, so the number alone does
    not name a fixed set of bytes.
    """
    content = _api("GET", f"/dagSources/{quote(dag_id, safe='')}")["content"]
    if source_digest is not None and md5(content.encode("utf-8")).hexdigest() != source_digest:
        raise DagFileDriftError(
            f"the parsed source of {dag_id} is no longer the version this request was authorized "
            f"against; it may now define a Dag that was never checked — try again"
        )
    return content


class DagFileDriftError(DagFileError):
    """Raised when the file on disk is not the version Airflow parsed."""


@contextmanager
def _exclusive(path: Path) -> Iterator[None]:
    """Hold the Dag file for a whole read-check-write.

    Validating and then writing as two separate steps loses an edit that lands in
    between — the buffer written back was computed from bytes that are no longer
    there. This closes the window against every writer that takes the same lock;
    a human with an editor does not, which is inherent to a file-backed bundle.
    """
    with path.open("r+", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _write_if_unchanged(path: Path, expected: str, content: str) -> None:
    """Replace the file, but only if it still holds what was checked."""
    if path.read_text() != expected:
        raise DagFileDriftError(
            f"{path.name} changed while the patch was being prepared, so applying it would "
            f"overwrite an edit nobody reviewed; try again"
        )
    path.write_text(content)


def _backup_path(path: Path) -> Path:
    """Where the one-time original is kept, jailed like the Dag file itself.

    ``with_suffix`` builds a name, not a location: a symlink planted under that
    name would send the Dag's source wherever it points, and this is the one
    write whose destination is not otherwise checked.
    """
    backup = path.with_suffix(".py.airy-bak")
    if backup.resolve().parent != path.resolve().parent:
        raise DagFileError(f"{backup.name} does not resolve to a file next to {path.name}")
    return backup


def _read_reviewed_file(dag_id: str, path: Path, source_digest: str | None = None) -> str:
    """Read a Dag's file, refusing if Airflow has not parsed what is in it.

    Access to this file was authorized against the Dags Airflow parsed out of it.
    If the bytes on disk have moved on — an edit mid-flight, a Dag added and not
    yet processed — then those are not the bytes anyone approved touching.
    """
    on_disk = path.read_text()
    if on_disk != _parsed_source(dag_id, source_digest):
        raise DagFileDriftError(
            f"{path.name} on disk is not the version Airflow has parsed, so what it now "
            f"contains has not been reviewed; wait for the Dag processor and try again"
        )
    return on_disk


def _latest_version(dag_id: str) -> int | None:
    versions = _api(
        "GET", _dag_url(dag_id, "/dagVersions"), params={"order_by": "-version_number", "limit": 1}
    )["dag_versions"]
    return versions[0]["version_number"] if versions else None


def _force_reparse(dag_id: str, file_token: str, previous_version: int | None) -> tuple[str, int | None]:
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
            return f"reparsed — Dag version {previous_version} → {current}", current
    return (
        f"reparse requested, but the Dag version did not change within {REPARSE_TIMEOUT_S:g}s",
        previous_version,
    )


def _tail(content: Any) -> str:
    """Last few log lines, whatever shape the API returned them in."""
    if isinstance(content, list):
        lines = content[-LOG_TAIL_LINES:]
        text = "\n".join(line if isinstance(line, str) else json.dumps(line) for line in lines)
    else:
        text = str(content)
    return text[-LOG_TAIL_CHARS:]


def _run_task_instances(dag_id: str, run_path: str) -> tuple[list[dict[str, Any]], int]:
    """Every task instance in one run, and how many are still missing."""
    tis: list[dict[str, Any]] = []
    total = 0
    while True:
        resp = _api(
            "GET",
            _dag_url(dag_id, f"{run_path}/taskInstances"),
            params={"limit": TASK_INSTANCE_PAGE, "offset": len(tis)},
        )
        page = resp["task_instances"]
        total = resp.get("total_entries", len(page))
        tis += page
        # An empty page ends it whatever the count says: a total that never
        # comes down would otherwise loop for as long as the ceiling allows.
        if not page or len(tis) >= min(total, TASK_INSTANCE_SCAN_LIMIT):
            break
    return tis, max(total - len(tis), 0)


def _tasks(dag_id: str) -> list[dict[str, Any]]:
    """The current tasks and their edges.

    ``/tasks`` is the only *public* route that carries ``downstream_task_ids``;
    the richer structure view lives under ``/ui`` and is not part of the API this
    server is allowed to speak.
    """
    return _api("GET", _dag_url(dag_id, "/tasks"))["tasks"]


def _display_order(tasks: list[dict[str, Any]]) -> tuple[list[str], set[int]]:
    """Topological order, plus the positions the graph does not pin down.

    The API returns tasks sorted by ``task_id``, which is *not* what the Grid
    shows — for the demo Dag alphabetical order puts ``report`` second and
    ``summarize`` third, exactly inverting them. So "the third task" is resolved
    against the graph instead, and only where the graph actually decides it: two
    tasks ready at once could be listed either way round, and an ordinal there
    is a guess with a 50 % chance of clearing the wrong task.

    Ambiguity ends when the tie does: a diamond makes its two middle positions
    interchangeable, but the task the branches rejoin at is back to being the
    only one that can sit there.
    """
    downstream = {task["task_id"]: sorted(task.get("downstream_task_ids") or []) for task in tasks}
    indegree: dict[str, int] = dict.fromkeys(downstream, 0)
    for children in downstream.values():
        for child in children:
            if child in indegree:
                indegree[child] += 1

    order: list[str] = []
    ready = sorted(task_id for task_id, degree in indegree.items() if degree == 0)
    while ready:
        node = ready.pop(0)
        order.append(node)
        for child in downstream[node]:
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
        ready.sort()
    if len(order) < len(downstream):
        # A cycle cannot happen in a parsed Dag, but a partial graph read can
        # look like one; never silently drop the tasks it left out, and trust
        # none of the order.
        return order + sorted(set(downstream) - set(order)), set(range(len(downstream)))
    return order, _ambiguous_positions(downstream)


def _reachable(edges: dict[str, list[str]]) -> dict[str, set[str]]:
    """Everything each node can reach, following the edges given."""
    seen: dict[str, set[str]] = {}

    def walk(node: str) -> set[str]:
        if node not in seen:
            seen[node] = set()  # placeholder: a cycle must not recurse forever
            reached: set[str] = set()
            for child in edges.get(node, []):
                reached |= {child} | walk(child)
            seen[node] = reached
        return seen[node]

    for node in edges:
        walk(node)
    return seen


def _ambiguous_positions(downstream: dict[str, list[str]]) -> set[int]:
    """The positions where more than one task could legitimately be listed.

    A task can sit anywhere from "after all its ancestors" to "before all its
    descendants". Wherever two tasks' ranges overlap, an ordinal names neither
    of them in particular — and that is broader than the fan-out itself: with
    ``start -> [a, b]`` and ``a -> c -> d``, ``b`` can be listed anywhere after
    ``start``, so the last position is open too.
    """
    upstream: dict[str, list[str]] = {task_id: [] for task_id in downstream}
    for task_id, children in downstream.items():
        for child in children:
            if child in upstream:
                upstream[child].append(task_id)
    descendants, ancestors = _reachable(downstream), _reachable(upstream)
    total = len(downstream)
    candidates: dict[int, int] = dict.fromkeys(range(total), 0)
    for task_id in downstream:
        for position in range(len(ancestors[task_id]), total - len(descendants[task_id])):
            candidates[position] += 1
    return {position for position, count in candidates.items() if count != 1}


# ``task_ids='summarise'`` — matched as text, not as an AST call, because the
# demo's own second bug lives inside a Jinja template string where an AST walk
# sees one opaque literal.
# The lookbehind is load-bearing: without it ``external_task_id="load"`` reads as
# a declaration of ``load``, and an ExternalTaskSensor that merely *waits* on a
# task would stand in for the task itself.
_XCOM_REF_RE = re.compile(r"(?<![\w.])task_ids\s*=\s*(['\"])(?P<ref>[^'\"]+)\1")
_TASK_ID_RE = re.compile(r"(?<![\w.])task_id\s*=\s*(['\"])(?P<task_id>[^'\"]+)\1")
# Anything that can move an asset edge, and so a *different* Dag's schedule.
_ASSET_RE = re.compile(r"\b(Asset|outlets|inlets|schedule)\b")
# A Dag being built: the constructor, or the TaskFlow decorator that stands in
# for it. More than one, and the file is not this Dag's alone.
_DAG_DEF_RE = re.compile(r"\bDAG\s*\(|@dag\b")


def _changed_lines_touch_assets(source: str, patched: str) -> bool:
    """Whether the patch itself touches an asset edge — not merely the file."""
    return any(
        line[0] in "+-" and not line.startswith(("---", "+++")) and _ASSET_RE.search(line)
        for line in difflib.unified_diff(source.splitlines(), patched.splitlines(), n=0)
    )


def _referenced_task_ids(source: str) -> set[str]:
    return {m.group("ref") for m in _XCOM_REF_RE.finditer(source)}


def _declared_task_ids(source: str) -> set[str]:
    return {m.group("task_id") for m in _TASK_ID_RE.finditer(source)}


def _static_checks(source: str, task_ids: set[str]) -> list[dict[str, str]]:
    """Problems visible in the source and graph alone, before anything runs.

    Only for a file that defines this Dag and nothing else. A source file can
    hold several Dags, and then every reference to a *co-located* Dag's task
    looks exactly like a reference to a task that does not exist — reporting
    those as latent blockers would send the model off fixing working code. Two
    signals say the file holds more than this Dag: a task id it declares that
    this Dag does not have, and more than one Dag being constructed. Either is
    enough to stop guessing, and the second catches a co-located Dag built
    entirely from TaskFlow tasks, which declares no task id at all.
    """
    checks = []
    strangers = _declared_task_ids(source) - task_ids
    if strangers or len(_DAG_DEF_RE.findall(source)) > 1:
        return [
            {
                "kind": "source_graph_disagreement",
                "detail": (
                    "the file defines more than this Dag"
                    + (f" — it also declares task_id(s) {sorted(strangers)}" if strangers else "")
                    + ", so reference checks are skipped: this Dag's graph cannot say whether "
                    "another Dag's task ids are valid"
                ),
            }
        ]
    for ref in sorted(_referenced_task_ids(source) - task_ids):
        checks.append(
            {
                "kind": "unknown_xcom_task_id",
                "detail": (
                    f"an XCom pull references task_ids={ref!r}, which is not a task in this Dag, "
                    f"so the pull returns None"
                ),
            }
        )
    return checks


def diagnose_dag(dag_id: str, source_digest: str | None = None) -> dict[str, Any]:
    """
    Find out what is wrong with a Dag's latest run.

    Returns every task instance and its state, the log tail of **every** failed
    one, the task graph, the full Dag source, and deterministic checks that spot
    broken task references before they ever run. Report every finding, not only
    the task that failed first.

    ``source_digest`` is set by the caller's permissions, not by you.
    """
    runs = _api("GET", _dag_url(dag_id, "/dagRuns"), params={"order_by": "-run_after", "limit": 5})[
        "dag_runs"
    ]
    if not runs:
        return {"dag_id": dag_id, "diagnosis": "this Dag has never run"}
    run = next((r for r in runs if r["state"] == "failed"), runs[0])
    run_path = f"/dagRuns/{quote(run['dag_run_id'], safe='')}"

    tis, omitted = _run_task_instances(dag_id, run_path)

    result: dict[str, Any] = {
        "dag_id": dag_id,
        "dag_run_id": run["dag_run_id"],
        "run_state": run["state"],
        "dag_version": _run_version(run),
        "task_instances": [
            {
                "task_id": ti["task_id"],
                "state": ti.get("state"),
                "try_number": ti.get("try_number"),
                "map_index": ti.get("map_index", -1),
            }
            for ti in tis
        ],
    }
    if omitted:
        result["task_instances_omitted"] = omitted
    # The *parsed* source, not the file on disk. Permission to read this file was
    # granted against the Dags Airflow has parsed out of it; the live file may
    # already define one more, and handing that back would disclose a Dag nobody
    # authorized. A Dag outside the writable bundle is still worth reporting on.
    try:
        result["source"] = _parsed_source(dag_id, source_digest)
        result["source_file"] = str(_dag_path(dag_id))
    except (DagFileError, OSError, httpx.HTTPStatusError, KeyError) as e:
        result.setdefault("source", f"unavailable: {e}")

    try:
        tasks = _tasks(dag_id)
    except (httpx.HTTPStatusError, KeyError):
        tasks = []
    if tasks:
        order, ambiguous = _display_order(tasks)
        result["tasks"] = {
            "order": order,
            "ordering": "topological",
            "ambiguous_positions": sorted(p + 1 for p in ambiguous),
            "edges": {
                task["task_id"]: sorted(task.get("downstream_task_ids") or [])
                for task in tasks
                if task.get("downstream_task_ids")
            },
        }
    source = result.get("source")
    if tasks and isinstance(source, str) and not source.startswith("unavailable:"):
        result["checks"] = _static_checks(source, {task["task_id"] for task in tasks})

    failed = [ti for ti in tis if ti.get("state") == "failed"]
    if not failed:
        result["diagnosis"] = f"latest run is {run['state']}; no failed task instances"
        return result

    failures = []
    budget = DIAGNOSIS_LOG_BUDGET_CHARS
    for ti in failed:
        if budget <= 0:
            break
        log = _api(
            "GET",
            _dag_url(
                dag_id,
                f"{run_path}/taskInstances/{quote(ti['task_id'], safe='')}/logs/{ti['try_number']}",
            ),
            # The log route defaults to map_index=-1, which is a *different*
            # instance from a mapped one: without this a fan-out reports the
            # unmapped task's log, or none at all.
            params={"map_index": ti.get("map_index", -1)},
        )
        # From the end, like _tail itself: the exception and its traceback are
        # the last thing in the log, and keeping the first N characters of a
        # tail would spend the budget on the lines nobody needs.
        tail = _tail(log.get("content") if isinstance(log, dict) else log)[-budget:]
        budget -= len(tail)
        failures.append({"task_id": ti["task_id"], "map_index": ti.get("map_index", -1), "log_tail": tail})
    result["failures"] = failures
    if len(failures) < len(failed):
        result["logs_omitted"] = len(failed) - len(failures)
    # Kept for the single-failure case every prompt and card already speaks.
    result["failed_task_id"] = failures[0]["task_id"]
    result["log_tail"] = failures[0]["log_tail"]
    return result


def _definition_updates(dag_id: str, before: int | None, after: int | None) -> dict[str, Any]:
    """The UI refresh a landed source change earns — and only once it has landed.

    A write whose reparse has not produced a new version yet has not changed
    anything the Graph or Code view shows, so refreshing them would only promise
    the user that what they are looking at is current.
    """
    if after is None or after == before:
        return {"ui_updates": []}
    return {"ui_updates": [{"kind": "dag_definition", "dag_id": dag_id, "version_number": after}]}


def _normalized_changes(changes: Any) -> list[tuple[str, str]] | None:
    """The changes as comparable pairs, or ``None`` if they are not changes at all."""
    if not isinstance(changes, list) or not changes:
        return None
    pairs = []
    for change in changes:
        if not isinstance(change, dict):
            return None
        old, new = change.get("old"), change.get("new")
        if not isinstance(old, str) or not isinstance(new, str) or not old:
            return None
        pairs.append((old, new))
    return pairs


def _patch(source: str, pairs: list[tuple[str, str]]) -> tuple[str | None, str | None]:
    """Apply every replacement in order, or explain which one cannot be trusted.

    Uniqueness is re-checked against the buffer each replacement actually runs
    on, not against the original: two changes whose snippets overlap would
    otherwise pass a batch preflight and then hit a file the first edit changed.
    """
    patched = source
    for old, new in pairs:
        occurrences = patched.count(old)
        if occurrences != 1:
            return (
                None,
                f"{old!r} appears {occurrences} times at the point it is applied; it must appear exactly once",
            )
        patched = patched.replace(old, new)
    if patched == source:
        return None, "the changes leave the file exactly as it is"
    return patched, None


def _definition_count(source: str, task_id: str) -> int:
    """How many times this source *builds* a task of that name, rather than pointing at it.

    A literal ``task_id="…"``, or the ``@task``-decorated function a TaskFlow
    task takes its name from — that one declares no task id at all, so a diff of
    declarations alone would not notice it being deleted. The decorator is what
    makes it a task, so a plain function of the same name is not one: dropping
    the decorator removes the task as surely as deleting it.

    Counted, not tested: one source file can define several Dags, and two of
    them may each have a task called ``load``. Asking "is it still defined?"
    answers yes while one of the two is being deleted — so the question is how
    many definitions there were, and how many are left.
    """
    declared = sum(match.group("task_id") == task_id for match in _TASK_ID_RE.finditer(source))
    return declared + _taskflow_definition_count(source, task_id)


def _decorator_root(node: ast.expr) -> str:
    """The name a decorator hangs off: ``task`` for ``@task``, ``@task(...)``, ``@task.branch``."""
    if isinstance(node, ast.Call):
        node = node.func
    while isinstance(node, ast.Attribute):
        node = node.value
    return node.id if isinstance(node, ast.Name) else ""


def _taskflow_definition_count(source: str, task_id: str) -> int:
    """How many ``@task``-decorated functions of this name the source defines.

    Parsed rather than pattern-matched: a decorator can carry its arguments over
    several lines, or sit under others, and a regex that tries to allow for that
    either misses the real thing or spans half the file looking for it.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return 0
    return sum(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == task_id
        and any(_decorator_root(decorator) == "task" for decorator in node.decorator_list)
        for node in ast.walk(tree)
    )


def _mentions_task(source: str, task_id: str) -> bool:
    """Whether the source names the task anywhere at all.

    Deliberately the loosest of the three scans, because it only ever asks
    "is this name still in the file after its definition went?" — a bare
    ``registry = [orphan]`` is as much a live reference as a quoted one, and it
    is a `NameError` at parse time that the compile check cannot see. Erring
    towards blocking a removal costs the user an explanation; erring the other
    way costs them a Dag that no longer imports.
    """
    return bool(re.search(rf"\b{re.escape(task_id)}\b", source))


def _change_impact(dag_id: str, source: str, patched: str) -> dict[str, Any]:
    """What the patch does to the task graph — not just whether it still compiles.

    Static and conservative by construction: the tasks a Dag file builds are
    whatever running it produces, so a literal scan can only ever be a lower
    bound. It is stated as one, and it is enough to stop the case the compile
    check waves through — deleting a task that other tasks still point at.

    Removals are found against the *live graph* rather than against literal
    ``task_id=`` declarations: a TaskFlow task is named after its function and
    declares nothing, so a declaration diff would not see it disappear.
    """
    limits = (
        "literal scan of the source: task ids built dynamically, or referenced through a "
        "variable, are not visible to it"
    )
    try:
        tasks = _tasks(dag_id)
    except (httpx.HTTPStatusError, KeyError) as e:
        # Fail closed. Without the graph there is nothing to check a removal
        # against, and "found no problems" would be indistinguishable from
        # "could not look" — which is how a change that orphans half a Dag gets
        # a token.
        return {
            "removed_task_ids": [],
            "added_task_ids": [],
            "limits": limits,
            "blocking": [
                f"the task graph could not be read ({e}), so this change cannot be checked against it"
            ],
        }
    removed = sorted(
        task["task_id"]
        for task in tasks
        if _definition_count(patched, task["task_id"]) < _definition_count(source, task["task_id"])
    )
    added = sorted(_declared_task_ids(patched) - _declared_task_ids(source))
    impact: dict[str, Any] = {
        "removed_task_ids": removed,
        "added_task_ids": added,
        "limits": limits,
    }
    blocking: list[str] = []
    if removed:
        edges = {task["task_id"]: sorted(task.get("downstream_task_ids") or []) for task in tasks}
        for task_id in removed:
            downstream = edges.get(task_id, [])
            upstream = sorted(other for other, children in edges.items() if task_id in children)
            # Its definition is gone, so anything left naming it is a reference
            # to a task that will not exist.
            still_named = _mentions_task(patched, task_id)
            impact.setdefault("removed_task_edges", {})[task_id] = {
                "upstream": upstream,
                "downstream": downstream,
                "still_referenced": still_named,
            }
            if upstream or downstream or still_named:
                blocking.append(
                    f"removing {task_id!r} leaves upstream {upstream or 'none'} and downstream "
                    f"{downstream or 'none'} in the current graph"
                    + (
                        f", and the patched source still names {task_id!r}"
                        if still_named
                        else ", and nothing rewires them"
                    )
                )
    if _changed_lines_touch_assets(source, patched):
        # An asset edge reaches other Dags' schedules, which reading this one
        # file cannot show. The answer is not inlined here: it names other Dags,
        # and only get_blast_radius is authorized for that — this tool is not.
        impact["asset_review_needed"] = (
            "this change touches assets, inlets/outlets or the schedule, which can move other Dags' "
            "schedules; call get_blast_radius and tell the user what else is affected before applying"
        )
    impact["blocking"] = blocking
    return impact


def revert_dag_code(dag_id: str, source_digest: str | None = None) -> dict[str, Any]:
    """
    Restore a Dag's source to the original, discarding **every** change Airy
    applied — not just the most recent one. Returns the diff of what was undone.
    """
    dag = _api("GET", _dag_url(dag_id))
    path = _dag_path(dag_id, dag)
    try:
        backup = _backup_path(path)
    except DagFileError as e:
        return {"reverted": False, "mutation_applied": False, "error": str(e)}
    if not backup.exists():
        return {"reverted": False, "mutation_applied": False, "error": f"no backup for {path.name}"}
    version_before_write = _latest_version(dag_id)
    with _exclusive(path):
        try:
            current = _read_reviewed_file(dag_id, path, source_digest)
            original = backup.read_text()
            _write_if_unchanged(path, current, original)
        except DagFileDriftError as e:
            return {"reverted": False, "mutation_applied": False, "error": str(e)}
        backup.unlink()
    diff = "".join(
        difflib.unified_diff(
            current.splitlines(keepends=True),
            original.splitlines(keepends=True),
            f"a/{path.name}",
            f"b/{path.name}",
        )
    )
    version_after = version_before_write
    try:
        reparse, version_after = _force_reparse(dag_id, dag["file_token"], version_before_write)
    except Exception as e:  # the restore already landed; never raise past it
        reparse = f"file restored, but the reparse request failed: {e}"
    return {
        "reverted": True,
        "mutation_applied": True,
        "file": str(path),
        "diff": diff,
        "reparse": reparse,
        **_definition_updates(dag_id, version_before_write, version_after),
    }


def plan_dag_code_changes(
    dag_id: str, changes: list[dict[str, str]], source_digest: str | None = None
) -> dict[str, Any]:
    """
    Preview one atomic set of source edits, without writing anything.

    Read-only. ``changes`` is a list of ``{"old": ..., "new": ...}``; each
    ``old`` must appear exactly once at the point it is applied. Put **every**
    fix you intend to make in one call — a second plan made after the first one
    lands is a plan against source that no longer exists.

    Returns the combined diff, what the change does to the task graph, and a
    single-use ``plan_token``. Show the user the diff and every ``blocking``
    entry; a plan with blockers gets no token and must not be applied.

    ``source_digest`` is set by the caller's permissions, not by you.
    """
    pairs = _normalized_changes(changes)
    if pairs is None:
        return {"planned": False, "error": "changes must be a non-empty list of {'old': ..., 'new': ...}"}
    try:
        source = _parsed_source(dag_id, source_digest)
        path = _dag_path(dag_id)
    except (DagFileError, OSError, httpx.HTTPStatusError, KeyError) as e:
        return {"planned": False, "error": str(e)}

    patched, error = _patch(source, pairs)
    if patched is None:
        return {"planned": False, "error": error}
    try:
        compile(patched, str(path), "exec")
    except SyntaxError as e:
        return {"planned": False, "error": f"the patched file would not compile: {e}"}

    diff = "".join(
        difflib.unified_diff(
            source.splitlines(keepends=True),
            patched.splitlines(keepends=True),
            f"a/{path.name}",
            f"b/{path.name}",
        )
    )
    impact = _change_impact(dag_id, source, patched)
    preview = {
        "planned": True,
        "dag_id": dag_id,
        "file": str(path),
        "change_count": len(pairs),
        "diff": diff,
        "impact": impact,
    }
    if impact["blocking"]:
        # No token: this plan is not one the user can be asked to approve as it
        # stands, and issuing one would let the model apply it anyway.
        return {
            **preview,
            "planned": False,
            "error": (
                "this change breaks references that are still live; tell the user what it would break "
                "and plan the rewiring too, rather than applying it as-is"
            ),
        }
    return {
        **preview,
        "plan_token": _issue_token(
            "dag_code",
            {"dag_id": dag_id, "digest": md5(source.encode("utf-8")).hexdigest(), "changes": pairs},
        ),
    }


def apply_dag_code_changes(
    dag_id: str,
    changes: list[dict[str, str]],
    plan_token: str = "",
    source_digest: str | None = None,
) -> dict[str, Any]:
    """
    Apply the edits previewed by plan_dag_code_changes — all of them, or none.

    Pass back both the ``plan_token`` *and* the exact ``changes`` list that was
    planned. The changes go in the arguments so the confirmation the user clicks
    spells out every edit it writes; the token is what proves they were planned
    against the source that is still on disk.

    ``source_digest`` is set by the caller's permissions, not by you.
    """
    pairs = _normalized_changes(changes)
    if pairs is None:
        return {
            "applied": False,
            "mutation_applied": False,
            "error": "changes must be a non-empty list of {'old': ..., 'new': ...}",
        }
    plan = _redeem_token("dag_code", plan_token)
    if plan is None:
        return {
            "applied": False,
            "mutation_applied": False,
            "error": "no reviewed plan for this change; call plan_dag_code_changes and show the user the diff",
        }
    if plan["dag_id"] != dag_id or plan["changes"] != pairs:
        return {
            "applied": False,
            "mutation_applied": False,
            "error": (
                "these are not the changes that were planned, so the diff the user reviewed is not "
                "this one; re-plan and show them again"
            ),
        }

    dag = _api("GET", _dag_url(dag_id))
    path = _dag_path(dag_id, dag)
    version_before_write = _latest_version(dag_id)
    with _exclusive(path):
        try:
            source = _read_reviewed_file(dag_id, path, source_digest)
        except DagFileDriftError as e:
            return {"applied": False, "mutation_applied": False, "error": str(e)}
        # The plan's impact findings were computed from these exact bytes; if
        # they still hash the same there is nothing to recompute, and if they do
        # not, no amount of recomputing makes the reviewed diff the right one.
        if md5(source.encode("utf-8")).hexdigest() != plan["digest"]:
            return {
                "applied": False,
                "mutation_applied": False,
                "error": "the source changed since it was planned; re-plan and show the user the new diff",
            }
        patched, error = _patch(source, pairs)
        if patched is None:
            return {"applied": False, "mutation_applied": False, "error": error}
        try:
            compile(patched, str(path), "exec")
        except SyntaxError as e:
            return {
                "applied": False,
                "mutation_applied": False,
                "error": f"the patched file would not compile: {e}",
            }

        try:
            backup = _backup_path(path)
        except DagFileError as e:
            return {"applied": False, "mutation_applied": False, "error": str(e)}
        try:
            _write_if_unchanged(path, source, patched)
        except DagFileDriftError as e:
            return {"applied": False, "mutation_applied": False, "error": str(e)}
        # After the write, never before it: a backup taken for an edit that then
        # refused would sit there as "the original" until some later edit
        # succeeded, and revert would restore it over changes it never saw.
        backup_failure = None
        if not backup.exists():
            try:
                backup.write_text(source)
            except OSError as e:
                # The write is the commit point. Raising here would report a
                # failure over a file that did change, and the user would go
                # looking for an edit that is already on disk.
                backup_failure = (
                    f"the original could not be backed up ({e}), so revert has nothing to restore"
                )

    diff = "".join(
        difflib.unified_diff(
            source.splitlines(keepends=True),
            patched.splitlines(keepends=True),
            f"a/{path.name}",
            f"b/{path.name}",
        )
    )
    version_after = version_before_write
    try:
        reparse, version_after = _force_reparse(dag_id, dag["file_token"], version_before_write)
    except Exception as e:  # the write already landed; never raise past it
        reparse = f"file patched, but the reparse request failed: {e}"
    return {
        "applied": True,
        "mutation_applied": True,
        "file": str(path),
        "change_count": len(pairs),
        "diff": diff,
        "reparse": reparse,
        **({"warning": backup_failure} if backup_failure else {}),
        **_definition_updates(dag_id, version_before_write, version_after),
    }


def rerun_dag(dag_id: str, unpause: bool = False, unpause_token: str = "") -> dict[str, Any]:
    """
    Trigger a fresh run of a Dag on the latest code.

    A paused Dag will not run until it is unpaused, and unpausing also resumes
    its *scheduled* runs — a lasting change beyond this one run. So it cannot be
    part of a first proposal: calling this on a paused Dag returns a warning and
    an ``unpause_token``. Put the warning to the user in your own words, and only
    if they agree call again with ``unpause=True`` and that token.
    """
    if _api("GET", _dag_url(dag_id))["is_paused"]:
        if not unpause:
            return {
                "triggered": False,
                "mutation_applied": False,
                "unpause_token": _issue_token("unpause", {"dag_id": dag_id}),
                "error": (
                    f"{dag_id} is paused, so a new run would not start. Tell the user that re-running "
                    f"means unpausing, which also resumes its scheduled runs from now on, and ask them. "
                    f"If they agree, call again with unpause=True and this unpause_token."
                ),
            }
        warned = _redeem_token("unpause", unpause_token)
        if warned is None or warned["dag_id"] != dag_id:
            return {
                "triggered": False,
                "mutation_applied": False,
                "error": (
                    f"unpausing {dag_id} needs the unpause_token from its paused-Dag warning; "
                    f"call rerun_dag without unpause first and put that warning to the user"
                ),
            }
        _api("PATCH", _dag_url(dag_id), json={"is_paused": False})
        unpaused = True
    else:
        unpaused = False
    try:
        run = _api("POST", _dag_url(dag_id, "/dagRuns"), json={"logical_date": None, "conf": {}})
    except Exception as e:
        # The unpause already committed. Reporting only the failure would leave
        # the user thinking nothing happened, with the Dag now scheduling again.
        return {
            "triggered": False,
            # The unpause may well have committed, so this is not "nothing
            # happened" — but no run exists, and no view of one can be refreshed.
            "mutation_applied": False,
            "dag_id": dag_id,
            "unpaused": unpaused,
            "error": (
                f"triggering the run failed: {e}"
                + (f". {dag_id} was unpaused first and is still unpaused." if unpaused else "")
            ),
        }
    return {
        "triggered": True,
        "mutation_applied": True,
        "dag_id": dag_id,
        "dag_run_id": run["dag_run_id"],
        "state": run["state"],
        "unpaused": unpaused,
        "ui_updates": [{"kind": "dag_run", "dag_id": dag_id, "dag_run_id": run["dag_run_id"]}],
    }


def _run_version(run: dict[str, Any]) -> int | None:
    """The Dag version a run executed with — the last entry is the one in effect."""
    versions = run.get("dag_versions") or []
    return versions[-1].get("version_number") if versions else None


def _resolve_run(dag_id: str, dag_run_id: str) -> tuple[dict[str, Any] | None, str | None]:
    """Turn ``latest`` into one exact run, or confirm the exact one still exists."""
    if dag_run_id in ("", "latest"):
        runs = _api("GET", _dag_url(dag_id, "/dagRuns"), params={"order_by": "-run_after", "limit": 1})[
            "dag_runs"
        ]
        if not runs:
            return None, f"{dag_id} has no runs to clear"
        return runs[0], None
    try:
        return _api("GET", _dag_url(dag_id, f"/dagRuns/{quote(dag_run_id, safe='')}")), None
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None, f"{dag_id} has no run {dag_run_id!r}"
        raise


def _resolve_task(dag_id: str, task_id: str, position: int) -> tuple[str | None, str, str | None]:
    """The one task the request names — by id, or by where it sits in the graph.

    Returns ``(task_id, how_it_was_resolved, error)``. An ordinal is only
    honoured where the graph fixes the order; see ``_display_order``.
    """
    tasks = _tasks(dag_id)
    task_ids = {task["task_id"] for task in tasks}
    if task_id:
        if task_id not in task_ids:
            return None, "", f"{dag_id} has no task {task_id!r}"
        return task_id, "named explicitly", None
    if position < 1:
        return None, "", "name a task_id, or a 1-based position in the Dag"
    order, ambiguous = _display_order(tasks)
    if position > len(order):
        return None, "", f"{dag_id} has {len(order)} tasks, so there is no task {position}"
    if position - 1 in ambiguous:
        return (
            None,
            "",
            (
                f'{dag_id} branches, so "task {position}" is not one task — at that point the graph '
                f"allows more than one order. Ask the user which task id they mean; the tasks are {order}"
            ),
        )
    return order[position - 1], f"position {position} in topological order {order}", None


def _clear_body(
    dag_run_id: str,
    markers: list[Any],
    *,
    dry_run: bool,
    only_failed: bool,
    include_downstream: bool,
    run_on_latest_version: bool,
) -> dict[str, Any]:
    """The exact clear this tool performs — every flag stated, none defaulted.

    ``run_on_latest_version`` in particular: left out, Airflow resolves it from
    the Dag, then from ``[core] rerun_with_latest_version``, so omitting it is
    not the same as sending ``false``.

    ``include_downstream`` defaults on, as it does in Airflow's own clear dialog,
    because clearing a task alone usually does not finish the job: a downstream
    left in ``upstream_failed`` is never rescheduled, and one that already
    succeeded keeps the XCom value the re-run was supposed to replace. Widening
    it is not silent — the plan lists every instance it pulls in.
    """
    return {
        "dry_run": dry_run,
        "dag_run_id": dag_run_id,
        "task_ids": markers,
        "only_failed": only_failed,
        "only_running": False,
        "reset_dag_runs": True,
        "include_upstream": False,
        "include_downstream": include_downstream,
        "include_future": False,
        "include_past": False,
        # A running attempt would otherwise be killed into RESTARTING by a card
        # that never said so; this turns that case into a 409 we report instead.
        "prevent_running_task": True,
        "run_on_latest_version": run_on_latest_version,
    }


def _affected(response: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "task_id": ti["task_id"],
            "map_index": ti.get("map_index", -1),
            "state": ti.get("state"),
            "try_number": ti.get("try_number"),
        }
        for ti in response.get("task_instances") or []
    ]


def _identities(affected: list[dict[str, Any]]) -> list[tuple[str, int]]:
    """What makes the cleared set *that* set — order is not promised by either end."""
    return sorted((ti["task_id"], ti["map_index"]) for ti in affected)


def _version_drift(dag_id: str, dag_run_id: str) -> tuple[dict[str, list[str]] | None, str | None]:
    """Whether re-queuing this run would let the scheduler change its task set.

    Not only when ``run_on_latest_version`` is asked for: clearing re-queues the
    run either way, and the scheduler reconciles a re-queued run against the
    latest version whenever that version is not already one of the run's — which
    creates instances for tasks the new version added. So the same question is
    asked regardless, and asked again at the moment of the clear.
    """
    run_tis, omitted = _run_task_instances(dag_id, f"/dagRuns/{quote(dag_run_id, safe='')}")
    if omitted:
        return None, (
            f"run {dag_run_id} has more task instances than this tool will read "
            f"({omitted} not seen), so it cannot tell whether clearing would change the task set"
        )
    current = {ti["task_id"] for ti in run_tis}
    latest = {task["task_id"] for task in _tasks(dag_id)}
    if current == latest:
        return None, None
    return {"added": sorted(latest - current), "removed": sorted(current - latest)}, None


def plan_task_instance_clear(
    dag_id: str,
    task_id: str = "",
    position: int = 0,
    dag_run_id: str = "latest",
    map_index: int | None = None,
    only_failed: bool = True,
    include_downstream: bool = True,
    run_on_latest_version: bool = True,
) -> dict[str, Any]:
    """
    Preview clearing a task instance that already exists, without changing anything.

    Read-only. Clearing re-runs the *existing* instance inside its own Dag run —
    it is not a new run, so never reach for rerun_dag to do it.

    Name the task with ``task_id``. ``position`` (1-based) is only for turning a
    user's "the third task" into an id, and is refused where the graph does not
    fix the order. ``dag_run_id`` defaults to the latest run and is resolved to
    an exact id here.

    ``include_downstream`` is on by default, as in Airflow's own clear dialog:
    clearing a task alone leaves its downstream stuck — an ``upstream_failed``
    instance is never rescheduled, and a succeeded one keeps the XCom value the
    re-run exists to replace. Pass ``false`` only if the user asks for that one
    task and nothing after it.

    Show the user ``affected`` — that is what will be cleared, downstream
    included — then pass the
    ``plan_token`` and the same arguments to apply_task_instance_clear.
    """
    run, error = _resolve_run(dag_id, dag_run_id)
    if run is None:
        return {"planned": False, "error": error}
    resolved_run_id = run["dag_run_id"]
    task, resolved_by, error = _resolve_task(dag_id, task_id, position)
    if task is None:
        return {"planned": False, "dag_run_id": resolved_run_id, "error": error}

    markers: list[Any] = [[task, map_index] if map_index is not None else task]
    preview = _api(
        "POST",
        _dag_url(dag_id, "/clearTaskInstances"),
        json=_clear_body(
            resolved_run_id,
            markers,
            dry_run=True,
            only_failed=only_failed,
            include_downstream=include_downstream,
            run_on_latest_version=run_on_latest_version,
        ),
    )
    affected = _affected(preview)
    plan: dict[str, Any] = {
        "planned": True,
        "dag_id": dag_id,
        "dag_run_id": resolved_run_id,
        "task_ids": markers,
        "resolved_by": resolved_by,
        "only_failed": only_failed,
        "include_downstream": include_downstream,
        "run_on_latest_version": run_on_latest_version,
        "affected": affected,
        "creates_dag_run": False,
    }
    if not affected:
        return {
            **plan,
            "planned": False,
            "error": (
                f"nothing to clear: no task instance of {task!r} in run {resolved_run_id} matches"
                + (" (only_failed is on, so a task that did not fail is not a match)" if only_failed else "")
            ),
        }
    drift, error = _version_drift(dag_id, resolved_run_id)
    if error:
        return {**plan, "planned": False, "error": error}
    if drift:
        return {
            **plan,
            "planned": False,
            "migration": drift,
            "error": (
                f"the latest Dag version does not have the same tasks as run {resolved_run_id}, so "
                f"clearing would let the scheduler add or drop task instances nobody asked about; "
                f"re-run the Dag instead"
            ),
        }
    plan["plan_token"] = _issue_token(
        "clear",
        {
            "dag_id": dag_id,
            "dag_run_id": resolved_run_id,
            "task_ids": markers,
            "only_failed": only_failed,
            "include_downstream": include_downstream,
            "run_on_latest_version": run_on_latest_version,
            "affected": _identities(affected),
        },
    )
    return plan


def apply_task_instance_clear(
    dag_id: str,
    dag_run_id: str,
    task_ids: list[Any],
    plan_token: str = "",
    only_failed: bool = True,
    include_downstream: bool = True,
    run_on_latest_version: bool = True,
) -> dict[str, Any]:
    """
    Clear the task instances previewed by plan_task_instance_clear.

    Re-runs instances that already exist. It never creates a Dag run, and there
    is no fallback that does: if nothing matches, that is the answer.

    Pass back the ``plan_token`` and the exact ``dag_run_id`` and ``task_ids``
    that were planned, so the confirmation the user clicks names what it clears.
    """
    plan = _redeem_token("clear", plan_token)
    if plan is None:
        return {
            "cleared": False,
            "mutation_applied": False,
            "error": "no reviewed plan for this clear; call plan_task_instance_clear and show the user",
        }
    asked = (dag_id, dag_run_id, task_ids, only_failed, include_downstream, run_on_latest_version)
    planned = (
        plan["dag_id"],
        plan["dag_run_id"],
        plan["task_ids"],
        plan["only_failed"],
        plan["include_downstream"],
        plan["run_on_latest_version"],
    )
    if asked != planned:
        return {
            "cleared": False,
            "mutation_applied": False,
            "error": (
                f"these are not the task instances that were planned ({planned}); re-plan and show the user"
            ),
        }

    body = _clear_body(
        dag_run_id,
        task_ids,
        dry_run=True,
        only_failed=only_failed,
        include_downstream=include_downstream,
        run_on_latest_version=run_on_latest_version,
    )
    # The preview and the clear are two calls, so state can move between them:
    # a task that started running since would otherwise be killed by an approval
    # given for a failed one.
    now = _affected(_api("POST", _dag_url(dag_id, "/clearTaskInstances"), json=body))
    if _identities(now) != plan["affected"]:
        return {
            "cleared": False,
            "mutation_applied": False,
            "dag_run_id": dag_run_id,
            "affected": now,
            "error": (
                f"what this clear would affect changed since the user reviewed it "
                f"({len(plan['affected'])} instance(s) then, {len(now)} now); re-plan and show them"
            ),
        }
    # Last thing before the write, so the window where the Dag could gain a task
    # is as small as two REST calls allow. It cannot be closed from out here —
    # the same is true of the backfill preview — but it can be this narrow.
    drift, drift_error = _version_drift(dag_id, dag_run_id)
    if drift or drift_error:
        return {
            "cleared": False,
            "mutation_applied": False,
            "dag_run_id": dag_run_id,
            "migration": drift,
            "error": drift_error
            or (
                f"the Dag's tasks changed since the user reviewed this clear ({drift}), so re-queuing "
                f"the run would now add or drop instances they never saw; re-plan and show them"
            ),
        }
    try:
        cleared = _affected(
            _api("POST", _dag_url(dag_id, "/clearTaskInstances"), json={**body, "dry_run": False})
        )
    except httpx.HTTPStatusError as e:
        return {
            "cleared": False,
            "mutation_applied": False,
            "dag_run_id": dag_run_id,
            "error": f"the clear was refused: {e.response.text or e}",
        }
    return {
        "cleared": True,
        "mutation_applied": True,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_instances": cleared,
        # True of this call. The scheduler reconciles a re-queued run against the
        # latest version afterwards, which is why the plan refuses when that
        # version's task list differs.
        "created_dag_run": False,
        "created_task_instances": [],
        "ui_updates": [
            {
                "kind": "task_instances",
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "task_ids": sorted({ti["task_id"] for ti in cleared}),
            }
        ],
    }


def compare_dag_runs(dag_id: str, run_a: str, run_b: str, source_digest: str | None = None) -> dict[str, Any]:
    """
    Compare two runs of a Dag: per-task duration changes and conf differences.

    Answers "was it my change?" after a run that used to work starts failing.
    Names the Dag versions each run used, but does not diff them — an older
    version can contain a co-located Dag this caller was never authorized for.
    ``source_digest`` is set by the caller's permissions, not by you.
    """
    # Fail closed before any of it: the caller was authorized against one exact
    # source, and this is where we find out it is still that source.
    try:
        _parsed_source(dag_id, source_digest)
    except DagFileDriftError as e:
        return {"dag_id": dag_id, "error": str(e)}
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
        # No diff of historical versions. Permission was decided over the Dags in
        # the file *now*; an older version can hold a co-located Dag since
        # removed, which nobody was ever checked against. The version numbers are
        # enough to say a change happened.
        source_diff = (
            f"not shown: comparing v{ver_a} with v{ver_b} would mean reading source this request "
            f"was not authorized for. Ask about the current source instead."
        )

    return {
        "dag_id": dag_id,
        "run_a": summaries["run_a"],
        "run_b": summaries["run_b"],
        "task_durations": task_durations,
        "conf_changes": conf_changes,
        "source_diff": source_diff,
    }


def _error_signature(log_tail: str) -> str:
    """Collapse an error message so equivalent failures land in one cluster."""
    lines = [line.strip() for line in log_tail.splitlines() if line.strip()]
    if not lines:
        return "unknown failure"
    hits = [line for line in lines if re.search(r"(?i)\b(error|exception|failed|traceback)\b", line)]
    line = (hits or lines)[-1]
    line = re.sub(r"'[^']*'", "'…'", line)
    line = re.sub(r'"[^"]*"', '"…"', line)
    line = re.sub(r"\d+", "N", line)
    return line[:200]


def find_failure_clusters(hours: float = 24, dag_ids: list[str] | None = None) -> dict[str, Any]:
    """
    Group recent task failures by error signature.

    Answers "what is breaking, fleet-wide?" — biggest clusters first, each
    with example task instances to drill into.

    ``dag_ids`` is set by the caller's permissions, not by you: whatever you pass
    is replaced with the Dags the signed-in user may actually read.
    """
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    # The batch endpoint, not the wildcard GET: only this one filters by dag_ids,
    # and filtering locally after a fleet-wide page would let failures from Dags
    # the caller cannot see push the ones they can out of the limit.
    body: dict[str, Any] = {
        "state": ["failed"],
        "start_date_gte": since,
        "page_limit": FAILURE_SCAN_LIMIT,
    }
    if dag_ids is not None:
        body["dag_ids"] = list(dag_ids)
    tis = _api("POST", "/dags/~/dagRuns/~/taskInstances/list", json=body)["task_instances"]
    # Belt and braces: never fetch a log for a Dag outside the allowlist, whatever
    # the API returned.
    if dag_ids is not None:
        allowed = set(dag_ids)
        tis = [ti for ti in tis if ti["dag_id"] in allowed]

    clusters: dict[str, dict[str, Any]] = {}
    for ti in tis:
        log = _api(
            "GET",
            _dag_url(
                ti["dag_id"],
                f"/dagRuns/{quote(ti['dag_run_id'], safe='')}/taskInstances/"
                f"{quote(ti['task_id'], safe='')}/logs/{ti['try_number']}",
            ),
        )
        signature = _error_signature(_tail(log.get("content") if isinstance(log, dict) else log))
        cluster = clusters.setdefault(signature, {"error": signature, "count": 0, "examples": []})
        cluster["count"] += 1
        if len(cluster["examples"]) < 5:
            cluster["examples"].append(
                {"dag_id": ti["dag_id"], "task_id": ti["task_id"], "dag_run_id": ti["dag_run_id"]}
            )

    return {
        "window_hours": hours,
        "failures_scanned": len(tis),
        "clusters": sorted(clusters.values(), key=lambda c: c["count"], reverse=True),
    }


def _run_identity(entry: dict[str, Any]) -> tuple[str, str | None]:
    """What makes a planned run *that* run — a matching count is not a matching plan.

    Only the date is canonicalised, and by parsing rather than by text: the two
    endpoints being compared serialise the same instant differently (``Z`` versus
    ``+00:00``).  ``partition_key`` is an opaque string and is left exactly as it
    came, so two keys that merely look similar stay distinct.
    """
    raw = entry.get("logical_date")
    if raw is None:
        return "", entry.get("partition_key")
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return str(raw), entry.get("partition_key")
    # To the instant, not the offset: the dry run can answer in the Dag's
    # timezone while the created run is read back as UTC, and 00:00+00:00 is
    # 02:00+02:00. Comparing the text would cancel a perfectly good backfill.
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat(), entry.get("partition_key")


def _same_runs(left: list[tuple[str, Any]], right: list[tuple[str, Any]]) -> bool:
    """Compare as multisets: neither endpoint promises an order."""
    return sorted(left, key=repr) == sorted(right, key=repr)


def _dry_run_backfill(dag_id: str, from_date: str, to_date: str) -> list[dict[str, Any]]:
    resp = _api(
        "POST",
        "/backfills/dry_run",
        json={"dag_id": dag_id, "from_date": from_date, "to_date": to_date},
    )
    return resp.get("backfills", [])


# What the user was shown before consenting, keyed by token: the reviewed
# backfill plan, and the paused-Dag warning that has to precede an unpause.
# In-memory and per-process, like the plugin's pending approvals.
_issued_tokens: dict[str, dict[str, Any]] = {}
_TOKEN_TTL_S = 900.0
_TOKEN_MAX = 20


def _issue_token(kind: str, payload: dict[str, Any]) -> str:
    now = time.monotonic()
    for token in [t for t, p in _issued_tokens.items() if now - p["created_at"] > _TOKEN_TTL_S]:
        del _issued_tokens[token]
    while len(_issued_tokens) >= _TOKEN_MAX:
        del _issued_tokens[next(iter(_issued_tokens))]
    token = secrets.token_urlsafe(12)
    _issued_tokens[token] = {**payload, "kind": kind, "created_at": now}
    return token


def _redeem_token(kind: str, token: str) -> dict[str, Any] | None:
    """Single-use by construction: a token can only ever be redeemed once."""
    payload = _issued_tokens.pop(token, None)
    if payload is None or payload["kind"] != kind:
        return None
    if time.monotonic() - payload["created_at"] > _TOKEN_TTL_S:
        return None
    return payload


def plan_backfill(dag_id: str, from_date: str, to_date: str) -> dict[str, Any]:
    """
    Preview the runs a backfill would create, without creating anything.

    Read-only. Show the user every run listed in ``planned_runs``, then pass the
    ``plan_token`` back to run_backfill — that is what proves the backfill you
    create is the one they reviewed.
    """
    entries = _dry_run_backfill(dag_id, from_date, to_date)
    preview = {
        "dag_id": dag_id,
        "from_date": from_date,
        "to_date": to_date,
        "planned_run_count": len(entries),
        # Every run, and both halves of its identity: a partitioned Dag has no
        # logical_date, so a dates-only list would show the user nothing at all.
        "planned_runs": [
            {"logical_date": entry.get("logical_date"), "partition_key": entry.get("partition_key")}
            for entry in entries
        ],
    }
    if len(entries) > MAX_BACKFILL_RUNS:
        # No token: the plan is beyond what may be created anyway, and issuing one
        # would authorize runs this preview is too long to have really shown.
        return {
            **preview,
            "error": (
                f"{len(entries)} runs exceeds the {MAX_BACKFILL_RUNS}-run limit for one backfill; "
                f"narrow the date range before proposing it"
            ),
        }
    plan = {
        "dag_id": dag_id,
        "from_date": from_date,
        "to_date": to_date,
        "planned_runs": [_run_identity(entry) for entry in entries],
    }
    return {**preview, "plan_token": _issue_token("backfill", plan)}


def run_backfill(
    dag_id: str,
    from_date: str,
    to_date: str,
    plan_token: str = "",
    planned_runs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Create the backfill previewed by plan_backfill.

    Pass back both the ``plan_token`` *and* the exact ``planned_runs`` list that
    plan_backfill returned. The runs go in the arguments so the confirmation the
    user clicks spells out every run it creates; the token is what proves the
    list was not invented. Refuses if either is missing or they disagree.
    """
    plan = _redeem_token("backfill", plan_token)
    if plan is None:
        return {
            "created": False,
            "mutation_applied": False,
            "error": "no reviewed plan for this backfill; call plan_backfill and show the user the result",
        }
    if (plan["dag_id"], plan["from_date"], plan["to_date"]) != (dag_id, from_date, to_date):
        return {
            "created": False,
            "mutation_applied": False,
            "error": (
                f"these arguments are not the ones planned "
                f"({plan['dag_id']} {plan['from_date']}..{plan['to_date']}); re-plan and show the user"
            ),
        }
    quoted = [_run_identity(entry) for entry in planned_runs or []]
    if not _same_runs(quoted, plan["planned_runs"]):
        return {
            "created": False,
            "mutation_applied": False,
            "error": (
                f"planned_runs must repeat the {len(plan['planned_runs'])} runs plan_backfill returned, "
                f"so the confirmation shows the user what they are approving; re-plan and pass them back"
            ),
        }
    # Re-run the dry run at the moment of execution: schedule or state drift
    # between the preview and now would silently change what gets created.
    planned = [_run_identity(entry) for entry in _dry_run_backfill(dag_id, from_date, to_date)]
    reviewed = plan["planned_runs"]
    count = len(planned)
    if not _same_runs(planned, reviewed):
        return {
            "created": False,
            "mutation_applied": False,
            "planned_run_count": count,
            "error": (
                f"the backfill changed since the user reviewed it "
                f"({len(reviewed)} runs then, {count} now); re-plan and show the user"
            ),
        }
    if count > MAX_BACKFILL_RUNS:
        return {
            "created": False,
            "mutation_applied": False,
            "planned_run_count": count,
            "error": (
                f"{count} runs exceeds the {MAX_BACKFILL_RUNS}-run limit for one backfill; "
                f"narrow the date range"
            ),
        }
    resp = _api("POST", "/backfills", json={"dag_id": dag_id, "from_date": from_date, "to_date": to_date})
    # The preview and the create are two REST calls, so they cannot be atomic from
    # out here: state can move between them. Check what actually got created and
    # cancel it if it is not what the user approved.
    created = _backfill_runs(resp["id"])
    # A slot Airflow could not fill still comes back with the planned identity, and
    # carries no dag_run_id plus a reason. Matching on identity alone would call
    # that a success, so the run has to have actually been created.
    landed = [entry for entry in created if entry.get("dag_run_id") and not entry.get("exception_reason")]
    # Identity, not arity: the same number of runs can still be different runs.
    if not _same_runs([_run_identity(entry) for entry in landed], planned):
        return _abandon_backfill(resp["id"], planned=planned, created=created)
    return {
        "created": True,
        "mutation_applied": True,
        "backfill_id": resp["id"],
        "dag_id": resp["dag_id"],
        "from_date": resp["from_date"],
        "to_date": resp["to_date"],
        "planned_run_count": count,
        "is_paused": resp.get("is_paused", False),
        # The runs are new, so no single run id names them; the Dag's run list
        # is what went stale.
        "ui_updates": [{"kind": "dag_run", "dag_id": dag_id}],
    }


def _backfill_runs(backfill_id: int) -> list[dict[str, Any]]:
    resp = _api("GET", f"/backfills/{backfill_id}/dag_runs", params={"limit": MAX_BACKFILL_RUNS + 1})
    return resp.get("backfill_dag_runs", [])


def _abandon_backfill(
    backfill_id: int, *, planned: list[tuple[Any, Any]], created: list[dict[str, Any]]
) -> dict[str, Any]:
    """Undo as much of a backfill as cancelling can, and be explicit about the rest.

    Cancelling pauses the backfill and fails its *queued* runs. It does not
    delete rows, and a run the scheduler already picked up keeps going — so the
    surviving states are reported rather than implied.
    """
    try:
        _api("PUT", f"/backfills/{backfill_id}/cancel")
        cancelled = True
    except Exception:
        cancelled = False
    survivors = []
    try:
        survivors = [
            {"dag_run_id": entry.get("dag_run_id"), "state": entry.get("dag_run_state")}
            for entry in _backfill_runs(backfill_id)
            if entry.get("dag_run_state") not in (None, "failed")
        ]
    except Exception:
        survivors = [{"dag_run_id": None, "state": "unknown — could not re-read the backfill"}]
    aftermath = "cancelled" if cancelled else "CANCELLING IT FAILED"
    if survivors:
        aftermath += f", but {len(survivors)} run(s) were already past queued and are still going"
    return {
        "created": False,
        "mutation_applied": False,
        "backfill_id": backfill_id,
        "planned_run_count": len(planned),
        "created_run_count": len(created),
        "cancelled": cancelled,
        "surviving_runs": survivors,
        "error": (
            f"the backfill did not match the {len(planned)} runs the user approved; {aftermath}. "
            f"Tell the user to check backfill {backfill_id}."
        ),
    }


def get_blast_radius(dag_id: str) -> dict[str, Any]:
    """
    Show what a failure in this Dag knocks over: the assets it produces and
    the Dags scheduled on or reading those assets — plus the upstream side,
    the assets this Dag depends on and who produces them.
    """
    assets = _api("GET", "/assets", params={"limit": 100})["assets"]

    produces: list[str] = []
    consumes: list[str] = []
    downstream: set[str] = set()
    upstream: set[str] = set()
    for asset in assets:
        producers = {task.get("dag_id") for task in asset.get("producing_tasks") or []}
        consumers = {dag.get("dag_id") for dag in asset.get("scheduled_dags") or []} | {
            task.get("dag_id") for task in asset.get("consuming_tasks") or []
        }
        if dag_id in producers:
            produces.append(asset["name"])
            downstream |= consumers
        # An asset this Dag produces is an output, even when a self-loop also
        # lists the Dag as a consumer of it.
        elif dag_id in consumers:
            consumes.append(asset["name"])
            upstream |= producers

    for bucket in (downstream, upstream):
        bucket.discard(dag_id)
        bucket.discard(None)

    return {
        "dag_id": dag_id,
        "produces_assets": sorted(produces),
        "downstream_dags": sorted(downstream),
        "consumes_assets": sorted(consumes),
        "upstream_dags": sorted(upstream),
    }


# Registered here rather than with @mcp.tool so the module keeps exporting plain
# functions — directly callable from tests.
for _tool in (
    diagnose_dag,
    compare_dag_runs,
    find_failure_clusters,
    plan_backfill,
    run_backfill,
    get_blast_radius,
    plan_dag_code_changes,
    apply_dag_code_changes,
    plan_task_instance_clear,
    apply_task_instance_clear,
    revert_dag_code,
    rerun_dag,
):
    mcp.tool(_tool)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    # Loopback only: the transport is unauthenticated and the source tools write
    # Python that Airflow then executes.  The plugin dials localhost.
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()
    mcp.run(transport="http", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
