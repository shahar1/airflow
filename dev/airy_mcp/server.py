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
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
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
# The 13-field per-instance projection is what makes a dispatch-evidence reading
# possible at all, but a 500-instance fan-out of it is ~60 KB of context. Beyond
# this many instances the rest fall back to the 4-field projection, and the count
# that fell back is reported rather than silently dropped.
TASK_INSTANCE_DETAIL_LIMIT = 200
# ``/tries`` is one HTTP call per task instance, so it is spent only on the
# successes that a live-row reading cannot settle on its own.
TRIES_PROBE_LIMIT = int(os.environ.get("AIRY_MCP_TRIES_PROBE_LIMIT", "20"))
# One dispatch finding is a paragraph of prose, and how many there are is chosen
# by whoever wrote the states being read: 500 flagged successes is 500 paragraphs
# in one tool result. Past this many the rest are folded into a single aggregate
# entry, and the fold is reported as its own ``clean_blocker`` so it is never
# silent.
DISPATCH_FINDING_LIMIT = int(os.environ.get("AIRY_MCP_DISPATCH_FINDING_LIMIT", "25"))
# The same ceiling for the same reason on the source-side checks: how many unknown
# XCom references a file holds is the Dag author's choice, and 5000 of them was
# three quarters of a megabyte of ``checks`` on its own.
STATIC_CHECK_LIMIT = int(os.environ.get("AIRY_MCP_STATIC_CHECK_LIMIT", "25"))
# The summary is the prose the model echoes verbatim, so it needs the ceiling the
# log path already has — for the same reason and in the same units.
DIAGNOSIS_SUMMARY_BUDGET_CHARS = 12000
# ``TaskInstance.operator`` is a free String(1000) the Dag author picks, and it is
# copied once per detailed row AND once per finding, so a 1000-char operator over
# 500 flagged rows put the size of the result in the author's hands.
OPERATOR_CLAMP_CHARS = 120
# How many instances the coverage prose names before it stops naming them. Named
# at all so the count is checkable; capped because the population is chosen by
# whoever wrote the states being read.
COVERAGE_NAME_LIMIT = 5

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


def _explain_unknown_dag(dag_id: str, e: httpx.HTTPStatusError) -> str | None:
    """A relayable message for a Dag the caller cannot see — or ``None`` to re-raise.

    A typo'd or unauthorized dag_id is a conversation, not a traceback: the raw
    ``httpx`` error names internal URLs and reads as a crash, and the model
    cannot relay it. 403 gets the same words as 404 on purpose — telling an
    unauthorized caller "it exists, you just can't see it" confirms the id.
    """
    if e.response.status_code in (403, 404):
        return f"Dag {dag_id!r} does not exist or you cannot see it"
    return None


def _explain_error(e: Exception) -> str:
    """An error as words a refusal can carry.

    ``str()`` of an ``httpx.HTTPStatusError`` names the internal URL it hit, so
    it must never reach a relayable string; the response body's ``detail`` is
    the API's own words for what went wrong, and the status code is the fallback.
    A transport error is the same hazard by another route — ``httpx`` builds its
    message from the URL it could not reach — so only the class name is relayed.
    """
    if isinstance(e, httpx.RequestError):
        return e.__class__.__name__
    if not isinstance(e, httpx.HTTPStatusError):
        return str(e) or e.__class__.__name__
    detail = None
    with suppress(ValueError, AttributeError):  # non-JSON body, or a shapeless one
        detail = e.response.json().get("detail")
    if isinstance(detail, str) and detail.strip():
        return detail.strip()
    if detail is not None:
        return json.dumps(detail)[:400]
    return f"HTTP {e.response.status_code}"


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
    """Replace the file, but only if it still holds what was checked.

    Written to a sibling tempfile and moved into place: the Dag processor takes
    no lock, so an in-place truncate-then-write would let it read a torn file —
    and a crash mid-write would leave one behind.
    """
    if path.read_text() != expected:
        raise DagFileDriftError(
            f"{path.name} changed while the patch was being prepared, so applying it would "
            f"overwrite an edit nobody reviewed; try again"
        )
    # The tempfile lives next to the target so the move stays inside the jail
    # (and on the same filesystem, which is what makes os.replace atomic).
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.chmod(tmp_name, path.stat().st_mode)
        os.replace(tmp_name, path)
    except BaseException:
        with suppress(OSError):
            os.unlink(tmp_name)
        raise


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


_STATIC_CHECKS_FOLDED_KIND = "static_checks_folded"


def _static_checks(source: str, task_ids: set[str]) -> tuple[list[dict[str, str]], int]:
    """Problems visible in the source and graph alone, before anything runs.

    Returns the checks and how many were folded away rather than listed.

    Only for a file that defines this Dag and nothing else. A source file can
    hold several Dags, and then every reference to a *co-located* Dag's task
    looks exactly like a reference to a task that does not exist — reporting
    those as latent blockers would send the model off fixing working code. Two
    signals say the file holds more than this Dag: a task id it declares that
    this Dag does not have, and more than one Dag being constructed. Either is
    enough to stop guessing, and the second catches a co-located Dag built
    entirely from TaskFlow tasks, which declares no task id at all.

    Both the count of entries and the bytes inside each one are the Dag author's
    to choose, so both are bounded: the ids are quoted one by one rather than
    rendered as a Python list, and the tail past the limit is folded.
    """
    strangers = sorted(_declared_task_ids(source) - task_ids)
    if strangers or len(_DAG_DEF_RE.findall(source)) > 1:
        named = ", ".join(_quoted(name) for name in strangers[:STATIC_CHECK_LIMIT])
        unnamed = max(len(strangers) - STATIC_CHECK_LIMIT, 0)
        return (
            [
                {
                    "kind": "source_graph_disagreement",
                    "detail": (
                        "the file defines more than this Dag"
                        + (f" — it also declares task_id(s) {named}" if named else "")
                        + (f" and {unnamed} more" if unnamed else "")
                        + ", so reference checks are skipped: this Dag's graph cannot say whether "
                        "another Dag's task ids are valid"
                    ),
                }
            ],
            0,
        )
    unknown = sorted(_referenced_task_ids(source) - task_ids)
    checks: list[dict[str, str]] = [
        {
            "kind": "unknown_xcom_task_id",
            "detail": (
                f"an XCom pull references task_ids={_fenced(ref)}, which is not a task in this Dag, "
                f"so the pull returns None"
            ),
        }
        for ref in unknown[:STATIC_CHECK_LIMIT]
    ]
    suppressed = max(len(unknown) - STATIC_CHECK_LIMIT, 0)
    if suppressed:
        # First, not last, for the same reason the dispatch fold is first: the
        # summary has its own budget, and the entry that speaks for all the
        # others must not be the one it drops.
        checks = [
            {
                "kind": _STATIC_CHECKS_FOLDED_KIND,
                "detail": (
                    f"{len(unknown)} XCom pull(s) in this Dag's source reference a task_id that is "
                    f"not a task in this Dag; {len(checks)} are named individually below and the "
                    f"other {suppressed} are not, so this diagnosis does not name them"
                ),
            },
            *checks,
        ]
    return checks, suppressed


def _find_import_errors(dag: dict[str, Any] | None) -> list[dict[str, str]]:
    """Import errors for this Dag's file — the classic self-healing case.

    A file that stops parsing never produces a failed run: the old Dag keeps
    running its old code and every run-based signal looks healthy. The import
    error list is the only place that failure shows up.
    """
    if not dag:
        return []
    names = {name for name in (dag.get("fileloc"), dag.get("relative_fileloc")) if name}
    if not names:
        return []
    try:
        resp = _api("GET", "/importErrors", params={"limit": 100})
        errors = resp["import_errors"]
    except (httpx.HTTPStatusError, KeyError):
        return []
    checks = []
    dag_bundle = dag.get("bundle_name")
    for entry in errors:
        # The suffix match below is by file name, and two bundles can hold a
        # file of the same name — without this, another team's stack trace
        # would be attached to this Dag. Only enforced when both sides name
        # their bundle; a missing name falls back to the name match alone.
        entry_bundle = entry.get("bundle_name")
        if dag_bundle and entry_bundle and entry_bundle != dag_bundle:
            continue
        filename = entry.get("filename") or ""
        # The stored filename may be bundle-relative while the Dag reports an
        # absolute fileloc, or the other way round; match either direction.
        if filename in names or any(
            filename.endswith(f"/{name}") or name.endswith(f"/{filename}") for name in names
        ):
            # A stack trace is text a Dag author owns end to end: it is quoted
            # and clamped like any other value this tool did not write. The tail
            # is what is kept, because the exception is at the end of it.
            trace = _quoted((entry.get("stack_trace") or "").strip()[-400:], 420)
            checks.append(
                {
                    "kind": "import_error",
                    "detail": f"the Dag's file fails to import, so new code is not being loaded: {trace}",
                }
            )
    total = resp.get("total_entries", len(errors))
    if total > len(errors):
        checks.append(
            {
                "kind": "import_errors_truncated",
                "detail": (
                    f"only the first {len(errors)} of {total} import errors were checked, so an "
                    f"import error for this Dag's file may be missing from this diagnosis"
                ),
            }
        )
    return checks


# The fields the conjunction reads. A key that is ABSENT from the response is
# never read as null: absence of the key is a different fact from a null value,
# and inferring one from the other would manufacture the finding out of nothing
# but a shape change. ``duration``/``start_date``/``end_date`` are here because
# the conjunction reads them, NOT because either is evidence on its own — see
# ``_is_never_dispatched_attempt``.
_DISPATCH_EVIDENCE_KEYS = (
    "hostname",
    "pid",
    "queued_when",
    "scheduled_when",
    "try_number",
    "duration",
    "start_date",
    "end_date",
)

_TASK_INSTANCE_DETAIL_KEYS = (
    "task_id",
    "map_index",
    "state",
    "try_number",
    "max_tries",
    "start_date",
    "end_date",
    "duration",
    "hostname",
    "pid",
    "queued_when",
    "scheduled_when",
    "operator",
)

# Said the same way whether the budget ran out or the precondition never picked
# the instance: both are "this was not looked at", and neither is a measurement.
_HISTORY_NOT_CHECKED = "attempt history was not checked for this task instance"
_HISTORY_EMPTY = "attempt history returned no attempts"
_HISTORY_PARTIAL = "the attempt history came back truncated"


def _ti_key(ti: dict[str, Any]) -> tuple[str, int]:
    return ti["task_id"], ti.get("map_index", -1)


def _ti_where(ti: dict[str, Any]) -> str:
    task_id, map_index = _ti_key(ti)
    return f"{task_id}[{map_index}]" if map_index >= 0 else task_id


# Anything spliced into prose that a Dag author or a task's own output can choose
# has to be neutralised first. ``operator`` is ``TaskInstance.operator`` — a free
# String(1000) with no key validation behind it — and a log tail is whatever the
# task printed. Either can carry newlines, or a ``(3)`` that reads as one more
# entry in the summary's numbered list.
#  ``+`` is in the safe set because a run_id carries one (``manual__\u2026+00:00``) and
# a fence makes Markdown inert anyway; the strip is against breaking *out* of it.
_PROSE_UNSAFE = re.compile(r"[^\w+./:@\[\]-]")
_FORGEABLE_NUMBERING = re.compile(r"\((\d+)\)")
# json.dumps escapes the C0 controls; these three are the line breaks it leaves.
_PROSE_LINE_BREAKS = re.compile("[\u0085\u2028\u2029]")


def _quoted(value: Any, limit: int = 120) -> str:
    """A value from outside this tool, clamped and escaped for a prose sentence.

    JSON-quoting takes the newlines out, ``(3)`` is rewritten because it would
    otherwise read as one more entry in the summary's numbered list, and the
    clamp stops a single field from filling the whole result.

    Numbers and ``None`` are rendered as themselves rather than quoted: a null
    ``pid`` written as the string ``"None"`` says the opposite of "pid is null",
    which is what the rest of this tool's prose says about the same column.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = json.dumps(_PROSE_LINE_BREAKS.sub(" ", str(value)), ensure_ascii=False)
    text = _FORGEABLE_NUMBERING.sub(r"[\1]", text)
    return text if len(text) <= limit else text[: limit - 4] + '..."'


def _fenced(text: Any, limit: int = 80) -> str:
    """An identifier spliced into prose: code-fenced, and stripped to identifier bytes.

    A bare ``task_id`` renders as Markdown emphasis wherever underscores pair up,
    and a backtick or a newline inside one would break straight back out of the
    fence, so everything that is not identifier-shaped is replaced. Used for
    run ids too: Airflow's own validators are ``$``-anchored and Python's ``$``
    matches before a trailing newline, so a run_id ending in one is accepted.
    """
    return f"`{_PROSE_UNSAFE.sub('?', str(text))[:limit]}`"


def _clamped_operator(value: Any) -> Any:
    """``TaskInstance.operator`` cut to a size the Dag author does not choose."""
    if isinstance(value, str) and len(value) > OPERATOR_CLAMP_CHARS:
        return value[: OPERATOR_CLAMP_CHARS - 1] + "\u2026"
    return value


def _is_never_dispatched_attempt(ti: dict[str, Any]) -> bool:
    """Whether this attempt carries none of the fields dispatching one writes.

    Eight legs, every one necessary and none sufficient alone.

    hostname, pid, queued_when and scheduled_when are the four fields a dispatch
    writes, and they carry the argument. ``try_number`` is the weakest leg and is
    never read as evidence on its own — a mapped instance derived from an
    up_for_reschedule sensor is created with ``try_number`` still at 0 and really
    runs (``models/dagrun.py:2206-2207,2229-2236``, ``models/taskmap.py:223,265``)
    — but for one family it is the ONLY leg doing the excluding, and that is
    worth stating plainly rather than hedging.

    That family is the scheduler's EmptyOperator fast path. It is selected at
    ``models/dagrun.py:2198-2210`` and completed at
    ``models/dagrun.py:2229-2236``, which always writes ``try_number + 1``, so
    every instance it produces is at try_number >= 1. Everything else about it is
    the forged shape: hostname empty, pid null, queued_when null, scheduled_when
    null, duration 0.0. Its start_date and end_date do come from two separate
    ``utcnow()`` calls (``models/dagrun.py:2295-2296``) and are observed ~2us
    apart, but two clock reads landing on different values is a timing accident,
    not a guarantee, so that leg is not what may be relied on here. try_number is
    the one structural exclusion for this family — categorical for it, and only
    for it, which is why the conjunction reads it in full and never reads it
    alone.

    ``duration`` and ``start_date == end_date`` are legs and *only* legs — never
    read as evidence on their own, because a genuinely executed EmptyOperator
    writes 0.0 and equal timestamps too. What they exclude is the
    triggerer-completion path: a ``start_from_trigger`` operator that reaches
    up_for_reschedule at try_number 0 is deferred at ``models/dagrun.py:2204``,
    BEFORE the update at ``models/dagrun.py:2243-2248`` writes scheduled_dttm or
    increments try_number, and ``models/trigger.py:725`` finishes it with
    ``set_state``, which writes neither hostname nor pid. That instance really
    ran, and what separates it from a state written onto a row that never
    started is structural rather than a timing coincidence: it already has a
    start_date, so ``set_state`` keeps it and computes a real duration against a
    later end_date (``models/taskinstance.py:1013-1021``), whereas a row with no
    start_date takes both timestamps from one ``current_time`` and lands on a
    duration of 0.0.

    The log endpoint stays out of it: the route synthesises "no logs" from
    ``try_number`` before it ever consults a handler (``log_reader.py:94,118``).
    """
    if ti.get("state") != "success" or "state" not in ti:
        return False
    if not all(key in ti for key in _DISPATCH_EVIDENCE_KEYS):
        return False
    started, ended = ti["start_date"], ti["end_date"]
    return (
        not ti["hostname"]
        and ti["pid"] is None
        and ti["queued_when"] is None
        and ti["scheduled_when"] is None
        and ti["try_number"] == 0
        and ti["duration"] is not None
        and not ti["duration"]
        and started is not None
        and ended is not None
        and started == ended
    )


def _tries_probe_tier(ti: dict[str, Any]) -> str | None:
    """Which attempt-history probe this success earns, or ``None`` for no call.

    Tier A confirms a live-row reading and answers "did an *earlier* attempt
    execute?". Tier B is the case no live-row reading can settle: clearing an
    instance resets only its state and max_tries, so a success written over a
    cleared row is field-for-field identical to an honest one. What the clear
    does leave behind is arithmetic — it rewrites ``max_tries`` to at least
    ``try_number`` — so ``try_number > max_tries`` proves the row has not been
    cleared since its last attempt and needs no HTTP call at all. On the common
    retries=0 Dag that excludes every success, which is what keeps the probe
    free in the case that does not need it.

    The residual window this leaves is narrow and worth naming: between a clear
    and the re-schedule that follows it, ``try_number <= max_tries`` holds and
    the archived attempt has not been duplicated yet, so a success written into
    exactly that window is read by neither disjunct. Once the row is
    re-scheduled the window shuts from both sides — the increment puts
    ``try_number`` past ``max_tries``, and if it does not, the duplicate
    ``try_number`` in ``/tries`` is there to be seen.
    """
    if not all(key in ti for key in ("state", "try_number", "max_tries")):
        return None
    if ti["state"] != "success":
        return None
    if _is_never_dispatched_attempt(ti):
        return "A"
    try_number, max_tries = ti["try_number"], ti["max_tries"]
    if isinstance(try_number, int) and isinstance(max_tries, int):
        if try_number >= 1 and max_tries >= try_number:
            return "B"
    return None


def _attempt_history(dag_id: str, run_path: str, ti: dict[str, Any]) -> dict[str, Any]:
    """Every recorded attempt of one task instance, or why they could not be read.

    Never raises: an unreadable history downgrades what the diagnosis can
    conclude, and must not take the whole diagnosis down with it. ``TypeError``
    is in the net because ``_api`` returns ``None`` for an empty body, and
    subscripting that would otherwise take the whole diagnosis down. The
    unmapped route accepts ``map_index`` as a query parameter, so one URL serves
    mapped and unmapped instances, and it is not paginated.
    """
    path = _dag_url(dag_id, f"{run_path}/taskInstances/{quote(ti['task_id'], safe='')}/tries")
    try:
        resp = _api("GET", path, params={"map_index": ti.get("map_index", -1)})
        rows = resp["task_instances"]
        total = resp.get("total_entries", len(rows))
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        return {"status": "unavailable", "rows": [], "error": _explain_error(e)}
    if not rows:
        # Never read as "there were no earlier attempts" — it is the absence of
        # an answer, not an answer of absence.
        return {"status": "empty", "rows": [], "error": _HISTORY_EMPTY}
    if total > len(rows):
        # Presence-based conclusions survive a truncated list; absence-based
        # ones do not.
        return {"status": "partial", "rows": rows, "attempts_recorded": total, "error": _HISTORY_PARTIAL}
    return {"status": "checked", "rows": rows, "attempts_recorded": total}


_DISPATCH_FINDING_KIND = "success_without_attempt_dispatch_fields"
_DISPATCH_TRUNCATED_KIND = "dispatch_findings_folded"


def _dispatch_finding(ti: dict[str, Any], history: dict[str, Any]) -> dict[str, Any] | None:
    """A success whose recorded attempt carries none of the fields a dispatch writes.

    Two independent ways in. The live row can say it on its own — none of the
    fields a dispatch writes are set. Or the attempt history can say it: a
    try_number that appears twice means the attempt was archived and never
    re-dispatched, because archiving an attempt is always followed by a
    try_number increment before anything runs again. The up_for_reschedule path
    never archives at all, so a rescheduled sensor cannot produce that duplicate.

    States what the fields say and stops there. What wrote the state is not
    visible from here and is not guessed at.

    Note for anything built on top of this: an execution-API event-history leg is
    NOT a substitute for the ``duration``/``start_date == end_date`` legs in
    ``_is_never_dispatched_attempt``. A task instance completed inside the
    triggerer emits no execution-API row either, so an event-history reading
    would flag it exactly as the six-leg conjunction did.
    """
    status = history["status"]
    rows = history.get("rows") or []
    try_number = ti.get("try_number")
    never_dispatched = _is_never_dispatched_attempt(ti)
    archived_not_redispatched = (
        ti.get("state") == "success"
        and "try_number" in ti
        and status in ("checked", "partial")
        and sum(1 for row in rows if row.get("try_number") == try_number) >= 2
    )
    if not (never_dispatched or archived_not_redispatched):
        return None

    # The live row is in ``rows`` exactly once (the route drops only up_for_retry
    # rows, which state == 'success' already excludes), so subtracting it is
    # exact rather than an estimate.
    executing_rows = [row for row in rows if row.get("hostname") or row.get("pid") is not None]
    live_has_execution = bool(ti.get("hostname")) or ti.get("pid") is not None
    executed_earlier = (len(executing_rows) - (1 if live_has_execution else 0)) >= 1
    # "A different attempt" is a claim about try_number, so it is read off
    # try_number. The archived-not-redispatched disjunct fires on a DUPLICATE
    # try_number, and ``record_ti`` dedupes by try_number
    # (models/taskinstancehistory.py:196-206), so those two rows are one attempt
    # and its archive — saying "a different attempt" there would be false in
    # exactly the case that produced the finding.
    other_attempt_executed = any(row.get("try_number") != try_number for row in executing_rows)
    if status == "checked":
        earlier_attempt_executed: bool | None = executed_earlier
    elif status == "partial" and executed_earlier:
        earlier_attempt_executed = True
    else:
        earlier_attempt_executed = None

    where = _fenced(_ti_where(ti))
    attempts_recorded = history.get("attempts_recorded")
    if never_dispatched:
        core = (
            f"{where} is recorded state=success at try_number 0, and this attempt carries none of "
            f"the fields a dispatched attempt writes: hostname is empty, pid is null, queued_when "
            f"is null and scheduled_when is null, with duration 0 and start_date equal to end_date. "
            f"Only hostname and pid are written by a worker, and a task instance that completes "
            f"entirely inside the triggerer writes none of these fields either."
        )
        if archived_not_redispatched:
            core += f" Its attempt history also already holds a separate record for try_number {try_number}."
    else:
        core = (
            f"{where} is recorded state=success at try_number {try_number}, and its attempt history "
            f"already holds a separate record for try_number {try_number}, so the execution fields "
            f"on the live row (hostname {_quoted(ti.get('hostname'))}, pid {_quoted(ti.get('pid'))}, "
            f"start_date {_quoted(ti.get('start_date'))}) were recorded before that history entry "
            f"was made and do not describe a dispatch that happened after it."
        )
    if earlier_attempt_executed is True:
        recorded = attempts_recorded if attempts_recorded is not None else len(rows)
        further = max(recorded - 1, 0)
        history_text = (
            f"Its attempt history holds {further} further record(s) for this task instance in this "
            f"run, at least one carrying execution fields."
        )
        if other_attempt_executed:
            history_text += (
                " At least one of those is at a different try_number, so this task instance did "
                "execute earlier in this run - on a different attempt than the one recorded success."
            )
    elif earlier_attempt_executed is False:
        history_text = (
            f"Its attempt history records {attempts_recorded} attempt(s) and none of them carries "
            f"execution fields."
        )
    else:
        # The reachable content here is the API's own 404 detail, which is a
        # string this tool did not write — quoted like any other.
        history_text = (
            f"Its attempt history could not be read ({_quoted(history.get('error') or status, 240)}), "
            f"so whether an earlier attempt executed is not established."
        )

    finding: dict[str, Any] = {
        "kind": _DISPATCH_FINDING_KIND,
        "detail": " ".join(
            [core, history_text, "What wrote this state is not established by this diagnosis"]
        ),
        "task_id": ti["task_id"],
        "map_index": ti.get("map_index", -1),
        # Structured, never spliced into the prose: it is a free-form String(1000)
        # the Dag author chooses, and the summary is prose the model reads out.
        # Clamped even so — one copy per finding is 500 copies in a 500-finding
        # run, which is the result's size in the author's hands.
        "operator": _clamped_operator(ti.get("operator")),
        "current_attempt_dispatched": False,
        "attempt_history": status,
        "attribution": "not established",
    }
    if attempts_recorded is not None:
        finding["attempts_recorded"] = attempts_recorded
    if status != "not_checked":
        finding["earlier_attempt_executed"] = earlier_attempt_executed
    if history.get("error"):
        finding["tries_error"] = history["error"]
    return finding


def _check_dispatch_evidence(
    dag_id: str, run_path: str, tis: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[tuple[str, int], list[str]], dict[str, Any]]:
    """Read every scanned success for signs its recorded attempt was dispatched.

    Returns the findings, the successes whose response did not carry the fields
    to decide with, and the coverage numbers. The findings are evaluated for
    every success rather than only for probed ones, so a response that keeps the
    probe from being selected still cannot hide a live-row-visible finding.
    """
    successes = sorted((ti for ti in tis if ti.get("state") == "success"), key=_ti_key)
    incomplete: dict[tuple[str, int], list[str]] = {}
    for ti in successes:
        missing = [key for key in _DISPATCH_EVIDENCE_KEYS if key not in ti]
        # max_tries is deliberately NOT a leg of the conjunction, but the
        # clear-detection arithmetic in ``_tries_probe_tier`` cannot run without
        # it. Without this, a success whose max_tries never arrived was pooled
        # with the ones that needed no probe — "couldn't tell" counted as
        # "didn't need to", and the strong sentence earned off it.
        if "max_tries" not in ti:
            missing.append("max_tries")
        elif ti["max_tries"] is None:
            missing.append("max_tries (null)")
        if missing:
            incomplete[_ti_key(ti)] = missing
    # Only hostname and pid are written by a worker: scheduled_when is the
    # scheduler's (dagrun.py:2247) and queued_when is the executor hand-off
    # (scheduler_job_runner.py:1044-1045). A success carrying neither worker
    # field satisfies no disjunct here, so it is counted rather than passed over
    # in silence — INCLUDING the case with no dispatch field of any kind, which
    # is the scheduler's EmptyOperator fast path and the single largest real
    # population of it (dagrun.py:2286-2300).
    worker_fieldless: list[str] = []
    no_dispatch_field = 0
    for ti in successes:
        if not all(key in ti for key in ("hostname", "pid", "queued_when", "scheduled_when")):
            continue
        if ti["hostname"] or ti["pid"] is not None:
            continue
        worker_fieldless.append(_ti_where(ti))
        if ti["queued_when"] is None and ti["scheduled_when"] is None:
            no_dispatch_field += 1

    tiers: dict[str, list[dict[str, Any]]] = {"A": [], "B": []}
    for ti in successes:
        tier = _tries_probe_tier(ti)
        if tier:
            tiers[tier].append(ti)
    # Tier A first: those are the instances the live row already flags, so the
    # budget must never be spent elsewhere before them.
    probed: dict[tuple[str, int], dict[str, Any]] = {}
    checked = unchecked = 0
    for position, ti in enumerate(tiers["A"] + tiers["B"]):
        history = (
            _attempt_history(dag_id, run_path, ti)
            if position < TRIES_PROBE_LIMIT
            else {"status": "not_checked", "rows": [], "error": _HISTORY_NOT_CHECKED}
        )
        probed[_ti_key(ti)] = history
        if history["status"] == "checked":
            checked += 1
        else:
            unchecked += 1

    unprobed = {"status": "not_checked", "rows": [], "error": _HISTORY_NOT_CHECKED}
    checks = []
    for ti in successes:
        finding = _dispatch_finding(ti, probed.get(_ti_key(ti), unprobed))
        if finding:
            checks.append(finding)
    suppressed = max(len(checks) - DISPATCH_FINDING_LIMIT, 0)
    if suppressed:
        listed = checks[:DISPATCH_FINDING_LIMIT]
        # First, not last: the summary has its own character budget, and the one
        # entry that speaks for all the others must not be the one it drops.
        checks = [
            {
                "kind": _DISPATCH_TRUNCATED_KIND,
                "detail": (
                    f"{len(listed) + suppressed} successful task instance(s) in this run carry no "
                    f"dispatch fields for the attempt recorded success; {len(listed)} are described "
                    f"individually below and the other {suppressed} are not, so this diagnosis does "
                    f"not name them"
                ),
            },
            *listed,
        ]
    coverage: dict[str, Any] = {
        "successes_scanned": len(successes),
        "successes_with_incomplete_evidence": len(incomplete),
        "attempt_history_checked": checked,
        "attempt_history_unchecked": unchecked,
        # Numerator and denominator over the same set: every success is in
        # exactly one of checked / unchecked / no-probe-needed / evidence
        # incomplete. "No probe needed" is a claim the row's own fields settle
        # it, so a row whose fields did not all arrive cannot be in that bucket.
        "successes_without_history_check": len(successes) - checked,
        "successes_no_probe_needed": sum(
            1 for ti in successes if _ti_key(ti) not in incomplete and _tries_probe_tier(ti) is None
        ),
        "successes_without_worker_fields": len(worker_fieldless),
        "successes_with_no_dispatch_field": no_dispatch_field,
        "successes_without_worker_fields_named": worker_fieldless[:COVERAGE_NAME_LIMIT],
        "dispatch_findings_suppressed": suppressed,
        "static_checks_suppressed": 0,
    }
    return checks, incomplete, coverage


def _project_task_instances(
    tis: list[dict[str, Any]],
    flagged: set[tuple[str, int]],
    incomplete: dict[tuple[str, int], list[str]],
) -> tuple[list[dict[str, Any]], int]:
    """Every scanned instance, in full where it matters and reduced where it does not.

    Nulls are always written out: a dropped key and a null value are the same
    bytes to a reader, and the whole reading turns on which of the two it is.

    The rows that matter are ranked rather than exempted. Exempting them put the
    size of the result in the hands of whoever wrote the states being read — a
    run full of flagged successes was a run full of 13-field rows, however low
    the limit was set — so the ranking decides the order and the limit still
    decides the count.
    """
    ordered: list[tuple[str, int]] = []
    by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for ti in tis:
        by_key.setdefault(_ti_key(ti), ti)
    unrun = {key for key, ti in by_key.items() if ti.get("state") in ("failed", "up_for_retry")}
    for group in (flagged & set(by_key), unrun - flagged, set(by_key) - unrun - flagged):
        ordered.extend(sorted(group))
    detailed = set(ordered[:TASK_INSTANCE_DETAIL_LIMIT])

    projected = []
    for ti in tis:
        key = _ti_key(ti)
        if key not in detailed:
            projected.append(
                {
                    "task_id": ti["task_id"],
                    "state": ti.get("state"),
                    "try_number": ti.get("try_number"),
                    "map_index": ti.get("map_index", -1),
                }
            )
            continue
        entry = {name: ti.get(name) for name in _TASK_INSTANCE_DETAIL_KEYS}
        entry["map_index"] = ti.get("map_index", -1)
        entry["operator"] = _clamped_operator(entry["operator"])
        if key in incomplete:
            entry["dispatch_evidence_incomplete"] = incomplete[key]
        projected.append(entry)
    return projected, len(tis) - len(detailed)


def _run_health(
    run: dict[str, Any],
    tis: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    checks: list[dict[str, Any]],
    omitted: int,
    coverage: dict[str, Any],
) -> dict[str, Any]:
    """The one place that decides whether this run may be called clean.

    Computed once and handed to both the summary and the caller, so the sentence
    the model reads out and the structured findings cannot disagree: every reason
    the strong sentence is withheld is named in ``clean_blockers``.
    """
    census: dict[str, int] = {}
    for ti in tis:
        state = ti.get("state") or "none"
        census[state] = census.get(state, 0) + 1
    non_success = sum(count for state, count in census.items() if state != "success")

    blockers = []
    if run.get("state") != "success":
        blockers.append("run_not_success")
    if failures:
        blockers.append("failed_task_instances")
    if checks:
        blockers.append("checks_present")
    if non_success:
        blockers.append("non_success_task_instances")
    if omitted:
        blockers.append("task_instances_omitted")
    if coverage["successes_with_incomplete_evidence"]:
        blockers.append("dispatch_evidence_incomplete")
    if coverage["attempt_history_unchecked"]:
        blockers.append("attempt_history_unchecked")
    # The strong sentence claims something about worker dispatch; a success with
    # neither worker-written field is exactly the case it has not tested.
    if coverage["successes_without_worker_fields"]:
        blockers.append("successes_without_worker_fields")
    if coverage["dispatch_findings_suppressed"]:
        blockers.append("dispatch_findings_suppressed")
    if coverage.get("static_checks_suppressed"):
        blockers.append("static_checks_suppressed")
    # A run with nothing in it would otherwise earn the strongest sentence there
    # is, on the strength of having looked at nothing.
    if not tis:
        blockers.append("empty_run")
    return {
        "state": run.get("state"),
        "task_instance_states": dict(sorted(census.items())),
        "successes_scanned": coverage["successes_scanned"],
        "successes_with_incomplete_evidence": coverage["successes_with_incomplete_evidence"],
        "attempt_history_checked": coverage["attempt_history_checked"],
        "attempt_history_unchecked": coverage["attempt_history_unchecked"],
        "successes_without_history_check": coverage["successes_without_history_check"],
        "successes_no_probe_needed": coverage["successes_no_probe_needed"],
        "successes_without_worker_fields": coverage["successes_without_worker_fields"],
        "successes_with_no_dispatch_field": coverage.get("successes_with_no_dispatch_field", 0),
        "successes_without_worker_fields_named": coverage.get("successes_without_worker_fields_named", []),
        "dispatch_findings_suppressed": coverage["dispatch_findings_suppressed"],
        "static_checks_suppressed": coverage.get("static_checks_suppressed", 0),
        "task_instances_omitted": omitted,
        "clean": not blockers,
        "clean_blockers": blockers,
    }


_CHECK_LABELS = {
    "unknown_xcom_task_id": "Latent blocker",
    "import_error": "Import error",
    "import_errors_truncated": "Note",
    "source_graph_disagreement": "Note",
    # Names what was observed — the absence of the fields — and not what wrote
    # the state or whether anything ran.
    _DISPATCH_FINDING_KIND: "Success with no worker-dispatch fields for the recorded attempt",
    _DISPATCH_TRUNCATED_KIND: "Note",
    _STATIC_CHECKS_FOLDED_KIND: "Note",
}

# The entries that stand for other entries rather than being one themselves, so
# the headline can count problems instead of paragraphs.
_FOLD_KINDS = (_DISPATCH_TRUNCATED_KIND, _STATIC_CHECKS_FOLDED_KIND)


def _summarize_failure(failure: dict[str, Any]) -> str:
    where = _fenced(_ti_where(failure))
    # The log line is whatever the task printed, so it is quoted and clamped like
    # any other value this tool did not write.
    # ``_extract_error_line`` already clips at 400 on a word boundary; the limit
    # here only has to leave room for the quotes and the escapes it adds.
    line = _quoted(_extract_error_line(failure.get("log_tail") or "") or "no log available", 440)
    if failure.get("still_retrying"):
        return f"Still retrying: {where} failed and is up for retry; last error: {line} (see log)."
    return f"Confirmed failure: {where} failed with {line} (see log)."


def _census_clause(health: dict[str, Any]) -> str:
    """What did not succeed, when nothing failed outright.

    A run whose state is ``success`` can still hold skipped, upstream_failed or
    removed instances, and "nothing failed" is not the same claim as "everything
    ran".
    """
    others = {state: count for state, count in health["task_instance_states"].items() if state != "success"}
    if not others:
        return ""
    listed = ", ".join(f"{count} {state}" for state, count in sorted(others.items()))
    return f" {sum(others.values())} task instance(s) did not succeed: {listed}."


def _coverage_clauses(health: dict[str, Any]) -> str:
    """What this diagnosis did not get to look at, stated rather than implied."""
    clauses = []
    if health["task_instances_omitted"]:
        clauses.append(
            f" {health['task_instances_omitted']} more task instance(s) in this run were not "
            f"scanned, so this diagnosis does not cover them."
        )
    if health["successes_with_incomplete_evidence"]:
        clauses.append(
            f" {health['successes_with_incomplete_evidence']} successful task instance(s) did not "
            f"report the fields needed to tell whether they were dispatched, or whether the row had "
            f"been cleared since its last attempt."
        )
    # Stated over the set it ranges over: a success that earned no probe used to
    # appear in neither the checked nor the unchecked count, and a coverage
    # number that leaves instances out reads as coverage they never had.
    if health["successes_scanned"]:
        clauses.append(
            f" Attempt history was read for {health['attempt_history_checked']} of "
            f"{health['successes_scanned']} successful task instance(s)."
        )
        if health["successes_no_probe_needed"]:
            clauses.append(
                f" {health['successes_no_probe_needed']} of those needed no attempt-history read: "
                f"the live row already carries dispatch fields, or try_number is past max_tries, "
                f"which rules out a clear since the last attempt."
            )
        if health["attempt_history_unchecked"]:
            clauses.append(
                f" {health['attempt_history_unchecked']} were selected for an attempt-history read "
                f"that did not come back."
            )
    if health["successes_without_worker_fields"]:
        total = health["successes_without_worker_fields"]
        bare = health["successes_with_no_dispatch_field"]
        named = health["successes_without_worker_fields_named"]
        unnamed = total - len(named)
        clauses.append(
            f" {total} successful task instance(s) carry no worker-written field (hostname empty, "
            f"pid null), so this diagnosis does not establish that a worker ran them: "
            f"{total - bare} with a scheduler or executor field set (queued_when or "
            f"scheduled_when), and {bare} with no dispatch field recorded at all."
            + (f" Named: {', '.join(_fenced(where) for where in named)}." if named else "")
            + (f" {unnamed} further such instance(s) are not named here." if unnamed else "")
        )
    if health["dispatch_findings_suppressed"]:
        clauses.append(
            f" {health['dispatch_findings_suppressed']} further finding(s) of that kind were folded "
            f"into one entry rather than listed."
        )
    if health["static_checks_suppressed"]:
        clauses.append(
            f" {health['static_checks_suppressed']} further source-reference check(s) were folded "
            f"into one entry rather than listed."
        )
    return "".join(clauses)


def _build_diagnosis_summary(
    run_id: str,
    run_state: str,
    failures: list[dict[str, Any]],
    checks: list[dict[str, Any]],
    logs_omitted: int,
    health: dict[str, Any],
) -> str:
    """One deterministic digest the model can echo, enumerating every finding.

    Built server-side because a small model reliably repeats a numbered list it
    was handed, and just as reliably drops one finding out of two it has to
    assemble from separate fields.

    The strong "No problems found" sentence is gated on ``health['clean']`` and
    on nothing else, and every finding it would have to contradict is an entry
    in the same ``checks`` list this enumerates — so there is no second
    computation for it to drift from.
    """
    items = [_summarize_failure(failure) for failure in failures]
    items += [f"{_CHECK_LABELS.get(check['kind'], 'Check')}: {check['detail']}." for check in checks]
    if not items:
        if health["clean"]:
            # Says what the conjunction actually tested. ``clean`` withholds this
            # sentence unless every success reported all the fields AND carries a
            # worker-written one, so that — and not "no forgery" — is the claim.
            return (
                f"No problems found: run {_fenced(run_id)} is {run_state}; all "
                f"{health['successes_scanned']} task instances succeeded, and every one of them "
                f"carries a worker-written dispatch field (hostname or pid) on the attempt recorded "
                f"successful.{_coverage_clauses(health)}"
            )
        return (
            f"No failures found: run {_fenced(run_id)} is {run_state}."
            f"{_census_clause(health)}{_coverage_clauses(health)}"
        )
    # The same ceiling the log path has, for the same reason: how many findings
    # there are is not this tool's choice, and one tool result should not be
    # 190k tokens of prose. The first entry always survives, so a run whose
    # findings were folded still says so.
    numbered: list[str] = []
    budget = DIAGNOSIS_SUMMARY_BUDGET_CHARS
    for position, text in enumerate(items, 1):
        entry = f"({position}) {text}"
        if numbered and len(entry) > budget:
            break
        numbered.append(entry)
        budget -= len(entry) + 1
    unlisted = len(items) - len(numbered)
    # A fold entry stands for the findings it replaced, so counting it as one
    # problem reported 26 for a run holding 500. The headline counts problems;
    # the numbering counts entries, and the fold entry says which is which.
    folded_away = health["dispatch_findings_suppressed"] + health["static_checks_suppressed"]
    fold_entries = sum(1 for check in checks if check["kind"] in _FOLD_KINDS)
    problems = len(items) - fold_entries + folded_away
    head = f"{problems} problem{'s' if problems != 1 else ''} found."
    tail = (
        f" The logs of {logs_omitted} more failed task instance(s) were omitted for size."
        if logs_omitted
        else ""
    )
    if unlisted:
        tail += f" {unlisted} further finding(s) were left out of this summary for size."
    return f"{head} {' '.join(numbered)}{tail}{_coverage_clauses(health)}"


def diagnose_dag(dag_id: str, dag_run_id: str = "", source_digest: str | None = None) -> dict[str, Any]:
    """
    Find out what is wrong with a run of this Dag.

    ``dag_run_id`` names an exact run; left empty, the first failed of the last
    5 runs is diagnosed (else the newest). Returns every task instance with the
    fields that show whether its recorded attempt was dispatched, the log tail
    of **every** failed or retrying one, the task graph, the full Dag source,
    deterministic checks that spot broken task references, import errors and
    successes carrying no dispatch evidence, a ``run_health`` that names every
    reason the run cannot be called clean, and a ``summary`` that enumerates
    every finding. Relay the ``summary`` completely — every numbered item, not
    only the first. A green run is not automatically a healthy one: report what
    ``summary`` says, not what the run state says.

    ``source_digest`` is set by the caller's permissions, not by you.
    """
    stale_note = ""
    diagnosed_is_latest: bool | None = None
    newest_run_info: dict[str, Any] | None = None
    if dag_run_id:
        run, error = _resolve_run(dag_id, dag_run_id)
        if run is None:
            return {"dag_id": dag_id, "dag_run_id": dag_run_id, "error": error}
    else:
        try:
            runs = _api("GET", _dag_url(dag_id, "/dagRuns"), params={"order_by": "-run_after", "limit": 5})[
                "dag_runs"
            ]
        except httpx.HTTPStatusError as e:
            message = _explain_unknown_dag(dag_id, e)
            if message is None:
                raise
            return {"dag_id": dag_id, "error": message}
        if not runs:
            return {
                "dag_id": dag_id,
                "diagnosis": "this Dag has never run",
                "summary": "This Dag has never run, so there is no run to diagnose.",
            }
        run = next((r for r in runs if r["state"] == "failed"), runs[0])
        newest = runs[0]
        diagnosed_is_latest = run is newest
        # The first-failed-of-last-5 fallback can pick an old failed run while a
        # fresh re-run is still in flight — and "the re-run failed again" is
        # then a false report about a run this diagnosis never looked at. Said
        # in the summary itself: a small model echoes the summary, not a flag.
        if not diagnosed_is_latest:
            newest_run_info = {"dag_run_id": newest["dag_run_id"], "state": newest["state"]}
            # A run_id is caller-chosen text: Airflow's validators are
            # ``$``-anchored and Python's ``$`` matches before a trailing
            # newline, so one ending in ``\n`` is accepted by the API and would
            # otherwise break this note across lines.
            if newest["state"] in ("queued", "running"):
                stale_note = (
                    f"Note: the newest run {_fenced(newest['dag_run_id'])} is still "
                    f"{newest['state']} — this diagnosis is of the earlier run "
                    f"{_fenced(run['dag_run_id'])}, not of the run in progress."
                )
            else:
                # A newest run that already finished (e.g. succeeded) must not be
                # spoken for by an older failure either.
                stale_note = (
                    f"Note: the newest run {_fenced(newest['dag_run_id'])} finished with state "
                    f"{newest['state']} — this diagnosis is of the EARLIER run "
                    f"{_fenced(run['dag_run_id'])}, not of that newest run."
                )
    run_path = f"/dagRuns/{quote(run['dag_run_id'], safe='')}"

    tis, omitted = _run_task_instances(dag_id, run_path)
    dispatch_checks, incomplete_evidence, coverage = _check_dispatch_evidence(dag_id, run_path, tis)
    task_instances, detail_reduced = _project_task_instances(
        tis,
        # The folded-findings entry names no instance, so it is not one.
        {(check["task_id"], check["map_index"]) for check in dispatch_checks if "task_id" in check},
        incomplete_evidence,
    )

    result: dict[str, Any] = {
        "dag_id": dag_id,
        "dag_run_id": run["dag_run_id"],
        "run_state": run["state"],
        "dag_version": _run_version(run),
        "task_instances": task_instances,
    }
    if detail_reduced:
        result["task_instance_detail_reduced"] = detail_reduced
    if diagnosed_is_latest is not None:
        result["diagnosed_run_is_latest"] = diagnosed_is_latest
    if newest_run_info:
        result["newest_run"] = newest_run_info
    if omitted:
        result["task_instances_omitted"] = omitted
    try:
        dag = _api("GET", _dag_url(dag_id))
    except httpx.HTTPStatusError:
        dag = None
    # The *parsed* source, not the file on disk. Permission to read this file was
    # granted against the Dags Airflow has parsed out of it; the live file may
    # already define one more, and handing that back would disclose a Dag nobody
    # authorized. A Dag outside the writable bundle is still worth reporting on.
    try:
        result["source"] = _parsed_source(dag_id, source_digest)
        result["source_file"] = str(_dag_path(dag_id, dag))
    except (DagFileError, OSError, httpx.HTTPStatusError, KeyError) as e:
        result.setdefault("source", f"unavailable: {_explain_error(e)}")

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
    static_checks: list[dict[str, str]] | None = None
    if tasks and isinstance(source, str) and not source.startswith("unavailable:"):
        static_checks, coverage["static_checks_suppressed"] = _static_checks(
            source, {task["task_id"] for task in tasks}
        )
    import_checks = _find_import_errors(dag)
    checks: list[dict[str, Any]] = (static_checks or []) + import_checks + dispatch_checks
    if static_checks is not None or checks:
        result["checks"] = checks

    # up_for_retry counts: the task already failed at least once, and waiting
    # for the retries to burn down before diagnosing wastes exactly the time a
    # diagnosis is for.
    failed = [ti for ti in tis if ti.get("state") in ("failed", "up_for_retry")]
    if not failed:
        result["run_health"] = _run_health(run, tis, [], checks, omitted, coverage)
        result["diagnosis"] = f"latest run is {run['state']}; no failed task instances"
        result["summary"] = _build_diagnosis_summary(
            run["dag_run_id"], run["state"], [], checks, 0, result["run_health"]
        )
        if stale_note:
            result["summary"] = f"{stale_note} {result['summary']}"
        return result

    failures = []
    budget = DIAGNOSIS_LOG_BUDGET_CHARS
    for ti in failed:
        if budget <= 0:
            break
        try:
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
        except httpx.HTTPStatusError as e:
            log = f"log unavailable: HTTP {e.response.status_code}"
        # From the end, like _tail itself: the exception and its traceback are
        # the last thing in the log, and keeping the first N characters of a
        # tail would spend the budget on the lines nobody needs.
        tail = _tail(log.get("content") if isinstance(log, dict) else log)[-budget:]
        budget -= len(tail)
        failures.append(
            {
                "task_id": ti["task_id"],
                "map_index": ti.get("map_index", -1),
                "log_tail": tail,
                "still_retrying": ti.get("state") == "up_for_retry",
            }
        )
    result["failures"] = failures
    logs_omitted = len(failed) - len(failures)
    if logs_omitted:
        result["logs_omitted"] = logs_omitted
    result["run_health"] = _run_health(run, tis, failures, checks, omitted, coverage)
    result["summary"] = _build_diagnosis_summary(
        run["dag_run_id"], run["state"], failures, checks, logs_omitted, result["run_health"]
    )
    if stale_note:
        result["summary"] = f"{stale_note} {result['summary']}"
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
                f"the task graph could not be read ({_explain_error(e)}), so this change cannot "
                f"be checked against it"
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


def _find_unaddressed_findings(dag_id: str, patched: str) -> list[dict[str, str]]:
    """The deterministic findings the patched source would still trip.

    Report-only backstop for the one-plan rule: a plan that fixes one of two
    diagnosed problems is legal — a deliberate partial fix stays approvable, so
    the token is never withheld — but the plan has to say what it leaves
    unfixed, or the model forgets the other finding until the next failed run.
    The multi-Dag disagreement note is excluded: it says the checks were
    skipped, not that a problem remains.
    """
    try:
        task_ids = {task["task_id"] for task in _tasks(dag_id)}
    except (httpx.HTTPStatusError, KeyError):
        return []
    checks, _ = _static_checks(patched, task_ids)
    return [check for check in checks if check["kind"] != "source_graph_disagreement"]


def _build_asset_note(dag_id: str) -> str:
    """The asset context an asset-touching change must carry to its approval card.

    Names only this Dag's own produced/consumed assets — never other Dags' ids,
    which only get_blast_radius is authorized to hand back.
    """
    try:
        assets = _api("GET", "/assets", params={"limit": 100})["assets"]
    except (httpx.HTTPStatusError, KeyError):
        return (
            "this change touches assets, inlets/outlets or the schedule, and the asset catalog "
            "could not be read; call get_blast_radius and tell the user what else is affected "
            "before applying"
        )
    edges = _compute_asset_edges(dag_id, assets)
    produces = ", ".join(repr(name) for name in edges["produces"]) or "no assets"
    consumes = ", ".join(repr(name) for name in edges["consumes"]) or "no assets"
    return (
        f"this change touches assets, inlets/outlets or the schedule: {dag_id} produces "
        f"{produces} and consumes {consumes}; Dags scheduled on those assets can be affected"
    )


def _build_revert_diff(path: Path, current: str, original: str) -> str:
    return "".join(
        difflib.unified_diff(
            current.splitlines(keepends=True),
            original.splitlines(keepends=True),
            f"a/{path.name}",
            f"b/{path.name}",
        )
    )


def plan_revert_dag_code(dag_id: str, source_digest: str | None = None) -> dict[str, Any]:
    """
    Preview reverting a Dag's source to the original Airy backed up, without
    writing anything.

    Read-only. Reverting discards **every** change Airy applied — not just the
    most recent one. Returns the diff that reverting would apply (the backup
    against the current file), a ``summary`` to relay, and a single-use
    ``plan_token``. Show the user the diff, then pass the token *and* the same
    ``diff`` to revert_dag_code.

    ``source_digest`` is set by the caller's permissions, not by you.
    """
    try:
        dag = _api("GET", _dag_url(dag_id))
    except httpx.HTTPStatusError as e:
        message = _explain_unknown_dag(dag_id, e)
        if message is None:
            raise
        return {"planned": False, "error": message}
    try:
        path = _dag_path(dag_id, dag)
        backup = _backup_path(path)
    except DagFileError as e:
        return {"planned": False, "error": str(e)}
    if not backup.exists():
        return {
            "planned": False,
            "error": (
                f"no backup for {path.name} — Airy has not changed this Dag, so there is nothing to revert"
            ),
        }
    try:
        current = _read_reviewed_file(dag_id, path, source_digest)
    except DagFileDriftError as e:
        return {"planned": False, "error": str(e)}
    original = backup.read_text()
    diff = _build_revert_diff(path, current, original)
    return {
        "planned": True,
        "dag_id": dag_id,
        "file": str(path),
        "diff": diff,
        "summary": (
            f"Reverting restores {path.name} to the original Airy backed up, discarding "
            f"every change Airy applied since — not just the most recent one."
        ),
        "plan_token": _issue_token(
            "revert",
            {
                "dag_id": dag_id,
                "current_digest": md5(current.encode("utf-8")).hexdigest(),
                "diff": diff,
            },
        ),
    }


def revert_dag_code(
    dag_id: str, plan_token: str = "", diff: str = "", source_digest: str | None = None
) -> dict[str, Any]:
    """
    Restore a Dag's source to the original, discarding **every** change Airy
    applied — not just the most recent one.

    Pass back both the ``plan_token`` from plan_revert_dag_code *and* the exact
    ``diff`` it returned. The diff goes in the arguments so the confirmation
    the user clicks shows exactly what reverting discards; the token is what
    proves they reviewed it.

    ``source_digest`` is set by the caller's permissions, not by you.
    """
    plan = _redeem_token("revert", plan_token)
    if plan is None:
        return {
            "reverted": False,
            "mutation_applied": False,
            "error": "no reviewed plan for this revert; call plan_revert_dag_code and show the user the diff",
        }
    if plan["dag_id"] != dag_id or plan["diff"] != diff:
        return {
            "reverted": False,
            "mutation_applied": False,
            "error": (
                "this is not the revert that was planned, so the diff the user reviewed is not "
                "this one; re-plan and show them again"
            ),
        }
    try:
        dag = _api("GET", _dag_url(dag_id))
    except httpx.HTTPStatusError as e:
        message = _explain_unknown_dag(dag_id, e)
        if message is None:
            raise
        return {"reverted": False, "mutation_applied": False, "error": message}
    try:
        path = _dag_path(dag_id, dag)
        backup = _backup_path(path)
    except DagFileError as e:
        return {"reverted": False, "mutation_applied": False, "error": str(e)}
    if not backup.exists():
        return {"reverted": False, "mutation_applied": False, "error": f"no backup for {path.name}"}
    version_before_write = _latest_version(dag_id)
    with _exclusive(path):
        try:
            current = _read_reviewed_file(dag_id, path, source_digest)
        except DagFileDriftError as e:
            return {"reverted": False, "mutation_applied": False, "error": str(e)}
        if md5(current.encode("utf-8")).hexdigest() != plan["current_digest"]:
            return {
                "reverted": False,
                "mutation_applied": False,
                "error": (
                    "the source changed since the revert was planned, so the diff the user "
                    "reviewed is stale; re-plan and show them the new one"
                ),
            }
        original = backup.read_text()
        try:
            _write_if_unchanged(path, current, original)
        except DagFileDriftError as e:
            return {"reverted": False, "mutation_applied": False, "error": str(e)}
        backup.unlink()
    version_after = version_before_write
    try:
        reparse, version_after = _force_reparse(dag_id, dag["file_token"], version_before_write)
    except Exception as e:  # the restore already landed; never raise past it
        reparse = f"file restored, but the reparse request failed: {_explain_error(e)}"
    return {
        "reverted": True,
        "mutation_applied": True,
        "file": str(path),
        "diff": _build_revert_diff(path, current, original),
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
    lands is a plan against source that no longer exists, and a second plan made
    from the *same* source is refused outright and discards the earlier plan's
    token: one repair is one plan.

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
    except httpx.HTTPStatusError as e:
        return {"planned": False, "error": _explain_unknown_dag(dag_id, e) or _explain_error(e)}
    except (DagFileError, OSError, KeyError) as e:
        return {"planned": False, "error": str(e)}

    digest = md5(source.encode("utf-8")).hexdigest()
    if _discard_same_source_plans(dag_id, digest):
        # "An earlier plan", not "your plan": the store is cross-session, so the
        # discarded plan may belong to a conversation this caller never saw.
        return {
            "planned": False,
            "error": (
                f"an earlier plan for {dag_id} from this same source was discarded; make ONE plan "
                f"containing every change"
            ),
        }

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
    asset_note = _build_asset_note(dag_id) if "asset_review_needed" in impact else None
    if asset_note:
        preview["asset_note"] = asset_note
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
    unaddressed = _find_unaddressed_findings(dag_id, patched)
    if unaddressed:
        preview["unaddressed_findings"] = unaddressed
        preview["unaddressed_note"] = (
            f"this plan leaves {len(unaddressed)} deterministic finding(s) unfixed — include a fix "
            f"in this same plan or tell the user why not"
        )
    return {
        **preview,
        "plan_token": _issue_token(
            "dag_code",
            {
                "dag_id": dag_id,
                "digest": digest,
                "changes": pairs,
                "asset_note": asset_note,
            },
        ),
    }


# Appended to apply-time drift refusals: a small model that hits one tends to
# retry the doomed apply, so the refusal itself must spell out the way back.
_REPLAN_STEER = (
    "the file changed since this plan was made. Make a NEW plan from the current source "
    "that contains every remaining change, show it, and apply that instead."
)


def apply_dag_code_changes(
    dag_id: str,
    changes: list[dict[str, str]],
    plan_token: str = "",
    asset_note: str = "",
    source_digest: str | None = None,
) -> dict[str, Any]:
    """
    Apply the edits previewed by plan_dag_code_changes — all of them, or none.

    Pass back both the ``plan_token`` *and* the exact ``changes`` list that was
    planned. The changes go in the arguments so the confirmation the user clicks
    spells out every edit it writes; the token is what proves they were planned
    against the source that is still on disk. When the plan returned an
    ``asset_note``, repeat it verbatim too — the approval must show what the
    change can knock over.

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
    if plan.get("asset_note") and asset_note != plan["asset_note"]:
        return {
            "applied": False,
            "mutation_applied": False,
            "error": (
                "this change touches assets, so the approval must show what it can knock over; "
                "pass back asset_note exactly as plan_dag_code_changes returned it"
            ),
        }

    try:
        dag = _api("GET", _dag_url(dag_id))
    except httpx.HTTPStatusError as e:
        message = _explain_unknown_dag(dag_id, e)
        if message is None:
            raise
        return {"applied": False, "mutation_applied": False, "error": message}
    path = _dag_path(dag_id, dag)
    version_before_write = _latest_version(dag_id)
    with _exclusive(path):
        try:
            source = _read_reviewed_file(dag_id, path, source_digest)
        except DagFileDriftError as e:
            return {"applied": False, "mutation_applied": False, "error": f"{e} — {_REPLAN_STEER}"}
        # The plan's impact findings were computed from these exact bytes; if
        # they still hash the same there is nothing to recompute, and if they do
        # not, no amount of recomputing makes the reviewed diff the right one.
        if md5(source.encode("utf-8")).hexdigest() != plan["digest"]:
            return {
                "applied": False,
                "mutation_applied": False,
                "error": f"the source changed since it was planned — {_REPLAN_STEER}",
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
        reparse = f"file patched, but the reparse request failed: {_explain_error(e)}"
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


_JSON_TYPE_CHECKS = {
    "null": lambda value: value is None,
    "boolean": lambda value: isinstance(value, bool),
    "integer": lambda value: isinstance(value, int) and not isinstance(value, bool),
    "number": lambda value: isinstance(value, (int, float)) and not isinstance(value, bool),
    "string": lambda value: isinstance(value, str),
    "array": lambda value: isinstance(value, list),
    "object": lambda value: isinstance(value, dict),
}


def _describe_bounds(schema: dict[str, Any]) -> str:
    """The schema's numeric bounds as words, leading space included — or ``""``.

    The same words serve the catalog and the refusal, so a model that overshoots
    a range is told the whole allowed range, not just the edge it hit.
    """
    minimum, maximum = schema.get("minimum"), schema.get("maximum")
    exclusive_min, exclusive_max = schema.get("exclusiveMinimum"), schema.get("exclusiveMaximum")
    if minimum is not None and maximum is not None and exclusive_min is None and exclusive_max is None:
        return f" between {minimum} and {maximum}"
    bounds = []
    if exclusive_min is not None:
        bounds.append(f"greater than {exclusive_min}")
    elif minimum is not None:
        bounds.append(f"at least {minimum}")
    if exclusive_max is not None:
        bounds.append(f"less than {exclusive_max}")
    elif maximum is not None:
        bounds.append(f"at most {maximum}")
    return f" {' and '.join(bounds)}" if bounds else ""


def _describe_params(params: dict[str, Any]) -> str:
    """Every trigger parameter the Dag accepts, in one relayable line."""
    parts = []
    for name, spec in sorted(params.items()):
        schema = spec.get("schema") or {} if isinstance(spec, dict) else {}
        types = schema.get("type", "any")
        if isinstance(types, list):
            types = "/".join(str(t) for t in types)
        constraint = (
            f"one of {schema['enum']}" if schema.get("enum") else f"{types}{_describe_bounds(schema)}"
        )
        default = spec.get("value") if isinstance(spec, dict) else spec
        parts.append(f"{name} ({constraint}, default {default!r})")
    return "; ".join(parts)


def _validate_conf(dag_id: str, conf: Any, params: dict[str, Any]) -> str | None:
    """Why this conf cannot trigger this Dag — or ``None`` when it can.

    Checked against the Dag's own ``params`` schema so a bad value is refused
    here, in words, instead of producing a run that fails at parse time or a
    422 the model cannot relay.
    """
    if not isinstance(conf, dict):
        return "conf must be an object of parameter values"
    if not params:
        return f"{dag_id} takes no trigger parameters, so conf must be empty"
    unknown = sorted(set(conf) - set(params))
    if unknown:
        return f"unknown conf key(s) {unknown}; {dag_id} accepts: {_describe_params(params)}"
    for name in sorted(conf):
        value = conf[name]
        spec = params[name]
        schema = spec.get("schema") or {} if isinstance(spec, dict) else {}
        enum = schema.get("enum")
        if enum and value not in enum:
            return f"conf[{name!r}] must be one of {enum}, not {value!r}"
        types = schema.get("type")
        if types:
            allowed = types if isinstance(types, list) else [types]
            checks = [_JSON_TYPE_CHECKS.get(str(t)) for t in allowed]
            if not any(check(value) for check in checks if check):
                type_names = "/".join(str(t) for t in allowed)
                return f"conf[{name!r}] must be of type {type_names}, not {type(value).__name__} ({value!r})"
        # bool is an int to Python but not to a numeric range; a non-numeric
        # value under a numeric schema was already refused by the type check.
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            out_of_range = (
                (schema.get("minimum") is not None and value < schema["minimum"])
                or (schema.get("maximum") is not None and value > schema["maximum"])
                or (schema.get("exclusiveMinimum") is not None and value <= schema["exclusiveMinimum"])
                or (schema.get("exclusiveMaximum") is not None and value >= schema["exclusiveMaximum"])
            )
            if out_of_range:
                return f"conf[{name!r}] must be{_describe_bounds(schema)}, not {value!r}"
    return None


def rerun_dag(
    dag_id: str,
    conf: dict[str, Any] | None = None,
    note: str = "",
    unpause: bool = False,
    unpause_token: str = "",
) -> dict[str, Any]:
    """
    Trigger a fresh run of a Dag on the latest code.

    ``conf`` sets the Dag's trigger parameters and is validated against the
    Dag's own params schema — unknown keys and wrong types are refused with the
    list of what the Dag accepts. Leave it out for a Dag without parameters.
    ``note`` is attached to the created run.

    A paused Dag will not run until it is unpaused, and unpausing also resumes
    its *scheduled* runs — a lasting change beyond this one run. So it cannot be
    part of a first proposal: calling this on a paused Dag returns a warning and
    an ``unpause_token``. Put the warning to the user in your own words, and only
    if they agree call again with ``unpause=True`` and that token.
    """
    try:
        dag = _api("GET", _dag_url(dag_id))
    except httpx.HTTPStatusError as e:
        message = _explain_unknown_dag(dag_id, e)
        if message is None:
            raise
        return {"triggered": False, "mutation_applied": False, "error": message}
    if conf:
        # Validated before the pause flow so a bad conf cannot burn an
        # unpause_token the user's warning was already spent on.
        details = _api("GET", _dag_url(dag_id, "/details"))
        error = _validate_conf(dag_id, conf, details.get("params") or {})
        if error:
            return {"triggered": False, "mutation_applied": False, "error": error}
    if dag["is_paused"]:
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
        run = _api(
            "POST",
            _dag_url(dag_id, "/dagRuns"),
            json={"logical_date": None, "conf": conf or {}, "note": note or "Triggered via Airy"},
        )
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
                f"triggering the run failed: {_explain_error(e)}"
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
        # A model that diagnoses right after triggering gets served the *old*
        # failed run by the fallback and reports "it failed again"; the result
        # itself has to say the outcome is not in yet.
        "next_step": (
            f"created run {run['dag_run_id']} in state {run['state']} — its outcome is not known "
            f"yet; check it after it completes (diagnose_dag with dag_run_id={run['dag_run_id']!r}) "
            f"and never assume success or failure"
        ),
        "ui_updates": [{"kind": "dag_run", "dag_id": dag_id, "dag_run_id": run["dag_run_id"]}],
    }


def _run_version(run: dict[str, Any]) -> int | None:
    """The Dag version a run executed with — the last entry is the one in effect."""
    versions = run.get("dag_versions") or []
    return versions[-1].get("version_number") if versions else None


def _resolve_run(dag_id: str, dag_run_id: str) -> tuple[dict[str, Any] | None, str | None]:
    """Turn ``latest``/``previous`` into one exact run, or confirm the exact one exists."""
    if dag_run_id in ("", "latest", "previous"):
        wanted = 2 if dag_run_id == "previous" else 1
        try:
            runs = _api(
                "GET", _dag_url(dag_id, "/dagRuns"), params={"order_by": "-run_after", "limit": wanted}
            )["dag_runs"]
        except httpx.HTTPStatusError as e:
            message = _explain_unknown_dag(dag_id, e)
            if message is None:
                raise
            return None, message
        if len(runs) < wanted:
            missing = "no previous run — it has run once at most" if wanted == 2 else "no runs"
            return None, f"{dag_id} has {missing}"
        return runs[wanted - 1], None
    try:
        return _api("GET", _dag_url(dag_id, f"/dagRuns/{quote(dag_run_id, safe='')}")), None
    except httpx.HTTPStatusError as e:
        if e.response.status_code in (403, 404):
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
        # Said in words, not as a Python tuple: this refusal is relayed to the
        # user, and a raw repr of six positional values explains nothing.
        return {
            "cleared": False,
            "mutation_applied": False,
            "error": (
                f"these are not the task instances that were planned — the plan was run "
                f"{plan['dag_run_id']!r}, task_ids {plan['task_ids']}, only_failed={plan['only_failed']}, "
                f"include_downstream={plan['include_downstream']}, "
                f"run_on_latest_version={plan['run_on_latest_version']}; re-plan and show the user"
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
    # The dry run and the clear are still two calls; compare what actually
    # cleared against what the user approved. The clear happened either way —
    # this is truthful reporting of a drifted outcome, not a rollback.
    cleared_identities = _identities(cleared)
    planned_identities = plan["affected"]
    result: dict[str, Any] = {
        "cleared": True,
        "mutation_applied": True,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_instances": cleared,
        "cleared_matches_plan": cleared_identities == planned_identities,
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
    if not result["cleared_matches_plan"]:
        result["cleared_delta"] = {
            "missing": sorted(set(planned_identities) - set(cleared_identities)),
            "extra": sorted(set(cleared_identities) - set(planned_identities)),
        }
        result["warning"] = (
            f"the clear went through, but what it affected drifted from the reviewed plan "
            f"({len(planned_identities)} instance(s) planned, {len(cleared_identities)} cleared); "
            f"tell the user about cleared_delta"
        )
    return result


def compare_dag_runs(dag_id: str, run_a: str, run_b: str, source_digest: str | None = None) -> dict[str, Any]:
    """
    Compare two runs of a Dag: per-task duration changes and conf differences.

    Answers "was it my change?" after a run that used to work starts failing.
    ``run_a``/``run_b`` take exact run ids, or ``latest``/``previous`` — so
    "compare the last two runs" is run_a="previous", run_b="latest".
    A mapped task is aggregated per task: instance count and the longest
    instance's duration. Names the Dag versions each run used, but does not
    diff them — an older version can contain a co-located Dag this caller was
    never authorized for.
    ``source_digest`` is set by the caller's permissions, not by you.
    """
    # Fail closed before any of it: the caller was authorized against one exact
    # source, and this is where we find out it is still that source.
    try:
        _parsed_source(dag_id, source_digest)
    except DagFileDriftError as e:
        return {"dag_id": dag_id, "error": str(e)}
    except httpx.HTTPStatusError as e:
        message = _explain_unknown_dag(dag_id, e)
        if message is None:
            raise
        return {"dag_id": dag_id, "error": message}
    summaries: dict[str, dict[str, Any]] = {}
    instances: dict[str, dict[str, dict[str, Any]]] = {}
    for label, requested in (("run_a", run_a), ("run_b", run_b)):
        run, error = _resolve_run(dag_id, requested)
        if run is None:
            return {"dag_id": dag_id, "error": error}
        run_id = run["dag_run_id"]
        tis, omitted = _run_task_instances(dag_id, f"/dagRuns/{quote(run_id, safe='')}")
        summaries[label] = {
            "dag_run_id": run_id,
            "state": run.get("state"),
            "duration": run.get("duration"),
            "version": _run_version(run),
            "conf": run.get("conf") or {},
        }
        if omitted:
            summaries[label]["task_instances_omitted"] = omitted
        # Mapped instances aggregate to one row per task — the longest instance,
        # not whichever map_index the API listed last.
        per_task: dict[str, dict[str, Any]] = {}
        for ti in tis:
            info = per_task.setdefault(ti["task_id"], {"count": 0, "duration": None})
            info["count"] += 1
            duration = ti.get("duration")
            if duration is not None and (info["duration"] is None or duration > info["duration"]):
                info["duration"] = duration
        instances[label] = per_task

    task_durations = []
    for task_id in sorted(set(instances["run_a"]) | set(instances["run_b"])):
        info_a = instances["run_a"].get(task_id, {"count": 0, "duration": None})
        info_b = instances["run_b"].get(task_id, {"count": 0, "duration": None})
        a, b = info_a["duration"], info_b["duration"]
        entry = {
            "task_id": task_id,
            "run_a": a,
            "run_b": b,
            "delta": round(b - a, 3) if a is not None and b is not None else None,
        }
        if max(info_a["count"], info_b["count"]) > 1:
            entry["run_a_instances"] = info_a["count"]
            entry["run_b_instances"] = info_b["count"]
            entry["aggregation"] = "count of mapped instances; duration is the longest instance's"
        task_durations.append(entry)

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


def _clip_at_word(text: str, limit: int) -> str:
    """Clip to the limit at a word boundary, marking the cut with an ellipsis."""
    if len(text) <= limit:
        return text
    cut = text[:limit]
    head, _, _ = cut.rpartition(" ")
    return f"{head or cut}…"


def _extract_error_line(log_tail: str) -> str:
    """The one log line that names the failure, reduced to the exception itself."""
    lines = [line.strip() for line in log_tail.splitlines() if line.strip()]
    if not lines:
        return ""
    hits = [line for line in lines if re.search(r"(?i)\b(error|exception|failed|traceback)\b", line)]
    line = (hits or lines)[-1]
    # Airflow task logs are structured JSON lines; the exception lives in
    # error_detail, not in the raw line the keyword match found.
    try:
        record = json.loads(line)
    except ValueError:
        return line[:200]
    if isinstance(record, dict):
        for detail in record.get("error_detail") or []:
            if isinstance(detail, dict) and detail.get("exc_type"):
                # A wider cap than the raw-line one, and cut between words: the
                # exception value is already the precise message, and clipping
                # it mid-sentence loses exactly the instructions it carries.
                return _clip_at_word(f"{detail['exc_type']}: {detail.get('exc_value', '')}", 400)
        event = record.get("event")
        if isinstance(event, str) and event:
            return event[:200]
    return line[:200]


def _error_signature(log_tail: str) -> str:
    """Collapse an error message so equivalent failures land in one cluster."""
    line = _extract_error_line(log_tail)
    if not line:
        return "unknown failure"
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
    resp = _api("POST", "/dags/~/dagRuns/~/taskInstances/list", json=body)
    tis = resp["task_instances"]
    # What the window really held, minus the page that was read: a truncated
    # scan must say so, or "3 clusters" quietly means "of the 50 I looked at".
    failures_omitted = max(resp.get("total_entries", len(tis)) - len(tis), 0)
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
            # The log route defaults to map_index=-1 — a different instance from
            # a mapped one, whose failure would then be signed by the wrong log.
            params={"map_index": ti.get("map_index", -1)},
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
        "failures_omitted": failures_omitted,
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


def _discard_same_source_plans(dag_id: str, digest: str) -> bool:
    """Drop any live dag_code plan computed from these exact bytes, and say so.

    Two plans from the same source are the split-repair anti-pattern: the first
    apply bumps the Dag version, so the second approval is doomed before the
    user ever sees it. The older token is popped as well — the refusal makes the
    model re-plan, and the stale first apply must not land under it.
    """
    now = time.monotonic()
    stale = [
        token
        for token, payload in _issued_tokens.items()
        if payload["kind"] == "dag_code"
        and payload["dag_id"] == dag_id
        and payload["digest"] == digest
        and now - payload["created_at"] <= _TOKEN_TTL_S
    ]
    for token in stale:
        del _issued_tokens[token]
    return bool(stale)


def plan_backfill(dag_id: str, from_date: str, to_date: str) -> dict[str, Any]:
    """
    Preview the runs a backfill would create, without creating anything.

    Read-only. Show the user every run listed in ``planned_runs``, then pass the
    ``plan_token`` back to run_backfill — that is what proves the backfill you
    create is the one they reviewed.
    """
    try:
        entries = _dry_run_backfill(dag_id, from_date, to_date)
    except httpx.HTTPStatusError as e:
        message = _explain_unknown_dag(dag_id, e)
        if message is None:
            raise
        return {"dag_id": dag_id, "from_date": from_date, "to_date": to_date, "error": message}
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
    try:
        entries = _dry_run_backfill(dag_id, from_date, to_date)
    except httpx.HTTPStatusError as e:
        message = _explain_unknown_dag(dag_id, e)
        if message is None:
            raise
        return {"created": False, "mutation_applied": False, "error": message}
    planned = [_run_identity(entry) for entry in entries]
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


def _compute_asset_edges(dag_id: str, assets: list[dict[str, Any]]) -> dict[str, list[str]]:
    """This Dag's place in the asset graph, one hop out in both directions."""
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
        "produces": sorted(produces),
        "consumes": sorted(consumes),
        "downstream": sorted(downstream),
        "upstream": sorted(upstream),
    }


def get_blast_radius(dag_id: str) -> dict[str, Any]:
    """
    Show what a failure in this Dag knocks over: the assets it produces and
    the Dags scheduled on or reading those assets — plus the upstream side,
    the assets this Dag depends on and who produces them.
    """
    try:
        assets = _api("GET", "/assets", params={"limit": 100})["assets"]
    except httpx.HTTPStatusError as e:
        if e.response.status_code in (403, 404):
            return {
                "dag_id": dag_id,
                "error": (
                    f"the asset catalog could not be read (HTTP {e.response.status_code}), "
                    f"so the blast radius of {dag_id} is unknown"
                ),
            }
        raise

    edges = _compute_asset_edges(dag_id, assets)
    return {
        "dag_id": dag_id,
        "produces_assets": edges["produces"],
        "downstream_dags": edges["downstream"],
        "consumes_assets": edges["consumes"],
        "upstream_dags": edges["upstream"],
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
    plan_revert_dag_code,
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
