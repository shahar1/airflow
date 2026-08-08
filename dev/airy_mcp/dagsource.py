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
The Dag file: locate it, jail it, lock it, write it, parse it.

Everything that treats a Dag as bytes on disk or as text to read: resolving the
one file a ``dag_id`` names and refusing anything outside the bundle root,
holding it for a whole read-check-write, replacing it atomically, keeping the
one-time backup, and asking the Dag processor to re-read it.  Alongside them the
scans that answer questions about that text - what the graph orders, what the
source declares and references, what a patch would remove - because those read
the same bytes and share the same regexes.

Wave 5 of the move-only extraction in ``docs/extraction-plan.md``.  It knows
nothing about task instances, runs, events or tokens; it may reach ``reading``,
``transport`` and ``primitives`` and nothing else.  The three routes it issues
itself are point reads and one write - ``GET /dags/<id>``, ``GET /dagSources/<id>``
and ``PUT /parseDagFile/<token>`` - never a paginated list, so no completeness
number ever passes through here.

The suite rebinds ``DAGS_DIR``, ``REPARSE_TIMEOUT_S`` and ``_read_reviewed_file``,
so those three are deliberately NOT re-exported from ``server.py`` - a re-export
there would be a display symbol a ``monkeypatch.setattr(server, ...)`` could
change while the real jail, the real timeout and the real read went on using the
real ones.  They are reached through this module object instead.  The names the
suite only ever reads ARE re-exported, because a name that is never rebound is
the same object either way.
"""

from __future__ import annotations

import ast
import difflib
import fcntl
import os
import re
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from hashlib import md5
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

# ``_tasks`` is rebound by the suite, so it is reached through the module object
# and never imported by name; ``_latest_version`` is not, so it is.
import reading
import transport
from primitives import _fenced, _quoted
from reading import _latest_version
from transport import _dag_url, _explain_error

DAGS_DIR = Path(os.environ.get("AIRY_MCP_DAGS_DIR", "/files/dags"))

REPARSE_TIMEOUT_S = 45.0

# The same ceiling for the same reason on the source-side checks: how many unknown
# XCom references a file holds is the Dag author's choice, and 5000 of them was
# three quarters of a megabyte of ``checks`` on its own.
STATIC_CHECK_LIMIT = int(os.environ.get("AIRY_MCP_STATIC_CHECK_LIMIT", "25"))


class DagFileError(ValueError):
    """Raised when a Dag file cannot be located or safely written."""


def _dag_path(dag_id: str, dag: dict[str, Any] | None = None) -> Path:
    """Resolve a Dag's source file, jailed to ``DAGS_DIR``.

    The path never comes from the caller — only the ``dag_id`` does — and the
    result is re-checked against the bundle root, so no traversal is possible.
    """
    relative = (dag or transport._api("GET", _dag_url(dag_id))).get("relative_fileloc")
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
    content = transport._api("GET", f"/dagSources/{quote(dag_id, safe='')}")["content"]
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
        transport._api("PUT", f"/parseDagFile/{quote(file_token, safe='')}")
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


def _display_order(tasks: reading.Reading) -> tuple[list[str], set[int]]:
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

    A SHORT task list is total ambiguity, not a smaller graph: an order over a
    subset of the tasks looks exactly as confident as one over all of them, and
    a positional resolve against it names the wrong task without saying so.
    """
    downstream = {task["task_id"]: sorted(task.get("downstream_task_ids") or []) for task in tasks.rows}
    if not tasks.complete:
        partial = sorted(downstream)
        return partial, set(range(len(partial)))
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


def _downstream_task_ids(task_id: str, edges: dict[str, list[str]]) -> list[str]:
    """Every task the graph puts downstream of this one, transitively."""
    seen = {task_id}
    queue = [task_id]
    reached: list[str] = []
    while queue:
        for nxt in edges.get(queue.pop(), ()):
            if nxt in seen:
                continue
            seen.add(nxt)
            queue.append(nxt)
            reached.append(nxt)
    return sorted(reached)


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
        graph = reading._tasks(dag_id)
    except (httpx.HTTPStatusError, KeyError) as e:
        graph = reading.failed_read(reading._TASKS_ROUTE, _explain_error(e))
    if not graph.complete:
        # Fail closed, for a short read exactly as for an unreadable one. Without
        # the WHOLE graph there is nothing to check a removal against, and "found
        # no problems" is indistinguishable from "could not look" — which is how
        # a change that orphans half a Dag gets a token.
        return {
            "removed_task_ids": None,
            "added_task_ids": sorted(_declared_task_ids(patched) - _declared_task_ids(source)),
            "limits": limits,
            "blocking": [
                f"the task graph could not be read ({graph.error}), so this change cannot be "
                f"checked against it"
                if graph.read_failed
                else f"the task graph was not read whole ({graph.reason}), so this change cannot "
                f"be checked against it"
            ],
        }
    tasks = list(graph.rows)
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
        graph = reading._tasks(dag_id)
    except (httpx.HTTPStatusError, KeyError) as e:
        graph = reading.failed_read(reading._TASKS_ROUTE, _explain_error(e))
    if not graph.complete:
        # An empty list here reads as "the patch leaves nothing unaddressed",
        # which is a claim about findings this never got to compute.
        return [
            {
                "kind": "unaddressed_findings_not_checked",
                "detail": (
                    f"whether the patched source still trips a deterministic finding was NOT "
                    f"checked: the Dag's task list was not read whole ({graph.reason})"
                ),
            }
        ]
    checks, _ = _static_checks(patched, {task["task_id"] for task in graph.rows})
    return [check for check in checks if check["kind"] != "source_graph_disagreement"]


def _build_revert_diff(path: Path, current: str, original: str) -> str:
    return "".join(
        difflib.unified_diff(
            current.splitlines(keepends=True),
            original.splitlines(keepends=True),
            f"a/{path.name}",
            f"b/{path.name}",
        )
    )


def _resolve_task(dag_id: str, task_id: str, position: int) -> tuple[str | None, str, str | None]:
    """The one task the request names — by id, or by where it sits in the graph.

    Returns ``(task_id, how_it_was_resolved, error)``. An ordinal is only
    honoured where the graph fixes the order; see ``_display_order``.
    """
    tasks = reading._tasks(dag_id)
    named = reading.find(
        tasks,
        lambda task: task.get("task_id") == task_id,
        f"no task called {task_id!r} was among the tasks this reading listed",
    )
    if task_id:
        if named.is_unknown():
            return (
                None,
                "",
                (
                    f"whether {dag_id} has a task {task_id!r} is not established: its task list was not "
                    f"read whole ({tasks.reason})"
                ),
            )
        if named.is_absent():
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
