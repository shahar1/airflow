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
The boundary: every bounded read, and every clamp that bounds one.

Each function here issues a list-shaped read against Airflow, follows its pages,
applies whatever ceiling this server puts on it, and hands back the rows
together with whatever the route said about how many there are.  The scan, page
and clamp constants live here too, next to the reads they bound, so how far a
read got and how far it was allowed to get are never in two different files.

Wave 4 of the move-only extraction in ``docs/extraction-plan.md``.  Nothing in
this module knows what a diagnosis, a plan, a token or a clear is; it may reach
``transport`` and ``primitives`` and nothing else.

The suite rebinds ``_tasks``, ``_run_task_instances``, ``_expandable_probe`` and
seven of the bounds, so those are deliberately NOT re-exported from
``server.py`` - a re-export there would be a display symbol a
``monkeypatch.setattr(server, ...)`` could change while the real read went on
using the real one.  They are reached through this module object instead, which
is the one binding every call site shares.  The names the suite only ever reads
ARE re-exported, because a name that is never rebound is the same object either
way.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, NoReturn
from urllib.parse import quote

import httpx
import transport
from primitives import (
    _carries_execution_fields,
    _clamped_operator,
    _later_than,
    _quoted,
)
from transport import (
    _api_detail,
    _dag_url,
    _explain_error,
    _explain_unknown_dag,
)

FAILURE_SCAN_LIMIT = 50
LOG_TAIL_LINES = 40
LOG_TAIL_CHARS = 4000
MAX_BACKFILL_RUNS = int(os.environ.get("AIRY_MCP_MAX_BACKFILL_RUNS", "50"))
# ``taskInstances`` pages, and a silently short list would let a diagnosis miss a
# failure or a clear compare the wrong task set. Asking for a bigger page does
# not help — ``[api] maximum_page_limit`` (100) clamps it — so the pages are
# followed, up to a ceiling, and whatever is still missing is reported.
TASK_INSTANCE_PAGE = 100
TASK_INSTANCE_SCAN_LIMIT = 500
# ``GET /eventLogs`` clamps ``limit`` to 100 (verified live: limit=1000 returned
# 100 rows of 1378), so asking for more does not help and the pages are followed.
EVENT_SCAN_PAGE = 100
# At most three HTTP calls per diagnosis whatever the Dag's size. The scan is
# ordered ``-when``, so what a truncation drops is always the OLDEST rows and a
# per-instance "latest event" stays correct under it — only ABSENCE becomes
# unreliable, which is what the ``partial`` status exists to say.
EVENT_SCAN_LIMIT = int(os.environ.get("AIRY_MCP_EVENT_SCAN_LIMIT", "300"))
RUN_HISTORY_LIMIT = int(os.environ.get("AIRY_MCP_RUN_HISTORY_LIMIT", "10"))
TASK_COMPARISON_LIMIT = int(os.environ.get("AIRY_MCP_TASK_COMPARISON_LIMIT", "5"))
# One attempt's log, in the plan and again in the verification. Small: it is
# carried per instance and the model reads every byte of it.
RECOVERY_LOG_TAIL_CHARS = 600
RECOVERY_ATTEMPT_LIMIT = 10
# ``/dagVersions`` pages like everything else; one page settles whether the
# run's own version is still listed for every Dag this demo has.
DAG_VERSION_SCAN = 100

_HISTORY_EMPTY = "attempt history returned no attempts"
_HISTORY_PARTIAL = "the attempt history came back truncated"

_RECOVERY_LOG_CAVEAT = (
    "The log route synthesises its 'no logs available' answer from try_number before it consults a "
    "handler, so an empty log is not evidence that nothing ran."
)
_NO_LOGS_MARKER = "no logs available"

# The 404 detail ``/listMapped`` answers with when the task is genuinely not
# expandable. Matched on, rather than on the bare status, because a 404 also
# covers "no such task" and "no such run" — neither of which settles anything.
_LIST_MAPPED_NOT_MAPPED = "is not mapped"

_XCOM_NOT_PERMITTED = "XCom records are not readable by the signed-in user"
_XCOM_NOT_SCOPED = "no XCom scope was supplied, so the instance's output records were not read"

_TASK_COMPARISON_KEYS = (
    "dag_run_id",
    "map_index",
    "state",
    "try_number",
    "duration",
    "hostname",
    "pid",
    "queued_when",
    "scheduled_when",
    "start_date",
    "end_date",
)

_TASK_COMPARISON_SELECTION = (
    "task instances this diagnosis flagged, then failed or retrying ones, newest-first, capped at "
    f"{TASK_COMPARISON_LIMIT} task ids"
)

_HISTORY_CLAMPED = (
    f"the page held more attempts than this reading keeps ({RECOVERY_ATTEMPT_LIMIT}), so the "
    f"attempts below are not the whole history and an absence among them is not an absence"
)


# ---------------------------------------------------------------------------
# The type every bounded read hands back, and the three-valued answer that is
# the only way from rows to a negative.
#
# Completeness used to survive as a bare int, a bare str, a bare bool, a tuple
# slot and a dict key nobody read, derived longhand at nine sites in four
# mutually incompatible forms.  There is one derivation now and it is a property
# of the only type that carries rows, so a consumer that holds rows cannot hold
# a second number to compare them against.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Reading:
    """Rows that were kept, and whether they are all of them.

    Three numbers, because three different things truncate a list: the route's
    own paging (``_claimed`` above ``_delivered``), this tool's clamps (``kept``
    below ``_delivered``), and a source whose count is lower than what it handed
    over.  ``_pages``/``_exhausted`` are the pagination evidence — how many calls
    were made and whether the loop reached a natural end rather than a ceiling.

    ``error`` is set only when the read did not happen: a read that happened and
    came back short is incomplete, not failed, and the two are different answers
    to "why can nothing be concluded from this".
    """

    rows: tuple[Mapping[str, Any], ...] = ()
    route: str = ""
    _delivered: int = 0
    _claimed: int | None = None
    _pages: int = 1
    _exhausted: bool = True
    error: str | None = None

    @property
    def kept(self) -> int:
        """How many rows a conclusion may be drawn over."""
        return len(self.rows)

    @property
    def universe(self) -> int:
        """The largest number of rows anyone involved accounted for.

        A read that discarded rows is truncated whatever the source said its
        total was, and a source that accounts for more than it sent is truncated
        whatever this tool did with the rows.
        """
        claimed = self._claimed
        return max(self._delivered, claimed) if isinstance(claimed, int) else self._delivered

    @property
    def complete(self) -> bool:
        """Whether this reading holds every record anything involved accounts for.

        THE one completeness derivation in the tree.  It is a property rather
        than a free function so that there is no version of it a caller could
        invoke with the wrong three numbers.
        """
        if self.error is not None:
            return False
        return self.kept >= self.universe

    @property
    def read_failed(self) -> bool:
        """Whether the read did not happen at all, as opposed to coming back short."""
        return self.error is not None

    @property
    def omitted(self) -> int:
        """How many records this reading did not look at."""
        return max(self.universe - self.kept, 0)

    @property
    def reason(self) -> str:
        """Why nothing may be concluded from an absence here — empty when complete."""
        if self.error is not None:
            return f"{self.route or 'the read'} could not be read ({self.error})"
        if self.complete:
            return ""
        claimed = self._claimed
        if isinstance(claimed, int) and claimed > self._delivered:
            source = f"the route accounted for {claimed} and handed over {self._delivered}"
        elif self.kept < self._delivered:
            source = f"{self._delivered} row(s) came back and this reading keeps {self.kept}"
        else:
            source = f"{self.kept} of {self.universe} row(s) were read"
        pages = f"; {self._pages} page(s) were followed" if self._pages > 1 else ""
        ceiling = "" if self._exhausted else "; the scan stopped at its own ceiling"
        return f"{source}{pages}{ceiling}"

    def describe(self) -> str:
        """The sentence a refusal or an unestablished check carries."""
        if self.complete:
            return f"{self.route or 'the read'} was read whole ({self.kept} row(s))"
        return f"{self.route or 'the read'} was NOT read whole: {self.reason}"

    def clamp(self, limit: int) -> Reading:
        """A clamp is a method, never a slice: completeness is recomputed.

        Slicing ``.rows`` yields a plain tuple, which neither ``find`` nor
        ``none_match`` accepts — so a caller that clamps by slicing loses the
        ability to draw any conclusion at all, which is the correct incentive.
        """
        if limit >= self.kept:
            return self
        return replace(self, rows=self.rows[:limit])

    def filter(self, keep: Callable[[Mapping[str, Any]], bool]) -> Reading:
        """A discard is a method too — dropped rows reduce kept, exactly like a clamp."""
        return replace(self, rows=tuple(row for row in self.rows if keep(row)))

    def project(self, shape: Callable[[Mapping[str, Any]], Mapping[str, Any]]) -> Reading:
        """Reshape every kept row one-for-one. Row count does not change, so completeness does not."""
        return replace(self, rows=tuple(shape(row) for row in self.rows))

    def failed(self, error: str) -> Reading:
        """The same route, read to nothing, with the reason it could not be read."""
        return replace(self, rows=(), _delivered=0, _claimed=None, error=error)


def read_of(
    resp: Any,
    key: str,
    route: str,
    *,
    pages: int = 1,
    exhausted: bool = True,
    delivered: int | None = None,
) -> Reading:
    """One list-shaped response body as a ``Reading``.

    ``delivered`` is passed only by a paginating caller, which has accumulated
    more rows than the last page holds.
    """
    body = resp if isinstance(resp, dict) else {}
    rows = body.get(key) or []
    if not isinstance(rows, list):
        rows = []
    kept = tuple(row for row in rows if isinstance(row, dict))
    claimed = body.get("total_entries")
    return Reading(
        rows=kept,
        route=route,
        _delivered=len(kept) if delivered is None else delivered,
        _claimed=claimed if isinstance(claimed, int) and not isinstance(claimed, bool) else None,
        _pages=pages,
        _exhausted=exhausted,
    )


def failed_read(route: str, error: str) -> Reading:
    """A read that did not happen, named so a refusal can say which one."""
    return Reading(route=route, error=error)


class Outcome(Enum):
    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class Verdict:
    """A three-valued answer about one question asked of one ``Reading``.

    ``UNKNOWN`` is deliberately hard to misuse: there is no truthiness, and the
    only conversion to a JSON value returns ``None`` for it.  ``False`` is a
    claim about the world and it has to be earned by a read that was whole.
    """

    outcome: Outcome
    row: Mapping[str, Any] | None = None
    route: str = ""
    why: str = ""

    def __bool__(self) -> NoReturn:
        raise TypeError(
            "a Verdict is three-valued; use .as_field(), .is_present()/.is_absent()/.is_unknown(), "
            "or match on .outcome"
        )

    def as_field(self) -> bool | None:
        """The ONLY conversion to a JSON value. UNKNOWN is None, never False."""
        return {Outcome.PRESENT: True, Outcome.ABSENT: False}.get(self.outcome)

    def is_present(self) -> bool:
        return self.outcome is Outcome.PRESENT

    def is_absent(self) -> bool:
        return self.outcome is Outcome.ABSENT

    def is_unknown(self) -> bool:
        return self.outcome is Outcome.UNKNOWN

    def detail(self) -> str:
        """The sentence an unestablished answer carries, naming the read that fell short."""
        if self.outcome is not Outcome.UNKNOWN:
            return ""
        route = self.route or "the read behind it"
        return f"{self.why} — {route} was not read whole, so an absence among the rows read is not an absence"


def find(reading: Reading, question: Callable[[Mapping[str, Any]], bool], why: str = "") -> Verdict:
    """Is there a row satisfying this question?

    PRESENT is returned before completeness is ever consulted: presence-based
    conclusions survive a truncated list, absence-based ones do not.
    """
    hit = next((row for row in reading.rows if question(row)), None)
    if hit is not None:
        return Verdict(Outcome.PRESENT, row=hit, route=reading.route)
    if not reading.complete:
        return Verdict(Outcome.UNKNOWN, route=reading.route, why=why or reading.reason)
    return Verdict(Outcome.ABSENT, route=reading.route)


def none_match(reading: Reading, question: Callable[[Mapping[str, Any]], bool], why: str = "") -> Verdict:
    """Is the matching set empty? The inverse claim, behind the same guard.

    PRESENT means the claim holds and the whole universe was examined; ABSENT
    means a counterexample row was found, which is a presence and so survives
    truncation; UNKNOWN means nothing matched but the list was not read whole.
    """
    hit = next((row for row in reading.rows if question(row)), None)
    if hit is None and not reading.complete:
        return Verdict(Outcome.UNKNOWN, route=reading.route, why=why or reading.reason)
    if hit is None:
        return Verdict(Outcome.PRESENT, route=reading.route)
    return Verdict(Outcome.ABSENT, row=hit, route=reading.route)


@dataclass(frozen=True, slots=True)
class Clipped:
    """Text that was cut to a size this tool chose, and whether it was cut.

    The field-level sibling of ``Reading``: a log tail that returned a bare
    ``str`` let every consumer read a truncated log as the whole of one.
    """

    text: str
    truncated: bool = False

    def __str__(self) -> str:
        return self.text


def _latest_version(dag_id: str) -> int | None:
    versions = transport._api(
        "GET", _dag_url(dag_id, "/dagVersions"), params={"order_by": "-version_number", "limit": 1}
    )["dag_versions"]
    return versions[0]["version_number"] if versions else None


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
        resp = transport._api(
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


def _tasks_reading(dag_id: str) -> tuple[list[dict[str, Any]], int]:
    """The current tasks and their edges, with the count the route accounted for.

    ``/tasks`` is the only *public* route that carries ``downstream_task_ids``;
    the richer structure view lives under ``/ui`` and is not part of the API this
    server is allowed to speak. The total comes back so a caller that reasons
    about the task set being COMPLETE can say whether it read all of it.
    """
    resp = transport._api("GET", _dag_url(dag_id, "/tasks"))
    rows = resp["tasks"]
    return rows, resp.get("total_entries", len(rows))


def _tasks(dag_id: str) -> list[dict[str, Any]]:
    """The current tasks and their edges."""
    return _tasks_reading(dag_id)[0]


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
        resp = transport._api("GET", "/importErrors", params={"limit": 100})
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
        resp = transport._api("GET", path, params={"map_index": ti.get("map_index", -1)})
        rows = resp["task_instances"]
        total = resp.get("total_entries", len(rows))
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        return {"status": "unavailable", "rows": [], "error": _explain_error(e)}
    if not rows:
        # Never read as "there were no earlier attempts" — it is the absence of
        # an answer, not an answer of absence. ``attempts_recorded`` still comes
        # back, because a page that carries no rows while accounting for some is
        # a truncated read wearing the same word, and a caller deciding whether
        # it read the whole history has to be able to tell the two apart.
        return {"status": "empty", "rows": [], "attempts_recorded": total, "error": _HISTORY_EMPTY}
    if total > len(rows):
        # Presence-based conclusions survive a truncated list; absence-based
        # ones do not.
        return {"status": "partial", "rows": rows, "attempts_recorded": total, "error": _HISTORY_PARTIAL}
    return {"status": "checked", "rows": rows, "attempts_recorded": total}


def _task_comparison(dag_id: str, runs: list[dict[str, Any]], task_ids: list[str]) -> dict[str, Any]:
    """The same task's rows across the runs in the window.

    One call per compared task id, and none at all when there is nothing to
    compare. The fleet-scoped batch route is deliberately not used: it is
    ``dag_id`` ``Literal["~"]`` AND carries ``Depends(action_logging())``, so it
    reads outside the per-Dag boundary the plugin enforces and writes an audit
    row into the very table this tool reads as evidence.
    """
    compared = task_ids[:TASK_COMPARISON_LIMIT]
    result: dict[str, Any] = {
        "selection": _TASK_COMPARISON_SELECTION,
        "task_ids_compared": compared,
        "task_ids_omitted": max(len(task_ids) - len(compared), 0),
        "runs_not_covered": [],
        "tasks": {},
        # Per task id, how many rows the route said it held beyond the page that
        # came back. Never read before, so a task with more history than
        # RUN_HISTORY_LIMIT rows was silently reported as if the page were all
        # of it.
        "rows_omitted": {},
        "error": None,
    }
    if not compared or not runs:
        return result
    window = {run["dag_run_id"] for run in runs}
    oldest = runs[-1].get("run_after")
    tasks: dict[str, list[dict[str, Any]]] = {}
    omitted: dict[str, int] = {}
    covered: set[str] = set()
    try:
        for task_id in compared:
            params: dict[str, Any] = {
                "task_id": task_id,
                "order_by": "-run_after",
                "limit": RUN_HISTORY_LIMIT,
            }
            if oldest:
                params["run_after_gte"] = oldest
            resp = transport._api("GET", _dag_url(dag_id, "/dagRuns/~/taskInstances"), params=params)
            returned = resp["task_instances"]
            omitted[task_id] = max(resp.get("total_entries", len(returned)) - len(returned), 0)
            rows = []
            for row in returned:
                if row.get("dag_run_id") not in window:
                    continue
                covered.add(row["dag_run_id"])
                entry = {name: row.get(name) for name in _TASK_COMPARISON_KEYS}
                entry["operator"] = _clamped_operator(row.get("operator"))
                rows.append(entry)
            tasks[task_id] = rows
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        result["error"] = _explain_error(e)
        return result
    result["tasks"] = tasks
    result["rows_omitted"] = omitted
    result["runs_not_covered"] = [run["dag_run_id"] for run in runs if run["dag_run_id"] not in covered]
    return result


def _recent_runs(dag_id: str) -> tuple[list[dict[str, Any]], int]:
    """The newest runs of this Dag. Raises — the callers differ on what a failure means.

    On the run-resolving path a failure must not be turned into "this Dag has
    never run"; on the exact-run path it only costs the history field.
    """
    resp = transport._api(
        "GET", _dag_url(dag_id, "/dagRuns"), params={"order_by": "-run_after", "limit": RUN_HISTORY_LIMIT}
    )
    runs = resp["dag_runs"]
    return runs, resp.get("total_entries", len(runs))


def _build_asset_note(dag_id: str) -> str:
    """The asset context an asset-touching change must carry to its approval card.

    Names only this Dag's own produced/consumed assets — never other Dags' ids,
    which only get_blast_radius is authorized to hand back.
    """
    try:
        assets = transport._api("GET", "/assets", params={"limit": 100})["assets"]
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


def _resolve_run(dag_id: str, dag_run_id: str) -> tuple[dict[str, Any] | None, str | None]:
    """Turn ``latest``/``previous`` into one exact run, or confirm the exact one exists."""
    if dag_run_id in ("", "latest", "previous"):
        wanted = 2 if dag_run_id == "previous" else 1
        try:
            runs = transport._api(
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
        return transport._api("GET", _dag_url(dag_id, f"/dagRuns/{quote(dag_run_id, safe='')}")), None
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            # Collapsing this into "no such run" made an authorization failure
            # read as a typo, and a caller that believes the run is absent looks
            # for another one instead of reporting what happened. The Dag itself
            # is already known to this caller by the time a run under it is
            # fetched, so saying which of the two it was leaks nothing new.
            return None, (
                f"the run {dag_run_id!r} of {dag_id} could not be read (HTTP 403); this is a "
                f"permission refusal, not evidence that the run does not exist"
            )
        if e.response.status_code == 404:
            return None, (
                f"{dag_id} has no run {dag_run_id!r}. Run ids are exact, including the UTC offset "
                f"— pass 'latest' or 'previous' instead of composing one"
            )
        raise


def _attempt_log(dag_id: str, run_path: str, ti: dict[str, Any], try_number: Any) -> dict[str, Any]:
    """The log the API holds for one attempt of one instance, clamped, never raising."""
    entry: dict[str, Any] = {"try_number": try_number, "caveat": _RECOVERY_LOG_CAVEAT}
    if not isinstance(try_number, int):
        return {**entry, "status": "unavailable", "error": "the row carries no try_number to read a log for"}
    try:
        resp = transport._api(
            "GET",
            _dag_url(
                dag_id,
                f"{run_path}/taskInstances/{quote(ti['task_id'], safe='')}/logs/{try_number}",
            ),
            params={"map_index": ti.get("map_index", -1)},
        )
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        return {**entry, "status": "unavailable", "error": _explain_error(e)}
    tail = _tail(resp.get("content") if isinstance(resp, dict) else resp)[-RECOVERY_LOG_TAIL_CHARS:]
    stripped = tail.strip()
    if not stripped:
        status = "empty"
    elif _NO_LOGS_MARKER in stripped.lower():
        # The route's own sentence, not the task's. Reported as its own status so
        # nothing downstream mistakes it for output the task produced.
        status = "no_logs_reported"
    else:
        status = "present"
    return {**entry, "status": status, "tail": tail}


def _attempt_rows(history: dict[str, Any]) -> list[dict[str, Any]]:
    """The attempts ``/tries`` returned, reduced to the fields this reading uses."""
    return [
        {
            "try_number": row.get("try_number"),
            "state": row.get("state"),
            "hostname": row.get("hostname"),
            "pid": row.get("pid"),
            "duration": row.get("duration"),
            "start_date": row.get("start_date"),
            "end_date": row.get("end_date"),
        }
        for row in (history.get("rows") or [])[:RECOVERY_ATTEMPT_LIMIT]
    ]


def _attempt_reading(history: dict[str, Any]) -> dict[str, Any]:
    """The attempt history as this reading actually READ it, status included.

    The local clamp is part of the read, so it has to be part of the status.
    ``_attempt_history`` honestly reports ``checked`` over a whole page;
    ``_attempt_rows`` then keeps at most ``RECOVERY_ATTEMPT_LIMIT`` of it, and
    every absence-based leg downstream keys off the word ``checked``. Deriving
    the status from what was returned rather than from what was read let those
    legs assert hard negatives over records they never looked at.
    """
    rows = _attempt_rows(history)
    status = history["status"]
    recorded = history.get("attempts_recorded")
    error = history.get("error")
    if status in ("checked", "partial"):
        returned = len(history.get("rows") or [])
        if not isinstance(recorded, int) or recorded < returned:
            recorded = returned
        if len(rows) < returned:
            status = "partial"
            error = error or _HISTORY_CLAMPED
    return {"status": status, "rows": rows, "attempts_recorded": recorded, "error": error}


def _version_context(dag_id: str, run: dict[str, Any], run_on_latest_version: bool | None) -> dict[str, Any]:
    """What this clear can and cannot promise about which Dag version, and which code, re-runs.

    Three separate things that are routinely said as one:

    * the flag — what this request asks for;
    * the binding — which version the re-queued instance ends up on, which the
      scheduler decides, not this flag: ``_verify_integrity_if_dag_changed``
      rebinds every unfinished instance of a re-queued run to the latest version
      whenever that version is not already one of the run's;
    * the code — which bytes the worker imports, which under an unversioned
      bundle is the file on disk at re-run time and is pinned by nothing here.
    """
    run_versions = [v.get("version_number") for v in run.get("dag_versions") or []]
    asked = {
        None: (
            "this clear does not send run_on_latest_version at all, so Airflow resolves it — the "
            "Dag's own rerun_with_latest_version, then [core] rerun_with_latest_version, then false"
        ),
        False: "this clear asks Airflow not to move the instance to the latest Dag version",
        True: "this clear asks Airflow to move the instance to the latest Dag version",
    }[run_on_latest_version]
    context: dict[str, Any] = {
        "run_versions": run_versions,
        "run_on_latest_version_sent": "omitted" if run_on_latest_version is None else run_on_latest_version,
        "asked_for": asked,
        "guarantee": (
            "Not a promise that the run's original Dag version is preserved. Clearing re-queues the "
            "instance, and the scheduler rebinds every unfinished instance of a re-queued run to the "
            "latest version whenever that version is not already one of the run's — observed doing "
            "exactly that with run_on_latest_version false."
        ),
        "code_note": (
            "A Dag version does not pin the code either. Under an unversioned bundle the worker "
            "imports the Dag file as it stands on disk at re-run time, so what re-runs is the "
            "current file whatever this says about versions."
        ),
    }
    try:
        listed = transport._api(
            "GET",
            _dag_url(dag_id, "/dagVersions"),
            params={"order_by": "-version_number", "limit": DAG_VERSION_SCAN},
        )
        known = [v.get("version_number") for v in listed["dag_versions"]]
        total = listed.get("total_entries", len(known))
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        context["versions_status"] = "unavailable"
        context["error"] = _explain_error(e)
        return context
    context["latest_version"] = known[0] if known else None
    context["versions_status"] = "checked" if len(known) >= total else "partial"
    if context["versions_status"] == "checked" and run_versions:
        missing = [v for v in run_versions if v not in known]
        context["original_version_listed"] = not missing
        if missing:
            context["missing_versions"] = missing
            context["missing_version_note"] = (
                f"the Dag no longer lists version(s) {missing} that this run recorded, so the code "
                f"the run executed cannot be identified from here and cannot be asked for"
            )
    return context


def _expandable_probe(dag_id: str, run_path: str, task_id: str) -> bool | None:
    """Whether Airflow itself says this task's instance set is recomputed on re-run.

    ``/listMapped`` is the only public route gated on ``get_needs_expansion()``
    — "MappedOperator **or is in a mapped task group**"
    (``_internal/abstractoperator.py:343-354``, route at
    ``routes/public/task_instances.py:251-253``). That is the real predicate, and
    it is NOT what ``/tasks`` exposes as ``is_mapped``: a task inside
    ``@task_group.expand`` reports ``is_mapped: false`` there while the scheduler
    still recomputes its instances.

    A 200 means the route accepted the task as expandable; a 404 that says so in
    as many words means it is not. Anything else settles nothing and answers
    ``None`` — which the callers must treat as possibly-expandable, never as no.
    """
    path = _dag_url(dag_id, f"{run_path}/taskInstances/{quote(task_id, safe='')}/listMapped")
    try:
        transport._api("GET", path, params={"limit": 1})
        return True
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404 and _LIST_MAPPED_NOT_MAPPED in _api_detail(e.response):
            return False
        return None
    except (httpx.RequestError, KeyError, TypeError, ValueError):
        return None


def _read_is_complete(kept: int, delivered: int, claimed: Any) -> bool:
    """Whether a list read holds every record anything involved accounts for.

    Three numbers, because three different things truncate a list: the route's
    own paging (``claimed`` above ``delivered``), this tool's clamps (``kept``
    below ``delivered``), and a source whose count is simply lower than what it
    handed over. Completeness is what was KEPT measured against the largest
    universe anyone claimed — the derivation ``_attempt_reading`` already makes.
    A read that discarded rows is truncated whatever the source said its total
    was, and a source that accounts for more than it sent is truncated whatever
    this tool did with the rows.
    """
    universe = max(delivered, claimed) if isinstance(claimed, int) else delivered
    return kept >= universe


def _recorded_output(dag_id: str, run_path: str, ti: dict[str, Any], xcom_scope: str) -> dict[str, Any]:
    """The XCom entries this instance has recorded, by key and timestamp only.

    The key and the timestamp, never the value: the value is bytes the task
    chose, and this reading only needs to know that an output record exists and
    when it was written. It is the task's own report of its work — the closest
    thing to the external artefact that stays inside the API this tool speaks —
    and it is not an observation of the external system.

    FAILS CLOSED on the scope, exactly like ``_event_history``: the read happens
    only when the caller's permissions said so, and an empty scope is reported as
    ``not_scoped`` rather than ``not_permitted`` because no argument arrived and
    so no permission was refused.
    """
    if xcom_scope != "granted":
        return {
            "status": "not_permitted" if xcom_scope else "not_scoped",
            "entries": [],
            "total_entries": 0,
            "error": _XCOM_NOT_PERMITTED if xcom_scope else _XCOM_NOT_SCOPED,
        }
    path = _dag_url(dag_id, f"{run_path}/taskInstances/{quote(ti['task_id'], safe='')}/xcomEntries")
    try:
        resp = transport._api("GET", path, params={"map_index": ti.get("map_index", -1)})
        rows = resp["xcom_entries"]
        total = resp.get("total_entries", len(rows))
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        return {"status": "unavailable", "entries": [], "total_entries": 0, "error": _explain_error(e)}
    entries = [
        {"key": _clamped_operator(row.get("key")), "timestamp": row.get("timestamp")}
        for row in rows[:RECOVERY_ATTEMPT_LIMIT]
    ]
    return {
        # Derived from what was READ, never from what the route returned. The
        # clamp above is this tool's own truncation and is indistinguishable, to
        # every absence-based leg downstream, from the route's: both leave
        # records unlooked-at. Comparing len(rows) against total called a
        # locally-clamped read "checked" and routed it into the negative branch.
        "status": "checked" if len(entries) >= total else "partial",
        "entries": entries,
        "total_entries": total,
        "entries_omitted": max(total - len(entries), 0),
    }


def _audit_transitions(history: dict[str, Any], ti: dict[str, Any], after: str) -> dict[str, Any]:
    """The ``running``/``success`` rows this API can see for one instance since the clear.

    Scoped to what the reading can carry: a positive row establishes that Airflow
    recorded the transition, never that the callable did the work it was for. And
    the REST view of the audit log is not the audit log — rows for a Dag that no
    longer has a row in the ``dag`` table are invisible here while still present
    in the database — so an absence is an absence *in this view*.
    """
    if history["status"] not in ("checked", "partial"):
        return {"status": history["status"], "error": history.get("error"), "events": []}
    seen: dict[str, Any] = {}
    for row in history.get("rows") or []:
        if row.get("task_id") != ti["task_id"]:
            continue
        name = row.get("event")
        if name not in ("running", "success") or name in seen:
            continue
        if after and _later_than(row.get("when"), after) is not True:
            continue
        seen[name] = {"event": name, "when": row.get("when"), "owner": _clamped_operator(row.get("owner"))}
    return {
        "status": history["status"],
        "events": [seen[name] for name in ("running", "success") if name in seen],
        "pair_recorded": len(seen) == 2,
    }


def _duration_baseline(
    dag_id: str, dag_run_id: str, ti: dict[str, Any], rows: list[dict[str, Any]]
) -> tuple[list[float], str]:
    """What this task's own successful work costs, to compare one attempt against.

    Only DISPATCHED attempts contribute. The attempt that made this recovery
    necessary is exactly the one with duration 0 and no worker fields, and
    letting it into the baseline would make "faster than nothing" a pass.

    Drawn from two places, because either alone can be empty: this instance's
    other attempts, and the same task in the Dag's other runs.
    """
    samples = [
        row["duration"]
        for row in rows
        if row.get("try_number") != ti.get("try_number")
        and _carries_execution_fields(row)
        and isinstance(row.get("duration"), (int, float))
        and row["duration"] > 0
    ]
    source = "this instance's other dispatched attempts"
    try:
        resp = transport._api(
            "GET",
            _dag_url(dag_id, "/dagRuns/~/taskInstances"),
            params={"task_id": ti["task_id"], "order_by": "-run_after", "limit": RUN_HISTORY_LIMIT},
        )
        rest = resp["task_instances"]
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError):
        return samples, source
    for row in rest:
        if row.get("dag_run_id") == dag_run_id or row.get("map_index", -1) != ti.get("map_index", -1):
            continue
        if not _carries_execution_fields(row):
            continue
        duration = row.get("duration")
        if isinstance(duration, (int, float)) and duration > 0:
            samples.append(duration)
    return samples, f"{source} and the same task in this Dag's other runs"


def _dry_run_backfill(dag_id: str, from_date: str, to_date: str) -> list[dict[str, Any]]:
    resp = transport._api(
        "POST",
        "/backfills/dry_run",
        json={"dag_id": dag_id, "from_date": from_date, "to_date": to_date},
    )
    return resp.get("backfills", [])


def _backfill_runs(backfill_id: int) -> list[dict[str, Any]]:
    resp = transport._api(
        "GET", f"/backfills/{backfill_id}/dag_runs", params={"limit": MAX_BACKFILL_RUNS + 1}
    )
    return resp.get("backfill_dag_runs", [])


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
