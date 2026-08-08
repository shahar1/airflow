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
    _ti_where,
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

_DAG_VERSIONS_ROUTE = "GET /dags/<dag>/dagVersions"

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
    # Set where the shortfall is better described than by the three numbers —
    # a selection over a scan knows what the scan missed, not what it kept.
    note: str | None = None

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
        # The exhaustion evidence is part of the answer, not decoration beside
        # it: a reading that stopped at a ceiling has not reached the end of the
        # list whatever the arithmetic makes of the count it was given.
        return self._exhausted and self.kept >= self.universe

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
        # Every shortfall this reading carries, not the first one that matches:
        # the branches used to be exclusive and the note was returned ahead of
        # all of them, so a clamp applied after a short read was described by
        # the sentence the short read had already written.
        claimed = self._claimed
        parts = []
        if self.note:
            parts.append(self.note)
        if isinstance(claimed, int) and claimed > self._delivered:
            parts.append(f"the route accounted for {claimed} and handed over {self._delivered}")
        if self.kept < self._delivered:
            parts.append(f"{self._delivered} row(s) came back and this reading keeps {self.kept}")
        if not parts:
            parts.append(f"{self.kept} of {self.universe} row(s) were read")
        source = "; ".join(parts)
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

    def clamp_last(self, limit: int) -> Reading:
        """The same clamp from the other end, for a route that appends the live row LAST.

        ``/tries`` returns oldest-first and puts the attempt now running at the
        end, so keeping the first ten of twelve dropped exactly the attempt every
        leg asks about — and reported "no record at try_number 11 is among them"
        over a record the route DID return.
        """
        if limit >= self.kept:
            return self
        return replace(self, rows=self.rows[-limit:])

    def reordered(self, key: Callable[[Mapping[str, Any]], Any], *, reverse: bool = False) -> Reading:
        """The same rows in a different order. The count does not change, so completeness does not.

        A clamp keeps a prefix, so which rows a clamp keeps is decided by the
        order the route chose — and ``/xcomEntries`` orders alphabetically while
        every question asked of it here is chronological.
        """
        return replace(self, rows=tuple(sorted(self.rows, key=key, reverse=reverse)))

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
        # A body whose list key is not a list is a body this reading could not
        # read. It used to become an empty list, and an empty list over a
        # malformed body is a complete read of nothing — which lets ``none_match``
        # answer a confident negative over a response nobody could parse.
        return failed_read(route, f"{key!r} came back as {type(rows).__name__} and not as a list")
    kept = tuple(row for row in rows if isinstance(row, dict))
    claimed = body.get("total_entries")
    if claimed is not None and (not isinstance(claimed, int) or isinstance(claimed, bool)):
        # The route accounted for its rows in a form this reading cannot use, so
        # it cannot say it saw all of them. Dropping it to "no count at all" made
        # an unreadable total indistinguishable from an exhaustive read.
        claimed = len(rows) + 1
    return Reading(
        rows=kept,
        route=route,
        # What CAME BACK, not what this reading could use: a row it discarded is
        # a row it did not look at, and counting only the usable ones made the
        # discard cancel out and the read report itself whole.
        _delivered=len(rows) if delivered is None else delivered,
        _claimed=claimed if isinstance(claimed, int) and not isinstance(claimed, bool) else None,
        _pages=pages,
        _exhausted=exhausted,
    )


def paged_read(
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    route: str,
    *,
    claimed: Any,
    pages: int,
    exhausted: bool,
) -> Reading:
    """A read that followed its own pages, with the evidence of how far it got."""
    kept = tuple(rows)
    return Reading(
        rows=kept,
        route=route,
        _delivered=len(kept),
        _claimed=claimed if isinstance(claimed, int) and not isinstance(claimed, bool) else None,
        _pages=pages,
        _exhausted=exhausted,
    )


def matches_of(
    matched: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    scanned: int,
    claimed: Any,
    route: str,
) -> Reading:
    """The rows that answered a question, over a scan that may not have covered everything.

    Selecting the rows that answer the question is not a truncation, so the
    matches are the whole of what was kept. What the scan did NOT look at is,
    and every unexamined row could have matched — so the universe is raised by
    exactly that many, and completeness comes out True only for an exhaustive
    scan.
    """
    unexamined = (
        max(claimed - scanned, 0) if isinstance(claimed, int) and not isinstance(claimed, bool) else 0
    )
    return Reading(
        rows=tuple(matched),
        route=route,
        _delivered=len(matched),
        _claimed=len(matched) + unexamined,
        note=(
            f"only the first {scanned} of {scanned + unexamined} row(s) were scanned, so any of the "
            f"{unexamined} that were not could have matched"
            if unexamined
            else None
        ),
    )


def selection_of(source: Reading, rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> Reading:
    """Rows picked out of another reading, inheriting exactly its shortfall.

    Picking the rows that answer a question is not a truncation, but the rows
    the SOURCE never saw are one, and a conclusion drawn over the selection is
    a conclusion over the source's universe.
    """
    if source.read_failed:
        return failed_read(source.route, source.error or "")
    return matches_of(list(rows), scanned=source.kept, claimed=source.universe, route=source.route)


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


def all_of(*verdicts: Verdict, route: str = "") -> Verdict:
    """The conjunction, as three-valued as its weakest member.

    Returns one of the inputs rather than building a new negative: a definite
    counterexample settles the conjunction whatever the rest are, and one
    unsettled member leaves the whole thing unsettled.
    """
    for verdict in verdicts:
        if verdict.is_absent():
            return verdict
    for verdict in verdicts:
        if verdict.is_unknown():
            return verdict
    return Verdict(Outcome.PRESENT, route=route)


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


def _tail(content: Any) -> Clipped:
    """Last few log lines, whatever shape the API returned them in.

    Carries whether anything was cut. A bare ``str`` here let every consumer
    read a truncated log as the whole of one, and an absence in a tail is not
    an absence in the log.
    """
    if isinstance(content, list):
        lines = content[-LOG_TAIL_LINES:]
        cut = len(content) > LOG_TAIL_LINES
        text = "\n".join(line if isinstance(line, str) else json.dumps(line) for line in lines)
    else:
        cut = False
        text = str(content)
    return Clipped(text[-LOG_TAIL_CHARS:], cut or len(text) > LOG_TAIL_CHARS)


_TASK_INSTANCES_ROUTE = "GET /dags/<dag>/dagRuns/<run>/taskInstances"


def _run_task_instances(dag_id: str, run_path: str) -> Reading:
    """Every task instance in one run, and whether they are all of them."""
    tis: list[dict[str, Any]] = []
    total = 0
    pages = 0
    exhausted = False
    while True:
        resp = transport._api(
            "GET",
            _dag_url(dag_id, f"{run_path}/taskInstances"),
            params={"limit": TASK_INSTANCE_PAGE, "offset": len(tis)},
        )
        page = resp["task_instances"]
        claimed = resp.get("total_entries")
        tis += page
        pages += 1
        if isinstance(claimed, int) and not isinstance(claimed, bool):
            total = claimed
        else:
            # No count at all. Taking the page's own length as the total made
            # the route's silence end the scan AND certify it: 250 rows behind a
            # 100-row page came back complete and exhausted, and a row past the
            # first page came back ABSENT. A FULL page is evidence of at least
            # one more row, which is the sentinel ``_backfill_runs`` already uses.
            total = len(tis) + (1 if len(page) >= TASK_INSTANCE_PAGE else 0)
        # An empty page ends it whatever the count says: a total that never
        # comes down would otherwise loop for as long as the ceiling allows.
        if not page:
            exhausted = True
            break
        if len(tis) >= min(total, TASK_INSTANCE_SCAN_LIMIT):
            exhausted = len(tis) >= total
            break
    return Reading(
        rows=tuple(tis),
        route=_TASK_INSTANCES_ROUTE,
        _delivered=len(tis),
        _claimed=total if isinstance(total, int) else None,
        _pages=pages,
        _exhausted=exhausted,
    )


_TASKS_ROUTE = "GET /dags/<dag>/tasks"


def _tasks_reading(dag_id: str) -> Reading:
    """The current tasks and their edges, and whether they are all of them.

    ``/tasks`` is the only *public* route that carries ``downstream_task_ids``;
    the richer structure view lives under ``/ui`` and is not part of the API this
    server is allowed to speak. A caller that reasons about the task set being
    COMPLETE — a positional resolve, a removed-task check, an expandability
    closure — has to be able to say whether it read all of it.
    """
    return read_of(transport._api("GET", _dag_url(dag_id, "/tasks")), "tasks", _TASKS_ROUTE)


def _tasks(dag_id: str) -> Reading:
    """The current tasks and their edges, with the completeness of the read attached."""
    return _tasks_reading(dag_id)


_IMPORT_ERRORS_ROUTE = "GET /importErrors"


def _find_import_errors(dag: dict[str, Any] | None) -> Reading:
    """Import errors for this Dag's file — the classic self-healing case.

    A file that stops parsing never produces a failed run: the old Dag keeps
    running its old code and every run-based signal looks healthy. The import
    error list is the only place that failure shows up.
    """
    if not dag:
        # NOT an empty complete read: no lookup happened, so whether this Dag's
        # file still imports is not established by it.
        return failed_read(_IMPORT_ERRORS_ROUTE, "the Dag record was not read, so it names no file to match")
    names = {name for name in (dag.get("fileloc"), dag.get("relative_fileloc")) if name}
    if not names:
        return failed_read(_IMPORT_ERRORS_ROUTE, "the Dag record names no file to match an import error to")
    try:
        resp = transport._api("GET", "/importErrors", params={"limit": 100})
        errors = resp["import_errors"]
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        # A bare empty list here was indistinguishable from "this Dag's file
        # imports cleanly", which is the one thing an unreadable import-error
        # list must never be mistaken for. The caller turns this into a check so
        # it reaches the summary; it is NOT smuggled into ``rows``, because
        # ``rows`` is the numerator of ``complete``.
        return failed_read(_IMPORT_ERRORS_ROUTE, _explain_error(e))
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
    # Nothing but ``clamp``, ``filter`` and ``project`` may change ``rows``: it
    # is the numerator of ``complete``, so appending one synthetic note row
    # raised ``kept`` by one while ``_delivered`` and ``_claimed`` stood still —
    # and a read that had left exactly one row unexamined then reported itself
    # WHOLE, with ``omitted`` zero. The truncation travels as the reading's own
    # note, and the caller renders it as a check.
    return matches_of(
        checks, scanned=len(errors), claimed=resp.get("total_entries"), route=_IMPORT_ERRORS_ROUTE
    )


_TRIES_ROUTE = "GET /dags/<dag>/dagRuns/<run>/taskInstances/<task>/tries"


def _attempt_history(dag_id: str, run_path: str, ti: dict[str, Any]) -> Reading:
    """Every recorded attempt of one task instance, or why they could not be read.

    Never raises: an unreadable history downgrades what the diagnosis can
    conclude, and must not take the whole diagnosis down with it. ``TypeError``
    is in the net because ``_api`` returns ``None`` for an empty body, and
    subscripting that would otherwise take the whole diagnosis down. The
    unmapped route accepts ``map_index`` as a query parameter, so one URL serves
    mapped and unmapped instances, and it is not paginated.

    An empty page is NOT "there were no earlier attempts" whenever the route
    accounted for some: that is a truncated read wearing the same word, and the
    reading keeps the two apart by construction rather than by a status string.
    """
    path = _dag_url(dag_id, f"{run_path}/taskInstances/{quote(ti['task_id'], safe='')}/tries")
    try:
        resp = transport._api("GET", path, params={"map_index": ti.get("map_index", -1)})
        resp["task_instances"]
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        return failed_read(_TRIES_ROUTE, _explain_error(e))
    return read_of(resp, "task_instances", _TRIES_ROUTE)


_RUN_TASK_HISTORY_ROUTE = "GET /dags/<dag>/dagRuns/~/taskInstances"


def _task_comparison(dag_id: str, runs: list[dict[str, Any]], task_ids: list[str]) -> dict[str, Any]:
    """The same task's rows across the runs in the window.

    One call per compared task id, and none at all when there is nothing to
    compare. The fleet-scoped batch route is deliberately not used: it is
    ``dag_id`` ``Literal["~"]`` AND carries ``Depends(action_logging())``, so it
    reads outside the per-Dag boundary the plugin enforces and writes an audit
    row into the very table this tool reads as evidence.
    """
    compared = task_ids[:TASK_COMPARISON_LIMIT]
    # The task ids this comparison did not even ask about are a truncation of
    # the comparison itself, and every clause drawn over it has to see them.
    ids_omitted = max(len(task_ids) - len(compared), 0)
    result: dict[str, Any] = {
        "selection": _TASK_COMPARISON_SELECTION,
        "task_ids_compared": compared,
        "task_ids_omitted": ids_omitted,
        "runs_not_covered": [],
        # Per task id, the rows this comparison holds AND whether they are all
        # of them. A task with more history than RUN_HISTORY_LIMIT rows used to
        # be reported as if the page were the whole of it.
        "tasks": {},
        "rows_omitted": {},
        "error": None,
    }
    if not compared or not runs:
        return result
    window = {run["dag_run_id"] for run in runs}
    oldest = runs[-1].get("run_after")
    tasks: dict[str, Reading] = {}
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
            rows = []
            for row in returned:
                if row.get("dag_run_id") not in window:
                    continue
                covered.add(row["dag_run_id"])
                entry = {name: row.get(name) for name in _TASK_COMPARISON_KEYS}
                entry["operator"] = _clamped_operator(row.get("operator"))
                rows.append(entry)
            reading = matches_of(
                rows,
                scanned=len(returned),
                claimed=resp.get("total_entries"),
                route=_RUN_TASK_HISTORY_ROUTE,
            )
            tasks[task_id] = reading
            omitted[task_id] = reading.omitted
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        result["error"] = _explain_error(e)
        return result
    result["tasks"] = tasks
    result["rows_omitted"] = omitted
    result["runs_not_covered"] = [run["dag_run_id"] for run in runs if run["dag_run_id"] not in covered]
    return result


def flatten_comparison(comparison: dict[str, Any]) -> None:
    """Turn each compared task's reading back into plain rows, in place.

    Called once every clause that may draw an absence has run. The readings
    cannot survive into the payload — they do not serialize, and a reader that
    still held one could conclude from it after the point where the tool decided
    what it was willing to conclude.
    """
    tasks = comparison.get("tasks")
    if isinstance(tasks, dict):
        comparison["tasks"] = {
            task_id: [dict(row) for row in per_task.rows] if isinstance(per_task, Reading) else per_task
            for task_id, per_task in tasks.items()
        }


def comparison_rows(comparison: dict[str, Any], task_id: str) -> Reading:
    """One compared task's rows, exactly as its own route call answered.

    A task id the comparison never asked about comes back as a FAILED read, so
    no clause can be drawn over it at all. What it does NOT do is charge that
    omission to the other tasks: the comparison used to add ``task_ids_omitted``
    to every compared task's universe, so a task whose own call was honest at
    three of three came back incomplete, every clause over it refused, and the
    refusal quoted a shortfall belonging to a different task's rows entirely.
    """
    if comparison.get("error") is not None:
        return failed_read(_RUN_TASK_HISTORY_ROUTE, str(comparison["error"]))
    per_task = (comparison.get("tasks") or {}).get(task_id)
    if not isinstance(per_task, Reading):
        return failed_read(
            _RUN_TASK_HISTORY_ROUTE, f"{task_id!r} was not among the task ids this comparison read"
        )
    return per_task


_DAG_RUNS_ROUTE = "GET /dags/<dag>/dagRuns"


def _recent_runs(dag_id: str) -> Reading:
    """The newest runs of this Dag. Raises — the callers differ on what a failure means.

    On the run-resolving path a failure must not be turned into "this Dag has
    never run"; on the exact-run path it only costs the history field.
    """
    resp = transport._api(
        "GET", _dag_url(dag_id, "/dagRuns"), params={"order_by": "-run_after", "limit": RUN_HISTORY_LIMIT}
    )
    resp["dag_runs"]
    return read_of(resp, "dag_runs", _DAG_RUNS_ROUTE)


def _build_asset_note(dag_id: str) -> str:
    """The asset context an asset-touching change must carry to its approval card.

    Names only this Dag's own produced/consumed assets — never other Dags' ids,
    which only get_blast_radius is authorized to hand back.
    """
    catalog = read_asset_catalog()
    if catalog.read_failed:
        return (
            "this change touches assets, inlets/outlets or the schedule, and the asset catalog "
            "could not be read; call get_blast_radius and tell the user what else is affected "
            "before applying"
        )
    edges = _compute_asset_edges(dag_id, catalog)
    produces = ", ".join(repr(name) for name in edges["produces"] or []) or "no assets"
    consumes = ", ".join(repr(name) for name in edges["consumes"] or []) or "no assets"
    note = (
        f"this change touches assets, inlets/outlets or the schedule: {dag_id} produces "
        f"{produces} and consumes {consumes}; Dags scheduled on those assets can be affected"
    )
    if not catalog.complete:
        # "no assets" over a truncated catalog is a claim about edges nobody
        # read, and this note is what the approval card renders.
        note += (
            f". The asset catalog was NOT read whole ({catalog.reason}), so an edge this Dag has "
            f"may be missing from that sentence"
        )
    return note


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
    clipped = _tail(resp.get("content") if isinstance(resp, dict) else resp)
    tail = clipped.text[-RECOVERY_LOG_TAIL_CHARS:]
    truncated = clipped.truncated or len(clipped.text) > RECOVERY_LOG_TAIL_CHARS
    stripped = tail.strip()
    if not stripped:
        status = "empty"
    elif _NO_LOGS_MARKER in stripped.lower():
        # The route's own sentence, not the task's. Reported as its own status so
        # nothing downstream mistakes it for output the task produced.
        status = "no_logs_reported"
    else:
        status = "present"
    # A tail is not the log. Both negative statuses are claims about the whole
    # of it, and neither may be drawn from the last few hundred characters.
    return {**entry, "status": status, "tail": tail, "tail_truncated": truncated}


def _attempt_shape(row: Mapping[str, Any]) -> dict[str, Any]:
    """One attempt reduced to the fields this reading uses."""
    return {
        "try_number": row.get("try_number"),
        "state": row.get("state"),
        "hostname": row.get("hostname"),
        "pid": row.get("pid"),
        "duration": row.get("duration"),
        "start_date": row.get("start_date"),
        "end_date": row.get("end_date"),
    }


def _attempt_rows(history: Reading) -> list[dict[str, Any]]:
    """The attempts this reading kept, reduced to the fields it uses."""
    return [_attempt_shape(row) for row in _attempt_reading(history).rows]


def _attempt_reading(history: Reading) -> Reading:
    """The attempt history as this reading actually READ it.

    The local clamp is part of the read, so it is applied as a clamp on the
    reading rather than as a slice beside it: ``/tries`` honestly hands over a
    whole page, this keeps at most ``RECOVERY_ATTEMPT_LIMIT`` of it, and every
    absence-based leg downstream would otherwise be concluding over records it
    never looked at.

    From the END of the page: the route returns the attempts oldest-first and
    the attempt this recovery is about is the last one.
    """
    return history.clamp_last(RECOVERY_ATTEMPT_LIMIT).project(_attempt_shape)


NOT_CHECKED = "not_checked"


def history_status(history: Reading | None) -> str:
    """The word this server's prose uses for how far a bounded read got.

    Display only, and derived from the reading's own ``complete`` rather than
    from a second comparison — the four incompatible longhand derivations this
    replaces are exactly what let one site call a clamped read ``checked``.
    """
    if history is None:
        return NOT_CHECKED
    if history.read_failed:
        return "unavailable"
    if not history.complete:
        return "partial"
    return "empty" if not history.rows else "checked"


def attempt_error(history: Reading) -> str | None:
    """The one sentence that says why this attempt history settles nothing."""
    if history.error is not None:
        return history.error
    if not history.complete:
        return _HISTORY_CLAMPED if history.kept < history._delivered else _HISTORY_PARTIAL
    return _HISTORY_EMPTY if not history.rows else None


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
        listed["dag_versions"]
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        context["versions_status"] = "unavailable"
        context["error"] = _explain_error(e)
        return context
    versions = read_of(listed, "dag_versions", _DAG_VERSIONS_ROUTE)
    known = [row.get("version_number") for row in versions.rows]
    context["latest_version"] = known[0] if known else None
    context["versions_status"] = "checked" if versions.complete else "partial"
    if run_versions:
        # One verdict per version the run recorded: "still listed" is a presence
        # and survives truncation, "no longer listed" is an absence and does not.
        def listed(version: Any) -> Callable[[Mapping[str, Any]], bool]:
            return lambda row: row.get("version_number") == version

        verdicts = {
            version: find(
                versions,
                listed(version),
                f"version {version} was not among the Dag versions this reading listed",
            )
            for version in run_versions
        }
        context["original_version_listed"] = all_of(*verdicts.values(), route=versions.route).as_field()
        missing = [version for version, verdict in verdicts.items() if verdict.is_absent()]
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


_XCOM_ROUTE = "GET /dags/<dag>/dagRuns/<run>/taskInstances/<task>/xcomEntries"


def _recorded_output(dag_id: str, run_path: str, ti: dict[str, Any], xcom_scope: str) -> Reading:
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
        return failed_read(_XCOM_ROUTE, _XCOM_NOT_PERMITTED if xcom_scope else _XCOM_NOT_SCOPED)
    path = _dag_url(dag_id, f"{run_path}/taskInstances/{quote(ti['task_id'], safe='')}/xcomEntries")
    try:
        resp = transport._api("GET", path, params={"map_index": ti.get("map_index", -1)})
        resp["xcom_entries"]
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        return failed_read(_XCOM_ROUTE, _explain_error(e))
    # The clamp is applied as a clamp on the reading, not as a slice beside it.
    # This tool's own truncation is indistinguishable, to every absence-based
    # leg downstream, from the route's: both leave records unlooked-at.
    # Ordered before it is clamped. ``/xcomEntries`` sorts by
    # (dag_id, task_id, run_id, map_index, key) — alphabetically by key — and
    # every question asked of this reading is about WHEN a record was written,
    # so an alphabetical prefix drops exactly the newest records.
    return (
        read_of(resp, "xcom_entries", _XCOM_ROUTE)
        .reordered(lambda row: str(row.get("timestamp") or ""), reverse=True)
        .clamp(RECOVERY_ATTEMPT_LIMIT)
        .project(lambda row: {"key": _clamped_operator(row.get("key")), "timestamp": row.get("timestamp")})
    )


def output_scope_refused(output: Reading) -> bool:
    """Whether the output read was declined on permissions rather than attempted."""
    return output.error in (_XCOM_NOT_PERMITTED, _XCOM_NOT_SCOPED)


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
    events: Reading = history["reading"]
    seen: dict[str, Any] = {}
    verdicts = []

    def transition(name: str) -> Callable[[Mapping[str, Any]], bool]:
        return lambda row: (
            row.get("task_id") == ti["task_id"]
            and row.get("event") == name
            and (not after or _later_than(row.get("when"), after) is True)
        )

    for name in ("running", "success"):
        hit = find(
            events,
            transition(name),
            f"no {name} transition of {_ti_where(ti)} was recorded since the clear",
        )
        verdicts.append(hit)
        if hit.is_present() and hit.row is not None:
            seen[name] = {
                "event": name,
                "when": hit.row.get("when"),
                "owner": _clamped_operator(hit.row.get("owner")),
            }
    return {
        "status": history["status"],
        "events": [seen[name] for name in ("running", "success") if name in seen],
        # PRESENT only when both transitions were found; a missing one over a
        # scan that stopped short is UNKNOWN, never a recorded absence.
        "pair_recorded": all_of(*verdicts, route=events.route),
    }


_DURATION_HISTORY_SOURCE = "this instance's other dispatched attempts"


def _duration_baseline(
    dag_id: str, dag_run_id: str, ti: dict[str, Any], attempts: Reading
) -> tuple[Reading, str]:
    """What this task's own successful work costs, to compare one attempt against.

    Only DISPATCHED attempts contribute. The attempt that made this recovery
    necessary is exactly the one with duration 0 and no worker fields, and
    letting it into the baseline would make "faster than nothing" a pass.

    Drawn from two places, because either alone can be empty: this instance's
    other attempts, and the same task in the Dag's other runs.
    """

    def usable(row: Mapping[str, Any]) -> bool:
        duration = row.get("duration")
        return (
            _carries_execution_fields(row)
            and isinstance(duration, (int, float))
            and not isinstance(duration, bool)
            and duration > 0
        )

    samples = [
        {"duration": row["duration"]}
        for row in attempts.rows
        if row.get("try_number") != ti.get("try_number") and usable(row)
    ]
    try:
        resp = transport._api(
            "GET",
            _dag_url(dag_id, "/dagRuns/~/taskInstances"),
            params={"task_id": ti["task_id"], "order_by": "-run_after", "limit": RUN_HISTORY_LIMIT},
        )
        resp["task_instances"]
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        # The second leg did not run, so the sample is one-legged and the source
        # string must not name a leg that never happened. It used to.
        return (
            Reading(
                rows=tuple(samples),
                route=_RUN_TASK_HISTORY_ROUTE,
                _delivered=len(samples),
                # One leg ran and the other did not, so the sample is short by an
                # unknown amount — expressed as at least one unread record.
                _claimed=len(samples) + attempts.omitted + 1,
            ),
            f"{_DURATION_HISTORY_SOURCE} only — the same task's rows in this Dag's other runs "
            f"could not be read ({_explain_error(e)})",
        )
    rest = read_of(resp, "task_instances", _RUN_TASK_HISTORY_ROUTE)
    samples += [
        {"duration": row["duration"]}
        for row in rest.rows
        if row.get("dag_run_id") != dag_run_id
        and row.get("map_index", -1) == ti.get("map_index", -1)
        and usable(row)
    ]
    # A median is a statistic OVER A SAMPLE and does not need the population, so
    # the sample size this tool asked for is not a shortfall: ``limit`` is
    # RUN_HISTORY_LIMIT, and a page that comes back AT it is the sample that was
    # requested. Charging it to the reading made the leg unanswerable for every
    # task with more than ten runs — a permanent null, not a caution.
    #
    # A page that comes back UNDER the limit while the route accounts for more IS
    # a read that fell short, and that one still counts.
    unread = rest.omitted if rest.kept < RUN_HISTORY_LIMIT else 0
    # Rows EXAMINED, not rows kept: ``usable()`` is this reading's own filter for
    # what may enter a baseline, and charging its discards to the shortfall
    # counted the tool's own judgement as unread records.
    examined = len(attempts.rows) + len(rest.rows)
    sampled = f"the most recent {RUN_HISTORY_LIMIT} run(s) of this task" if rest.omitted else "every run read"
    return (
        matches_of(
            samples,
            scanned=examined,
            claimed=examined + unread,
            route=_RUN_TASK_HISTORY_ROUTE,
        ),
        f"{_DURATION_HISTORY_SOURCE} and the same task in this Dag's other runs, sampled over {sampled}",
    )


_DRY_RUN_BACKFILL_ROUTE = "POST /backfills/dry_run"
_BACKFILL_RUNS_ROUTE = "GET /backfills/<id>/dag_runs"


def _dry_run_backfill(dag_id: str, from_date: str, to_date: str) -> Reading:
    """The runs a backfill would create. The identity compare over it authorizes a write."""
    resp = transport._api(
        "POST",
        "/backfills/dry_run",
        json={"dag_id": dag_id, "from_date": from_date, "to_date": to_date},
    )
    return read_of(resp, "backfills", _DRY_RUN_BACKFILL_ROUTE)


def _backfill_runs(backfill_id: int) -> Reading:
    """The runs a backfill created, and whether they are all of them.

    The ``+1`` is an overflow sentinel: a page that comes back FULL at
    ``MAX_BACKFILL_RUNS + 1`` means the backfill holds at least one more run
    than this reading can see, and the identity compare that decides whether to
    keep or abandon it would otherwise be made over a subset.
    """
    limit = MAX_BACKFILL_RUNS + 1
    resp = transport._api("GET", f"/backfills/{backfill_id}/dag_runs", params={"limit": limit})
    reading = read_of(resp, "backfill_dag_runs", _BACKFILL_RUNS_ROUTE)
    if reading.kept >= limit and reading._claimed is None:
        return replace(reading, _claimed=reading.kept + 1)
    return reading


ASSET_CATALOG_ROUTE = "GET /assets"
ASSET_CATALOG_LIMIT = 100


def read_asset_catalog() -> Reading:
    """The asset catalog, one page of it, and whether that is all of it."""
    try:
        resp = transport._api("GET", "/assets", params={"limit": ASSET_CATALOG_LIMIT})
        resp["assets"]
    except (httpx.HTTPStatusError, KeyError) as e:
        return failed_read(ASSET_CATALOG_ROUTE, _explain_error(e))
    return read_of(resp, "assets", ASSET_CATALOG_ROUTE)


def _compute_asset_edges(dag_id: str, catalog: Reading) -> dict[str, list[str] | None]:
    """This Dag's place in the asset graph, one hop out in both directions.

    An empty list is a claim that the catalog holds no such edge. Over a
    catalog that was not read whole it is not one, so it comes back ``None`` —
    four empty lists used to mean both "no declared edge" and "catalog
    truncated", and nothing in the payload separated them.
    """
    produces: list[str] = []
    consumes: list[str] = []
    downstream: set[str] = set()
    upstream: set[str] = set()
    for asset in catalog.rows:
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

    def stated(names: list[str] | set[str]) -> list[str] | None:
        found = sorted(names)
        return found if found or catalog.complete else None

    return {
        "produces": stated(produces),
        "consumes": stated(consumes),
        "downstream": stated(downstream),
        "upstream": stated(upstream),
    }
