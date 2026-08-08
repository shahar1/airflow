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
What is wrong with this run of this Dag - and the prose that says so.

The four read-only tools and the whole narrative layer they share.
``diagnose_dag`` is the one that reads a run whole: its task instances, the
event history behind them, the dispatch evidence for each recorded attempt, the
task graph, the source, and the Dag's recent runs - and then folds all of it into
one deterministic ``summary`` a small model can echo without assembling anything.
``compare_dag_runs`` answers "was it my change?" across two runs,
``find_failure_clusters`` groups the fleet's recent failures by error signature,
and ``get_blast_radius`` says what else an asset edge carries.

The narrative layer is the point.  A finding is a paragraph, not a row: the
contrast with the same task's dispatched rows elsewhere, how far back a missing
worker field goes, what ran downstream of it, and what this diagnosis did not get
to look at all reach the reader inside ``detail`` and ``summary``, because that is
the part of a 40 kB payload that is actually read.

Wave 7 of the move-only extraction in ``docs/extraction-plan.md``, and the first
wave to carry tools.  All four are read-only: nothing here can write.

It knows nothing about tokens, plans, or how a file is written.  It does still
reach ``transport`` directly for the three reads issued inline inside a tool body
- ``diagnose_dag``'s per-failure log, ``find_failure_clusters``' failure scan and
its logs, and ``get_blast_radius``' asset catalog.  Relocating those into
``reading`` is a reshaping of a tool body rather than a move of a definition, so
it is left to the typed redesign along with the deferrals it would have to serve;
this wave keeps every body byte-identical.

The suite rebinds ``DIAGNOSIS_LOG_BUDGET_CHARS``, ``DIAGNOSIS_SUMMARY_BUDGET_CHARS``,
``DISPATCH_CONTRAST_RUN_LIMIT`` and ``DISPATCH_IMPACT_TASK_LIMIT``, so those four
are deliberately NOT re-exported from ``server.py`` - a re-export there would be a
display symbol a ``monkeypatch.setattr(server, ...)`` could change while the real
log budget, the real summary budget and the real clause ceilings went on reading
the real ones.  All four are read only inside this module, so this module object
is the one place to patch.  The names the suite only ever reads - the four tools
among them - ARE re-exported, because a name that is never rebound is the same
object either way.
"""

from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import httpx

# ``FAILURE_SCAN_LIMIT``, ``_run_task_instances`` and ``_tasks`` are rebound by the
# suite, so the reads that use them go through the module object and are never
# imported by name; ``_api`` is rebound too, for the same reason.
import reading
import transport
from dagsource import (
    _STATIC_CHECKS_FOLDED_KIND,
    DagFileDriftError,
    DagFileError,
    _dag_path,
    _display_order,
    _downstream_task_ids,
    _parsed_source,
    _static_checks,
)
from evidence import (
    _DISPATCH_FINDING_KIND,
    _DISPATCH_TRUNCATED_KIND,
    _NOT_ESTABLISHED,
    _R1,
    _R2,
    _attribution_reader,
    _check_dispatch_evidence,
    _enforce_attribution_ceiling,
    _event_history,
    _project_task_instances,
    _prune_unknowns_legend,
    _run_health,
)
from primitives import (
    _carries_worker_field,
    _clip_at_word,
    _fenced,
    _quoted,
    _run_version,
    _ti_where,
)
from reading import (
    _TASK_COMPARISON_SELECTION,
    RUN_HISTORY_LIMIT,
    _compute_asset_edges,
    _find_import_errors,
    _recent_runs,
    _resolve_run,
    _tail,
    _task_comparison,
)
from transport import _dag_url, _explain_error, _explain_unknown_dag

# One diagnosis now carries every failed task's log, so it needs a ceiling the
# per-log tail does not give: a fan-out of 200 failed mapped instances would
# otherwise return 800 KB and blow the model's context on its way through.
DIAGNOSIS_LOG_BUDGET_CHARS = 12000
# The summary is the prose the model echoes verbatim, so it needs the ceiling the
# log path already has — for the same reason and in the same units.
DIAGNOSIS_SUMMARY_BUDGET_CHARS = 12000


_RUN_HISTORY_KEYS = (
    "dag_run_id",
    "state",
    "run_type",
    "logical_date",
    "queued_at",
    "start_date",
    "end_date",
    "duration",
    "triggered_by",
    "triggering_user_name",
)


def _comparison_task_ids(checks: list[dict[str, Any]], tis: list[dict[str, Any]]) -> list[str]:
    """Which tasks are worth looking at across runs — chosen by the diagnosis, not the caller.

    Empty when the diagnosis found nothing and nothing failed, which is what
    keeps the comparison at zero HTTP calls in the common case.
    """
    ordered: list[str] = []
    for check in checks:
        if check["kind"] == _DISPATCH_FINDING_KIND and check["task_id"] not in ordered:
            ordered.append(check["task_id"])
    for ti in tis:
        if ti.get("state") in ("failed", "up_for_retry") and ti["task_id"] not in ordered:
            ordered.append(ti["task_id"])
    return ordered


def _widen_not_covered(
    comparison: dict[str, Any], beyond_window: list[Any], windowed: reading.Reading
) -> dict[str, Any]:
    """Charge the runs the WINDOW dropped to the comparison's own coverage list.

    A run outside the window is a run this comparison holds no rows for, exactly
    like a run inside it that returned none — and keeping the two apart let the
    list empty out as the window narrowed.
    """
    named = list(comparison.get("runs_not_covered") or [])
    comparison["runs_not_covered"] = named + [run for run in beyond_window if run not in named]
    comparison["runs_not_covered_read_whole"] = windowed.complete
    return comparison


def _run_history(
    dag_id: str,
    diagnosed_run_id: str | None,
    runs: reading.Reading,
    error: str | None,
    task_ids: list[str],
) -> dict[str, Any]:
    """The Dag's recent runs, and the diagnosed run's place among them.

    A field rather than a tool: every question it answers is a question about the
    run already under diagnosis, and the comparison set is chosen BY the
    diagnosis rather than guessed at by a caller.
    """
    # A clamp, not a slice: whatever this window drops is dropped from what any
    # clause below may conclude an absence over.
    windowed = runs.clamp(RUN_HISTORY_LIMIT)
    window = list(windowed.rows)
    # Runs this comparison did not reach AT ALL. ``runs_not_covered`` used to be
    # relative to the window, so narrowing the window EMPTIED it — an
    # enumeration that reads "every run was covered" produced by the tool
    # covering fewer of them.
    beyond_window = [row.get("dag_run_id") for row in runs.rows[len(window) :]]
    listed = []
    for run in window:
        entry = {name: run.get(name) for name in _RUN_HISTORY_KEYS}
        entry["dag_version"] = _run_version(run)
        entry["is_diagnosed_run"] = run.get("dag_run_id") == diagnosed_run_id
        listed.append(entry)
    return {
        "returned": len(listed),
        "total_entries": windowed.universe,
        "runs_omitted": windowed.omitted,
        "runs_read_whole": windowed.complete,
        "window": (
            f"the most recent {len(listed)} run(s) of this Dag by run_after, newest first"
            if listed
            else "this Dag has no runs"
        ),
        "runs": listed,
        "task_comparison": (
            _widen_not_covered(_task_comparison(dag_id, window, task_ids), beyond_window, windowed)
            if error is None
            else {
                "selection": _TASK_COMPARISON_SELECTION,
                "task_ids_compared": [],
                "task_ids_omitted": 0,
                "runs_not_covered": [],
                "runs_not_covered_read_whole": False,
                "tasks": {},
                "rows_omitted": {},
                "error": error,
            }
        ),
        "limits": [_R1, _R2],
        "error": error,
    }


# How many runs a dispatch finding may name when it contrasts this attempt with
# the same task's rows elsewhere. A finding is a paragraph, not a run list.
DISPATCH_CONTRAST_RUN_LIMIT = 4
# The same ceiling for the downstream tasks a finding names.
DISPATCH_IMPACT_TASK_LIMIT = 6


def _compared_rows_by_run(comparison: dict[str, Any], task_id: str, map_index: int) -> reading.Reading:
    """The compared task's newest row per run, for the one instance a finding names.

    Comes back as a reading rather than a dict: an empty one used to mean both
    "this task has no row on any compared run" and "the comparison never got
    that far", and both clauses below draw absences over it.
    """
    per_task = reading.comparison_rows(comparison, task_id)
    rows: dict[str, dict[str, Any]] = {}
    for row in per_task.rows:
        run_id = row.get("dag_run_id")
        if row.get("map_index", -1) != map_index or not isinstance(run_id, str):
            continue
        rows.setdefault(run_id, dict(row))
    return reading.selection_of(per_task, list(rows.values()))


def _rows_by_run(rows: reading.Reading) -> dict[str, dict[str, Any]]:
    return {row["dag_run_id"]: dict(row) for row in rows.rows if isinstance(row.get("dag_run_id"), str)}


# Always interpolated after "… and ", so it starts lower case: the sentence used
# to render as "…, and The same task's rows…".
_NOT_COMPARED_WHOLE = (
    "the same task's rows on the other runs were NOT read whole, so nothing here rules that in or out"
)


def _said_once(clauses: list[str]) -> list[str]:
    """The comparison's shortfall stated once per finding, not once per clause.

    Two of the three clauses draw over the same comparison, so both carried the
    same sentence and the same parenthesised reason — the reader got the
    identical clause twice inside one summary entry.
    """
    marker = f", and {_NOT_COMPARED_WHOLE}"
    kept = []
    stated = False
    for clause in clauses:
        if marker not in clause:
            kept.append(clause)
            continue
        kept.append(clause.split(marker)[0] if stated else clause)
        stated = True
    return kept


def _recurrence_clause(order: list[str], rows: reading.Reading, run_id: str) -> str:
    """How far back this run's missing dispatch evidence goes, counted rather than implied.

    "1 problem found" is a statement about one run. A task that has recorded no
    worker field for four cycles is a different fact, and it is one the compared
    rows already hold — so it is stated instead of left for the reader to count.

    A streak that runs off the end of what was read is not a streak that ended,
    so a short comparison says so rather than reporting a shorter run of cycles.
    """
    if run_id not in order:
        return ""
    by_run = _rows_by_run(rows)
    streak = []
    ran_off_the_end = False
    for other in order[order.index(run_id) :]:
        row = by_run.get(other)
        if row is None:
            ran_off_the_end = True
            break
        if _carries_worker_field(row):
            break
    else:
        ran_off_the_end = True
    for other in order[order.index(run_id) :]:
        row = by_run.get(other)
        if row is None or _carries_worker_field(row):
            break
        streak.append(other)
    if len(streak) < 2:
        if streak and ran_off_the_end and not rows.complete:
            return (
                f"How far back this goes is NOT established: the same task carries no worker-written "
                f"field on the run(s) that were read ending with this one, and {_NOT_COMPARED_WHOLE} "
                f"({rows.reason})"
            )
        return ""
    named = streak[:DISPATCH_CONTRAST_RUN_LIMIT]
    text = (
        f"This is not confined to this run: the same task carries no worker-written field "
        f"(hostname empty, pid null) on {len(streak)} consecutive run(s) ending with this one, out "
        f"of the {len(order)} most recent run(s) this diagnosis compared — "
        f"{', '.join(_fenced(other) for other in named)}"
    )
    if len(streak) > len(named):
        text += f" and {len(streak) - len(named)} more"
    return text


def _contrast_clause(
    order: list[str],
    rows: reading.Reading,
    run_id: str,
    dag_version: int | None,
    versions: dict[str, int | None],
) -> str:
    """The same task's dispatched rows elsewhere — what they exclude, and what they do not.

    Two explanations for a bare attempt are otherwise left open by the finding
    itself: a task instance that completes entirely inside the triggerer (which
    writes no hostname or pid on any run), and a task that legitimately does
    nothing. Another run of the SAME task carrying a worker field, and work, is
    what closes them — so the closing fact is written down rather than left as
    rows for a reader to compare.

    An empty clause used to mean "no dispatched row elsewhere", concluded over
    five task ids and ten rows each. It now says which of the two it is.
    """
    by_run = _rows_by_run(rows)
    dispatched = reading.find(
        rows,
        lambda row: row.get("dag_run_id") != run_id and _carries_worker_field(row),
        "no other compared run of this task records a worker-written field",
    )
    if dispatched.is_unknown():
        return (
            f"Whether the same task IS dispatched on another run is NOT established: none of the "
            f"rows this comparison read records a worker-written field, and {_NOT_COMPARED_WHOLE} "
            f"({rows.reason})"
        )
    for other in order:
        row = by_run.get(other)
        if other == run_id or row is None or not _carries_worker_field(row):
            continue
        duration = row.get("duration")
        did_work = isinstance(duration, (int, float)) and not isinstance(duration, bool) and duration > 0
        text = (
            f"The same task was dispatched on run {_fenced(other)}: that row records hostname "
            f"{_quoted(row.get('hostname'))} and pid {_quoted(row.get('pid'))}"
        )
        if did_work:
            text += (
                f" with duration {duration}, so this task does record worker fields and real work "
                f"when it is dispatched — neither a completion entirely inside the triggerer, which "
                f"records no hostname or pid on any run, nor a task that does nothing accounts for "
                f"the attempt diagnosed here"
            )
        else:
            text += (
                ", so this task does record worker fields when it is dispatched — a completion "
                "entirely inside the triggerer, which records no hostname or pid on any run, does "
                "not account for the attempt diagnosed here"
            )
        other_version = versions.get(other)
        if other_version is not None and dag_version is not None:
            if other_version == dag_version:
                # Stated as what the record says and no further: ``_R1`` is why
                # "same recorded version" is not "same code executed".
                text += (
                    f". Both runs are recorded at dag_version {dag_version}, so the recorded Dag "
                    f"version does not differ between the run where this task was dispatched and "
                    f"this one"
                )
            else:
                text += (
                    f". That run is recorded at dag_version {other_version} and this one at "
                    f"dag_version {dag_version}, so the two runs do not carry the same recorded "
                    f"Dag version"
                )
        return text
    return ""


def _impact_clause(
    task_id: str,
    run_state: str,
    edges: dict[str, list[str]],
    ti_states: dict[str, list[str]],
) -> str:
    """What a run carrying this finding still reports, and what ran behind it.

    The one thing no field in this result says today: a green run raises nothing,
    so a task instance with no dispatch evidence is carried past every alert the
    deployment has.
    """
    reached = [other for other in _downstream_task_ids(task_id, edges) if other in ti_states]
    # The no-alert claim is earned by the run state and by nothing else: on a run
    # that is recorded failed, alerting HAS fired, and saying otherwise would be
    # a false statement about the deployment rather than a finding about a task.
    text = (
        "Operational effect: the run is recorded success, so this finding raises no failure and "
        "fires no alert of its own"
        if run_state == "success"
        else f"Operational effect: the run is recorded {run_state}, so whatever that state raises "
        f"is raised by the run and not by this finding"
    )
    if not reached:
        return text
    named = reached[:DISPATCH_IMPACT_TASK_LIMIT]
    listed = ", ".join(f"{_fenced(other)} ({'/'.join(ti_states[other])})" for other in named)
    text += (
        f", and {len(reached)} task(s) downstream of it in the task graph are recorded in this run "
        f"with this attempt already marked success: {listed}"
    )
    if len(reached) > len(named):
        text += f" and {len(reached) - len(named)} more"
    return text


def _augment_dispatch_findings(
    checks: list[dict[str, Any]],
    run_id: str,
    run_state: str,
    dag_version: int | None,
    run_history: dict[str, Any],
    tasks: dict[str, Any] | None,
    tis: list[dict[str, Any]],
) -> None:
    """Fold the evidence that is only in the raw rows into the finding's own prose.

    Everything here is already in this result — the compared rows, the run
    versions, the task graph, the instance states. It is folded into ``detail``
    because ``detail`` is what reaches ``summary``, and ``summary`` is the only
    part of a 40 kB payload a small model reliably reads out. A reader that has
    to join four top-level keys to rule out "it was a no-op" will not do it.
    """
    findings = [
        check for check in checks if check.get("kind") == _DISPATCH_FINDING_KIND and "task_id" in check
    ]
    if not findings:
        return
    runs_read_whole = run_history.get("runs_read_whole", True)
    order = [
        entry["dag_run_id"]
        for entry in run_history.get("runs") or []
        if isinstance(entry.get("dag_run_id"), str)
    ]
    versions = {
        entry["dag_run_id"]: entry.get("dag_version")
        for entry in run_history.get("runs") or []
        if isinstance(entry.get("dag_run_id"), str)
    }
    comparison = run_history.get("task_comparison") or {}
    edges = (tasks or {}).get("edges") or {}
    ti_states: dict[str, list[str]] = {}
    for ti in tis:
        state = ti.get("state")
        if isinstance(state, str) and state not in ti_states.setdefault(ti["task_id"], []):
            ti_states[ti["task_id"]].append(state)
    for check in findings:
        rows = _compared_rows_by_run(comparison, check["task_id"], check.get("map_index", -1))
        if not runs_read_whole:
            # The run window itself was short, so every clause drawn over it is
            # drawn over a list that stops before the history does.
            rows = replace(rows, _claimed=rows.universe + 1)
        clauses = [
            clause
            for clause in (
                _contrast_clause(order, rows, run_id, dag_version, versions),
                _recurrence_clause(order, rows, run_id),
                _impact_clause(check["task_id"], run_state, edges, ti_states),
            )
            if clause
        ]
        clauses = _said_once(clauses)
        if not clauses:
            continue
        added = ". ".join(clauses)
        detail = check["detail"]
        if detail.endswith(_NOT_ESTABLISHED):
            head = detail[: -len(_NOT_ESTABLISHED)].rstrip()
            check["detail"] = f"{head} {added}. {_NOT_ESTABLISHED}"
        else:
            check["detail"] = f"{detail}. {added}"


_CHECK_LABELS = {
    "unknown_xcom_task_id": "Latent blocker",
    "import_error": "Import error",
    "import_errors_truncated": "Note",
    # Read-coverage caveats, the same kind of entry as the truncation note above:
    # they say what this diagnosis did not get to look at, which is not a problem
    # found IN the run. Missing here, they rendered as "Check:" and were counted
    # into "this diagnosis found N problems".
    "import_errors_unreadable": "Note",
    "task_list_truncated": "Note",
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


def _is_a_problem(check: dict[str, Any]) -> bool:
    """Whether this entry is a finding about the run rather than about coverage.

    Derived from the label rather than from a second list: everything this file
    labels "Note" is a caveat — a fold entry standing for others, or a read that
    did not cover everything — and neither is a problem the run has.
    """
    return _CHECK_LABELS.get(check["kind"], "Check") != "Note"


def _summarize_failure(failure: dict[str, Any]) -> str:
    where = _fenced(_ti_where(failure))
    # The log line is whatever the task printed, so it is quoted and clamped like
    # any other value this tool did not write.
    # ``_extract_error_line`` already clips at 400 on a word boundary; the limit
    # here only has to leave room for the quotes and the escapes it adds.
    line = _quoted(_extract_error_line(failure.get("log_tail") or "") or "no log available", 440)
    # The line is drawn from a TAIL. Where the tail is not the log, the error it
    # names is the last one in what was kept and not necessarily the cause, so
    # the sentence says so rather than asserting the failure was that line.
    cut = (
        " The log was read as a tail only, so this line is the last error in the part that was "
        "read and may not be the one that failed the task."
        if failure.get("log_tail_truncated")
        else ""
    )
    if failure.get("still_retrying"):
        return f"Still retrying: {where} failed and is up for retry; last error: {line} (see log).{cut}"
    return f"Confirmed failure: {where} failed with {line} (see log).{cut}"


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
            unchecked = health["attempt_history_unchecked"]
            clauses.append(
                f" {unchecked} {'was' if unchecked == 1 else 'were'} selected for an "
                f"attempt-history read that did not come back."
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
    nothing_failed: reading.Verdict | None = None,
) -> str:
    """One deterministic digest the model can echo, enumerating every finding.

    Built server-side because a small model reliably repeats a numbered list it
    was handed, and just as reliably drops one finding out of two it has to
    assemble from separate fields.

    Both no-finding sentences are gated on a verdict rather than on the absence
    of entries. The strong one is gated on ``health['clean']``; the medium one is
    gated on ``nothing_failed``, which is the same three-valued answer the
    ``no_task_instance_failed`` field carries — because "No failures found" over
    a run whose instance list stopped at the scan ceiling is a claim about
    instances nobody read, and this is the field the plugin tells the model
    outranks every other one.
    """
    items = [_summarize_failure(failure) for failure in failures]
    items += [f"{_CHECK_LABELS.get(check['kind'], 'Check')}: {check['detail']}." for check in checks]
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
    # Every "Note" is a caveat rather than a finding: a fold entry standing for
    # others, or a read that did not cover everything. A read-coverage caveat
    # counted as a problem told the reader the run holds a problem it does not.
    caveats = sum(1 for check in checks if not _is_a_problem(check))
    problems = len(items) - caveats + folded_away
    if problems:
        # The run's id and state, in the branch that reports findings. The clean
        # branch always named them; this one did not, so the sentence a model had
        # to assemble to say "the run is green and something in it still did not
        # run" was spread over three keys — two of which say "success" on their own.
        head = (
            f"Run {_fenced(run_id)} is recorded {run_state}, and this diagnosis still found "
            f"{problems} problem{'s' if problems != 1 else ''}."
        )
    # The three no-finding sentences are gated on PROBLEMS, not on the presence
    # of entries. A caveat is not a finding: adding one — a truncated task list,
    # an unreadable import-error list — makes the picture strictly WORSE, and
    # gating on ``items`` let it delete the sentence saying what was not
    # established and replace it with a hard count of zero problems.
    elif health["clean"]:
        # Says what the conjunction actually tested. ``clean`` withholds this
        # sentence unless every success reported all the fields AND carries a
        # worker-written one, so that — and not "no forgery" — is the claim.
        head = (
            f"No problems found: run {_fenced(run_id)} is {run_state}; all "
            f"{health['successes_scanned']} task instances succeeded, and every one of them "
            f"carries a worker-written dispatch field (hostname or pid) on the attempt recorded "
            f"successful."
        )
    elif nothing_failed is not None and not nothing_failed.is_present():
        head = (
            f"NO FAILURE was found among the task instances this diagnosis READ, and that is not "
            f"the same as no failure: run {_fenced(run_id)} is {run_state}, and its instance list "
            f"was NOT read whole, so whether an instance this diagnosis did not reach failed is "
            f"NOT established.{_census_clause(health)}"
        )
    else:
        head = f"No failures found: run {_fenced(run_id)} is {run_state}.{_census_clause(health)}"
    tail = (
        f" The logs of {logs_omitted} more failed task instance(s) were omitted for size."
        if logs_omitted
        else ""
    )
    if unlisted:
        tail += f" {unlisted} further finding(s) were left out of this summary for size."
    listed = f" {' '.join(numbered)}" if numbered else ""
    return f"{head}{listed}{tail}{_coverage_clauses(health)}"


def diagnose_dag(
    dag_id: str, dag_run_id: str = "", source_digest: str | None = None, audit_scope: str = ""
) -> dict[str, Any]:
    """
    Find out what is wrong with a run of this Dag.

    ``dag_run_id`` takes an exact run id, or ``latest``/``previous`` — use those
    rather than composing a run id from a date, which is how a run that exists
    comes back as "no such run". Left empty, the first failed of the last 5 runs
    is diagnosed (else the newest). Returns every task instance with the
    fields that show whether its recorded attempt was dispatched, the log tail
    of **every** failed or retrying one, the task graph, the full Dag source,
    deterministic checks that spot broken task references, import errors and
    successes carrying no dispatch evidence, a ``run_health`` that names every
    reason the run cannot be called clean, and a ``summary`` that enumerates
    every finding. Relay the ``summary`` completely — every numbered item, not
    only the first. A green run is not automatically a healthy one: report what
    ``summary`` says, not what the run state says.

    Also returns ``event_history`` — what the Dag's audit log recorded for this
    run — with a ``last_state_change`` on every detailed task instance naming
    what was recorded, when, against which principal NAME, through which
    recorded interface, and, in ``unknowns``, what that does not establish. A
    recorded row proves an action was ATTEMPTED, never that it wrote the state;
    NO recorded row is not evidence of a direct database write. And
    ``run_history`` — the Dag's recent runs and the same task's rows across them.

    ``source_digest`` and ``audit_scope`` are set by the caller's permissions, not by you.
    """
    stale_note = ""
    diagnosed_is_latest: bool | None = None
    newest_run_info: dict[str, Any] | None = None
    if dag_run_id:
        run, error = _resolve_run(dag_id, dag_run_id)
        if run is None:
            return {"dag_id": dag_id, "dag_run_id": dag_run_id, "error": error}
        runs_error = None
        try:
            recent_runs = _recent_runs(dag_id)
        except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
            runs_error = _explain_error(e)
            recent_runs = reading.failed_read(reading._DAG_RUNS_ROUTE, runs_error)
    else:
        runs_error = None
        try:
            # Fetched at the run-history depth and resolved off the first five,
            # so the run list this diagnosis already needed is the one the field
            # reports rather than a second call for the same rows.
            recent_runs = _recent_runs(dag_id)
            runs = list(recent_runs.clamp(5).rows)
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
                # Emitted on this path above all: it is exactly where a model
                # with no run list in front of it invents a run id.
                "run_history": _run_history(dag_id, None, recent_runs, runs_error, []),
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

    instances = reading._run_task_instances(dag_id, run_path)
    tis = list(instances.rows)
    omitted = instances.omitted
    event_history = _event_history(dag_id, run["dag_run_id"], audit_scope)
    attribution_of = _attribution_reader(event_history)
    dispatch_checks, incomplete_evidence, coverage = _check_dispatch_evidence(
        dag_id, run_path, tis, attribution_of
    )
    task_instances, detail_reduced, attribution_census = _project_task_instances(
        tis,
        # The folded-findings entry names no instance, so it is not one.
        {(check["task_id"], check["map_index"]) for check in dispatch_checks if "task_id" in check},
        incomplete_evidence,
        attribution_of,
    )
    # The rows come out and the reading with them: a reader that still held the
    # scan could draw an absence from it after this point, and everything that
    # legitimately does so has already run.
    event_history.pop("reading", None)
    _enforce_attribution_ceiling(event_history, task_instances)
    _prune_unknowns_legend(event_history, task_instances, dispatch_checks)
    run_history = _run_history(
        dag_id,
        run["dag_run_id"],
        recent_runs,
        runs_error,
        _comparison_task_ids(dispatch_checks, tis),
    )

    result: dict[str, Any] = {
        "dag_id": dag_id,
        "dag_run_id": run["dag_run_id"],
        "run_state": run["state"],
        "dag_version": _run_version(run),
        # The same caveat the run-history rows carry. The top-level display is
        # the one a model reads out, so it cannot be the one without it.
        "dag_version_limits": [_R1],
        "task_instances": task_instances,
        "event_history": event_history,
        "run_history": run_history,
    }
    # Owned by the projection that measured it, and reported at the level the
    # projection lives at. It used to be written onto the event-history payload,
    # where a reader attributes a projection's shortfall to the event scan.
    result["task_instance_detail_reduced"] = detail_reduced
    if diagnosed_is_latest is not None:
        result["diagnosed_run_is_latest"] = diagnosed_is_latest
    if newest_run_info:
        result["newest_run"] = newest_run_info
    if omitted:
        result["task_instances_omitted"] = omitted
    try:
        dag = transport._api("GET", _dag_url(dag_id))
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
        task_graph = reading._tasks(dag_id)
    except (httpx.HTTPStatusError, KeyError) as e:
        task_graph = reading.failed_read(reading._TASKS_ROUTE, _explain_error(e))
    tasks = list(task_graph.rows)
    if tasks:
        order, ambiguous = _display_order(task_graph)
        result["tasks"] = {
            "order": order,
            "ordering": "topological",
            "ambiguous_positions": sorted(p + 1 for p in ambiguous),
            "task_list_read_whole": task_graph.complete,
            "edges": {
                task["task_id"]: sorted(task.get("downstream_task_ids") or [])
                for task in tasks
                if task.get("downstream_task_ids")
            },
        }
    source = result.get("source")
    static_checks: list[dict[str, str]] | None = None
    if tasks and isinstance(source, str) and not source.startswith("unavailable:"):
        static_checks, static_suppressed = _static_checks(source, {task["task_id"] for task in tasks})
        coverage = {**coverage, "static_checks_suppressed": static_suppressed}
    import_errors = _find_import_errors(dag)
    import_checks = list(import_errors.rows)
    # The read's coverage travels as checks, built HERE from the reading's own
    # numbers. It used to travel inside ``rows``, where it raised ``kept`` by one
    # and made a read that had missed exactly one row report itself whole.
    if import_errors.read_failed:
        import_checks.append(
            {
                "kind": "import_errors_unreadable",
                "detail": (
                    f"the import-error list could not be read ({import_errors.error}), so whether "
                    f"this Dag's file still imports is NOT established by this diagnosis"
                ),
            }
        )
    elif not import_errors.complete:
        import_checks.append(
            {
                "kind": "import_errors_truncated",
                "detail": (
                    f"the import-error list was not read whole ({import_errors.reason}), so an "
                    f"import error for this Dag's file may be missing from this diagnosis"
                ),
            }
        )
    if not task_graph.complete:
        import_checks = [
            *import_checks,
            {
                "kind": "task_list_truncated",
                "detail": (
                    f"the Dag's task list was not read whole ({task_graph.reason}), so the graph, "
                    f"the ordering and the source checks above cover only the tasks that were read"
                ),
            },
        ]
    checks: list[dict[str, Any]] = (static_checks or []) + import_checks + dispatch_checks
    # After the graph and the run history are both in hand, and before the
    # summary is built off ``detail``.
    _augment_dispatch_findings(
        checks,
        run["dag_run_id"],
        run["state"],
        result["dag_version"],
        run_history,
        result.get("tasks"),
        tis,
    )
    reading.flatten_comparison(run_history.get("task_comparison") or {})
    if static_checks is not None or checks:
        result["checks"] = checks

    # up_for_retry counts: the task already failed at least once, and waiting
    # for the retries to burn down before diagnosing wastes exactly the time a
    # diagnosis is for.
    # The claim "no task instance in it failed" is an absence, so it is asked of
    # the reading rather than of the rows: a run whose instance list stopped at
    # the scan ceiling used to get the all-clear over instances nobody read.
    nothing_failed = reading.none_match(
        instances,
        lambda ti: ti.get("state") in ("failed", "up_for_retry"),
        f"no failed or retrying task instance appeared in the {instances.kept} instance(s) read",
    )
    failed = [ti for ti in tis if ti.get("state") in ("failed", "up_for_retry")]
    if not failed:
        result["run_health"] = _run_health(
            run, tis, [], checks, omitted, coverage, attribution_census, event_history, run_history
        )
        # Never a second verdict. This field used to read "latest run is success;
        # no failed task instances" beside a summary saying a task in it never
        # ran — and it is the key literally called ``diagnosis``, so that is the
        # sentence that got read out. It now says what ``summary`` says, and
        # points at it.
        result["summary"] = _build_diagnosis_summary(
            run["dag_run_id"], run["state"], [], checks, 0, result["run_health"], nothing_failed
        )
        # Not a count of its own — the count belongs to ``summary`` and a second
        # computation of it is a second thing to drift.
        result["diagnosis"] = (
            f"run {run['dag_run_id']} is {run['state']} and no task instance in it failed"
            if nothing_failed.is_present()
            else f"run {run['dag_run_id']} is {run['state']} and none of the "
            f"{instances.kept} task instance(s) this diagnosis READ failed — the run's instance "
            f"list was not read whole ({instances.reason}), so whether one of the instances it "
            f"did not reach failed is NOT established"
        ) + (
            # A caveat is not a problem. Keyed on the bare list, this line said
            # the diagnosis had found problems while ``summary`` — over the same
            # list — said it had found none, in the same payload.
            ", but this diagnosis found problems in it — read `summary`, not this line"
            if any(_is_a_problem(check) for check in checks)
            else "; see `summary` for what was and was not established"
        )
        result["no_task_instance_failed"] = nothing_failed.as_field()
        if stale_note:
            result["summary"] = f"{stale_note} {result['summary']}"
        return result

    failures = []
    budget = DIAGNOSIS_LOG_BUDGET_CHARS
    for ti in failed:
        if budget <= 0:
            break
        try:
            log = transport._api(
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
        clipped = _tail(log.get("content") if isinstance(log, dict) else log)
        tail = clipped.text[-budget:]
        # ``Clipped`` carries whether anything was cut and the slice above threw
        # it away: this text lands verbatim in ``summary``, and the error line
        # named there is drawn from the tail. A real error at the top of a 5000
        # line log left the summary confidently naming a progress line as the
        # cause, with nothing in the payload to say otherwise.
        budget -= len(tail)
        failures.append(
            {
                "task_id": ti["task_id"],
                "map_index": ti.get("map_index", -1),
                "log_tail": tail,
                "log_tail_truncated": clipped.truncated or len(clipped.text) > len(tail),
                "still_retrying": ti.get("state") == "up_for_retry",
            }
        )
    result["failures"] = failures
    logs_omitted = len(failed) - len(failures)
    if logs_omitted:
        result["logs_omitted"] = logs_omitted
    result["run_health"] = _run_health(
        run, tis, failures, checks, omitted, coverage, attribution_census, event_history, run_history
    )
    result["summary"] = _build_diagnosis_summary(
        run["dag_run_id"], run["state"], failures, checks, logs_omitted, result["run_health"], nothing_failed
    )
    if stale_note:
        result["summary"] = f"{stale_note} {result['summary']}"
    # Kept for the single-failure case every prompt and card already speaks.
    result["failed_task_id"] = failures[0]["task_id"]
    result["log_tail"] = failures[0]["log_tail"]
    # The caveat travels WITH the tail wherever the tail does. Copied without it,
    # a top-level ``log_tail`` read as the log, and every absence in it as an
    # absence in the log.
    result["log_tail_truncated"] = failures[0].get("log_tail_truncated", False)
    return result


def _worker_field_verdict(scan: reading.Reading, task_id: str) -> reading.Verdict:
    """Whether any instance of this task on this run recorded a worker-written field."""
    return reading.find(
        scan,
        lambda ti: ti.get("task_id") == task_id and _carries_worker_field(ti),
        f"no instance of {task_id!r} in the part of this run that was read records a hostname or pid",
    )


def compare_dag_runs(dag_id: str, run_a: str, run_b: str, source_digest: str | None = None) -> dict[str, Any]:
    """
    Compare two runs of a Dag: per-task duration changes and conf differences.

    Answers "was it my change?" after a run that used to work starts failing.
    ``run_a``/``run_b`` take exact run ids, or ``latest``/``previous`` — so
    "compare the last two runs" is run_a="previous", run_b="latest".
    A mapped task is aggregated per task: instance count and the longest
    instance's duration. Each row also carries ``run_a_worker_field`` /
    ``run_b_worker_field`` — whether any instance of that task on that run
    recorded a hostname or pid. They are THREE-valued: true, false, and null.
    null is NOT false — it means that run's rows do not settle the question,
    either because its instance list came back incomplete or because the run
    holds no instance of that task at all, and the accompanying
    ``*_worker_field_note`` says which. A task whose duration is unchanged at 0
    on both runs has NOT been stable if those flags differ, or if both are
    false; two flags differing because one of them is null is a task that was
    added or removed, not a task that stopped being dispatched.
    Names the Dag versions each run used, but does not
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
    readings: dict[str, reading.Reading] = {}
    for label, requested in (("run_a", run_a), ("run_b", run_b)):
        run, error = _resolve_run(dag_id, requested)
        if run is None:
            return {"dag_id": dag_id, "error": error}
        run_id = run["dag_run_id"]
        scan = reading._run_task_instances(dag_id, f"/dagRuns/{quote(run_id, safe='')}")
        readings[label] = scan
        summaries[label] = {
            "dag_run_id": run_id,
            "state": run.get("state"),
            "duration": run.get("duration"),
            "version": _run_version(run),
            "conf": run.get("conf") or {},
            "task_instances_read_whole": scan.complete,
        }
        if scan.omitted:
            summaries[label]["task_instances_omitted"] = scan.omitted
        # Mapped instances aggregate to one row per task — the longest instance,
        # not whichever map_index the API listed last.
        per_task: dict[str, dict[str, Any]] = {}
        for ti in scan.rows:
            info = per_task.setdefault(ti["task_id"], {"count": 0, "duration": None})
            info["count"] += 1
            duration = ti.get("duration")
            if duration is not None and (info["duration"] is None or duration > info["duration"]):
                info["duration"] = duration
        instances[label] = per_task

    task_durations = []
    empty: dict[str, Any] = {"count": 0, "duration": None}
    for task_id in sorted(set(instances["run_a"]) | set(instances["run_b"])):
        info_a = instances["run_a"].get(task_id, empty)
        info_b = instances["run_b"].get(task_id, empty)
        a, b = info_a["duration"], info_b["duration"]
        # Durations alone cannot answer "was it my change?" for a task that
        # stopped being dispatched: a task recorded success without ever running
        # has duration 0 on BOTH runs, and this comparison then reports it as the
        # most stable task in the Dag. The flag is therefore asked of the run's
        # READING — a worker-bearing instance sitting past the scan ceiling used
        # to come back as a measured false.
        worker_fields = {
            label: _worker_field_verdict(readings[label], task_id) for label in ("run_a", "run_b")
        }
        # A task with NO instance at all on this run did not run there, and that
        # is not the same fact as "it ran and recorded no worker field". ``find``
        # over the empty selection answers a vacuous ABSENT, so the second
        # sentence is what the reader got — and since DIFFERING flags are exactly
        # the forgery signal this comparison exists to raise, a task merely added
        # or removed between two Dag versions manufactured it.
        absent_here = {
            label: info["count"] == 0 and readings[label].complete
            for label, info in (("run_a", info_a), ("run_b", info_b))
        }
        entry = {
            "task_id": task_id,
            "run_a": a,
            "run_b": b,
            "delta": round(b - a, 3) if a is not None and b is not None else None,
            # Named for what was observed - a worker-written field on the row -
            # and not for what ran: this says nothing about who or what wrote
            # the state.
            "run_a_worker_field": None if absent_here["run_a"] else worker_fields["run_a"].as_field(),
            "run_b_worker_field": None if absent_here["run_b"] else worker_fields["run_b"].as_field(),
        }
        # The type's OWN sentence for an unsettled answer, not a thirteenth
        # hand-written copy of it. ``Verdict.detail()`` and ``Verdict.route``
        # were dead in production while twelve call sites wrote the sentence
        # again, and two of those copies had already drifted apart.
        for label, verdict in worker_fields.items():
            if absent_here[label]:
                entry[f"{label}_worker_field_note"] = (
                    f"{label} holds no instance of {_fenced(task_id)} at all, so it records neither "
                    f"a worker-written field nor the absence of one — this task was not run there"
                )
            elif verdict.is_unknown():
                entry[f"{label}_worker_field_note"] = verdict.detail()
        if max(info_a["count"], info_b["count"]) > 1:
            # A count over a truncated scan is the ceiling presented as the
            # fan-out. Null says the fan-out was not read, which is what happened.
            entry["run_a_instances"] = info_a["count"] if readings["run_a"].complete else None
            entry["run_b_instances"] = info_b["count"] if readings["run_b"].complete else None
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
        # The three-valued fields say what null MEANS, in the payload, next to
        # the nulls. The docstring defines only true and false, and a small model
        # handed a null under a two-valued contract reads it as false — which is
        # the one reading these fields exist to prevent.
        "scope": (
            "`run_a_worker_field` / `run_b_worker_field` and `run_a_instances` / `run_b_instances` "
            "are three-valued. null is NOT false. It means one of two things, and the row's "
            "`*_worker_field_note` says which: that run's task-instance list came back incomplete, "
            "so an instance carrying a worker-written field — or a further mapped instance — may "
            "be sitting past what this comparison read; or that run holds no instance of the task "
            "at all, so it was not run there and records neither a worker field nor the absence of "
            "one. false means that run DOES hold instances of the task, the rows were all read, "
            "and none of them records a worker-written field. Read `task_instances_read_whole` on "
            "each run before treating any null here as an answer."
        ),
    }


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


_FAILURE_SCAN_ROUTE = "POST /dags/~/dagRuns/~/taskInstances/list"


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
        "page_limit": reading.FAILURE_SCAN_LIMIT,
    }
    if dag_ids is not None:
        body["dag_ids"] = list(dag_ids)
    resp = transport._api("POST", "/dags/~/dagRuns/~/taskInstances/list", json=body)
    scan = reading.read_of(resp, "task_instances", _FAILURE_SCAN_ROUTE)
    # Belt and braces: never fetch a log for a Dag outside the allowlist, whatever
    # the API returned. A dropped row is a row this scan did not cover, so the
    # filter reduces what was kept — the omitted count used to be computed off
    # the pre-filter list and so described a list nothing was concluded from.
    if dag_ids is not None:
        allowed = set(dag_ids)
        scan = scan.filter(lambda ti: ti.get("dag_id") in allowed)
    tis = list(scan.rows)
    failures_omitted = scan.omitted

    clusters: dict[str, dict[str, Any]] = {}
    unreadable: list[str] = []
    clipped_logs = 0
    for ti in tis:
        try:
            log = transport._api(
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
        except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
            # One 403 among fifty logs used to take the whole fleet scan down.
            # This payload already carries partial coverage as three fields, so
            # the unreadable log joins them instead of raising past the caller.
            unreadable.append(f"{_ti_where(ti)} in {ti['dag_id']}/{ti['dag_run_id']} ({_explain_error(e)})")
            continue
        clipped = _tail(log.get("content") if isinstance(log, dict) else log)
        if clipped.truncated:
            # The signature is drawn from the last error in the tail, which over
            # a cut log is not necessarily the error that failed the task — so
            # two instances can land in different clusters for no other reason.
            clipped_logs += 1
        signature = _error_signature(clipped.text)
        cluster = clusters.setdefault(signature, {"error": signature, "count": 0, "examples": []})
        cluster["count"] += 1
        if len(cluster["examples"]) < 5:
            cluster["examples"].append(
                {"dag_id": ti["dag_id"], "task_id": ti["task_id"], "dag_run_id": ti["dag_run_id"]}
            )

    clustered = len(tis) - len(unreadable)
    # The one prose field this tool has, and it used to assert unconditionally
    # that no clusters means no failed instance in the window — over a scan that
    # reports its own omissions three fields above. ``get_blast_radius`` builds
    # its scope the same way three functions down.
    scope = (
        "task instances recorded state=failed only. No clusters means no FAILED task "
        "instance in the window — it does not mean the Dags are healthy. A run recorded "
        "success whose task never ran is invisible here; diagnose_dag finds those."
        if scan.complete and not unreadable
        else (
            "task instances recorded state=failed only, and this list is short of the window: "
            # Each shortfall named ONLY when it happened. The sentence used to
            # lead with "this scan was not whole" and then state the scan's own
            # omission count whatever it was, so a whole scan with one
            # unreadable log announced "0 failed task instance(s) were not
            # scanned AND 1 log could not be read" — a false reason beside a
            # true one.
            + ", ".join(
                clause
                for clause in (
                    (
                        f"{failures_omitted} failed task instance(s) in the window were not scanned"
                        if not scan.complete
                        else ""
                    ),
                    (
                        f"{len(unreadable)} log(s) could not be read, so the failures they belong to "
                        f"are not clustered by their error"
                        if unreadable
                        else ""
                    ),
                )
                if clause
            )
            + ". An empty or short cluster list therefore says nothing about the failures this "
            "scan did not reach. A run recorded success whose task never ran is invisible here "
            "whatever the coverage; diagnose_dag finds those."
        )
    )
    if clipped_logs:
        # A clipped log can split ONE failure into two clusters, because the
        # signature is drawn from the tail and the tail is not the log. The
        # count had no prose anywhere and the reader had no way to know what it
        # meant for the grouping.
        scope += (
            f" {clipped_logs} log(s) were read as a TAIL only, so their error signature is "
            f"the last error in what was read: one failure can appear as two clusters, and two as "
            f"one."
        )
    result = {
        "window_hours": hours,
        "failures_scanned": clustered,
        "failures_omitted": failures_omitted,
        "failures_read_whole": scan.complete and not unreadable,
        "scope": scope,
        "clusters": sorted(clusters.values(), key=lambda c: c["count"], reverse=True),
    }
    if unreadable:
        result["failures_unreadable"] = unreadable
    if clipped_logs:
        result["logs_read_as_a_tail"] = clipped_logs
    return result


def get_blast_radius(dag_id: str) -> dict[str, Any]:
    """
    Show what a failure in this Dag knocks over: the assets it produces and
    the Dags scheduled on or reading those assets — plus the upstream side,
    the assets this Dag depends on and who produces them.
    """
    # The boundary's own read, not a second copy of it. This site used to inline
    # the same call with different error handling — no KeyError arm, and an
    # empty body raising TypeError on ``resp["assets"]`` — so the two drifted on
    # the one thing they exist to agree about.
    catalog = reading.read_asset_catalog()
    if catalog.read_failed:
        return {
            "dag_id": dag_id,
            "error": (
                f"the asset catalog could not be read ({catalog.error}), "
                f"so the blast radius of {dag_id} is unknown"
            ),
        }

    edges = _compute_asset_edges(dag_id, catalog)
    return {
        "dag_id": dag_id,
        "produces_assets": edges["produces"],
        "downstream_dags": edges["downstream"],
        "consumes_assets": edges["consumes"],
        "upstream_dags": edges["upstream"],
        "asset_catalog_read_whole": catalog.complete,
        # Four empty lists read as "nothing depends on this Dag". They mean the
        # asset catalog holds no edge for it, which is the common case for a Dag
        # that has real consequences and simply does not declare assets — and
        # over a catalog that was not read whole they would mean neither, so
        # they come back null instead.
        "scope": (
            "asset edges only. Empty lists mean this Dag declares no asset dependency in the "
            "catalog — not that a failure in it has no consequences. Task-level impact inside a "
            "run is in diagnose_dag's task graph, not here."
        )
        + (
            ""
            if catalog.complete
            # Said over EVERY list, not only the null ones. A non-empty
            # enumeration drawn from a truncated catalog is just as short of an
            # edge as an empty one, and it shipped with no caveat at all —
            # ``_build_asset_note`` gets this right over the same data.
            else f" The catalog was NOT read whole ({catalog.reason}), so a null list above means "
            f"this reading cannot say whether an edge exists, and a list that DOES name Dags or "
            f"assets is not closed: an edge this Dag has may be missing from it."
        ),
    }
