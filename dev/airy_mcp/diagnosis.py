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


def _run_history(
    dag_id: str,
    diagnosed_run_id: str | None,
    runs: list[dict[str, Any]],
    total: int,
    error: str | None,
    task_ids: list[str],
) -> dict[str, Any]:
    """The Dag's recent runs, and the diagnosed run's place among them.

    A field rather than a tool: every question it answers is a question about the
    run already under diagnosis, and the comparison set is chosen BY the
    diagnosis rather than guessed at by a caller.
    """
    window = runs[:RUN_HISTORY_LIMIT]
    listed = []
    for run in window:
        entry = {name: run.get(name) for name in _RUN_HISTORY_KEYS}
        entry["dag_version"] = _run_version(run)
        entry["is_diagnosed_run"] = run.get("dag_run_id") == diagnosed_run_id
        listed.append(entry)
    return {
        "returned": len(listed),
        "total_entries": total,
        "runs_omitted": max(total - len(listed), 0),
        "window": (
            f"the most recent {len(listed)} run(s) of this Dag by run_after, newest first"
            if listed
            else "this Dag has no runs"
        ),
        "runs": listed,
        "task_comparison": (
            _task_comparison(dag_id, window, task_ids)
            if error is None
            else {
                "selection": _TASK_COMPARISON_SELECTION,
                "task_ids_compared": [],
                "task_ids_omitted": 0,
                "runs_not_covered": [],
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


def _compared_rows_by_run(
    comparison: dict[str, Any], task_id: str, map_index: int
) -> dict[str, dict[str, Any]]:
    """The compared task's newest row per run, for the one instance a finding names."""
    rows: dict[str, dict[str, Any]] = {}
    for row in (comparison.get("tasks") or {}).get(task_id) or []:
        run_id = row.get("dag_run_id")
        if row.get("map_index", -1) != map_index or not isinstance(run_id, str):
            continue
        rows.setdefault(run_id, row)
    return rows


def _recurrence_clause(order: list[str], rows: dict[str, dict[str, Any]], run_id: str) -> str:
    """How far back this run's missing dispatch evidence goes, counted rather than implied.

    "1 problem found" is a statement about one run. A task that has recorded no
    worker field for four cycles is a different fact, and it is one the compared
    rows already hold — so it is stated instead of left for the reader to count.
    """
    if run_id not in order:
        return ""
    streak = []
    for other in order[order.index(run_id) :]:
        row = rows.get(other)
        if row is None or _carries_worker_field(row):
            break
        streak.append(other)
    if len(streak) < 2:
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
    rows: dict[str, dict[str, Any]],
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
    """
    for other in order:
        row = rows.get(other)
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
        clauses = [
            clause
            for clause in (
                _contrast_clause(order, rows, run_id, dag_version, versions),
                _recurrence_clause(order, rows, run_id),
                _impact_clause(check["task_id"], run_state, edges, ti_states),
            )
            if clause
        ]
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
    # The run's id and state, in the branch that reports findings. The clean
    # branch always named them; this one did not, so the sentence a model had to
    # assemble to say "the run is green and something in it still did not run"
    # was spread over three keys — two of which say "success" on their own.
    head = (
        f"Run {_fenced(run_id)} is recorded {run_state}, and this diagnosis still found "
        f"{problems} problem{'s' if problems != 1 else ''}."
    )
    tail = (
        f" The logs of {logs_omitted} more failed task instance(s) were omitted for size."
        if logs_omitted
        else ""
    )
    if unlisted:
        tail += f" {unlisted} further finding(s) were left out of this summary for size."
    return f"{head} {' '.join(numbered)}{tail}{_coverage_clauses(health)}"


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
            recent_runs, runs_total = _recent_runs(dag_id)
        except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
            recent_runs, runs_total, runs_error = [], 0, _explain_error(e)
    else:
        runs_error = None
        try:
            # Fetched at the run-history depth and resolved off the first five,
            # so the run list this diagnosis already needed is the one the field
            # reports rather than a second call for the same rows.
            recent_runs, runs_total = _recent_runs(dag_id)
            runs = recent_runs[:5]
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
                "run_history": _run_history(dag_id, None, [], runs_total, runs_error, []),
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

    tis, omitted = reading._run_task_instances(dag_id, run_path)
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
    event_history.pop("rows", None)
    event_history["instances_without_attribution"] = detail_reduced
    _enforce_attribution_ceiling(event_history, task_instances)
    _prune_unknowns_legend(event_history, task_instances, dispatch_checks)
    run_history = _run_history(
        dag_id,
        run["dag_run_id"],
        recent_runs,
        runs_total,
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
    if detail_reduced:
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
        tasks = reading._tasks(dag_id)
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
    if static_checks is not None or checks:
        result["checks"] = checks

    # up_for_retry counts: the task already failed at least once, and waiting
    # for the retries to burn down before diagnosing wastes exactly the time a
    # diagnosis is for.
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
            run["dag_run_id"], run["state"], [], checks, 0, result["run_health"]
        )
        # Not a count of its own — the count belongs to ``summary`` and a second
        # computation of it is a second thing to drift.
        result["diagnosis"] = (
            f"run {run['dag_run_id']} is {run['state']} and no task instance in it failed"
            + (
                ", but this diagnosis found problems in it — read `summary`, not this line"
                if checks
                else "; see `summary` for what was and was not established"
            )
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
    result["run_health"] = _run_health(
        run, tis, failures, checks, omitted, coverage, attribution_census, event_history, run_history
    )
    result["summary"] = _build_diagnosis_summary(
        run["dag_run_id"], run["state"], failures, checks, logs_omitted, result["run_health"]
    )
    if stale_note:
        result["summary"] = f"{stale_note} {result['summary']}"
    # Kept for the single-failure case every prompt and card already speaks.
    result["failed_task_id"] = failures[0]["task_id"]
    result["log_tail"] = failures[0]["log_tail"]
    return result


def compare_dag_runs(dag_id: str, run_a: str, run_b: str, source_digest: str | None = None) -> dict[str, Any]:
    """
    Compare two runs of a Dag: per-task duration changes and conf differences.

    Answers "was it my change?" after a run that used to work starts failing.
    ``run_a``/``run_b`` take exact run ids, or ``latest``/``previous`` — so
    "compare the last two runs" is run_a="previous", run_b="latest".
    A mapped task is aggregated per task: instance count and the longest
    instance's duration. Each row also carries ``run_a_worker_field`` /
    ``run_b_worker_field`` — whether any instance of that task on that run
    recorded a hostname or pid. A task whose duration is unchanged at 0 on both
    runs has NOT been stable if those flags differ, or if both are false.
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
    for label, requested in (("run_a", run_a), ("run_b", run_b)):
        run, error = _resolve_run(dag_id, requested)
        if run is None:
            return {"dag_id": dag_id, "error": error}
        run_id = run["dag_run_id"]
        tis, omitted = reading._run_task_instances(dag_id, f"/dagRuns/{quote(run_id, safe='')}")
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
            info = per_task.setdefault(
                ti["task_id"], {"count": 0, "duration": None, "worker_dispatched": False}
            )
            info["count"] += 1
            duration = ti.get("duration")
            if duration is not None and (info["duration"] is None or duration > info["duration"]):
                info["duration"] = duration
            # Durations alone cannot answer "was it my change?" for a task that
            # stopped being dispatched: a task recorded success without ever
            # running has duration 0 on BOTH runs, and this comparison then
            # reports it as the most stable task in the Dag.
            if _carries_worker_field(ti):
                info["worker_dispatched"] = True
        instances[label] = per_task

    task_durations = []
    empty = {"count": 0, "duration": None, "worker_dispatched": False}
    for task_id in sorted(set(instances["run_a"]) | set(instances["run_b"])):
        info_a = instances["run_a"].get(task_id, empty)
        info_b = instances["run_b"].get(task_id, empty)
        a, b = info_a["duration"], info_b["duration"]
        entry = {
            "task_id": task_id,
            "run_a": a,
            "run_b": b,
            "delta": round(b - a, 3) if a is not None and b is not None else None,
            # Named for what was observed - a worker-written field on the row -
            # and not for what ran: this says nothing about who or what wrote
            # the state.
            "run_a_worker_field": info_a["worker_dispatched"],
            "run_b_worker_field": info_b["worker_dispatched"],
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
        "page_limit": reading.FAILURE_SCAN_LIMIT,
    }
    if dag_ids is not None:
        body["dag_ids"] = list(dag_ids)
    resp = transport._api("POST", "/dags/~/dagRuns/~/taskInstances/list", json=body)
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
        # An empty result here is not an all-clear, and nothing else in this
        # payload says so: the scan only ever sees task instances in state
        # `failed`, which is exactly the state the interesting cases are not in.
        "scope": (
            "task instances recorded state=failed only. No clusters means no FAILED task "
            "instance in the window — it does not mean the Dags are healthy. A run recorded "
            "success whose task never ran is invisible here; diagnose_dag finds those."
        ),
        "clusters": sorted(clusters.values(), key=lambda c: c["count"], reverse=True),
    }


def get_blast_radius(dag_id: str) -> dict[str, Any]:
    """
    Show what a failure in this Dag knocks over: the assets it produces and
    the Dags scheduled on or reading those assets — plus the upstream side,
    the assets this Dag depends on and who produces them.
    """
    try:
        assets = transport._api("GET", "/assets", params={"limit": 100})["assets"]
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
        # Four empty lists read as "nothing depends on this Dag". They mean the
        # asset catalog holds no edge for it, which is the common case for a Dag
        # that has real consequences and simply does not declare assets.
        "scope": (
            "asset edges only. Empty lists mean this Dag declares no asset dependency in the "
            "catalog — not that a failure in it has no consequences. Task-level impact inside a "
            "run is in diagnose_dag's task graph, not here."
        ),
    }
