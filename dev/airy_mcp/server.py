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
import contextlib
import difflib
import json
import os  # noqa: F401 - re-exported for ``server.os``, which the suite patches ``replace`` on
import re
from datetime import datetime, timedelta, timezone
from hashlib import md5
from typing import Any
from urllib.parse import quote

# Wave 5 of the move-only extraction: the Dag file - where it is, the jail around
# it, the lock, the atomic replace, the reparse, and the scans over the text it
# holds - lives in ``dagsource.py`` now.  ``DAGS_DIR``, ``REPARSE_TIMEOUT_S`` and
# ``_read_reviewed_file`` are rebound by the suite, so they are reached through the
# module object and never imported by name; the rest are re-exported below, where a
# re-export is the same object because nothing ever rebinds them.
import dagsource

# Wave 6 of the move-only extraction: what the event log and the dispatch fields
# establish about an attempt - and what they cannot - lives in ``evidence.py`` now,
# together with the legend of caveats it ships as data.  ``TASK_INSTANCE_DETAIL_LIMIT``,
# ``TRIES_PROBE_LIMIT`` and ``DISPATCH_FINDING_LIMIT`` are rebound by the suite and are
# read only inside that module, so they are deliberately not re-exported here; the rest
# are, where a re-export is the same object because nothing ever rebinds them.
import httpx

# Wave 4 of the move-only extraction: every bounded read, and every scan, page and
# clamp bound that bounds one, lives in ``reading.py`` now.  ``_tasks``,
# ``_run_task_instances``, ``_expandable_probe`` and the seven bounds the suite rebinds
# are reached through the module object and never imported by name, so the module the
# read runs in is the one place that has to be patched for the patch to be felt.
import reading

# Wave 2 of the move-only extraction: the HTTP conversation lives in ``transport.py``
# now.  ``_api`` and ``API_URL`` are reached through the module object and never
# imported by name, so one binding serves every call site and the suite has exactly one
# place to patch.
import transport

# Wave 3 of the move-only extraction: the plan tokens, the approved-instance-set
# baselines and the run-identity rule live in ``approvals.py`` now.  The functions and
# the two stores are re-exported here so every ``server.X`` reference keeps resolving -
# the stores are mutated in place, never rebound, so this is the one dict object.  The
# three bounds (``_TOKEN_TTL_S``, ``_TOKEN_MAX``, ``_APPROVED_SET_MAX``) are NOT
# re-exported: the suite rebinds the TTL, and a rebound number is only ever seen by the
# module that reads it.
from approvals import (
    _approved_clear_sets,  # noqa: F401 - re-exported for ``server._approved_clear_sets``
    _approved_set_record,
    _discard_same_source_plans,
    _issue_token,
    _issued_tokens,  # noqa: F401 - re-exported for ``server._issued_tokens``
    _peek_token,
    _record_approved_set,
    _redeem_token,
    _run_identity,
    _same_runs,
)
from dagsource import (
    _STATIC_CHECKS_FOLDED_KIND,
    STATIC_CHECK_LIMIT,  # noqa: F401 - re-exported for ``server.STATIC_CHECK_LIMIT``
    DagFileDriftError,
    DagFileError,
    _backup_path,
    _build_revert_diff,
    _change_impact,
    _dag_path,
    _definition_updates,
    _display_order,
    _downstream_task_ids,
    _exclusive,
    _find_unaddressed_findings,
    _force_reparse,
    _normalized_changes,
    _parsed_source,
    _patch,
    _resolve_task,
    _static_checks,
    _write_if_unchanged,
)
from evidence import (
    _AUDITED_TI_STATE_ACTIONS,  # noqa: F401 - re-exported for ``server._AUDITED_TI_STATE_ACTIONS``
    _DISPATCH_EVIDENCE_KEYS,
    _DISPATCH_FINDING_KIND,
    _DISPATCH_TRUNCATED_KIND,
    _L1,  # noqa: F401 - re-exported for ``server._L1``
    _L2,  # noqa: F401 - re-exported for ``server._L2``
    _L3,  # noqa: F401 - re-exported for ``server._L3``
    _L4,  # noqa: F401 - re-exported for ``server._L4``
    _L5,  # noqa: F401 - re-exported for ``server._L5``
    _L6,  # noqa: F401 - re-exported for ``server._L6``
    _L7,  # noqa: F401 - re-exported for ``server._L7``
    _NOT_ESTABLISHED,
    _R1,
    _R2,
    _TASK_INSTANCE_DETAIL_KEYS,  # noqa: F401 - re-exported for ``server._TASK_INSTANCE_DETAIL_KEYS``
    _U1,  # noqa: F401 - re-exported for ``server._U1``
    _UNKNOWNS,  # noqa: F401 - re-exported for ``server._UNKNOWNS``
    ATTRIBUTION_PAYLOAD_LIMIT_CHARS,  # noqa: F401 - re-exported for ``server.ATTRIBUTION_PAYLOAD_LIMIT_CHARS``
    COVERAGE_NAME_LIMIT,  # noqa: F401 - re-exported for ``server.COVERAGE_NAME_LIMIT``
    EVENT_HISTORY_PER_INSTANCE,  # noqa: F401 - re-exported for ``server.EVENT_HISTORY_PER_INSTANCE``
    EXTRA_KEY_CLAMP_CHARS,  # noqa: F401 - re-exported for ``server.EXTRA_KEY_CLAMP_CHARS``
    EXTRA_KEY_LIMIT,  # noqa: F401 - re-exported for ``server.EXTRA_KEY_LIMIT``
    EXTRA_LIST_LIMIT,  # noqa: F401 - re-exported for ``server.EXTRA_LIST_LIMIT``
    RUN_SCOPED_EVENT_LIMIT,  # noqa: F401 - re-exported for ``server.RUN_SCOPED_EVENT_LIMIT``
    _attribution_reader,
    _check_dispatch_evidence,
    _classify_event,  # noqa: F401 - re-exported for ``server._classify_event``
    _enforce_attribution_ceiling,
    _event_history,
    _is_never_dispatched_attempt,
    _is_request_settable_targeting,  # noqa: F401 - re-exported for ``server._is_request_settable_targeting``
    _project_task_instances,
    _prune_unknowns_legend,
    _run_health,
    _tries_probe_tier,  # noqa: F401 - re-exported for ``server._tries_probe_tier``
)
from fastmcp import FastMCP

# Wave 1 of the move-only extraction: these live in ``primitives.py`` now and are
# re-exported here so every ``server.X`` reference - the suite's and this file's -
# keeps resolving to the one object.  None of them is monkeypatched by the suite.
from primitives import (
    _ATTR_AUDITED_OTHER,  # noqa: F401 - re-exported for ``server._ATTR_AUDITED_OTHER``
    _ATTR_AUDITED_PATCH,  # noqa: F401 - re-exported for ``server._ATTR_AUDITED_PATCH``
    _ATTRIBUTION_DETAIL,  # noqa: F401 - re-exported for ``server._ATTRIBUTION_DETAIL``
    _ATTRIBUTION_SENTENCE,  # noqa: F401 - re-exported for ``server._ATTRIBUTION_SENTENCE``
    _DEMOTED_DETAIL,  # noqa: F401 - re-exported for ``server._DEMOTED_DETAIL``
    _DEMOTED_SENTENCE,  # noqa: F401 - re-exported for ``server._DEMOTED_SENTENCE``
    _FORGEABLE_NUMBERING,  # noqa: F401 - re-exported for ``server._FORGEABLE_NUMBERING``
    _PROSE_LINE_BREAKS,  # noqa: F401 - re-exported for ``server._PROSE_LINE_BREAKS``
    _PROSE_UNSAFE,  # noqa: F401 - re-exported for ``server._PROSE_UNSAFE``
    _TASK_LOG_SOURCE,  # noqa: F401 - re-exported for ``server._TASK_LOG_SOURCE``
    EVENT_EXTRA_CLAMP_CHARS,  # noqa: F401 - re-exported for ``server.EVENT_EXTRA_CLAMP_CHARS``
    EVENT_NAME_CLAMP_CHARS,  # noqa: F401 - re-exported for ``server.EVENT_NAME_CLAMP_CHARS``
    EVENT_OWNER_CLAMP_CHARS,  # noqa: F401 - re-exported for ``server.EVENT_OWNER_CLAMP_CHARS``
    OPERATOR_CLAMP_CHARS,  # noqa: F401 - re-exported for ``server.OPERATOR_CLAMP_CHARS``
    _carries_execution_fields,
    _carries_worker_field,
    _check,
    _clip_at_word,
    _fenced,
    _later_than,
    _now_iso,
    _quoted,
    _run_version,
    _tagged_log,
    _ti_key,
    _ti_where,
)

# The reads and bounds the suite only ever reads, never rebinds, so a re-export here is
# the same object the call sites use.  The rebound ones are absent on purpose: see the
# ``import reading`` note above.
from reading import (
    _HISTORY_CLAMPED,  # noqa: F401 - re-exported for ``server._HISTORY_CLAMPED``
    _TASK_COMPARISON_SELECTION,
    LOG_TAIL_CHARS,  # noqa: F401 - re-exported for ``server.LOG_TAIL_CHARS``
    RUN_HISTORY_LIMIT,
    _attempt_history,
    _attempt_log,
    _attempt_reading,
    _attempt_rows,  # noqa: F401 - re-exported for ``server._attempt_rows``
    _audit_transitions,
    _backfill_runs,
    _build_asset_note,
    _compute_asset_edges,
    _dry_run_backfill,
    _duration_baseline,
    _find_import_errors,
    _latest_version,
    _read_is_complete,
    _recent_runs,
    _recorded_output,
    _resolve_run,
    _tail,
    _task_comparison,
    _tasks_reading,
    _version_context,
)

# The four transport helpers the suite only ever reads, never patches, so a re-export
# here is the same object the call sites use.
from transport import (
    _api_detail,
    _dag_url,
    _explain_error,
    _explain_unknown_dag,
)

# One diagnosis now carries every failed task's log, so it needs a ceiling the
# per-log tail does not give: a fan-out of 200 failed mapped instances would
# otherwise return 800 KB and blow the model's context on its way through.
DIAGNOSIS_LOG_BUDGET_CHARS = 12000
# The summary is the prose the model echoes verbatim, so it needs the ceiling the
# log path already has — for the same reason and in the same units.
DIAGNOSIS_SUMMARY_BUDGET_CHARS = 12000

mcp: FastMCP = FastMCP("airy-selfheal")


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
        dag = transport._api("GET", _dag_url(dag_id))
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
        current = dagsource._read_reviewed_file(dag_id, path, source_digest)
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
        dag = transport._api("GET", _dag_url(dag_id))
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
            current = dagsource._read_reviewed_file(dag_id, path, source_digest)
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
        dag = transport._api("GET", _dag_url(dag_id))
    except httpx.HTTPStatusError as e:
        message = _explain_unknown_dag(dag_id, e)
        if message is None:
            raise
        return {"applied": False, "mutation_applied": False, "error": message}
    path = _dag_path(dag_id, dag)
    version_before_write = _latest_version(dag_id)
    with _exclusive(path):
        try:
            source = dagsource._read_reviewed_file(dag_id, path, source_digest)
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
        dag = transport._api("GET", _dag_url(dag_id))
    except httpx.HTTPStatusError as e:
        message = _explain_unknown_dag(dag_id, e)
        if message is None:
            raise
        return {"triggered": False, "mutation_applied": False, "error": message}
    if conf:
        # Validated before the pause flow so a bad conf cannot burn an
        # unpause_token the user's warning was already spent on.
        details = transport._api("GET", _dag_url(dag_id, "/details"))
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
        transport._api("PATCH", _dag_url(dag_id), json={"is_paused": False})
        unpaused = True
    else:
        unpaused = False
    try:
        run = transport._api(
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


def _clear_body(
    dag_run_id: str,
    markers: list[Any],
    *,
    dry_run: bool,
    only_failed: bool,
    include_downstream: bool,
    run_on_latest_version: bool | None,
) -> dict[str, Any]:
    """The exact clear this tool performs — every flag stated bar one, and that one deliberately.

    ``run_on_latest_version`` is the exception. Sending it always, as this tool
    used to, inverts Airflow's own answer and bypasses two layers of it: the
    route resolves an omitted value from the Dag's ``rerun_with_latest_version``,
    then from ``[core] rerun_with_latest_version``, then falls back to ``False``.
    So ``None`` means "omit the key and let Airflow decide", which is the default
    here, and a boolean is only sent when the user actually chose one.

    ``include_downstream`` defaults on, as it does in Airflow's own clear dialog,
    because clearing a task alone usually does not finish the job: a downstream
    left in ``upstream_failed`` is never rescheduled, one that already succeeded
    keeps the XCom value the re-run was supposed to replace, and one that
    reported completion on the strength of a state nobody executed keeps saying
    so. Widening it is not silent — the plan lists every instance it pulls in.

    ``include_future``/``include_past`` stay off and are never offered: on a run
    with no logical date the route rejects them outright (HTTP 400).
    """
    body: dict[str, Any] = {
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
    }
    if run_on_latest_version is not None:
        body["run_on_latest_version"] = run_on_latest_version
    return body


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
    run_tis, omitted = reading._run_task_instances(dag_id, f"/dagRuns/{quote(dag_run_id, safe='')}")
    if omitted:
        return None, (
            f"run {dag_run_id} has more task instances than this tool will read "
            f"({omitted} not seen), so it cannot tell whether clearing would change the task set"
        )
    tasks, tasks_total = _tasks_reading(dag_id)
    if len(tasks) < tasks_total:
        return None, (
            f"the latest version of {dag_id} lists more tasks than this tool read "
            f"({len(tasks)} of {tasks_total}), so it cannot tell whether clearing would change "
            f"the task set"
        )
    current = {ti["task_id"] for ti in run_tis}
    latest = {task["task_id"] for task in tasks}
    if current == latest:
        return None, None
    return {"added": sorted(latest - current), "removed": sorted(current - latest)}, None


# ---------------------------------------------------------------------------
# Recovery: what a clear has to show before anyone approves it, and what has to
# be true afterwards before it may be called a recovery.
#
# The whole point of this block is that turning the square green is not the job.
# A clear that re-runs a task nobody can show ran, or that leaves a downstream
# artefact asserting a completion that never happened, has changed a colour and
# nothing else.
# ---------------------------------------------------------------------------

# What the plan quotes off the live row. Deliberately the same field list the
# dispatch reading is built from, so the prose and the numbers cannot drift.
_RECOVERY_ROW_KEYS = (
    "state",
    "try_number",
    "max_tries",
    "hostname",
    "pid",
    "queued_when",
    "scheduled_when",
    "start_date",
    "end_date",
    "duration",
)
# The four a dispatch writes. Only hostname and pid come from a worker.
_EXECUTION_FIELDS = ("hostname", "pid", "queued_when", "scheduled_when")
# How far below its own history an attempt's duration may fall before the
# comparison stops vouching for it. A ratio, not a constant: what "too fast"
# means is the task's own business.
RECOVERY_DURATION_FLOOR = 0.5

_RECOVERY_SCOPE_NOTE = (
    "This reading describes the attempt recorded on the live row. It is not a statement about "
    "earlier attempts: 'not dispatched on this attempt' is never 'never executed historically'."
)
_RECOVERY_EXTERNAL_NOTE = (
    "Nothing in this tool observes the system the task talks to. Airflow's fields describe dispatch "
    "and nothing else, so whether the external operation happened — in whole, in part, or not at "
    "all — has to be established there, by the operator, before this clear is approved."
)


def _plan_target(plan: dict[str, Any]) -> str:
    """The task the clear was aimed at — the seed, before include_downstream widened it."""
    markers = plan.get("task_ids") or []
    if not markers:
        return ""
    marker = markers[0]
    return marker if isinstance(marker, str) else str(marker[0])


_PARTIAL_UNSETTLED_BY_HISTORY = (
    "Whether an earlier attempt of this instance left an external effect is NOT settled here: that "
    "needs the attempt history read whole, with no attempt carrying execution fields."
)


def _recovery_evidence(dag_id: str, run_path: str, ti: dict[str, Any]) -> dict[str, Any]:
    """What the evidence says about the attempt this instance has recorded.

    Quotes the row rather than summarising it, then says — in one sentence, with
    the reach it actually has — which of three readings the fields support:
    nothing was dispatched on this attempt, something *was* dispatched and the
    row cannot tell a finished attempt from a killed one, or the response did
    not carry the fields to decide with.
    """
    row = {name: ti.get(name) for name in _RECOVERY_ROW_KEYS}
    row["rendered_fields_present"] = bool(ti.get("rendered_fields"))
    # The reading, not the response: what this function may conclude an absence
    # from is what it read, and it reads at most RECOVERY_ATTEMPT_LIMIT rows.
    history = _attempt_reading(_attempt_history(dag_id, run_path, ti))
    rows = history["rows"]
    live_executed = _carries_execution_fields(ti)
    others = [r for r in rows if r.get("try_number") != ti.get("try_number")]
    if history["status"] in ("checked", "partial"):
        earlier_executed: bool | None = any(_carries_execution_fields(r) for r in others)
        # A truncated list can only ever prove presence.
        if history["status"] == "partial" and not earlier_executed:
            earlier_executed = None
    else:
        earlier_executed = None

    missing = [key for key in _DISPATCH_EVIDENCE_KEYS if key not in ti]
    present = [name for name in _EXECUTION_FIELDS if ti.get(name) not in (None, "")]
    # ``False`` here is a positive claim that nothing outside Airflow can have
    # been touched, and it SUPPRESSES the half-operation warning. It is only ever
    # earned by a history that was read whole and holds no attempt with execution
    # fields; a truncated history, an unread one, or one that does hold such an
    # attempt leaves the question open, which is ``None``, not ``False``.
    history_rules_out_partial = history["status"] == "checked" and earlier_executed is False
    if _is_never_dispatched_attempt(ti):
        dispatched: bool | None = False
        partial_possible: bool | None = False if history_rules_out_partial else None
        reading = (
            f"{_ti_where(ti)} is recorded {_quoted(ti.get('state'))} at try_number "
            f"{_quoted(ti.get('try_number'))} and this attempt carries none of the fields a "
            f"dispatched attempt writes: hostname is empty, pid is null, queued_when is null and "
            f"scheduled_when is null, with duration 0 and start_date equal to end_date. That is "
            f"consistent with the task process never having been dispatched on this attempt. It is "
            f"not a finding about the external system, and it does not say what wrote the state."
        ) + ("" if history_rules_out_partial else f" {_PARTIAL_UNSETTLED_BY_HISTORY}")
    elif missing:
        dispatched = None
        partial_possible = None
        reading = (
            f"the response for {_ti_where(ti)} did not carry {missing}, so nothing here establishes "
            f"whether this attempt was dispatched."
        )
    elif not present:
        # The row carries every field this reading needs and none of them is set,
        # yet the shape is not the one ``_is_never_dispatched_attempt`` recognises
        # — an ordinary failed, upstream_failed or freshly-cleared instance lands
        # here. Reading that as "dispatched: false, nothing partial is possible"
        # was the tool asserting the external system was untouched on the
        # strength of four empty fields it has no such reach over.
        dispatched = None
        partial_possible = None
        reading = (
            f"{_ti_where(ti)} is recorded {_quoted(ti.get('state'))} at try_number "
            f"{_quoted(ti.get('try_number'))} with duration {_quoted(ti.get('duration'))}, and none "
            f"of hostname, pid, queued_when or scheduled_when is set — but the row does not match "
            f"the shape of an attempt that was never dispatched either. These fields therefore "
            f"settle neither whether this attempt was dispatched nor whether an external effect is "
            f"possible. {_PARTIAL_UNSETTLED_BY_HISTORY}"
        )
    else:
        dispatched = True
        partial_possible = True
        reading = (
            f"{_ti_where(ti)} is recorded {_quoted(ti.get('state'))} at try_number "
            f"{_quoted(ti.get('try_number'))} and this attempt carries execution fields "
            f"({', '.join(present)}), duration {_quoted(ti.get('duration'))}. A row like "
            f"this cannot distinguish an attempt that finished its work from one that was killed "
            f"part-way and had a state written over it, so a partial external effect is possible "
            f"and only the external system settles it."
        )
    return {
        "task_id": ti["task_id"],
        "map_index": ti.get("map_index", -1),
        "row": row,
        "reading": reading,
        "current_attempt_dispatched": dispatched,
        "partial_external_effect_possible": partial_possible,
        "attempt_history": {
            "status": history["status"],
            "attempts_recorded": history.get("attempts_recorded"),
            "attempts": rows,
            "earlier_attempt_carries_execution_fields": earlier_executed,
            "error": history.get("error"),
        },
        "live_row_carries_execution_fields": live_executed,
        "log_for_recorded_attempt": _tagged_log(_attempt_log(dag_id, run_path, ti, ti.get("try_number"))),
        "scope_note": _RECOVERY_SCOPE_NOTE,
        "external_system": {"observed": False, "note": _RECOVERY_EXTERNAL_NOTE},
    }


def _mapped_in_closure(dag_id: str, run_path: str, affected: list[dict[str, Any]]) -> dict[str, Any]:
    """Task ids in this closure whose instance set the scheduler recomputes on re-run.

    Three markers, because the two cheap ones are both blind to the same case —
    a task group that expanded to zero. A live row at ``map_index >= 0`` proves
    the task expanded in THIS run, but a zero-expansion leaves one placeholder at
    ``map_index = -1``; ``is_mapped`` on ``/tasks`` is ``_is_mapped``, true only
    for a ``MappedOperator`` and false for every task inside a mapped task group.
    So every task the cheap markers have not settled POSITIVELY is put to
    ``/listMapped``, which is gated on the real predicate.

    ``settled`` is false when any probe declined to answer. The callers must then
    warn and refuse to state a creation count: an empty list is a claim.
    """
    names = {ti["task_id"] for ti in affected}
    mapped = {ti["task_id"] for ti in affected if ti.get("map_index", -1) >= 0}
    # Suppressed rather than propagated: the authoritative probe below covers
    # every task this marker would have, so losing it costs calls, not answers.
    with contextlib.suppress(httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError):
        mapped |= {
            task["task_id"]
            for task in reading._tasks(dag_id)
            if task.get("task_id") in names and task.get("is_mapped")
        }
    unprobed: list[str] = []
    for task_id in sorted(names - mapped):
        answer = reading._expandable_probe(dag_id, run_path, task_id)
        if answer is True:
            mapped.add(task_id)
        elif answer is None:
            unprobed.append(task_id)
    return {"tasks": sorted(mapped), "settled": not unprobed, "unprobed": unprobed}


def _recovery_warnings(
    affected: list[dict[str, Any]],
    target: str,
    *,
    include_downstream: bool,
    partial_possible: bool | None,
    mapped_tasks: list[str],
    mapped_settled: bool = True,
    unprobed_tasks: list[str] | None = None,
) -> list[str]:
    """What the operator has to be told before approving, whether or not they ask."""
    downstream = [_ti_where(ti) for ti in affected if _ti_where(ti) != target]
    warnings = [
        "Re-running performs the task's external operation again. Clearing is not idempotent for "
        "anything outside Airflow: if the operation already happened, in whole or in part, this "
        "produces a duplicate.",
        _RECOVERY_EXTERNAL_NOTE,
        "The live task-instance row will be overwritten. Afterwards it reads as an ordinary "
        "success, and the attempt recorded now survives only in /tries, in that attempt's own log, "
        "and in whatever audit rows this API can see.",
        "reset_dag_runs is on, so the Dag run's start_date, end_date and queued_at are rewritten to "
        "the re-run. The run row will stop recording when the original run happened.",
        "The Dag version does not pin the code: what executes is the Dag file as it stands on disk "
        "at re-run time.",
    ]
    if partial_possible is not False:
        warnings.append(
            "This instance's row does not rule out a partly-completed attempt, so the duplicate "
            "above may be a duplicate of half an operation."
        )
    if mapped_tasks:
        warnings.append(
            f"The mapped task(s) {mapped_tasks} are in this clear, and a mapped task's fan-out is "
            f"recomputed from its upstream's output when the run is re-queued — it is not taken "
            f"from the list above. So this clear MAY CREATE task instances that are not enumerated "
            f"here, and MAY leave instances that are enumerated here with no counterpart after the "
            f"re-run. The list is what exists now, not what will exist; the expansion happens in "
            f"the scheduler, after the clear returns."
        )
    if not mapped_settled:
        warnings.append(
            f"Whether this closure holds a task whose fan-out the scheduler recomputes is NOT "
            f"settled: the check that decides it did not answer for {sorted(unprobed_tasks or [])}. "
            f"A task inside a mapped task group reports is_mapped false and, when the group expanded "
            f"to nothing, leaves a single row at map_index -1 — so the enumerated list above cannot "
            f"be read as closed. Treat this clear as one that MAY CREATE task instances that are not "
            f"enumerated here, and MAY leave enumerated ones with no counterpart after the re-run."
        )
    if include_downstream and downstream:
        warnings.append(
            f"The downstream instances {downstream} re-run too, so anything they wrote on the "
            f"strength of the current state — completion notices, summaries, notifications — is "
            f"produced again and replaces what is there now."
        )
    elif not include_downstream:
        warnings.append(
            "include_downstream is off, so anything the downstream tasks already wrote on the "
            "strength of the current state SURVIVES unchanged — including any artefact asserting "
            "that this task completed. Nothing in Airflow flags that artefact as stale."
        )
    return warnings


def _clear_flags(
    *,
    only_failed: bool,
    include_downstream: bool,
    target_state: Any,
    run_on_latest_version: bool | None,
) -> dict[str, Any]:
    """Every flag this clear sends, with the reason it carries that value."""
    if only_failed:
        only_failed_why = (
            "on, so only a failed or upstream_failed instance is a match; a task in any other state "
            "is not cleared and the request succeeds having changed nothing"
        )
    else:
        only_failed_why = (
            f"off, because the instance is recorded {_quoted(target_state)} rather than failed. With "
            f"only_failed on, the endpoint answers HTTP 200 with an empty list and clears nothing."
        )
    return {
        "only_failed": {"sent": only_failed, "why": only_failed_why},
        "include_downstream": {
            "sent": include_downstream,
            "why": (
                "on, and transitive — it takes everything reachable from the task, not one hop. The "
                "tasks released by the current state wrote artefacts that assert completion; "
                "clearing the task alone leaves them standing."
                if include_downstream
                else "off at the caller's request; the downstream keeps whatever it already wrote."
            ),
        },
        "include_upstream": {
            "sent": False,
            "why": (
                "off: the upstream instances have their own execution records, and re-running them "
                "would repeat their own side effects for no benefit"
            ),
        },
        "include_future": {"sent": False, "why": "off: rejected outright on a run with no logical date"},
        "include_past": {"sent": False, "why": "off: rejected outright on a run with no logical date"},
        "only_running": {
            "sent": False,
            "why": (
                f"off: the target is recorded {_quoted(target_state)}, and only_running would "
                f"restrict the clear to instances that are running now"
                if target_state is not None
                else "off: no state was read for the target, and only_running would restrict the "
                "clear to instances that are running now"
            ),
        },
        "reset_dag_runs": {
            "sent": True,
            "why": "on, so the run is re-queued and actually re-executes",
            "cost": "it rewrites the run's start_date, end_date and queued_at to the re-run",
        },
        "prevent_running_task": {
            "sent": True,
            "why": (
                "on, so an attempt that reached RUNNING since the plan turns this into a refusal "
                "rather than a silent kill into RESTARTING. It covers that one state — Airflow "
                "raises for a task instance in `running` and no other (models/taskinstance.py:387) "
                "— so a queued or scheduled instance is cleared, not refused"
            ),
        },
        "run_on_latest_version": {
            "sent": "omitted" if run_on_latest_version is None else run_on_latest_version,
            "why": (
                "omitted so Airflow's own precedence applies — the Dag's rerun_with_latest_version, "
                "then [core] rerun_with_latest_version, then false. Sending it unconditionally "
                "inverts that default and bypasses both layers."
                if run_on_latest_version is None
                else "sent explicitly because the caller chose it"
            ),
        },
    }


# States in which the target is on its way to a worker, or already there. The
# refusal covers all three, but for two different reasons, and only ONE of them
# is the API's: Airflow fires AirflowClearRunningTaskException on the single
# state ``running`` (models/taskinstance.py:387), so a queued or scheduled target
# clears normally. Planning one of those promises a write that may become
# unmakeable between the plan and the click, which is caution, not a refusal by
# the route.
_IN_FLIGHT_TARGET_STATES = ("running", "queued", "scheduled")
_REFUSED_BY_THE_API_STATE = "running"

# Statuses the clear route answers with BEFORE it writes anything. 409 is only
# one of these when the route explains itself — a bare 409 is not evidence of
# where in the request it was raised.
_PRE_MUTATION_STATUSES = (400, 403, 404)

# Said instead of an empty list, because an empty list is a claim. The clear
# re-queues the run and returns; the scheduler expands mapped tasks afterwards,
# so at the moment this answer is written the count is not zero — it is unknown.
_MAPPED_CREATION_NOT_ESTABLISHED = (
    "not established — the scheduler re-expands mapped tasks after this call returns, from upstream "
    "output rather than from the approved list, so instances outside that list may be created"
)

# The in-request half of the same unknown. Said instead of an empty list because
# this call does not read the run back after the write, and the clear route
# itself can create instances before it returns.
_IN_REQUEST_CREATION_NOT_ESTABLISHED = (
    "not established — this call does not read the run back after the write, and the clear route "
    "can create instances within the request itself (run_on_latest_version on a finished run "
    "re-verifies the run's integrity, creating the missing tasks including mapped ones, "
    "models/dagrun.py:1859-1863). Read the run's instance list to find out what it holds now"
)


def _clear_flag_error(only_failed: Any, include_downstream: Any, run_on_latest_version: Any) -> str | None:
    """Refuse a flag that is not a real bool, before anything compares it.

    ``0 == False`` and ``1 == True`` in Python, so a tuple comparison between the
    approved plan and the requested call would accept ``only_failed=0`` as the
    ``False`` the user approved — and accept ``1`` as a ``True`` they did not.
    The flags decide what gets cleared, so they are checked for type, not just
    for value.
    """
    for name, value, nullable in (
        ("only_failed", only_failed, False),
        ("include_downstream", include_downstream, False),
        ("run_on_latest_version", run_on_latest_version, True),
    ):
        if value is None and nullable:
            continue
        if isinstance(value, bool):
            continue
        return (
            f"{name} must be true or false{' or omitted' if nullable else ''}, not {value!r}. "
            f"Python treats 0 and 1 as False and True, so a value like this would compare equal to "
            f"a flag the user never approved."
        )
    return None


def plan_task_instance_clear(
    dag_id: str,
    task_id: str = "",
    position: int = 0,
    dag_run_id: str = "latest",
    map_index: int | None = None,
    only_failed: bool = True,
    include_downstream: bool = True,
    run_on_latest_version: bool | None = None,
) -> dict[str, Any]:
    """
    Preview clearing a task instance that already exists, without changing anything.

    Read-only, and the whole recovery proposal: it reads the instance's current
    state, quotes the evidence for what its recorded attempt did, enumerates
    every instance the clear would touch by identity, states every flag it will
    send with the reason, and carries the warnings the user has to see before
    approving. Clearing re-runs the *existing* instance inside its own Dag run —
    it is not a new run, so never reach for rerun_dag to do it.

    Name the task with ``task_id``. ``position`` (1-based) is only for turning a
    user's "the third task" into an id, and is refused where the graph does not
    fix the order. ``dag_run_id`` defaults to the latest run and is resolved to
    an exact id here.

    ``only_failed`` is on by default, as in Airflow's own body. An instance that
    is not failed is not a match — the request then succeeds and clears nothing —
    so re-planning a *succeeded* instance needs ``only_failed=false``, and the
    refusal says so.

    ``include_downstream`` is on by default, as in Airflow's own clear dialog:
    clearing a task alone leaves its downstream stuck — an ``upstream_failed``
    instance is never rescheduled, a succeeded one keeps the XCom value the
    re-run exists to replace, and one that already reported completion keeps
    saying so. Pass ``false`` only if the user asks for that one task and
    nothing after it, and relay the warning that says what then survives.

    ``run_on_latest_version`` is omitted by default so Airflow's own precedence
    decides it. It never promises the run's original Dag version is preserved;
    read ``version`` for what it does and does not guarantee.

    Show the user ``affected``, ``warnings`` and ``recovery_evidence``, then pass
    the ``plan_token``, the same arguments, and ``blast_radius.instances`` as
    ``reviewed_instances`` to apply_task_instance_clear.
    """
    flag_error = _clear_flag_error(only_failed, include_downstream, run_on_latest_version)
    if flag_error:
        return {"planned": False, "error": flag_error}
    run, error = _resolve_run(dag_id, dag_run_id)
    if run is None:
        return {"planned": False, "error": error}
    resolved_run_id = run["dag_run_id"]
    task, resolved_by, error = _resolve_task(dag_id, task_id, position)
    if task is None:
        return {"planned": False, "dag_run_id": resolved_run_id, "error": error}

    run_path = f"/dagRuns/{quote(resolved_run_id, safe='')}"
    markers: list[Any] = [[task, map_index] if map_index is not None else task]
    wanted_index = -1 if map_index is None else map_index
    run_tis, omitted = reading._run_task_instances(dag_id, run_path)
    target_row = next(
        (ti for ti in run_tis if ti["task_id"] == task and ti.get("map_index", -1) == wanted_index), None
    )
    preview = transport._api(
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
    target_state = target_row.get("state") if target_row else None
    plan: dict[str, Any] = {
        "planned": True,
        "dag_id": dag_id,
        "dag_run_id": resolved_run_id,
        "task_ids": markers,
        "resolved_by": resolved_by,
        "only_failed": only_failed,
        "include_downstream": include_downstream,
        "run_on_latest_version": run_on_latest_version,
        "target": {
            "task_id": task,
            "map_index": wanted_index,
            "state": target_state,
            "try_number": target_row.get("try_number") if target_row else None,
        },
        "affected": affected,
        "creates_dag_run": False,
    }
    if not affected:
        refusal = {
            **plan,
            "planned": False,
            "error": (
                f"nothing to clear: no task instance of {task!r} in run {resolved_run_id} matches"
                + (" (only_failed is on, so a task that did not fail is not a match)" if only_failed else "")
            ),
        }
        if only_failed and target_row is not None and target_state not in ("failed", "upstream_failed"):
            refusal["next_step"] = (
                f"{_ti_where(target_row)} is recorded {_quoted(target_state)}, which only_failed "
                f"excludes; the endpoint would answer HTTP 200 and change nothing. If the user wants "
                f"this instance re-run, plan again with only_failed=false and tell them that is the "
                f"flag that had to change and why."
            )
        return refusal
    if omitted:
        return {
            **plan,
            "planned": False,
            "error": (
                f"run {resolved_run_id} has more task instances than this tool will read "
                f"({omitted} not seen), so it cannot enumerate what this clear would touch"
            ),
        }
    if target_state in _IN_FLIGHT_TARGET_STATES:
        # No token. Both branches refuse; only the first is a refusal by the
        # route. Saying the API rejects a queued or scheduled clear would be
        # false — Airflow raises on the one state — and a false reason is not
        # made harmless by a sound conclusion.
        where_now = _ti_where(target_row or {"task_id": task, "map_index": wanted_index})
        if target_state == _REFUSED_BY_THE_API_STATE:
            reason = (
                f"{where_now} is recorded {_quoted(target_state)}, so a worker already has it. This "
                f"clear is sent with prevent_running_task on, and Airflow raises "
                f"AirflowClearRunningTaskException for a task instance in exactly that state "
                f"(models/taskinstance.py:387) rather than killing the attempt into RESTARTING — so "
                f"approving it would buy a write that cannot land. Let the attempt settle and plan "
                f"again; if the user wants it stopped, that is a different operation and not one "
                f"this tool makes."
            )
        else:
            reason = (
                f"{where_now} is recorded {_quoted(target_state)}, so it is on its way to a worker. "
                f"A clear of a {target_state} instance is NOT refused by the API — Airflow raises "
                f"only for {_quoted(_REFUSED_BY_THE_API_STATE)} (models/taskinstance.py:387) — and "
                f"this tool refuses anyway: by the time the user clicks, the instance may be running, "
                f"and the approval they gave would name a state it had left. Let the attempt settle "
                f"and plan again."
            )
        return {**plan, "planned": False, "error": reason}
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
    evidence: dict[str, Any] = (
        _recovery_evidence(dag_id, run_path, target_row)
        if target_row is not None
        else {
            "task_id": task,
            "map_index": wanted_index,
            "reading": (
                f"no task instance of {task!r} at map_index {wanted_index} was found in run "
                f"{resolved_run_id}'s instance list, so there is no row to read evidence off"
            ),
            "current_attempt_dispatched": None,
            "partial_external_effect_possible": None,
            "scope_note": _RECOVERY_SCOPE_NOTE,
            "external_system": {"observed": False, "note": _RECOVERY_EXTERNAL_NOTE},
        }
    )
    where = _ti_where({"task_id": task, "map_index": wanted_index})
    expansion = _mapped_in_closure(dag_id, run_path, affected)
    mapped_tasks = expansion["tasks"]
    mapped_settled = expansion["settled"]
    reviewed_instances = [_ti_where(ti) for ti in affected]
    plan["recovery_evidence"] = evidence
    plan["blast_radius"] = {
        "instances": reviewed_instances,
        "target": where,
        "downstream_included": include_downstream,
        "downstream_instances": [_ti_where(ti) for ti in affected if _ti_where(ti) != where],
        "mapped_tasks": mapped_tasks,
        "mapped_tasks_settled": mapped_settled,
        "expansion_unprobed_tasks": expansion["unprobed"],
        "note": (
            "Enumerated by identity, not by count: two different flag sets can affect the same "
            "number of instances and a different set of them. include_downstream is transitive."
        )
        + (
            " This set is NOT closed: it holds mapped task(s), whose fan-out the scheduler "
            "recomputes after the clear, so instances outside this list may be created."
            if mapped_tasks
            else ""
        )
        + (
            f" This set is NOT closed: whether it holds an expandable task could not be established "
            f"for {expansion['unprobed']}, so instances outside this list may be created."
            if not mapped_settled
            else ""
        ),
    }
    plan["flags"] = _clear_flags(
        only_failed=only_failed,
        include_downstream=include_downstream,
        target_state=target_state,
        run_on_latest_version=run_on_latest_version,
    )
    plan["version"] = _version_context(dag_id, run, run_on_latest_version)
    partial: bool | None = evidence["partial_external_effect_possible"]
    plan["warnings"] = _recovery_warnings(
        affected,
        where,
        include_downstream=include_downstream,
        partial_possible=partial,
        mapped_tasks=mapped_tasks,
        mapped_settled=mapped_settled,
        unprobed_tasks=expansion["unprobed"],
    )
    plan["next_step"] = (
        "Show the user recovery_evidence, blast_radius and every entry of warnings, then propose "
        "apply_task_instance_clear with this plan_token AND reviewed_instances set to "
        "blast_radius.instances, so the approval card names the instances that were enumerated "
        "here. The clear is not the recovery: verify it afterwards with "
        "verify_task_instance_recovery."
    )
    plan["plan_token"] = _issue_token(
        "clear",
        {
            "dag_id": dag_id,
            "dag_run_id": resolved_run_id,
            "task_ids": markers,
            "only_failed": only_failed,
            "include_downstream": include_downstream,
            "run_on_latest_version": run_on_latest_version,
            "reviewed_instances": reviewed_instances,
            "affected": _identities(affected),
            "attempts": {_ti_where(ti): ti.get("try_number") for ti in affected},
            # The evidence card the approval rests on, kept in the fields
            # ``_identities`` throws away. An identity set that still matches
            # says nothing about an attempt that landed in between.
            "states": {_ti_where(ti): ti.get("state") for ti in affected},
            # Carried, not recomputed: the apply must make no network call
            # between the write and the answer it returns, because a read that
            # raises there turns a clear that landed into a reported failure.
            "mapped_tasks": mapped_tasks,
            "mapped_settled": mapped_settled,
            "expansion_unprobed_tasks": expansion["unprobed"],
        },
    )
    return plan


# ---------------------------------------------------------------------------
# Containment: what has to be shown READ WHOLE before the clear is written.
#
# Every read this write rests on is bounded — by the route's paging, by
# RECOVERY_ATTEMPT_LIMIT, by TASK_INSTANCE_SCAN_LIMIT — and a bounded read used
# as if it were the whole universe turns a record nobody looked at into a record
# that is not there. So the question asked here is not "does the evidence say
# yes". It is "was the evidence read whole, at the moment immediately before the
# write". One rule, one place, evaluated immediately before the POST: a read
# that cannot show it was complete refuses the write, and the refusal names the
# READ rather than the conclusion it could not reach.
#
# This gate will refuse clears that would have been fine. That is the trade it
# exists to make: the cost of refusing a good clear is a re-plan, and the cost of
# writing on a partial read is a mutation nobody reviewed.
# ---------------------------------------------------------------------------

_NOTHING_CLEARED = "Nothing was cleared — this was checked before the write was sent."

# What a refusal must not do: send the caller round again to get past the guard.
_DO_NOT_BYPASS = (
    "Tell the user nothing was cleared and name the read that could not be shown complete. Do not "
    "re-issue this call to get past it: re-plan, and if the read is still incomplete the clear "
    "stays unsent."
)

_NO_EXPANSION: dict[str, Any] = {"tasks": [], "settled": False, "unprobed": []}


def _incomplete_read(dag_id: str, dag_run_id: str, read: str, route: str, why: str) -> dict[str, Any]:
    """The refusal for a decision-bearing read that could not be shown complete.

    Pre-mutation by construction: the gate runs before the POST, so "nothing was
    cleared" is a fact about this call and not an inference from a response. It
    says which read fell short and how far, and it says nothing whatever about
    what the records it did not read contain.
    """
    return {
        "cleared": False,
        "mutation_applied": False,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "incomplete_read": {"read": read, "route": route, "detail": why},
        "error": (
            f"this clear was NOT sent: the {read} could not be shown to be complete at the moment "
            f"before the write ({why}). A decision taken over part of a list is a decision about "
            f"the part that was read, and this one would have authorised a write. "
            f"{_NOTHING_CLEARED} What the unread records hold is not established here, in either "
            f"direction."
        ),
        "next_step": _DO_NOT_BYPASS,
    }


def _expired_evidence(dag_id: str, dag_run_id: str, why: str, **fields: Any) -> dict[str, Any]:
    """The refusal for a read that was complete when taken and is no longer current.

    Staleness is the same defect as truncation wearing a different coat: the
    approval names a universe, and by the time of the write that universe has
    moved. Also pre-mutation, so it carries the same "not applied" shape.
    """
    return {
        "cleared": False,
        "mutation_applied": False,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        **fields,
        "error": (
            f"this clear was NOT sent: {why}. The approval the user gave named the state as it was "
            f"read at plan time, so it no longer describes what would be written. "
            f"{_NOTHING_CLEARED}"
        ),
        "next_step": _DO_NOT_BYPASS,
    }


def _containment_gate(
    dag_id: str,
    dag_run_id: str,
    run_path: str,
    plan: dict[str, Any],
    preview: Any,
    now: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Every decision-bearing read behind this write, re-asked and proven whole.

    Returns ``(refusal, expansion)``. A refusal is returned when any read the
    authorization, the target set, the closure, the idempotency or the safety of
    this write depends on is truncated, clamped, unreadable, stale or internally
    inconsistent. ``expansion`` is handed back so the caller does not re-ask the
    probe after the write — a network call there turns a clear that landed into a
    reported failure.
    """
    # R1 — the target set. The preview IS the set that gets written, so a preview
    # that accounted for more instances than it handed over describes a write
    # this call cannot enumerate.
    body = preview if isinstance(preview, dict) else {}
    delivered = len(body.get("task_instances") or [])
    claimed = body.get("total_entries")
    if not _read_is_complete(len(now), delivered, claimed):
        universe = max(delivered, claimed) if isinstance(claimed, int) else delivered
        return (
            _incomplete_read(
                dag_id,
                dag_run_id,
                "clear preview",
                "POST /dags/<dag>/clearTaskInstances (dry_run=true)",
                f"{len(now)} instance(s) were read of {universe} the route accounted for",
            ),
            _NO_EXPANSION,
        )

    # R1 — and the same set the user reviewed, by identity.
    if _identities(now) != plan["affected"]:
        return (
            {
                "cleared": False,
                "mutation_applied": False,
                "dag_run_id": dag_run_id,
                "affected": now,
                "error": (
                    f"what this clear would affect changed since the user reviewed it "
                    f"({len(plan['affected'])} instance(s) then, {len(now)} now); re-plan and show them"
                ),
            },
            _NO_EXPANSION,
        )

    # R2 — the fields ``_identities`` throws away. An identity set that still
    # matches proves nothing about an attempt that landed in between, and the
    # plan's own in-flight refusal was never re-asked before the write.
    live_states = {_ti_where(ti): ti.get("state") for ti in now}
    live_attempts = {_ti_where(ti): ti.get("try_number") for ti in now}
    in_flight = sorted(where for where, state in live_states.items() if state in _IN_FLIGHT_TARGET_STATES)
    if in_flight:
        return (
            _expired_evidence(
                dag_id,
                dag_run_id,
                f"{in_flight} is on its way to a worker or already has one "
                f"({sorted({str(live_states[where]) for where in in_flight})}). Airflow itself "
                f"refuses a clear only for {_quoted(_REFUSED_BY_THE_API_STATE)} "
                f"(models/taskinstance.py:387), so a queued or scheduled instance would clear "
                f"through and be dispatched twice",
                in_flight_instances=in_flight,
            ),
            _NO_EXPANSION,
        )
    planned_states = plan.get("states") or {}
    planned_attempts = plan.get("attempts") or {}
    moved = sorted(
        where
        for where in live_states
        if (where in planned_states and live_states[where] != planned_states[where])
        or (where in planned_attempts and live_attempts[where] != planned_attempts[where])
    )
    if moved:
        return (
            _expired_evidence(
                dag_id,
                dag_run_id,
                f"{moved} moved since the plan was shown — state or try_number is not what was read "
                f"then (planned "
                f"{ {where: (planned_states.get(where), planned_attempts.get(where)) for where in moved} }, "
                f"now { {where: (live_states[where], live_attempts[where]) for where in moved} })",
                instances_that_moved=moved,
            ),
            _NO_EXPANSION,
        )

    # R3 / R4 — the run's instance list and the Dag's task list, both of which
    # decide whether re-queuing this run changes its task set. ``_version_drift``
    # reports its own incompleteness for each; the decision is taken here.
    drift, drift_error = _version_drift(dag_id, dag_run_id)
    if drift or drift_error:
        return (
            {
                "cleared": False,
                "mutation_applied": False,
                "dag_run_id": dag_run_id,
                "migration": drift,
                "error": drift_error
                or (
                    f"the Dag's tasks changed since the user reviewed this clear ({drift}), so "
                    f"re-queuing the run would now add or drop instances they never saw; re-plan "
                    f"and show them"
                ),
            },
            _NO_EXPANSION,
        )

    # R5 / R6 — the closure. ``_version_drift`` compares task-id SETS, so it is
    # blind to a task that became expandable under the same id: the fan-out
    # changes and the id does not. The plan's answer is therefore not carried
    # into the write; it is asked again here, against the live Dag.
    expansion = _mapped_in_closure(dag_id, run_path, now)
    gained = sorted(set(expansion["tasks"]) - set(plan.get("mapped_tasks") or []))
    if gained:
        return (
            {
                "cleared": False,
                "mutation_applied": False,
                "dag_run_id": dag_run_id,
                "newly_expandable_tasks": gained,
                "error": (
                    f"the task(s) {gained} became expandable since the user reviewed this clear, so "
                    f"their instances are now recomputed from upstream output when the run is "
                    f"re-queued and this clear MAY CREATE instances the reviewed plan did not "
                    f"enumerate; re-plan and show them. {_NOTHING_CLEARED}"
                ),
                "next_step": _DO_NOT_BYPASS,
            },
            _NO_EXPANSION,
        )
    if not expansion["settled"]:
        # A probe that declines leaves the closure unread, and the closure is
        # what says whether this write can create instances nobody reviewed. It
        # used to unsettle a claim in the result; it now refuses the write.
        return (
            _incomplete_read(
                dag_id,
                dag_run_id,
                "expandability probe over the cleared closure",
                "GET /dags/<dag>/dagRuns/<run>/taskInstances/<task>/listMapped",
                f"the probe did not answer for {expansion['unprobed']}, so whether this clear's "
                f"instance set is closed was not established",
            ),
            _NO_EXPANSION,
        )

    # R7 — the attempt history behind the safety reading the approval rests on:
    # whether an earlier attempt of the target already reached the outside world,
    # which is what makes this clear a possible duplicate of half an operation.
    target = _plan_target(plan)
    marker = (plan.get("task_ids") or [None])[0]
    wanted = marker[1] if isinstance(marker, (list, tuple)) and len(marker) > 1 else -1
    for ti in now:
        if ti["task_id"] != target or ti.get("map_index", -1) != wanted:
            continue
        history = _attempt_reading(_attempt_history(dag_id, run_path, ti))
        recorded = history.get("attempts_recorded")
        kept = len(history["rows"])
        if history["status"] == "checked" or (history["status"] == "empty" and not recorded):
            break
        return (
            _incomplete_read(
                dag_id,
                dag_run_id,
                f"attempt history of {_ti_where(ti)}",
                "GET /dags/<dag>/dagRuns/<run>/taskInstances/<task>/tries",
                f"the reading came back {history['status']} — {kept} attempt(s) read of "
                f"{recorded if isinstance(recorded, int) else 'an unknown number'} "
                f"({_quoted(history.get('error'), 200)})",
            ),
            _NO_EXPANSION,
        )
    return None, expansion


def _clear_outcome_unknown(
    dag_id: str, dag_run_id: str, plan: dict[str, Any], status: int | None, detail: str
) -> dict[str, Any]:
    """The answer when the write went out and its outcome did not come back.

    ``cleared: False`` says only that this tool did not observe a clear — which
    is what makes the drawer refuse to paint it green — and ``mutation_applied``
    is absent rather than false.
    """
    # The write may have landed, so the verification this answer sends the caller
    # to needs the server-side baseline. Written here for the same reason it is
    # NOT written on the refusal paths: those settled that nothing was applied.
    _record_approved_set(dag_id, dag_run_id, plan["affected"])
    return {
        "cleared": False,
        "mutation_outcome": "unknown",
        "dag_run_id": dag_run_id,
        "http_status": status,
        "error": (
            f"the clear request failed ({_quoted(detail, 240)}); the request had "
            f"already been sent and Airflow did not say where it stopped, so whether it was "
            f"applied — in whole or in part — is not established from here. Do not report it as "
            f"made and do not report it as not made."
        ),
        "verify_with": {
            "tool": "verify_task_instance_recovery",
            "args": {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "instances": [[task_id, index] for task_id, index in plan["affected"]],
                "target_task_id": _plan_target(plan),
                "prior_attempts": plan.get("attempts") or {},
                "cleared_after": _now_iso(),
            },
        },
        "next_step": (
            "Read the state back before saying anything about it: call "
            "verify_task_instance_recovery with verify_with.args, and tell the user the request "
            "failed with the outcome unknown."
        ),
    }


def apply_task_instance_clear(
    dag_id: str,
    dag_run_id: str,
    task_ids: list[Any],
    plan_token: str = "",
    only_failed: bool = True,
    include_downstream: bool = True,
    run_on_latest_version: bool | None = None,
    reviewed_instances: list[str] | None = None,
) -> dict[str, Any]:
    """
    Clear the task instances previewed by plan_task_instance_clear.

    Re-runs instances that already exist. It never creates a Dag run, and there
    is no fallback that does: if nothing matches, that is the answer.

    Pass back the ``plan_token`` and the exact ``dag_run_id`` and ``task_ids``
    that were planned, so the confirmation the user clicks names what it clears.

    ``reviewed_instances`` is REQUIRED and must be exactly the plan's
    ``blast_radius.instances``. ``task_ids`` is the seed, not the scope — with
    ``include_downstream`` on it is routinely one name standing for five
    instances — and the approval card renders these, so a card built without
    them describes a smaller change than the one it authorizes. It is checked
    against the plan like every other argument.

    A clear that returns here has re-queued the instances and nothing more. It
    has NOT established that anything re-ran, so this result is never the
    recovery: follow the ``verify_with`` it hands back.
    """
    # Peeked, not spent: the argument check below refuses without writing
    # anything, and burning the approved plan on a mis-parameterised call would
    # answer the corrected call with "there is no plan" — which is not true and
    # is not the reason it was refused.
    plan = _peek_token("clear", plan_token)
    if plan is None:
        return {
            "cleared": False,
            "mutation_applied": False,
            "error": "no reviewed plan for this clear; call plan_task_instance_clear and show the user",
        }
    flag_error = _clear_flag_error(only_failed, include_downstream, run_on_latest_version)
    if flag_error:
        return {"cleared": False, "mutation_applied": False, "error": flag_error}
    asked = (
        dag_id,
        dag_run_id,
        task_ids,
        only_failed,
        include_downstream,
        run_on_latest_version,
        reviewed_instances,
    )
    planned = (
        plan["dag_id"],
        plan["dag_run_id"],
        plan["task_ids"],
        plan["only_failed"],
        plan["include_downstream"],
        plan["run_on_latest_version"],
        plan["reviewed_instances"],
    )
    if asked != planned:
        # Said in words, not as a Python tuple: this refusal is relayed to the
        # user, and a raw repr of seven positional values explains nothing.
        version = plan["run_on_latest_version"]
        return {
            "cleared": False,
            "mutation_applied": False,
            "error": (
                f"these are not the task instances that were planned — the plan was run "
                f"{plan['dag_run_id']!r}, task_ids {plan['task_ids']}, only_failed={plan['only_failed']}, "
                f"include_downstream={plan['include_downstream']}, "
                f"run_on_latest_version={'omitted' if version is None else version}, "
                f"reviewed_instances={plan['reviewed_instances']}; re-plan and show "
                f"the user. Changing the scope needs a new plan, not a re-worded apply — the "
                f"confirmation the user gave named the old scope."
            ),
        }
    # From here the plan is committed to this call, whatever the outcome: one
    # approval buys one attempt at the write, never a second.
    _redeem_token("clear", plan_token)
    # The BASELINE is not written here. Redeeming the token spends the approval;
    # it does not make a clear happen, and this route still refuses on drift, on
    # a changed affected set and on a newly expandable task. A baseline written
    # before those checks outlived a clear that never landed, and a later
    # verification read it as a real approval of a set no write was made for. It
    # is written on the two paths where a write may have reached the API, and
    # nowhere else.
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
    preview = transport._api("POST", _dag_url(dag_id, "/clearTaskInstances"), json=body)
    now = _affected(preview)
    # Every write precondition is re-established HERE, in one place, immediately
    # before the POST, so the window where the world could move under the
    # approval is as narrow as REST calls allow. It cannot be closed from out
    # here — the same is true of the backfill preview — but nothing decided at
    # plan time is carried into the write unre-checked, and no read the decision
    # rests on is used without first showing it was read whole. A refusal on this
    # path is PRE-mutation: the POST has not been sent, so "not applied" is a
    # fact, not an inference.
    contained, expansion = _containment_gate(
        dag_id, dag_run_id, f"/dagRuns/{quote(dag_run_id, safe='')}", plan, preview, now
    )
    if contained is not None:
        return contained
    try:
        cleared = _affected(
            transport._api("POST", _dag_url(dag_id, "/clearTaskInstances"), json={**body, "dry_run": False})
        )
    except httpx.HTTPStatusError as e:
        # Split from the transport arm on purpose. A route that answers 4xx with
        # its own ``detail`` REJECTED the request — it validated, refused and
        # returned without touching a row — and reporting that as "whether it was
        # applied is not established" invents a doubt the API had already
        # settled. Only a status that says nothing about where in the request it
        # was raised (5xx, or a bare 409) keeps the unknown outcome.
        status = e.response.status_code
        detail = _api_detail(e.response)
        if status in _PRE_MUTATION_STATUSES or (status == 409 and detail):
            refusal = (
                f"the clear was REFUSED by Airflow (HTTP {status}"
                f"{': ' + _quoted(detail, 240) if detail else ''}). "
                # Only said when there IS one. A 409 with no body reached this
                # arm too, and telling the user the route explained itself when
                # it returned nothing invents an explanation to relay.
                + (
                    "The route answered with its own explanation instead of a result, and the "
                    if detail
                    else "The route rejected the request before writing anything, and the "
                )
                + "request's session is rolled back on that path, so nothing was cleared."
            )
            if status == 409:
                refusal += (
                    " This clear is sent with prevent_running_task on, so a target that started "
                    "running since the plan is refused rather than killed into RESTARTING — which "
                    "is exactly what the plan said would happen. Report it as a refusal, not as a "
                    "failure of the recovery, and re-plan once the attempt has settled."
                )
            return {
                "cleared": False,
                "mutation_applied": False,
                "mutation_outcome": "refused",
                "dag_run_id": dag_run_id,
                "http_status": status,
                "api_detail": detail,
                "error": refusal,
                "next_step": (
                    "Tell the user the request was refused and nothing changed, and relay "
                    "api_detail. Re-plan if they still want the clear."
                ),
            }
        return _clear_outcome_unknown(dag_id, dag_run_id, plan, status, detail or str(e))
    except httpx.RequestError as e:
        # The response never arrived. The request had already gone out, and a
        # clear that swept several instances can be applied to some of them
        # before the connection breaks, so ``mutation_applied`` is deliberately
        # ABSENT rather than false: asserting that nothing was written would be a
        # claim this tool has no evidence for.
        return _clear_outcome_unknown(dag_id, dag_run_id, plan, None, str(e))
    # The dry run and the clear are still two calls; compare what actually
    # cleared against what the user approved. The clear happened either way —
    # this is truthful reporting of a drifted outcome, not a rollback.
    cleared_identities = _identities(cleared)
    planned_identities = plan["affected"]
    # The write landed. THIS is where the approved set becomes a fact worth
    # recording — the baseline is what the operator approved, not what cleared,
    # so it is still the plan's set and not the drifted one.
    _record_approved_set(dag_id, dag_run_id, planned_identities)
    # Read off the pre-write probe, off the plan and off the rows the write
    # itself returned — never off a fresh HTTP call. A network read HERE raised
    # out of both ``except`` arms and turned a clear that had LANDED into "no
    # reviewed plan for this clear" on the retry, which the drawer painted red.
    # The probe is not a post-write call: it ran before the POST, on this path,
    # and its answer is carried rather than re-asked.
    mapped_tasks = sorted(
        set(plan.get("mapped_tasks") or [])
        | set(expansion["tasks"])
        | {ti["task_id"] for ti in cleared if ti.get("map_index", -1) >= 0}
    )
    # Three conditions, all necessary. The plan settled it for the set the plan
    # enumerated; the pre-write probe settled it for the live Dag at the moment
    # of the write; and a drifted set is a different set that neither has
    # probed. A declining probe leaves the claim unsettled — never true.
    mapped_settled = (
        bool(plan.get("mapped_settled")) and expansion["settled"] and cleared_identities == planned_identities
    )
    unprobed_tasks = sorted(set(plan.get("expansion_unprobed_tasks") or []) | set(expansion["unprobed"]))
    result: dict[str, Any] = {
        "cleared": True,
        "mutation_applied": True,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_instances": cleared,
        "cleared_matches_plan": cleared_identities == planned_identities,
        # Stated in the same breath as the clear, because "cleared" is the field
        # that gets read as "fixed". Re-queuing is not re-running, and this tool
        # returns long before any worker has picked the instance up.
        "recovery_verified": False,
        "verification_status": "not checked — this call re-queued the instances and returned",
        "verify_with": {
            "tool": "verify_task_instance_recovery",
            "args": {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "instances": [[task_id, index] for task_id, index in cleared_identities],
                "target_task_id": _plan_target(plan),
                "prior_attempts": plan.get("attempts") or {},
                "cleared_after": _now_iso(),
            },
        },
        "clock_note": (
            "cleared_after is this tool's own UTC clock, not Airflow's. Treat it as approximate "
            "when comparing it against timestamps the API returns."
        ),
        "next_step": (
            "Do not report this as a recovery. Wait for the re-run, then call "
            "verify_task_instance_recovery with verify_with.args and relay what it says — including "
            "the external check it cannot perform."
        ),
        # True of this call. The scheduler reconciles a re-queued run against the
        # latest version afterwards, which is why the plan refuses when that
        # version's task list differs.
        "created_dag_run": False,
        # Neither of these was ever read. The route can create instances INSIDE
        # this request — a clear with run_on_latest_version on a finished run
        # re-verifies the run's integrity, which "creates the missing tasks,
        # including mapped tasks" (models/dagrun.py:1859-1863) — and the
        # scheduler creates more afterwards. An empty list here was a literal
        # standing in for a read that never happened, and an empty list is a
        # claim.
        "created_by_this_request": _IN_REQUEST_CREATION_NOT_ESTABLISHED,
        "created_by_this_request_excludes": (
            "anything the scheduler creates after this call returns — re-expansion of mapped "
            "tasks, and any reconciliation of the re-queued run"
        ),
        "created_task_instances": _MAPPED_CREATION_NOT_ESTABLISHED,
        "mapped_tasks": mapped_tasks,
        "mapped_tasks_settled": mapped_settled,
        "mapped_tasks_source": (
            "re-probed against the live Dag immediately before the write, then unioned with the "
            "reviewed plan and with the map_index the write itself returned"
        ),
        "expansion_unprobed_tasks": unprobed_tasks,
        "ui_updates": [
            {
                "kind": "task_instances",
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "task_ids": sorted({ti["task_id"] for ti in cleared}),
            }
        ],
    }
    if not mapped_settled:
        # Said in the result and not only in the plan's warnings: this is the
        # field a reader turns into "nothing else was created", and it must
        # carry its own limitation wherever it is read.
        result["mapped_tasks_unsettled_warning"] = (
            f"Whether this closure holds a task whose fan-out the scheduler recomputes is NOT "
            f"settled here. The pre-write probe did not answer for "
            f"{sorted(set(expansion['unprobed']))}"
            + (
                ""
                if cleared_identities == planned_identities
                else ", and what cleared drifted from the reviewed plan, which nothing has probed"
            )
            + ". Treat mapped_tasks as a floor, not a closed list: this clear MAY CREATE task "
            "instances that are not enumerated here."
        )
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


_NO_OUTPUT_AT_ALL = "this instance recorded no output at all, so its work cannot be dated from inside Airflow"


_DOWNSTREAM_DATING = "output_post_dates_the_task_it_reports_on"
_STALE_ARTEFACT = (
    "or it is still the artefact produced under the state that was replaced — an artefact asserting "
    "a completion that had not happened when it was written"
)


def _downstream_dating_check(
    output: dict[str, Any], upstream_end: str | None, *, target_resolved: bool
) -> dict[str, Any]:
    """Whether this instance's output was written after the target's re-run ended.

    The stale-artefact sentence is a claim about a thing that exists, so it is
    reserved for the one case that supports it: records were read, and none of
    them post-dates the target. No target, no end time, no readable records or no
    records at all are each "not established" — the leg still appears, because a
    leg that vanishes is indistinguishable from one that passed.
    """
    if not target_resolved:
        return _check(
            _DOWNSTREAM_DATING,
            None,
            "no target task was resolved for this verification — target_task_id was not supplied, "
            "or it names no instance in the approved set — so there is no re-run end time to date "
            "this instance's output against, and its role in the clear is unclassified",
        )
    if not upstream_end:
        return _check(
            _DOWNSTREAM_DATING,
            None,
            "the target instance carries no end_date, so the moment its re-run finished is not "
            "known and this instance's output cannot be placed against it",
        )
    if output["status"] in ("not_permitted", "not_scoped", "unavailable"):
        return _check(
            _DOWNSTREAM_DATING,
            None,
            f"the instance's output records could not be read ({_quoted(output.get('error'), 200)}), "
            f"so whether they post-date the target's re-run is not established",
        )
    if not output.get("total_entries"):
        return _check(_DOWNSTREAM_DATING, None, f"{_NO_OUTPUT_AT_ALL}, and there is no record here to date")
    consistent = any(_later_than(entry.get("timestamp"), upstream_end) for entry in output["entries"])
    if output["status"] == "partial" and not consistent:
        # The stale-artefact sentence is a claim about a thing that exists; a
        # truncated read cannot support it, because the record that post-dates
        # the target may simply be one of the ones not read.
        return _check(
            _DOWNSTREAM_DATING,
            None,
            f"only {len(output['entries'])} of {output['total_entries']} output record(s) were read, "
            f"so an absence among them is not an absence — none of the records that were read "
            f"post-dates the target's re-run ({upstream_end}), and the unread ones were not looked "
            f"at. Records seen: {[entry.get('timestamp') for entry in output['entries']]}.",
        )
    return _check(
        _DOWNSTREAM_DATING,
        consistent,
        f"this instance's output must be written after the target's re-run ended ({upstream_end}), "
        + (f"{_STALE_ARTEFACT}. " if not consistent else ". ")
        + f"Records seen: {[entry.get('timestamp') for entry in output['entries']]}.",
    )


def _verify_instance(
    dag_id: str,
    dag_run_id: str,
    run_path: str,
    ti: dict[str, Any],
    *,
    prior_try_number: Any,
    cleared_after: str,
    events: dict[str, Any],
    upstream_end: str | None,
    is_target: bool,
    target_resolved: bool,
    xcom_scope: str,
) -> dict[str, Any]:
    """Every leg of "did this instance actually re-run", reported together.

    Together on purpose. Every one of these is individually satisfiable by an
    instance that did not do its work — a state written onto a running row
    carries a real hostname, a real pid and a real duration, and its log is a
    real log. What the legs are worth is what they are worth as a conjunction,
    so they are never collapsed into one verdict field without the list beside
    it.
    """
    where = _ti_where(ti)
    try_number = ti.get("try_number")
    history = _attempt_reading(_attempt_history(dag_id, run_path, ti))
    rows = history["rows"]
    checks = []

    if not isinstance(prior_try_number, int) or not isinstance(try_number, int):
        checks.append(
            _check(
                "attempt_advanced",
                None,
                f"no pre-clear try_number was supplied for {where} (or the row carries none), so "
                f"nothing here establishes that a new attempt exists",
            )
        )
    else:
        checks.append(
            _check(
                "attempt_advanced",
                try_number > prior_try_number,
                f"try_number was {prior_try_number} when the clear was planned and is {try_number} now",
            )
        )

    if history["status"] not in ("checked", "partial"):
        checks.append(
            _check(
                "prior_attempt_preserved",
                None,
                f"the attempt history could not be read ({_quoted(history.get('error'), 200)}), so "
                f"whether the earlier attempt survived is not established",
            )
        )
    elif not isinstance(prior_try_number, int):
        checks.append(
            _check(
                "prior_attempt_preserved",
                None,
                "no pre-clear try_number was supplied, so there is nothing to look for in /tries",
            )
        )
    elif any(row.get("try_number") == prior_try_number for row in rows):
        checks.append(
            _check(
                "prior_attempt_preserved",
                True,
                f"/tries holds a record at try_number {prior_try_number}; the live row has been "
                f"overwritten either way",
            )
        )
    elif history["status"] == "partial":
        # A truncated read can only ever prove presence. Truncated by the route
        # or truncated by this tool's own clamp is the same incompleteness, so
        # the count is what was READ, never what /tries returned.
        checks.append(
            _check(
                "prior_attempt_preserved",
                None,
                f"this reading looked at {len(rows)} of {history.get('attempts_recorded')} recorded "
                f"attempt(s) and no record at try_number {prior_try_number} is among them, so "
                f"whether the earlier attempt survived is not established — it may be one of the "
                f"attempts this read did not look at ({_quoted(history.get('error'), 200)})",
            )
        )
    else:
        checks.append(
            _check(
                "prior_attempt_preserved",
                False,
                f"/tries does not hold a record at try_number {prior_try_number}; the live row has "
                f"been overwritten either way",
            )
        )

    state = ti.get("state")
    checks.append(
        _check(
            "state_success",
            state == "success",
            f"the instance is recorded {_quoted(state)}"
            + ("" if state == "success" else " — a re-run that has not finished is not a recovery"),
        )
    )

    started, ended, duration = ti.get("start_date"), ti.get("end_date"), ti.get("duration")
    legs = {
        "hostname": bool(ti.get("hostname")),
        "pid": ti.get("pid") is not None,
        "queued_when": ti.get("queued_when") is not None,
        "scheduled_when": ti.get("scheduled_when") is not None,
        "end_date > start_date": _later_than(ended, started or "") is True,
        "duration > 0": isinstance(duration, (int, float)) and duration > 0,
    }
    failing = sorted(name for name, ok in legs.items() if not ok)
    checks.append(
        _check(
            "execution_fields_present",
            not failing,
            (
                f"hostname {_quoted(ti.get('hostname'))}, pid {_quoted(ti.get('pid'))}, queued_when "
                f"{_quoted(ti.get('queued_when'))}, scheduled_when {_quoted(ti.get('scheduled_when'))}, "
                f"start_date {_quoted(started)}, end_date {_quoted(ended)}, duration "
                f"{_quoted(duration)}. Read together: any one of these alone is satisfiable by a "
                f"state written onto an instance that was already running."
            )
            + (f" Not satisfied: {failing}." if failing else ""),
        )
    )

    others, source = _duration_baseline(dag_id, dag_run_id, ti, rows)
    if not isinstance(duration, (int, float)):
        checks.append(_check("duration_in_line_with_history", None, "the row carries no duration to compare"))
    elif not others:
        checks.append(
            _check(
                "duration_in_line_with_history",
                None,
                f"there is no dispatched attempt of this task to compare {duration} against, so a "
                f"non-zero duration is not evidence the work completed — an attempt killed part-way "
                f"records a real duration too",
            )
        )
    else:
        reference = sorted(others)[len(others) // 2]
        checks.append(
            _check(
                "duration_in_line_with_history",
                duration >= RECOVERY_DURATION_FLOOR * reference,
                f"this attempt took {duration} against a median of {reference} over {source} "
                f"{sorted(others)}",
            )
        )

    log = _attempt_log(dag_id, run_path, ti, try_number)
    checks.append(
        _check(
            "log_for_new_attempt",
            {"present": True, "empty": False, "no_logs_reported": False}.get(log["status"]),
            f"the log for try_number {_quoted(try_number)} is {log['status']}. A log that exists is "
            f"not a log that shows the work finished — an attempt killed part-way writes a real log "
            f"too, so read its tail.",
        )
    )

    audit = _audit_transitions(events, ti, cleared_after)
    if audit["status"] not in ("checked", "partial"):
        checks.append(
            _check(
                "audit_running_success_pair",
                None,
                f"the event log could not be read for this run ({_quoted(audit.get('error'), 200)})",
            )
        )
    elif audit["status"] == "partial" and not audit["pair_recorded"]:
        # Same shape as prior_attempt_preserved: the event page stopped short, so
        # the missing transition may simply be on a page that was not read. The
        # deleted-Dag caveat is the wrong hedge to reach for here — the reason
        # this cannot conclude is the truncation, and it is nameable.
        checks.append(
            _check(
                "audit_running_success_pair",
                None,
                f"the event log was read to {events.get('events_scanned')} of "
                f"{events.get('total_entries')} row(s) ({events.get('events_omitted')} not seen), so "
                f"an absence among them is not an absence. Events found since the clear: "
                f"{audit['events']}.",
            )
        )
    else:
        checks.append(
            _check(
                "audit_running_success_pair",
                audit["pair_recorded"],
                f"events visible to this API since the clear: {audit['events']}. A positive row "
                f"establishes that Airflow recorded the transition — not that the callable did its "
                f"work — and rows for a deleted Dag are absent from this view while present in the "
                f"database, so an absence here is an absence in this view.",
            )
        )

    output = _recorded_output(dag_id, run_path, ti, xcom_scope)
    fresh = [entry for entry in output["entries"] if _later_than(entry.get("timestamp"), cleared_after)]
    # Distinguished from "records exist and none of them are fresh". An instance
    # that records nothing — an EmptyOperator, a callable returning None, a task
    # with do_xcom_push off — has no output to date, and calling that a failed
    # check made ``verified`` unreachable for a whole class of correct re-runs.
    no_output = output["status"] != "unavailable" and not output.get("total_entries")
    if output["status"] in ("not_permitted", "not_scoped"):
        checks.append(_check("recorded_output_post_dates_clear", None, str(output.get("error"))))
    elif output["status"] == "unavailable":
        checks.append(
            _check(
                "recorded_output_post_dates_clear",
                None,
                f"the instance's output records could not be read ({_quoted(output.get('error'), 200)})",
            )
        )
    elif no_output:
        checks.append(
            _check(
                "recorded_output_post_dates_clear",
                None,
                f"{_NO_OUTPUT_AT_ALL}. The output route answered and the instance has no records at "
                f"all, which is ordinary for a task that pushes none — it is not evidence that the "
                f"re-run did nothing.",
            )
        )
    elif not cleared_after:
        checks.append(
            _check(
                "recorded_output_post_dates_clear",
                None,
                f"no reference time was supplied, so an output record cannot be told from one "
                f"written before the clear; the instance has {output['total_entries']} record(s)",
            )
        )
    elif output["status"] == "partial" and not fresh:
        # Presence-based conclusions survive a truncated list; absence-based ones
        # do not. Nothing consulted this status, so a page that stopped short
        # reported a hard "no output post-dates the clear" over records it had
        # never read.
        checks.append(
            _check(
                "recorded_output_post_dates_clear",
                None,
                f"only {len(output['entries'])} of {output['total_entries']} output record(s) were "
                f"read, so an absence among them is not an absence — none of the records that were "
                f"read post-dates the clear, and the unread ones were not looked at",
            )
        )
    else:
        checks.append(
            _check(
                "recorded_output_post_dates_clear",
                bool(fresh),
                f"{len(fresh)} of {output['total_entries']} output record(s) post-date the clear "
                f"({[entry['key'] for entry in fresh]}). This is the task's OWN report of its work, "
                f"written by the task; it is not an observation of the external system.",
            )
        )

    # Emitted for every non-target instance, whatever the answer. Dropping the
    # leg when it could not be computed made a check that had never run look like
    # one that had passed.
    if not is_target:
        checks.append(_downstream_dating_check(output, upstream_end, target_resolved=target_resolved))

    failed = [check["check"] for check in checks if check["passed"] is False]
    unestablished = [check["check"] for check in checks if check["passed"] is None]
    return {
        "instance": where,
        "task_id": ti["task_id"],
        "map_index": ti.get("map_index", -1),
        "role": "target" if is_target else "downstream" if target_resolved else "unclassified",
        "state": state,
        "try_number": try_number,
        "verdict": "verified" if not failed and not unestablished else "unverified",
        "failed_checks": failed,
        "unestablished_checks": unestablished,
        "checks": checks,
        "log_tail": log.get("tail"),
        "end_date": ended,
    }


_APPROVED_SET_SCOPE_NOTE = (
    "Restricted to the task ids the approval named. An instance of any other task in this run is "
    "outside this comparison and is neither reported nor implied to be absent."
)
# There is no baseline, so there is no comparison, so there is nothing to
# enumerate. An empty list here reads as "none were added" / "none are absent" —
# a positive claim standing in for a comparison that never ran.
_APPROVED_SET_NOT_ESTABLISHED = (
    "not established: no approved set is on record for this run, so nothing was compared"
)


def _approved_instance_set_check(
    record: dict[str, Any] | None, run_tis: list[dict[str, Any]], omitted: int
) -> dict[str, Any]:
    """What the run holds now for the approved tasks, against what was approved.

    The baseline is the set THIS server recorded when it redeemed the plan token
    — never the ``instances`` list the caller passed in. This is the only leg
    that catches an instance the approval never enumerated, and taking its
    baseline from the caller meant handing it the post-clear reality turned it
    green and produced a positive claim about what an operator approved out of a
    list the caller wrote.

    Scoped to the task ids the approval named, deliberately. Comparing against
    the whole run would report every instance that was simply never in scope as
    an "addition"; comparing within those task ids is what makes ``fan[2]``
    appearing beside an approved ``fan[0]`` and ``fan[1]`` legible as what it is
    — the scheduler re-expanding a mapped task after the clear returned.
    """
    if record is None:
        return {
            "check": _check(
                "approved_instance_set_unchanged",
                None,
                "the approved set for this run is not on record here: this server did not redeem a "
                "clear plan for it (a different process, a restarted one, or a clear made outside "
                "this tool), so there is no server-side baseline to compare the run against. The "
                "instances named in this call are the caller's assertion about what was approved "
                "and are not used as one.",
            ),
            "baseline_source": "none",
            "approved": _APPROVED_SET_NOT_ESTABLISHED,
            "added_since_approval": _APPROVED_SET_NOT_ESTABLISHED,
            "absent_since_approval": _APPROVED_SET_NOT_ESTABLISHED,
            "scope_note": _APPROVED_SET_SCOPE_NOTE,
        }
    wanted: list[tuple[str, int]] = list(record["approved"])
    approved = set(wanted)
    tasks = {task for task, _ in wanted}
    present = {_ti_key(ti) for ti in run_tis if ti["task_id"] in tasks}
    added = sorted(present - approved)
    absent = sorted(approved - present)
    identities = [_ti_where({"task_id": task, "map_index": index}) for task, index in added]
    gone = [_ti_where({"task_id": task, "map_index": index}) for task, index in absent]
    if omitted:
        detail: str = (
            f"run has more task instances than this tool will read ({omitted} not seen), so its "
            f"current set cannot be compared against the approved one"
        )
        passed: bool | None = None
    elif added or absent:
        detail = (
            f"the run's instances of the approved task(s) {sorted(tasks)} no longer match what was "
            f"approved: {len(identities)} present that were not approved ({identities}), "
            f"{len(gone)} approved that are no longer present ({gone}). A mapped task is re-expanded "
            f"by the scheduler from upstream output after a clear, so additions here are instances "
            f"the approval never enumerated."
        )
        passed = False
    else:
        detail = (
            f"every instance the run holds for the approved task(s) {sorted(tasks)} is one that was "
            f"approved, and every approved one is still present"
        )
        passed = True
    return {
        "check": _check("approved_instance_set_unchanged", passed, detail),
        "baseline_source": (
            f"recorded by this server when the clear plan was redeemed at {record['recorded_at']}, "
            f"not supplied by this call"
        ),
        "approved": [_ti_where({"task_id": task, "map_index": index}) for task, index in sorted(approved)],
        "added_since_approval": identities,
        "absent_since_approval": gone,
        "scope_note": _APPROVED_SET_SCOPE_NOTE,
    }


def verify_task_instance_recovery(
    dag_id: str,
    dag_run_id: str,
    instances: list[Any] | None = None,
    prior_attempts: dict[str, Any] | None = None,
    target_task_id: str = "",
    cleared_after: str = "",
    audit_scope: str = "",
    xcom_scope: str = "",
) -> dict[str, Any]:
    """
    Establish whether a cleared task instance actually re-ran, or say why that is unsettled.

    Read-only, and the step that makes a clear a recovery rather than a colour
    change. Call it after apply_task_instance_clear with the arguments that call
    handed back in ``verify_with.args``; if the run is still going, call it
    again rather than reporting the earlier answer.

    For every instance it reports each leg separately — a new attempt exists, the
    earlier attempt survived in /tries, the state, the execution fields read
    together, the duration against this instance's own history, the log for the
    new attempt, the audit transitions this API can see, and whether the
    instance recorded output after the clear. A downstream instance is also held
    to reporting output written *after* the target's re-run finished, which is
    what catches a completion notice that survived from before.

    Pass ``target_task_id``. Without it no instance is classified as the target,
    every instance's role is ``unclassified``, and the dating leg reports "not
    established" rather than passing.

    It also diffs the run's current instance set against the set THIS server
    recorded when it redeemed the clear's plan token, so a mapped task the
    scheduler re-expanded shows up as an addition by identity rather than
    silently. That baseline is never taken from ``instances``: where no such
    record exists the leg reports "not established" and ``verified`` stays
    false. ``instances`` only says which instances to read.

    It never observes the external system. ``external_system_checked`` is always
    false and ``operator_action_required`` says what a person still has to do.
    Relay both: a verdict of ``verified`` here means Airflow's own record is
    consistent with a real re-run, not that the work outside Airflow is correct.

    ``audit_scope`` and ``xcom_scope`` are set by the caller's permissions, not
    by you. Neither gates the tool: without them the transitions leg and the two
    output legs report "not established" and the rest of the reading stands.
    """
    run, error = _resolve_run(dag_id, dag_run_id)
    if run is None:
        return {"verified": False, "error": error}
    resolved_run_id = run["dag_run_id"]
    run_path = f"/dagRuns/{quote(resolved_run_id, safe='')}"
    wanted: list[tuple[str, int]] = []
    for marker in instances or []:
        if isinstance(marker, str):
            wanted.append((marker, -1))
        elif isinstance(marker, (list, tuple)) and marker:
            index = marker[1] if len(marker) > 1 and isinstance(marker[1], int) else -1
            wanted.append((str(marker[0]), index))
    if not wanted:
        return {
            "verified": False,
            "dag_run_id": resolved_run_id,
            "error": (
                "name the instances to verify — pass the same identities apply_task_instance_clear "
                "reported as cleared"
            ),
        }

    run_tis, omitted = reading._run_task_instances(dag_id, run_path)
    by_key = {_ti_key(ti): ti for ti in run_tis}
    missing = [
        f"{task}[{index}]" if index >= 0 else task for task, index in wanted if (task, index) not in by_key
    ]
    events = _event_history(dag_id, resolved_run_id, audit_scope)
    prior = prior_attempts or {}

    target_row = next((by_key[key] for key in wanted if key in by_key and key[0] == target_task_id), None)
    upstream_end = target_row.get("end_date") if target_row else None
    results = []
    for key in wanted:
        ti = by_key.get(key)
        if ti is None:
            continue
        where = _ti_where(ti)
        results.append(
            _verify_instance(
                dag_id,
                resolved_run_id,
                run_path,
                ti,
                prior_try_number=prior.get(where, prior.get(ti["task_id"])),
                cleared_after=cleared_after,
                events=events,
                upstream_end=upstream_end if isinstance(upstream_end, str) else None,
                is_target=ti["task_id"] == target_task_id,
                target_resolved=target_row is not None,
                xcom_scope=xcom_scope,
            )
        )

    instance_set = _approved_instance_set_check(
        _approved_set_record(dag_id, resolved_run_id), run_tis, omitted
    )
    verified = bool(results) and not missing and not omitted
    verified = verified and instance_set["check"]["passed"] is True
    verified = verified and all(entry["verdict"] == "verified" for entry in results)
    unverified = [entry["instance"] for entry in results if entry["verdict"] != "verified"]
    if verified:
        summary = (
            f"Every cleared instance in run {resolved_run_id} carries a complete Airflow-side record "
            f"of a new attempt: a higher try_number with the earlier one still in /tries, execution "
            f"fields consistent with a dispatched attempt, a log for the new attempt, the recorded "
            f"transitions, and output written after the clear. That is Airflow's account of itself."
        )
    else:
        summary = (
            f"This clear is performed but NOT verified. Unverified instance(s): "
            f"{unverified or missing or 'none'}. Report it that way and name the failing checks "
            f"— a state of success is not a verification."
        )
        if instance_set["check"]["passed"] is not True:
            summary += (
                f" The run's instance set for the approved tasks also no longer matches the "
                f"approval: {instance_set['check']['detail']}"
            )
    result: dict[str, Any] = {
        "verified": verified,
        "dag_id": dag_id,
        "dag_run_id": resolved_run_id,
        "instances": results,
        "approved_instance_set": instance_set,
        "unverified_instances": unverified,
        "summary": summary,
        "external_system_checked": False,
        "operator_action_required": (
            "Nothing here observes the system the task talks to. Confirm in that system that the "
            "operation happened exactly once — a re-run of an operation that had already partly "
            "completed leaves a duplicate this tool cannot see."
        ),
        "next_step": (
            "Relay the per-instance verdicts and operator_action_required. Never report the recovery "
            "as complete on the strength of the state alone."
        ),
    }
    if missing:
        result["instances_not_found"] = missing
        result["error"] = (
            f"{missing} are not in run {resolved_run_id}'s instance list, so nothing was verified for them"
        )
    if omitted:
        result["instances_omitted"] = omitted
        result["error"] = (
            f"run {resolved_run_id} has more task instances than this tool will read ({omitted} not "
            f"seen), so this reading is not complete"
        )
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
    if len(entries) > reading.MAX_BACKFILL_RUNS:
        # No token: the plan is beyond what may be created anyway, and issuing one
        # would authorize runs this preview is too long to have really shown.
        return {
            **preview,
            "error": (
                f"{len(entries)} runs exceeds the {reading.MAX_BACKFILL_RUNS}-run limit for one backfill; "
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
    if count > reading.MAX_BACKFILL_RUNS:
        return {
            "created": False,
            "mutation_applied": False,
            "planned_run_count": count,
            "error": (
                f"{count} runs exceeds the {reading.MAX_BACKFILL_RUNS}-run limit for one backfill; "
                f"narrow the date range"
            ),
        }
    resp = transport._api(
        "POST", "/backfills", json={"dag_id": dag_id, "from_date": from_date, "to_date": to_date}
    )
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


def _abandon_backfill(
    backfill_id: int, *, planned: list[tuple[Any, Any]], created: list[dict[str, Any]]
) -> dict[str, Any]:
    """Undo as much of a backfill as cancelling can, and be explicit about the rest.

    Cancelling pauses the backfill and fails its *queued* runs. It does not
    delete rows, and a run the scheduler already picked up keeps going — so the
    surviving states are reported rather than implied.
    """
    try:
        transport._api("PUT", f"/backfills/{backfill_id}/cancel")
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
    verify_task_instance_recovery,
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
