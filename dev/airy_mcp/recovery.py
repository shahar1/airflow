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
Clear an instance that already exists, and prove the work came back.

The plan -> containment gate -> apply -> verify pipeline for a task-instance
clear.  ``plan_task_instance_clear`` is read-only and is the whole recovery
proposal: it quotes the evidence for what the recorded attempt did, enumerates
by identity every instance the clear would touch, states every flag it sends
with the reason, and carries the warnings the operator has to see before
approving.  ``apply_task_instance_clear`` re-establishes every one of those
preconditions immediately before the POST and refuses the write when any read
behind it cannot be shown to have been read whole.
``verify_task_instance_recovery`` is what makes the clear a recovery rather
than a colour change - a state of success is not a verification.

Wave 9 of the move-only extraction in ``docs/extraction-plan.md``, and the last
module to leave ``server.py``.

The containment gate is the point of the module.  Every read the write rests on
is bounded, and a bounded read used as if it were the whole universe turns a
record nobody looked at into a record that is not there - so the question the
gate asks is not "does the evidence say yes" but "was the evidence read whole,
at the moment immediately before the write".  ``_incomplete_read`` and
``_expired_evidence`` are the two refusal shapes that name the READ rather than
the conclusion it could not reach.

It knows nothing about diagnosis prose, how a backfill works, or Dag source text
beyond what ``dagsource._resolve_task`` hands it.  It reaches ``transport``
directly at three sites: the plan's dry-run preview, the apply's pre-write
preview, and the single mutating write.  Those are the enumerated exceptions
this module carries in ``docs/extraction-plan.md`` §6 N2; relocating the two
previews into ``reading`` is a reshaping of a tool body rather than a move of a
definition, so it is left to the typed redesign.  This wave keeps every body
byte-identical.

The one name the suite rebinds here is ``_mapped_in_closure``, which both
``plan_task_instance_clear`` and the gate call as a plain global, so the module
that owns it is the one place a patch has to land.  It is deliberately NOT
re-exported from ``server.py`` - a re-export there would be a display symbol a
patch could change while the real closure probe went on running.  The three
tools are never rebound, so re-exporting them is the same object either way.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

import httpx

# ``_run_task_instances``, ``_tasks`` and ``_expandable_probe`` are rebound by the
# suite, and ``_api`` is too, so those sites go through the module object and are
# never imported by name.
import reading
import transport
from approvals import (
    _approved_set_record,
    _issue_token,
    _peek_token,
    _record_approved_set,
    _redeem_token,
    eviction_note,
)
from dagsource import _resolve_task
from evidence import (
    _DISPATCH_EVIDENCE_KEYS,
    _event_history,
    _is_never_dispatched_attempt,
)
from primitives import (
    _carries_execution_fields,
    _check,
    _later_than,
    _now_iso,
    _quoted,
    _tagged_log,
    _ti_key,
    _ti_where,
)
from reading import (
    _attempt_history,
    _attempt_log,
    _attempt_reading,
    _audit_transitions,
    _duration_baseline,
    _recorded_output,
    _resolve_run,
    _tasks_reading,
    _version_context,
)
from transport import _api_detail, _dag_url, _explain_error


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


_CLEAR_PREVIEW_ROUTE = "POST /dags/<dag>/clearTaskInstances (dry_run=true)"


def _affected_row(ti: Any) -> dict[str, Any]:
    return {
        "task_id": ti["task_id"],
        "map_index": ti.get("map_index", -1),
        "state": ti.get("state"),
        "try_number": ti.get("try_number"),
    }


def _affected(response: dict[str, Any]) -> list[dict[str, Any]]:
    return [_affected_row(ti) for ti in (response or {}).get("task_instances") or []]


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
    run_scan = reading._run_task_instances(dag_id, f"/dagRuns/{quote(dag_run_id, safe='')}")
    if not run_scan.complete:
        return None, (
            f"run {dag_run_id} has more task instances than this tool will read "
            f"({run_scan.omitted} not seen on {run_scan.route}), so it cannot tell whether clearing "
            f"would change the task set"
        )
    tasks = _tasks_reading(dag_id)
    if not tasks.complete:
        return None, (
            f"the latest version of {dag_id} lists more tasks than this tool read "
            f"({tasks.kept} of {tasks.universe} on {tasks.route}), so it cannot tell whether clearing "
            f"would change the task set"
        )
    current = {ti["task_id"] for ti in run_scan.rows}
    latest = {task["task_id"] for task in tasks.rows}
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


def _partial_effect_possible(ti: dict[str, Any], history_rules_out_partial: bool) -> bool | None:
    """Whether a half-completed external operation can be ruled out for this row.

    The ONE place this is decided, because ``False`` is the value that suppresses
    the half-operation warning on the operator's card and it needs two conjuncts,
    not one: the recorded attempt itself carries nothing a dispatch writes, AND
    the attempt history was read whole with no other attempt carrying execution
    fields. The gate re-asked only the second and published ``False`` over an
    attempt whose own row named a hostname, a pid and a real duration.
    """
    if _is_never_dispatched_attempt(ti):
        return False if history_rules_out_partial else None
    if any(key not in ti for key in _DISPATCH_EVIDENCE_KEYS):
        return None
    if not any(ti.get(name) not in (None, "") for name in _EXECUTION_FIELDS):
        return None
    return True


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
    # The READ, not the display. ``/tries`` hands over the whole history in one
    # page, and the ten-row clamp below decides only how many of those rows are
    # SHOWN. Asking the safety question of the clamped reading made the plan card
    # answer "a half-operation cannot be ruled out" over a history the gate — one
    # find() away, on the same rows — settled outright, and it labelled a
    # whole-page read "partial", which is the word a route truncation gets.
    whole = _attempt_history(dag_id, run_path, ti)
    shown = _attempt_reading(whole)
    rows = list(shown.rows)
    history_status = reading.history_status(whole)
    live_executed = _carries_execution_fields(ti)
    # A truncated list can only ever prove presence, and this is the one field
    # that suppresses the half-operation warning when it comes back False.
    earlier_executed = reading.find(
        whole
        if history_status in ("checked", "partial")
        else reading.failed_read(whole.route, reading.attempt_error(whole) or history_status),
        lambda r: r.get("try_number") != ti.get("try_number") and _carries_execution_fields(r),
        f"no attempt of {_ti_where(ti)} other than the recorded one carries execution fields",
    ).as_field()

    missing = [key for key in _DISPATCH_EVIDENCE_KEYS if key not in ti]
    present = [name for name in _EXECUTION_FIELDS if ti.get(name) not in (None, "")]
    # ``False`` here is a positive claim that nothing outside Airflow can have
    # been touched, and it SUPPRESSES the half-operation warning. It is only ever
    # earned by a history that was read whole and holds no attempt with execution
    # fields; a truncated history, an unread one, or one that does hold such an
    # attempt leaves the question open, which is ``None``, not ``False``.
    history_rules_out_partial = history_status == "checked" and earlier_executed is False
    partial_possible = _partial_effect_possible(ti, history_rules_out_partial)
    if _is_never_dispatched_attempt(ti):
        dispatched: bool | None = False
        narrative = (
            f"{_ti_where(ti)} is recorded {_quoted(ti.get('state'))} at try_number "
            f"{_quoted(ti.get('try_number'))} and this attempt carries none of the fields a "
            f"dispatched attempt writes: hostname is empty, pid is null, queued_when is null and "
            f"scheduled_when is null, with duration 0 and start_date equal to end_date. That is "
            f"consistent with the task process never having been dispatched on this attempt. It is "
            f"not a finding about the external system, and it does not say what wrote the state."
        ) + ("" if history_rules_out_partial else f" {_PARTIAL_UNSETTLED_BY_HISTORY}")
    elif missing:
        dispatched = None
        narrative = (
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
        narrative = (
            f"{_ti_where(ti)} is recorded {_quoted(ti.get('state'))} at try_number "
            f"{_quoted(ti.get('try_number'))} with duration {_quoted(ti.get('duration'))}, and none "
            f"of hostname, pid, queued_when or scheduled_when is set — but the row does not match "
            f"the shape of an attempt that was never dispatched either. These fields therefore "
            f"settle neither whether this attempt was dispatched nor whether an external effect is "
            f"possible. {_PARTIAL_UNSETTLED_BY_HISTORY}"
        )
    else:
        dispatched = True
        narrative = (
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
        "reading": narrative,
        "current_attempt_dispatched": dispatched,
        "partial_external_effect_possible": partial_possible,
        "attempt_history": {
            "status": history_status,
            "attempts_recorded": None if whole.read_failed else whole.universe,
            "attempts": rows,
            # How many recorded attempts are NOT in the list above. The list is
            # a display and the read behind it was whole, so this is the one
            # place the clamp is disclosed — ``status`` no longer wears the word
            # a route truncation earns.
            "attempts_omitted": max(whole.kept - len(rows), 0),
            "earlier_attempt_carries_execution_fields": earlier_executed,
            "error": reading.attempt_error(whole),
            # The READ, named, whenever it fell short. ``status: "partial"``
            # says a read did, without saying which — and a card carrying three
            # readings gave the reader no way to tell them apart.
            **({} if whole.complete else {"not_read_whole": whole.describe()}),
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
            for task in reading._tasks(dag_id).rows
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
    run_scan = reading._run_task_instances(dag_id, run_path)
    target_row = next(
        (ti for ti in run_scan.rows if ti["task_id"] == task and ti.get("map_index", -1) == wanted_index),
        None,
    )
    # The preview IS the set the write would touch, so it is read as a Reading
    # here exactly as the gate reads it. Handing the raw body to ``_affected``
    # threw ``total_entries`` away, and every enumeration below — the approval
    # card, the mapped-task closure, the "nothing to clear" answer — was then
    # drawn over a page that could be a third of the set.
    preview_scan = reading.read_of(
        transport._api(
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
        ),
        "task_instances",
        _CLEAR_PREVIEW_ROUTE,
    ).project(_affected_row)
    affected = [dict(row) for row in preview_scan.rows]
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
    # Asked BEFORE anything is concluded from the rows: "nothing to clear" is a
    # flat hard negative and it used to be emitted straight off an empty page
    # whose route accounted for forty, and the enumeration below is a closed-set
    # claim the approval card renders as the whole of the change.
    if not preview_scan.complete:
        return {
            **plan,
            "planned": False,
            "error": (
                f"the clear preview was NOT read whole ({preview_scan.reason}), so this plan cannot "
                f"enumerate the instances the write would touch and nothing is proposed over it"
            ),
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
    if not run_scan.complete:
        return {
            **plan,
            "planned": False,
            "error": (
                f"run {resolved_run_id} has more task instances than this tool will read "
                f"({run_scan.omitted} not seen on {run_scan.route}), so it cannot enumerate what "
                f"this clear would touch"
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


@dataclass(frozen=True)
class _GateContext:
    """Everything the write preconditions are evaluated against, read once."""

    dag_id: str
    dag_run_id: str
    run_path: str
    plan: dict[str, Any]
    preview: reading.Reading
    now: list[dict[str, Any]]
    state: dict[str, Any] = field(default_factory=dict)

    def drift(self) -> tuple[dict[str, list[str]] | None, str | None]:
        """The run's instance list against the Dag's task list, read once for three rules."""
        if "drift" not in self.state:
            self.state["drift"] = _version_drift(self.dag_id, self.dag_run_id)
        return self.state["drift"]

    def expansion(self) -> dict[str, Any]:
        """The closure probe, read once for two rules and handed to the caller."""
        if "expansion" not in self.state:
            self.state["expansion"] = _mapped_in_closure(self.dag_id, self.run_path, self.now)
        return self.state["expansion"]

    def run_scan(self) -> reading.Reading:
        """The run's live task instances, unfiltered by the clear's own flags."""
        if "run_scan" not in self.state:
            self.state["run_scan"] = reading._run_task_instances(self.dag_id, self.run_path)
        return self.state["run_scan"]


@dataclass(frozen=True)
class _WritePrecondition:
    """One fact the clear may not be written without, and where it is asked.

    ``asked_at`` is the whole point. A rule the PLAN refuses on and the gate does
    not re-ask is a rule that stops applying the moment the user clicks, and the
    plan and the gate were two different rule sets with no way to see that they
    were. They are one declared set now, and a test asserts the plan's members
    are a subset of the gate's.
    """

    id: str
    asked_at: frozenset[str]
    what: str
    check: Callable[[_GateContext], dict[str, Any] | None]


def _rule_target_set_read_whole(ctx: _GateContext) -> dict[str, Any] | None:
    """The preview IS the set that gets written, so it has to be the whole of it."""
    if ctx.preview.complete:
        return None
    return _incomplete_read(
        ctx.dag_id,
        ctx.dag_run_id,
        "clear preview",
        _CLEAR_PREVIEW_ROUTE,
        f"{ctx.preview.kept} instance(s) were read of {ctx.preview.universe} the route accounted for",
    )


def _rule_target_set_matches_approval(ctx: _GateContext) -> dict[str, Any] | None:
    if _identities(ctx.now) == ctx.plan["affected"]:
        return None
    return {
        "cleared": False,
        "mutation_applied": False,
        "dag_run_id": ctx.dag_run_id,
        "affected": ctx.now,
        "error": (
            f"what this clear would affect changed since the user reviewed it "
            f"({len(ctx.plan['affected'])} instance(s) then, {len(ctx.now)} now); re-plan and show them"
        ),
    }


def _rule_target_not_in_flight(ctx: _GateContext) -> dict[str, Any] | None:
    """The plan's own in-flight refusal, re-asked against the live rows.

    An identity set that still matches proves nothing about an attempt that
    landed in between, and ``_identities`` throws state and try_number away.

    Asked of the RUN's rows, not of the clear preview. With ``only_failed`` on —
    the default, and how this tool is normally used — Airflow restricts the
    preview to [FAILED, UPSTREAM_FAILED], which is disjoint from the in-flight
    states: the rule that exists to stop a double dispatch could not fire at
    all, and an instance already on its way to a worker was refused by a
    different rule that never mentioned it.
    """
    closure = {(ti["task_id"], ti.get("map_index", -1)) for ti in ctx.now}
    target = _plan_target(ctx.plan)
    marker = (ctx.plan.get("task_ids") or [None])[0]
    closure.add((target, marker[1] if isinstance(marker, (list, tuple)) and len(marker) > 1 else -1))
    live_states: dict[str, Any] = {}
    for ti in ctx.run_scan().rows:
        if (ti["task_id"], ti.get("map_index", -1)) in closure:
            live_states[_ti_where(ti)] = ti.get("state")
    # A row the preview named and the run scan does not describe is a row this
    # rule has no state for. Falling back to the preview's own ``.get(...)``
    # default made an absent field read as permission to write.
    for ti in ctx.now:
        live_states.setdefault(_ti_where(ti), ti.get("state"))
    in_flight = sorted(where for where, state in live_states.items() if state in _IN_FLIGHT_TARGET_STATES)
    if not in_flight:
        return None
    return _expired_evidence(
        ctx.dag_id,
        ctx.dag_run_id,
        f"{in_flight} is on its way to a worker or already has one "
        f"({sorted({str(live_states[where]) for where in in_flight})}). Airflow itself "
        f"refuses a clear only for {_quoted(_REFUSED_BY_THE_API_STATE)} "
        f"(models/taskinstance.py:387), so a queued or scheduled instance would clear "
        f"through and be dispatched twice",
        in_flight_instances=in_flight,
    )


def _rule_target_attempt_not_moved(ctx: _GateContext) -> dict[str, Any] | None:
    live_states = {_ti_where(ti): ti.get("state") for ti in ctx.now}
    live_attempts = {_ti_where(ti): ti.get("try_number") for ti in ctx.now}
    planned_states = ctx.plan.get("states") or {}
    planned_attempts = ctx.plan.get("attempts") or {}
    moved = sorted(
        where
        for where in live_states
        if (where in planned_states and live_states[where] != planned_states[where])
        or (where in planned_attempts and live_attempts[where] != planned_attempts[where])
    )
    if not moved:
        return None
    return _expired_evidence(
        ctx.dag_id,
        ctx.dag_run_id,
        f"{moved} moved since the plan was shown — state or try_number is not what was read "
        f"then (planned "
        f"{ {where: (planned_states.get(where), planned_attempts.get(where)) for where in moved} }, "
        f"now { {where: (live_states[where], live_attempts[where]) for where in moved} })",
        instances_that_moved=moved,
    )


def _rule_run_and_task_lists_read_whole(ctx: _GateContext) -> dict[str, Any] | None:
    """Both lists the task-set comparison rests on, each proven whole.

    ``_version_drift`` reports its own incompleteness for the run's instance
    list and for the Dag's task list; the decision is taken here.
    """
    drift, drift_error = ctx.drift()
    if not drift_error:
        return None
    return {
        "cleared": False,
        "mutation_applied": False,
        "dag_run_id": ctx.dag_run_id,
        "migration": drift,
        "error": drift_error,
    }


def _rule_task_set_unchanged(ctx: _GateContext) -> dict[str, Any] | None:
    drift, _ = ctx.drift()
    if not drift:
        return None
    return {
        "cleared": False,
        "mutation_applied": False,
        "dag_run_id": ctx.dag_run_id,
        "migration": drift,
        "error": (
            f"the Dag's tasks changed since the user reviewed this clear ({drift}), so "
            f"re-queuing the run would now add or drop instances they never saw; re-plan "
            f"and show them"
        ),
    }


def _rule_no_newly_expandable_task(ctx: _GateContext) -> dict[str, Any] | None:
    """``_version_drift`` compares task-id SETS, so it is blind to a task that
    became expandable under the same id: the fan-out changes and the id does not."""
    gained = sorted(set(ctx.expansion()["tasks"]) - set(ctx.plan.get("mapped_tasks") or []))
    if not gained:
        return None
    return {
        "cleared": False,
        "mutation_applied": False,
        "dag_run_id": ctx.dag_run_id,
        "newly_expandable_tasks": gained,
        "error": (
            f"the task(s) {gained} became expandable since the user reviewed this clear, so "
            f"their instances are now recomputed from upstream output when the run is "
            f"re-queued and this clear MAY CREATE instances the reviewed plan did not "
            f"enumerate; re-plan and show them. {_NOTHING_CLEARED}"
        ),
        "next_step": _DO_NOT_BYPASS,
    }


def _rule_closure_settled(ctx: _GateContext) -> dict[str, Any] | None:
    """A probe that declines leaves the closure unread, and the closure is what
    says whether this write can create instances nobody reviewed."""
    expansion = ctx.expansion()
    if expansion["settled"]:
        return None
    return _incomplete_read(
        ctx.dag_id,
        ctx.dag_run_id,
        "expandability probe over the cleared closure",
        "GET /dags/<dag>/dagRuns/<run>/taskInstances/<task>/listMapped",
        f"the probe did not answer for {expansion['unprobed']}, so whether this clear's "
        f"instance set is closed was not established",
    )


def _rule_target_attempt_history_read_whole(ctx: _GateContext) -> dict[str, Any] | None:
    """The read behind the safety reading the approval rests on: whether an
    earlier attempt of the target already reached the outside world.

    Asked of the READ, never of the display. ``/tries`` is not paginated —
    Airflow builds ``list(TIH) + list(TI)`` and sets ``total_entries`` to the
    length of what it just built — so the route always hands over the whole
    history, and the only thing that could make ``_attempt_reading`` incomplete
    is this tool's own ten-row display clamp. Asking the clamped reading meant
    every instance with more than ten recorded attempts was refused
    PERMANENTLY, after the plan had already issued a token, and re-planning
    reproduced it exactly: an ordinary sensor with ``retries >= 10`` could never
    be recovered.
    """
    target = _plan_target(ctx.plan)
    marker = (ctx.plan.get("task_ids") or [None])[0]
    wanted = marker[1] if isinstance(marker, (list, tuple)) and len(marker) > 1 else -1
    for ti in ctx.now:
        if ti["task_id"] != target or ti.get("map_index", -1) != wanted:
            continue
        history = _attempt_history(ctx.dag_id, ctx.run_path, ti)
        if history.complete:
            # The CONTENT, not only the count. Whether an earlier attempt
            # reached the outside world is the value that SUPPRESSES the
            # half-operation warning on the operator's card, and it was computed
            # at plan time and never recomputed — the gate held the fresh rows
            # one find() away and read only how many there were.
            earlier_executed = reading.find(
                history,
                lambda row: row.get("try_number") != ti.get("try_number") and _carries_execution_fields(row),
                f"no attempt of {_ti_where(ti)} other than the recorded one carries execution fields",
            ).as_field()
            ctx.state["earlier_attempt_executed"] = earlier_executed
            # The SAME decision the plan card makes, taken through the same
            # function. Re-asking only "does an OTHER attempt carry execution
            # fields" answers a strictly weaker question, and publishing its
            # False as the half-operation verdict told the operator nothing
            # outside Airflow was touched over a row naming a hostname and a pid.
            #
            # Off the RUN's rows: the clear preview carries four fields and the
            # dispatch shape is read from eight, so the preview row cannot
            # answer it either way.
            live = next(
                (
                    row
                    for row in ctx.run_scan().rows
                    if row.get("task_id") == target and row.get("map_index", -1) == wanted
                ),
                None,
            )
            ctx.state["partial_effect_possible"] = (
                None if live is None else _partial_effect_possible(live, earlier_executed is False)
            )
            return None
        return _incomplete_read(
            ctx.dag_id,
            ctx.dag_run_id,
            f"attempt history of {_ti_where(ti)}",
            history.route,
            f"the reading came back {reading.history_status(history)} — {history.kept} attempt(s) "
            f"read of {'an unknown number' if history.read_failed else history.universe} "
            f"({_quoted(reading.attempt_error(history), 200)})",
        )
    return None


# Every fact this write may not be made without, in the order it is asked. The
# ``plan`` members are the ones ``plan_task_instance_clear`` refuses or withholds
# a token on; every one of them is re-asked here, immediately before the POST,
# because a fact established at plan time describes a world the user has since
# had time to change.
_WRITE_PRECONDITIONS: tuple[_WritePrecondition, ...] = (
    _WritePrecondition(
        "target_set_read_whole",
        frozenset({"plan", "gate"}),
        "the clear preview lists every instance the write would touch",
        _rule_target_set_read_whole,
    ),
    # Asked BEFORE the set comparison. With ``only_failed`` on, an instance that
    # starts running LEAVES the preview, so the set-mismatch rule fired first
    # and the operator was told the scope had changed rather than that their
    # target was on its way to a worker — which is the fact that decides what
    # they do next.
    _WritePrecondition(
        "target_not_in_flight",
        frozenset({"plan", "gate"}),
        "no instance in the closure is on its way to a worker or already has one",
        _rule_target_not_in_flight,
    ),
    _WritePrecondition(
        "target_set_matches_approval",
        frozenset({"gate"}),
        "the instances that would be cleared are the ones the user reviewed",
        _rule_target_set_matches_approval,
    ),
    _WritePrecondition(
        "target_attempt_not_moved",
        frozenset({"gate"}),
        "no instance's state or try_number has moved since the plan was shown",
        _rule_target_attempt_not_moved,
    ),
    _WritePrecondition(
        "run_and_task_lists_read_whole",
        frozenset({"plan", "gate"}),
        "the run's instance list and the Dag's task list were both read whole",
        _rule_run_and_task_lists_read_whole,
    ),
    _WritePrecondition(
        "task_set_unchanged",
        frozenset({"plan", "gate"}),
        "the latest Dag version has the same tasks as the run",
        _rule_task_set_unchanged,
    ),
    _WritePrecondition(
        "no_newly_expandable_task",
        frozenset({"gate"}),
        "no task in the closure became expandable since the plan",
        _rule_no_newly_expandable_task,
    ),
    # Both of these are declared at the GATE only, because that is where they
    # are asked. The plan states the unsettled closure in ``blast_radius`` and
    # warns on it rather than refusing, and it reads the attempt history for the
    # evidence card rather than as a precondition. ``asked_at`` describes the
    # code; it used to be checked only against itself, and every rule declaring
    # "gate" made the subset test unfailable in both directions.
    _WritePrecondition(
        "closure_expandability_settled",
        frozenset({"gate"}),
        "the expandability probe answered for every task in the closure",
        _rule_closure_settled,
    ),
    _WritePrecondition(
        "target_attempt_history_read_whole",
        frozenset({"gate"}),
        "the target's attempt history was read whole",
        _rule_target_attempt_history_read_whole,
    ),
)

# Which rules the last gate run actually evaluated. Instrumentation, not state a
# decision reads: a registry nothing iterates is a list, not a gate.
_LAST_GATE_RULES: list[str] = []


def _containment_gate(
    dag_id: str,
    dag_run_id: str,
    run_path: str,
    plan: dict[str, Any],
    preview: Any,
    now: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any], dict[str, Any]]:
    """Every decision-bearing read behind this write, re-asked and proven whole.

    Returns ``(refusal, expansion, recheck)``. A refusal is returned when any read the
    authorization, the target set, the closure, the idempotency or the safety of
    this write depends on is truncated, clamped, unreadable, stale or internally
    inconsistent. ``expansion`` is handed back so the caller does not re-ask the
    probe after the write — a network call there turns a clear that landed into a
    reported failure. ``recheck`` carries what the gate's own re-reads
    established, so a value the plan computed and the card renders is restated
    from fresh rows rather than carried forward unre-asked.

    The rules are a declared registry rather than a run of ``if`` statements, so
    "which facts does this write rest on" and "which of them are re-asked" are
    the same question with one answer.
    """
    ctx = _GateContext(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        run_path=run_path,
        plan=plan,
        preview=reading.read_of(preview, "task_instances", _CLEAR_PREVIEW_ROUTE).project(_affected_row),
        now=now,
    )
    _LAST_GATE_RULES.clear()
    for rule in _WRITE_PRECONDITIONS:
        refusal = rule.check(ctx)
        # Recorded AFTER the check returns, so the list is evidence the check
        # ran rather than evidence the registry was iterated.
        _LAST_GATE_RULES.append(rule.id)
        if refusal is not None:
            return {**refusal, "refused_precondition": rule.id}, _NO_EXPANSION, ctx.state
    return None, ctx.expansion(), ctx.state


def _approval_spent_before_the_write(
    dag_id: str, dag_run_id: str, which: str, error: Exception
) -> dict[str, Any]:
    """The answer when a read between the approval and the write did not come back.

    Three live reads sit between redeeming the token and the POST, and none of
    them was guarded: a 500 or a dropped connection raised out of the tool with
    the approval already spent, and the retry answered "no reviewed plan for this
    clear" — which is not what happened. Pre-mutation, so "not applied" is a
    fact rather than an inference.
    """
    return {
        "cleared": False,
        "mutation_applied": False,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "error": (
            f"this clear was NOT sent: {which} could not be read ({_quoted(_explain_error(error), 240)}), "
            f"so the facts this write rests on were not re-established. The approval was already "
            f"spent by this attempt and is gone — re-plan and show the user again. "
            f"{_NOTHING_CLEARED}"
        ),
        "next_step": _DO_NOT_BYPASS,
    }


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
            "error": (
                "no reviewed plan for this clear; call plan_task_instance_clear and show the user"
                + eviction_note("tokens")
            ),
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
    # The approval is already spent, so a read that raises here must not escape
    # past the caller: the retry then answers "no reviewed plan for this clear",
    # which is false and reads as a user error. Nothing has been written on this
    # path, so the honest answer is a refusal that says the approval is gone.
    try:
        preview = transport._api("POST", _dag_url(dag_id, "/clearTaskInstances"), json=body)
        now = _affected(preview)
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        return _approval_spent_before_the_write(dag_id, dag_run_id, "the pre-write preview", e)
    # Every write precondition is re-established HERE, in one place, immediately
    # before the POST, so the window where the world could move under the
    # approval is as narrow as REST calls allow. It cannot be closed from out
    # here — the same is true of the backfill preview — but nothing decided at
    # plan time is carried into the write unre-checked, and no read the decision
    # rests on is used without first showing it was read whole. A refusal on this
    # path is PRE-mutation: the POST has not been sent, so "not applied" is a
    # fact, not an inference.
    try:
        contained, expansion, recheck = _containment_gate(
            dag_id, dag_run_id, f"/dagRuns/{quote(dag_run_id, safe='')}", plan, preview, now
        )
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        return _approval_spent_before_the_write(dag_id, dag_run_id, "a read inside the write gate", e)
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
        # Re-asked at the gate, off the attempt history it re-read immediately
        # before this write — not carried forward from the plan. ``False`` is
        # what suppresses the half-operation warning on the operator's card, and
        # it used to be computed once, at plan time, and never looked at again.
        "partial_external_effect_possible": recheck.get("partial_effect_possible"),
        "partial_external_effect_source": (
            "re-read from the target's attempt history immediately before the write"
        ),
        "expansion_unprobed_tasks_note": _MAPPED_CREATION_NOT_ESTABLISHED,
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
    output: reading.Reading, upstream_end: str | None, *, target_resolved: bool
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
    if output.read_failed:
        return _check(
            _DOWNSTREAM_DATING,
            None,
            f"the instance's output records could not be read ({_quoted(output.error, 200)}), "
            f"so whether they post-date the target's re-run is not established",
        )
    if not output.universe:
        return _check(_DOWNSTREAM_DATING, None, f"{_NO_OUTPUT_AT_ALL}, and there is no record here to date")
    seen = [entry.get("timestamp") for entry in output.rows]
    # The stale-artefact sentence is a claim about a thing that exists; a
    # truncated read cannot support it, because the record that post-dates the
    # target may simply be one of the ones not read.
    dated = reading.find(
        output,
        lambda entry: _later_than(entry.get("timestamp"), upstream_end) is True,
        f"none of the output records that were read post-dates the target's re-run ({upstream_end})",
    )
    if dated.is_unknown():
        return _check(
            _DOWNSTREAM_DATING,
            None,
            f"only {output.kept} of {output.universe} output record(s) were read, so an absence "
            f"among them is not an absence — none of the records that were read post-dates the "
            f"target's re-run ({upstream_end}), and the unread ones were not looked at. "
            f"Records seen: {seen}.",
        )
    return _check(
        _DOWNSTREAM_DATING,
        dated.as_field(),
        f"this instance's output must be written after the target's re-run ended ({upstream_end}), "
        + (". " if dated.is_present() else f"{_STALE_ARTEFACT}. ")
        + f"Records seen: {seen}.",
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
    history_status = reading.history_status(history)
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

    if history_status not in ("checked", "partial"):
        checks.append(
            _check(
                "prior_attempt_preserved",
                None,
                f"the attempt history could not be read ({_quoted(reading.attempt_error(history), 200)}), "
                f"so whether the earlier attempt survived is not established",
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
    else:
        # A truncated read can only ever prove presence. Truncated by the route
        # or truncated by this tool's own clamp is the same incompleteness, so
        # the count is what was READ, never what /tries returned.
        preserved = reading.find(
            history,
            lambda row: row.get("try_number") == prior_try_number,
            f"no record at try_number {prior_try_number} is among the attempts this reading read",
        )
        if preserved.is_present():
            checks.append(
                _check(
                    "prior_attempt_preserved",
                    True,
                    f"/tries holds a record at try_number {prior_try_number}; the live row has been "
                    f"overwritten either way",
                )
            )
        elif preserved.is_unknown():
            checks.append(
                _check(
                    "prior_attempt_preserved",
                    None,
                    f"this reading looked at {history.kept} of {history.universe} recorded "
                    f"attempt(s) and no record at try_number {prior_try_number} is among them, so "
                    f"whether the earlier attempt survived is not established — it may be one of the "
                    f"attempts this read did not look at ({_quoted(reading.attempt_error(history), 200)})",
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

    baseline, source, beyond_the_sample = _duration_baseline(dag_id, dag_run_id, ti, history)
    others = sorted(row["duration"] for row in baseline.rows)
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
    elif not baseline.complete:
        # A median over a clamped sample is a median of the sample, not of the
        # task's history, and both directions of the comparison rest on it.
        checks.append(
            _check(
                "duration_in_line_with_history",
                None,
                f"the comparison sample was NOT read whole ({baseline.reason}), so the median of the "
                f"{len(others)} attempt(s) it holds is not this task's history and {duration} cannot "
                f"be placed against it. Sample read: {others} over {source}",
            )
        )
    else:
        reference = others[len(others) // 2]
        checks.append(
            _check(
                "duration_in_line_with_history",
                duration >= RECOVERY_DURATION_FLOOR * reference,
                f"this attempt took {duration} against a median of {reference} over {source} {others}",
            )
        )

    log = _attempt_log(dag_id, run_path, ti, try_number)
    # Both negatives are claims about the WHOLE log, and a tail is not the log.
    log_verdict = {"present": True, "empty": False, "no_logs_reported": False}.get(log["status"])
    if log_verdict is False and log.get("tail_truncated"):
        log_verdict = None
    checks.append(
        _check(
            "log_for_new_attempt",
            log_verdict,
            f"the log for try_number {_quoted(try_number)} is {log['status']}. A log that exists is "
            f"not a log that shows the work finished — an attempt killed part-way writes a real log "
            f"too, so read its tail."
            + (
                " Only the tail of it was read, so an absence in it is not an absence in the log."
                if log.get("tail_truncated")
                else ""
            ),
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
    elif audit["pair_recorded"].is_unknown():
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
                audit["pair_recorded"].as_field(),
                f"events visible to this API since the clear: {audit['events']}. A positive row "
                f"establishes that Airflow recorded the transition — not that the callable did its "
                f"work — and rows for a deleted Dag are absent from this view while present in the "
                f"database, so an absence here is an absence in this view.",
            )
        )

    output = _recorded_output(dag_id, run_path, ti, xcom_scope)
    fresh = [entry for entry in output.rows if _later_than(entry.get("timestamp"), cleared_after) is True]
    # Presence-based conclusions survive a truncated list; absence-based ones do
    # not. The status used to be derived from the route's claimed total against
    # a POST-clamp row count, so a page this tool had itself cut short called
    # itself checked and reported a hard "no output post-dates the clear" over
    # records it never read.
    dated = reading.find(
        output,
        lambda entry: _later_than(entry.get("timestamp"), cleared_after) is True,
        "none of the output records that were read post-dates the clear",
    )
    # Distinguished from "records exist and none of them are fresh". An instance
    # that records nothing — an EmptyOperator, a callable returning None, a task
    # with do_xcom_push off — has no output to date, and calling that a failed
    # check made ``verified`` unreachable for a whole class of correct re-runs.
    if reading.output_scope_refused(output):
        checks.append(_check("recorded_output_post_dates_clear", None, str(output.error)))
    elif output.read_failed:
        checks.append(
            _check(
                "recorded_output_post_dates_clear",
                None,
                f"the instance's output records could not be read ({_quoted(output.error, 200)})",
            )
        )
    elif not output.universe:
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
                f"written before the clear; the instance has {output.universe} record(s)",
            )
        )
    elif dated.is_unknown():
        checks.append(
            _check(
                "recorded_output_post_dates_clear",
                None,
                f"only {output.kept} of {output.universe} output record(s) were read, so an absence "
                f"among them is not an absence — none of the records that were read post-dates the "
                f"clear, and the unread ones were not looked at",
            )
        )
    else:
        checks.append(
            _check(
                "recorded_output_post_dates_clear",
                dated.as_field(),
                f"{len(fresh)} of {output.universe} output record(s) post-date the clear "
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
    # Every read behind this instance's legs that came back short, whether or not
    # a leg happened to turn on it. A read this verification made and did not
    # cover is a fact about this answer, and a leg that was skipped for an
    # unrelated reason used to swallow it entirely.
    short_reads = [
        f"{scan.route} was NOT read whole: {scan.reason}"
        for scan in (history, output, baseline)
        if not scan.complete and not scan.read_failed
    ]
    entry = {
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
    if short_reads:
        entry["reads_not_read_whole"] = short_reads
    if beyond_the_sample:
        # The comparison's own reach, said in the payload rather than only in
        # the prose of one check's detail. The sample size this leg asks the
        # route for is deliberately not charged to the reading — that made the
        # leg unanswerable for every task with more than ten runs — so this is
        # the one place the population behind it reaches the reader.
        entry["duration_sample_rows_omitted"] = beyond_the_sample
    return entry


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


def _approved_instance_set_check(record: dict[str, Any] | None, run_scan: reading.Reading) -> dict[str, Any]:
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
                "and are not used as one." + eviction_note("approved_sets"),
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
    present = {_ti_key(ti) for ti in run_scan.rows if ti["task_id"] in tasks}
    added = sorted(present - approved)
    absent = sorted(approved - present)
    identities = [_ti_where({"task_id": task, "map_index": index}) for task, index in added]
    gone = [_ti_where({"task_id": task, "map_index": index}) for task, index in absent]
    if not run_scan.complete:
        detail: str = (
            f"run has more task instances than this tool will read ({run_scan.omitted} not seen), so "
            f"its current set cannot be compared against the approved one"
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

    run_scan = reading._run_task_instances(dag_id, run_path)
    run_tis = list(run_scan.rows)
    omitted = run_scan.omitted
    by_key = {_ti_key(ti): ti for ti in run_tis}

    def _named(task: str, index: int) -> str:
        return f"{task}[{index}]" if index >= 0 else task

    # "This instance is not in the run" is an absence, so it is asked of the
    # reading and not of the rows: an instance past the scan ceiling is one this
    # read did not reach, and naming it as not found made the operator chase an
    # instance that is sitting in the run.
    not_located = [_named(task, index) for task, index in wanted if (task, index) not in by_key]
    missing = [
        _named(task, index)
        for task, index in wanted
        if reading.find(
            run_scan,
            lambda ti, t=task, i=index: _ti_key(ti) == (t, i),
            f"{_named(task, index)} was not among the instances this reading read",
        ).is_absent()
    ]
    unsettled_absence = [name for name in not_located if name not in missing]
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

    instance_set = _approved_instance_set_check(_approved_set_record(dag_id, resolved_run_id), run_scan)
    verified = bool(results) and not not_located and run_scan.complete
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
            f"{unverified or not_located or 'none'}. Report it that way and name the failing checks "
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
        # The audit read's own coverage, in the payload rather than only inside
        # the one branch that could not conclude from it. Every transition leg
        # rests on this scan, and a PRESENT verdict survives a truncation — so
        # the whole result came back byte-identical over a scan that had stopped
        # at a third of the rows, and nothing said so.
        "audit_read": {
            "status": events["status"],
            "events_scanned": events["events_scanned"],
            "total_entries": events["total_entries"],
            "events_omitted": events["events_omitted"],
            "events_read_whole": events["reading"].complete,
            "error": events.get("error"),
        },
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
    # Accumulated, never overwritten: the omitted sentence used to replace the
    # not-found one, so the operator kept the bare list of absent instances and
    # lost the reason it might not be one.
    reasons = []
    if missing:
        result["instances_not_found"] = missing
        reasons.append(
            f"{missing} are not in run {resolved_run_id}'s instance list, so nothing was verified for them"
        )
    if unsettled_absence:
        result["instances_not_located"] = unsettled_absence
        reasons.append(
            f"{unsettled_absence} were not among the instance(s) this reading read, and the run's "
            f"list was NOT read whole ({run_scan.reason}), so whether the run holds them is not "
            f"established and nothing was verified for them"
        )
    if omitted:
        result["instances_omitted"] = omitted
        reasons.append(
            f"run {resolved_run_id} has more task instances than this tool will read ({omitted} not "
            f"seen), so this reading is not complete"
        )
    if reasons:
        result["error"] = ". ".join(reasons)
    return result
