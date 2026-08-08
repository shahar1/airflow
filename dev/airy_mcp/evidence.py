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
What the platform's own records establish about an attempt - and what they do not.

Two readings, and the shared vocabulary of what neither can settle.  The event
log says who is recorded as having asked for a state, with every caveat about
what a recorded row does and does not prove shipped as data rather than as
prose.  The dispatch reading says whether the attempt a state was written onto
carries the fields only a dispatch writes.  Between them sit the projection that
carries one attribution per instance, the runtime ceiling that bounds it, and
the one place that decides whether a run may be called clean.

Wave 6 of the move-only extraction in ``docs/extraction-plan.md``.  It knows
nothing about Dag source text, tokens, plans, writes or any tool's result shape.

It holds the single declared boundary exception: ``_event_history`` still issues
its own paginated ``GET /eventLogs`` rather than going through ``reading``,
because splitting the read from the row shaping is a logic change (deferral
**D5**) and this extraction is move-only.  Placing ``_event_history`` here rather
than in ``reading`` is what keeps the ``reading`` <-> ``evidence`` cycle (C-1)
from being created at all.

The suite rebinds ``TASK_INSTANCE_DETAIL_LIMIT``, ``TRIES_PROBE_LIMIT`` and
``DISPATCH_FINDING_LIMIT``, so those three are deliberately NOT re-exported from
``server.py`` - a re-export there would be a display symbol a
``monkeypatch.setattr(server, ...)`` could change while the real projection, the
real probe budget and the real fold went on reading the real ones.  All three are
read only inside this module, so this module object is the one place to patch.
The names the suite only ever reads ARE re-exported, because a name that is never
rebound is the same object either way.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from typing import Any

import httpx

# ``EVENT_SCAN_LIMIT`` and ``EVENT_SCAN_PAGE`` are rebound by the suite, so the two
# reads that bound the event scan reach them through the module object and never
# import them by name; ``_api`` is rebound too, for the same reason.
import reading
import transport
from primitives import (
    _ATTR_AUDITED_OTHER,
    _ATTR_AUDITED_PATCH,
    _ATTR_NONE,
    _ATTR_NOT_SCOPED,
    _ATTR_OTHER,
    _ATTR_PLATFORM,
    _ATTR_TRUNCATED,
    _ATTR_UNAVAILABLE,
    _ATTR_UNKNOWN,
    _PARSED_EXTRA_KEY,
    EVENT_EXTRA_CLAMP_CHARS,
    EVENT_NAME_CLAMP_CHARS,
    EVENT_OWNER_CLAMP_CHARS,
    _attribution_detail,
    _attribution_sentence,
    _clamped_event_text,
    _clamped_operator,
    _fenced,
    _is_json_object,
    _parsed_extra,
    _parsed_extra_of,
    _quoted,
    _ti_key,
    _ti_where,
)
from reading import _attempt_history
from transport import _explain_error

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
# How many instances the coverage prose names before it stops naming them. Named
# at all so the count is checkable; capped because the population is chosen by
# whoever wrote the states being read.
COVERAGE_NAME_LIMIT = 5
# One instance's event list. The headline attribution always describes events[0],
# so this cap can never change the answer — only how much context comes with it.
# It is copied into every detailed row AND into the finding that embeds the same
# attribution object, so it is the multiplier on the largest repeated structure
# in the result and is kept as small as the reading allows.
EVENT_HISTORY_PER_INSTANCE = 2
RUN_SCOPED_EVENT_LIMIT = 10


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


# ---------------------------------------------------------------------------
# What the event log can and cannot establish.
#
# Every one of these ships in the DATA, not only in prose: a small model relays
# a field it was handed far more reliably than a caveat it has to reconstruct,
# and the caveats here are the whole point of the reading.
# ---------------------------------------------------------------------------

_U1 = (
    "A recorded audit row proves a request was received and logged, and nothing more. The row is "
    "written by a request dependency that commits before the handler runs "
    "(api_fastapi/logging/decorators.py:236-239) and before the authorization dependency, so it "
    "does not prove the request was accepted, that it was authorized, or that it wrote this state."
)
_U2 = (
    "No recorded event is not evidence of a direct database write. The bulk task-instance PATCH "
    "route changes state and records nothing "
    "(api_fastapi/core_api/routes/public/task_instances.py:1165-1169 carries no action_logging), "
    "and the log table is deleted by `airflow db clean` (utils/db_cleanup.py:189), so a row may "
    "have existed and been removed by retention."
)
_U3 = (
    "`owner` on an audited row is the authenticated principal name the API recorded for the "
    "request. It is not evidence that a human acted, and the row carries no source address, user "
    "agent, token identity, session or client name."
)
_U4 = (
    "`owner` on a platform execution event is the task's `owner` attribute from the Dag file, not "
    "an authenticated principal."
)
_U5 = (
    "`owner` on a `cli_*` event is the operating-system user inside the container that ran the "
    "command, not an authenticated principal."
)
_U6 = (
    "No recorded event carries the state this task instance was in before the change. The event "
    "log has no column for a prior state."
)
_U7 = (
    "The audited row records neither `map_index` nor `try_number`, so it does not establish which "
    "attempt or which mapped instance it targeted. No REST audit row ever can: the logging "
    "dependency never passes `map_index` to `Log(...)` (api_fastapi/logging/decorators.py:209-218) "
    "and `models/log.py:108-109` sets it only when it is present in the kwargs."
)
_U8 = (
    "The event history was cut off at the scan cap, so an older event for this task instance may "
    "exist and is not shown."
)
_U9 = "The event history could not be read, so nothing about a recorded event is established either way."
_U10 = "`new_state` in `extra` is the state the request asked for, not the state that was written."
_U11 = (
    "Which route wrote this state is not established. A single-instance PATCH, a task-group PATCH, "
    "a bulk PATCH, a direct database write, an in-process `TaskInstance.set_state` from a plugin "
    "or listener, and `airflow dags test --mark-success-pattern` are all consistent with what is "
    "recorded here."
)
_U12 = (
    "A task instance that completes entirely inside the triggerer records no execution event "
    "either, so the absence of one does not separate that case."
)
_U13 = (
    "The `dag_id`, `run_id` and `task_id` columns of an audited row are NOT trustworthy targeting, "
    "and no row can carry the proof either way. The logging dependency builds its parameter dict "
    "from the query and path parameters and then merges the request BODY over it "
    "(api_fastapi/logging/decorators.py:196-203), and reads the three ids out of that merged dict "
    "(:213-217) - so a query parameter or a body key wins over the path. The row is committed "
    "BEFORE the handler runs (:236-239), and "
    "api_fastapi/core_api/routes/public/task_instances.py:1187-1190 lists `Depends(action_logging())` "
    "BEFORE `Depends(requires_access_dag(...))`, so a request REJECTED with 422 or 403 still leaves "
    "a row naming whatever Dag, run and task the request chose. `extra` cannot expose that: "
    ":169-178 sets `fields_skip_logging` to exactly {csrf_token, _csrf_token, is_paused, dag_id, "
    "task_id, dag_run_id, run_id, logical_date} and :180-184 builds `extra` from the query and path "
    "parameters with those keys REMOVED - so a request that supplied `task_id` in the query string "
    "plants the column and leaves `extra` clean. `patch_task_group_instances` "
    "(task_instances.py:997-1005) has no `task_id` in its path and logs before authorization, so "
    "`?task_id=<victim>` with no body at all is enough. Presence of a row is not evidence that "
    "anything was received against THIS task instance."
)
_U14 = (
    "The event history was not read because the call carried no audit scope, so nothing about a "
    "recorded event is established either way."
)
_U15 = (
    "A recorded clear carried an `include_upstream`/`include_downstream`/`include_future`/"
    "`include_past` flag. Such a clear names only its seed task ids in `extra.task_ids`, and the "
    "further instances it swept in are named nowhere - so an instance with no row of its own may "
    "still have been cleared by it."
)

# Every caveat ships as a CODE plus one legend at the top of ``event_history``.
# The prose is constant and this tool wrote it; repeating six paragraphs on each
# of 200 detailed rows was ~197 KB of the result and bought nothing.
_UNKNOWNS: dict[str, str] = {
    "U1": _U1,
    "U2": _U2,
    "U3": _U3,
    "U4": _U4,
    "U5": _U5,
    "U6": _U6,
    "U7": _U7,
    "U8": _U8,
    "U9": _U9,
    "U10": _U10,
    "U11": _U11,
    "U12": _U12,
    "U13": _U13,
    "U14": _U14,
    "U15": _U15,
}

_L1 = (
    f"At most {reading.EVENT_SCAN_LIMIT} event rows are read, newest first by `when`; anything older than "
    f"that window was not looked at."
)
_L2 = (
    "The query is scoped to this Dag and this run, so it cannot see a row recorded against any "
    "other Dag or any other run."
)
_L3 = (
    "Rows with a null `dag_id` are unreachable by a dag_id-scoped query and need a separate "
    "fleet-wide audit-log permission, so this reading never covers them."
)
_L4 = (
    "The `log` table is cleanable by `airflow db clean` (utils/db_cleanup.py:189, included when "
    "`--tables` is omitted), so an empty history can also mean retention removed the rows."
)
_L5 = (
    "`cli_*` rows are written with NO `run_id` at all - the CLI action logger inserts only "
    "event, owner, extra, task_id, dag_id and logical_date (utils/cli_action_loggers.py:133-146, "
    "off the metrics built at utils/cli.py:210-224) - so `run_id` is null on every one of them and "
    "a run-scoped query structurally cannot return any. That includes `cli_dag_test`, the "
    "`airflow dags test --mark-success-pattern` route named in the unknowns above: a CLI-driven "
    "state change is invisible to this reading whether or not it happened."
)
_L6 = (
    "`extra` is projected to the four keys this tool reads (`method`, `new_state`, `task_ids`, "
    "`map_index`) and only for the HEADLINE row of each task instance; every other key is named in "
    "`extra_keys` and withheld, and the context rows under `events` carry no `extra` at all. "
    "Airflow masks `extra` by key NAME only, so relaying it whole passed through anything the "
    "requester chose to call something else."
)
_L7 = (
    "The attribution payload is bounded at runtime, not only by the per-field clamps: the "
    "serialized `last_state_change` objects are measured and, past the ceiling reported in "
    "`attribution_payload_limit`, context rows and then `extra` projections are dropped from the "
    "oldest instances first. Any row that lost content says so with `size_reduced`, and the count "
    "is in `attribution_reduced_for_size`."
)

_R1 = (
    "`dag_version` records the version stamped on the run's task instances, not the version that "
    "executed them. On a bundle with no `bundle_version` the scheduler rewrites it forward "
    "(jobs/scheduler_job_runner.py:3047-3057, models/dagbag.py:210-216) and it can name a version "
    "the run never ran."
)
_R2 = (
    "`triggering_user_name` is the authenticated principal recorded for triggering the run. It "
    "says nothing about who changed any task instance's state."
)

# States a platform component writes into ``Log.event``.
# execution_api/routes/task_instances.py:234 (event=RUNNING.value) and :491
# (event=updated_state.value); models/taskinstance.py:193,198,1430,1877;
# jobs/scheduler_job_runner.py:1558,3121,3138,3716.
_PLATFORM_STATE_EVENTS = frozenset(
    {
        "queued",
        "running",
        "success",
        "failed",
        "skipped",
        "up_for_retry",
        "up_for_reschedule",
        "restarting",
        "deferred",
        "removed",
        "upstream_failed",
        "scheduled",
        "fail task",
        "skip task",
        "state mismatch",
        "stuck in queued tries exceeded",
        "heartbeat timeout",
        "task stuck in queued reschedule",
    }
)
# The subset of those whose event NAME is itself the state that was written.
_TI_STATE_VALUES = frozenset(
    {
        "queued",
        "running",
        "success",
        "failed",
        "skipped",
        "up_for_retry",
        "up_for_reschedule",
        "restarting",
        "deferred",
        "removed",
        "upstream_failed",
        "scheduled",
    }
)
# Audited REST actions that can change a task instance's state.
_AUDITED_TI_STATE_ACTIONS = frozenset(
    {
        "patch_task_instance",
        "patch_task_group_instances",
        "post_clear_task_instances",
        "clear_dag_run",
        "patch_dag_run",
        "delete_task_instance",
    }
)
# ``cli_*`` is matched by prefix as well; these two are named because they are
# the ones that change a task instance's state.
_CLI_STATE_ACTIONS = frozenset({"cli_dag_test", "cli_task_clear"})
_HTTP_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})
# datamodels/task_instances.py:261-274 — the only states the PATCH body accepts.
_PATCH_NEW_STATES = frozenset({"success", "failed", "skipped"})

_AUDIT_NOT_PERMITTED = "the caller is not authorized to read this Dag's audit log"
_AUDIT_NOT_SCOPED = "no audit scope was supplied, so the audit log was not read"
_EVENT_QUERY = "GET /api/v2/eventLogs?dag_id=<dag>&run_id=<run> (order_by=-when)"


def _has_rest_audit_marker(extra: dict[str, Any] | None) -> bool:
    """Whether this row was written by the REST audit dependency, structurally.

    ``action_logging`` sets ``extra_fields["method"] = request.method``
    UNCONDITIONALLY at api_fastapi/logging/decorators.py:207 - after the
    variable (:186), connection (:190) and json-body (:194) branches have each
    already rebuilt or merged ``extra_fields``, and as the last write before
    ``json.dumps(extra_fields)`` at :215. So a request body carrying its own
    ``"method"`` key is merged at :195 and then OVERWRITTEN at :207; nothing a
    requester supplies can remove the key or decide its value. Every one of the
    46 routes carrying ``Depends(action_logging())`` reaches :207 or writes no
    row at all - the sole early return, :167-168, returns before ``Log(...)``.

    The key is read for one purpose only: to ADD request-settable targeting.
    Its ABSENCE never removes what the event NAME already established, so a
    missing marker cannot upgrade a row; and its coincidental presence on a
    platform or ``cli_*`` row demotes that row rather than promoting it to
    ``rest_api``.
    """
    return extra is not None and "method" in extra


def _classify_event(name: Any, extra: dict[str, Any] | None = None) -> tuple[str, str | None, str]:
    """``(attribution state, interface, recorded_principal_kind)`` for one event row.

    An event name in none of the sets is ``other_recorded_event`` and is still
    listed in full: a future Airflow event name must degrade to "a row I cannot
    classify", never to "there is no row".

    The fall-through kind is ``not_classified``, never ``not_recorded``: rows
    this tool does not recognise DO record a principal (``trigger_dag_run``
    carries owner "admin" live), and calling that "not recorded" is a false
    statement about the row rather than an honest one about the classifier.

    The INTERFACE of an otherwise unclassified row is decided structurally, by
    the audit marker, not by a hand-maintained list of REST event names: 46
    routes carry ``Depends(action_logging())`` and only six of them are names
    this tool knows, so an enumeration reports ``targeting_is_request_settable:
    False`` - an affirmative false statement - for every other one. The marker
    is never allowed to CHANGE an interface the name already decided, which is
    what keeps a platform or ``cli_*`` row from being promoted by it.
    """
    marker_interface = "rest_api" if _has_rest_audit_marker(extra) else None
    if not isinstance(name, str):
        return _ATTR_OTHER, marker_interface, "not_classified"
    if name == "patch_task_instance":
        return _ATTR_AUDITED_PATCH, "rest_api", "authenticated_api_principal"
    if name.startswith("cli_") or name in _CLI_STATE_ACTIONS:
        return _ATTR_AUDITED_OTHER, "cli", "os_user_from_cli"
    if name in _AUDITED_TI_STATE_ACTIONS:
        return _ATTR_AUDITED_OTHER, "rest_api", "authenticated_api_principal"
    if name in _PLATFORM_STATE_EVENTS:
        return _ATTR_PLATFORM, "platform", "dag_task_owner"
    return _ATTR_OTHER, marker_interface, "not_classified"


def _is_request_settable_targeting(name: Any, extra: dict[str, Any] | None) -> bool:
    """Whether this row's ``dag_id``/``run_id``/``task_id`` columns are the request's to choose.

    The UNION of the two signals, never either alone. The marker catches every
    REST-audited route including ones this tool has never heard of; the name
    catches a recognised REST event whose ``extra`` did not survive to here - a
    row whose ``extra`` is unparsable, clamped away, or absent must not be read
    as "not request-settable", which is the claim the enumeration got wrong.
    """
    return _has_rest_audit_marker(extra) or _classify_event(name)[1] == "rest_api"


def _recorded_principal_kind(name: Any, owner: Any) -> str:
    """``not_recorded`` only when the row genuinely carries no owner."""
    _, _, kind = _classify_event(name)
    if kind == "not_classified" and owner is None:
        return "not_recorded"
    return kind


# The only ``extra`` keys the attribution state machine reads, and therefore the
# only ones relayed. Everything else is withheld BY NAME, because Airflow's
# masker matches key names only: a national id under ``national_id`` and a
# presigned URL's ``X-Amz-Signature`` pass through it untouched, and a
# ``trigger_dag_run`` row carries the whole run conf and the run note verbatim.
_EXTRA_RELAYED_KEYS = ("method", "new_state", "task_ids", "map_index")
_EXTRA_INCLUDE_KEYS = ("include_upstream", "include_downstream", "include_future", "include_past")
# ``extra`` is unbounded Text written by whoever made the request, so both the
# breadth of what is relayed and the breadth of what is merely NAMED are capped.
EXTRA_LIST_LIMIT = 20
EXTRA_KEY_LIMIT = 20
# A withheld key is NAMED, not relayed, so the name needs only to be recognisable.
EXTRA_KEY_CLAMP_CHARS = 40
# The per-field clamps bound one row; they do not bound the RESULT, because the
# number of rows carrying them is TASK_INSTANCE_DETAIL_LIMIT. Every clamp at its
# maximum on every detailed instance - a relayed ``task_ids`` list, twenty
# withheld key names at their own clamp, both owner fields, and two context rows
# each - measured ~997 KB of attribution alone. This is the ceiling on the
# serialized attribution payload, enforced at runtime rather than argued for.
#
# It is set ABOVE the floor that reduction cannot go under: TASK_INSTANCE_DETAIL_LIMIT
# rows of this tool's own constant prose plus the clamped identifiers measures
# ~325 KB, and a ceiling below that would be one the tool could only report
# missing. ``attribution_payload_over_limit`` says so if it ever is.
ATTRIBUTION_PAYLOAD_LIMIT_CHARS = int(os.environ.get("AIRY_MCP_ATTRIBUTION_PAYLOAD_LIMIT", "400000"))


def _bounded_extra_value(value: Any, depth: int = 1) -> tuple[Any, bool]:
    """One relayed ``extra`` value, cut to a size this tool chooses."""
    if isinstance(value, str):
        return _clamped_event_text(value, EVENT_EXTRA_CLAMP_CHARS)
    if isinstance(value, bool) or isinstance(value, (int, float)) or value is None:
        return value, False
    if isinstance(value, list) and depth > 0:
        kept: list[Any] = []
        truncated = len(value) > EXTRA_LIST_LIMIT
        for item in value[:EXTRA_LIST_LIMIT]:
            bounded, cut = _bounded_extra_value(item, depth - 1)
            kept.append(bounded)
            truncated = truncated or cut
        return kept, truncated
    # A dict, a deeper list, or anything else: named, never relayed.
    return None, True


def _projected_extra(row: dict[str, Any]) -> dict[str, Any]:
    """``Log.extra`` reduced to the four keys read, plus the names of what was withheld."""
    raw = row.get("extra")
    parsed = _parsed_extra_of(row)
    projected: dict[str, Any] = {}
    truncated = False
    for key in _EXTRA_RELAYED_KEYS:
        if key in parsed:
            value, cut = _bounded_extra_value(parsed[key])
            projected[key] = value
            truncated = truncated or cut
    withheld = sorted(
        _clamped_event_text(key, EXTRA_KEY_CLAMP_CHARS)[0]
        for key in parsed
        if key not in _EXTRA_RELAYED_KEYS and isinstance(key, str)
    )
    return {
        "extra": projected,
        # ``Log.extra`` that is not a JSON object relays nothing at all: the raw
        # string was the leak, not the parse. The second parse is reached only
        # when the first produced nothing, so it is never paid twice on a
        # well-formed row.
        "extra_parsed": bool(parsed) or _is_json_object(raw),
        "extra_keys": withheld[:EXTRA_KEY_LIMIT],
        "extra_keys_omitted": max(len(withheld) - EXTRA_KEY_LIMIT, 0),
        "extra_truncated": truncated,
    }


def _compact_event(row: dict[str, Any]) -> dict[str, Any]:
    """One event-log row, clamped, classified, and never spliced into prose."""
    _, interface, _ = _classify_event(row.get("event"), _parsed_extra_of(row))
    owner, _ = _clamped_event_text(row.get("owner"), EVENT_OWNER_CLAMP_CHARS)
    principal_kind = _recorded_principal_kind(row.get("event"), row.get("owner"))
    event, _ = _clamped_event_text(row.get("event"), EVENT_NAME_CLAMP_CHARS)
    display, _ = _clamped_event_text(row.get("owner_display_name"), EVENT_OWNER_CLAMP_CHARS)
    return {
        "event_log_id": row.get("event_log_id"),
        "when": row.get("when"),
        "event": event,
        "event_owner": owner,
        "event_owner_display_name": display,
        "recorded_principal": owner if principal_kind == "authenticated_api_principal" else None,
        "recorded_principal_kind": principal_kind,
        "interface": interface,
        **_projected_extra(row),
    }


def _event_association(row: dict[str, Any], ti: dict[str, Any]) -> str | None:
    """How this row attaches to this task instance, or ``None`` for not at all."""
    extra = _parsed_extra_of(row)
    task_id, map_index = _ti_key(ti)
    row_task_id = row.get("task_id")
    if row_task_id is None:
        # A run-scoped row (a clear, a Dag-run PATCH) names its targets in
        # ``extra`` and nowhere else. Only that one shape is ever accepted.
        targets = extra.get("task_ids")
        if isinstance(targets, list) and any(t == task_id for t in targets if isinstance(t, str)):
            return "extra.task_ids"
        return None
    if row_task_id != task_id:
        return None
    row_map_index = row.get("map_index")
    if row_map_index is not None:
        return "row.task_id+map_index" if row_map_index == map_index else None
    # The by-map-index PATCH route puts map_index in the path, so ``extra`` can
    # carry it as a string. Accepted only when it parses to this instance's.
    raw_index = extra.get("map_index")
    if isinstance(raw_index, (int, str)) and not isinstance(raw_index, bool):
        try:
            if int(raw_index) == map_index:
                return "extra.map_index"
        except (TypeError, ValueError):
            pass
    return "row.task_id"


def _classification(state: str, request_settable: bool, demoted: bool) -> str | None:
    """Why an ``other_recorded_event`` is one — the three reasons are not the same.

    ``demoted`` is asked FIRST. A row that was never recognised is
    ``unrecognised`` however request-settable its targeting is: being demoted
    and never having been classified are different facts, and the prose the
    summary reads out is chosen from this value.
    """
    if state != _ATTR_OTHER:
        return None
    if not demoted:
        return "unrecognised"
    return "request_settable_targeting_fields" if request_settable else "uncorroborated_association"


def _bare_attribution(state: str, unknowns: list[str]) -> dict[str, Any]:
    """An attribution with no row behind it. Nulls are written out, never dropped."""
    return {
        "attribution": state,
        "attribution_detail": _attribution_detail(state),
        "event": None,
        "when": None,
        "event_log_id": None,
        "event_owner": None,
        "event_owner_display_name": None,
        "recorded_principal": None,
        "recorded_principal_kind": "not_recorded",
        "interface": None,
        "method": None,
        "method_raw": None,
        "new_state": None,
        "new_state_raw": None,
        "new_state_source": None,
        "old_state": None,
        "event_map_index": None,
        "event_try_number": None,
        "pins_map_index": False,
        "pins_try_number": False,
        "matches_recorded_attempt": None,
        "associated_via": None,
        "classification": None,
        "extra": {},
        "extra_parsed": False,
        "extra_keys": [],
        "extra_keys_omitted": 0,
        "extra_truncated": False,
        "targeting_is_request_settable": False,
        "corroborated_association": False,
        "size_reduced": False,
        "events_recorded": 0,
        "events_omitted_for_instance": 0,
        "events": [],
        "unknowns": unknowns,
    }


def _last_state_change(ti: dict[str, Any], history: dict[str, Any]) -> dict[str, Any]:
    """What the event log recorded last for one task instance — and what it cannot say.

    Strictly additive reporting. It never adds a dispatch finding, never removes
    one, and never enters ``_is_never_dispatched_attempt``: a positive
    ``patch_task_instance`` row proves an attempt and not an effect, and an
    absent row proves nothing at all.
    """
    status = history["status"]
    if status == "not_scoped":
        return _bare_attribution(_ATTR_NOT_SCOPED, ["U14"])
    if status in ("unavailable", "not_permitted"):
        return _bare_attribution(_ATTR_UNAVAILABLE, ["U9"])
    scan: reading.Reading = history["reading"]
    matched = [(row, via) for row in scan.rows if (via := _event_association(row, ti))]
    if not matched:
        # The only way to "no event found": a scan that covered its whole
        # universe. A scan that paged out, or that discarded rows, cannot
        # distinguish a row that is not there from one it never looked at.
        verdict = reading.find(
            scan,
            lambda row: bool(_event_association(row, ti)),
            f"no event-log row for {_ti_where(ti)} appeared in the scanned window",
        )
        if verdict.is_unknown():
            return _bare_attribution(_ATTR_TRUNCATED, ["U8"])
        absent = ["U2", "U11", "U12", "U13"]
        if history.get("clear_with_include_flags"):
            absent.append("U15")
        return _bare_attribution(_ATTR_NONE, absent)

    # Ordered ``-when``, so a match is newest-first within its rank. The RANK
    # comes first: an association corroborated by the row's own map_index column
    # outranks one the row merely claims by task_id, so a row-claimed association
    # cannot displace a corroborated platform event as the headline. Without this
    # a forged audit row - newer by construction, since it is planted after the
    # fact - took the headline off the platform's own execution events.
    matched.sort(key=lambda pair: 0 if pair[1] == "row.task_id+map_index" else 1)
    row, via = matched[0]
    compact = _compact_event(row)
    extra = _parsed_extra_of(row)
    raw_state, interface, _ = _classify_event(row.get("event"), extra)
    # The headline and the context rows must not report two different principal
    # kinds for the same row: both go through ``_recorded_principal_kind``.
    principal_kind = _recorded_principal_kind(row.get("event"), row.get("owner"))

    # Never keyed on a LIST OF EVENT NAMES. Every REST audit row's
    # dag_id/run_id/task_id columns are request-settable - a query parameter or a
    # body key merged over the path (decorators.py:196-203, :213-217) - and there
    # are 46 such routes, of which six were ever enumerated here. So the signal
    # is the structural audit marker (see ``_has_rest_audit_marker``) UNION the
    # names already recognised, and every request-settable row is DEMOTED out of
    # the attributing states: it is reported in full, and it is not counted as a
    # state-change claim about this task instance.
    #
    # ACCEPTED COST: legitimate ``post_clear_task_instances`` rows are demoted
    # too. ``decorators.py:217`` is ``params.get("run_id") or
    # params.get("dag_run_id")`` over the body-merged dict, so nothing on the row
    # separates a genuine clear from a plant. Carving ``dag_run_id`` back out
    # would restore the exact vector; under-claiming on a real clear is the
    # correct direction to fail.
    targeting_is_request_settable = _is_request_settable_targeting(row.get("event"), extra)
    demoted = (
        raw_state in (_ATTR_AUDITED_PATCH, _ATTR_AUDITED_OTHER)
        and (
            # An audited state survives ONLY on an association the row's own
            # map_index column corroborates, which a REST audit row structurally
            # cannot reach (decorators.py:209-218 never passes map_index).
            targeting_is_request_settable or via != "row.task_id+map_index"
        )
    ) or (
        # A platform-named row that carries the REST audit marker is a row whose
        # targeting the request could have chosen. It is demoted for that, never
        # promoted to ``rest_api`` on the strength of a key it merely carries.
        raw_state == _ATTR_PLATFORM and targeting_is_request_settable
    )
    state = _ATTR_OTHER if demoted else raw_state

    # ``extra.map_index`` is a value the requester wrote and ``map_index`` is NOT
    # in ``fields_skip_logging``, so a ``?map_index=3`` on a rejected request
    # lands there verbatim. Only the row's own column may pin an attempt.
    pins_map_index = via == "row.task_id+map_index"
    pins_try_number = pins_map_index and row.get("try_number") is not None
    matches_attempt = row.get("try_number") == ti.get("try_number") if pins_try_number else None

    raw_method = extra.get("method")
    method = raw_method if isinstance(raw_method, str) and raw_method in _HTTP_METHODS else None
    method_raw = _quoted(raw_method, 60) if raw_method is not None and method is None else None

    raw_new_state = extra.get("new_state")
    new_state = new_state_raw = new_state_source = None
    if isinstance(raw_new_state, str) and raw_new_state in _PATCH_NEW_STATES:
        new_state, new_state_source = raw_new_state, "extra.new_state"
    elif raw_new_state is not None:
        # The audit row is written by a dependency that commits BEFORE body
        # validation, so a rejected PATCH's extra really can carry arbitrary text.
        new_state_raw = _quoted(raw_new_state, 60)
    elif state == _ATTR_PLATFORM and row.get("event") in _TI_STATE_VALUES:
        new_state, new_state_source = row["event"], "event_name"

    classification = _classification(state, targeting_is_request_settable, demoted)

    unknowns: list[str] = []
    if state == _ATTR_AUDITED_PATCH:
        unknowns = ["U1", "U3", "U11", "U13"]
    elif state == _ATTR_AUDITED_OTHER:
        unknowns = ["U1", "U5" if principal_kind == "os_user_from_cli" else "U3", "U11", "U13"]
    elif state == _ATTR_PLATFORM:
        unknowns = ["U4"]
    elif demoted:
        # A demoted row is still a recognised row. It keeps every caveat its
        # undemoted state carries - including the one naming what its `owner`
        # field is - and gains U13, the reason it was demoted.
        owner_caveat = {"os_user_from_cli": "U5", "dag_task_owner": "U4"}.get(principal_kind, "U3")
        unknowns = ["U1", owner_caveat, "U11", "U13"]
    else:
        unknowns = ["U11"]
    if new_state_source == "extra.new_state":
        unknowns.append("U10")
    # Every rest_api row, without exception: no REST audit row can carry
    # map_index or try_number at all, so none of them establishes which attempt
    # or which mapped instance it named.
    if interface == "rest_api":
        unknowns.append("U7")
    # A row whose targeting the request could choose establishes nothing about
    # THIS task instance, so the caveats that ride on having no usable row must
    # survive it. Presence of such a row USED TO displace them, which let a
    # `trigger_dag_run` or `patch_dag` row silently drop U2 (absence of a row is
    # not evidence of a direct database write), U12 (a completion inside the
    # triggerer records nothing either) and U13 (the targeting itself).
    if targeting_is_request_settable:
        unknowns.extend(code for code in ("U2", "U12", "U13") if code not in unknowns)
    if any(bool(extra.get(key)) for key in _EXTRA_INCLUDE_KEYS):
        unknowns.append("U15")
    # ``old_state`` is hard-null for every state that has a row, so the reason it
    # is null travels with it rather than being left for the reader to supply.
    unknowns.append("U6")

    # Context rows, deliberately slimmer than the headline. ``extra`` is
    # projected only for the headline because the headline is the only row the
    # state machine reads — and one projection per context row was the largest
    # repeated structure in the whole result (``_L6`` says so in the data).
    events = [
        {
            "event_log_id": other.get("event_log_id"),
            "when": other.get("when"),
            "event": _clamped_event_text(other.get("event"), EVENT_NAME_CLAMP_CHARS)[0],
            "event_owner": _clamped_event_text(other.get("owner"), EVENT_OWNER_CLAMP_CHARS)[0],
            "recorded_principal_kind": _recorded_principal_kind(other.get("event"), other.get("owner")),
            "interface": _classify_event(other.get("event"), _parsed_extra_of(other))[1],
            "targeting_is_request_settable": _is_request_settable_targeting(
                other.get("event"), _parsed_extra_of(other)
            ),
            "event_map_index": other.get("map_index"),
            "event_try_number": other.get("try_number"),
            "associated_via": other_via,
        }
        for other, other_via in matched[:EVENT_HISTORY_PER_INSTANCE]
    ]
    return {
        "attribution": state,
        "attribution_detail": _attribution_detail(state, classification),
        "event": compact["event"],
        "when": compact["when"],
        "event_log_id": compact["event_log_id"],
        "event_owner": compact["event_owner"],
        "event_owner_display_name": compact["event_owner_display_name"],
        "recorded_principal": compact["recorded_principal"],
        "recorded_principal_kind": principal_kind,
        "interface": compact["interface"],
        "method": method,
        "method_raw": method_raw,
        "new_state": new_state,
        "new_state_raw": new_state_raw,
        "new_state_source": new_state_source,
        # NOT RECOVERABLE from anywhere: ``Log`` (models/log.py:36-72) has no
        # prior-state column and ``EventLogResponse`` exposes none. The execution
        # API computes ``previous_state`` but puts it only in a 409 detail. An
        # omitted key would read as unmeasured; an explicit null reads as absent.
        "old_state": None,
        "event_map_index": row.get("map_index"),
        "event_try_number": row.get("try_number"),
        "pins_map_index": pins_map_index,
        "pins_try_number": pins_try_number,
        "matches_recorded_attempt": matches_attempt,
        "associated_via": via,
        "classification": classification,
        "extra": compact["extra"],
        "extra_parsed": compact["extra_parsed"],
        "extra_keys": compact["extra_keys"],
        "extra_keys_omitted": compact["extra_keys_omitted"],
        "extra_truncated": compact["extra_truncated"],
        "targeting_is_request_settable": targeting_is_request_settable,
        # The row's own map_index column agrees with this instance. Anything else
        # is a claim the row makes about itself.
        "corroborated_association": via == "row.task_id+map_index",
        # Set by ``_enforce_attribution_ceiling`` when the runtime ceiling had to
        # take bulk off this object.
        "size_reduced": False,
        "events_recorded": len(matched),
        "events_omitted_for_instance": max(len(matched) - EVENT_HISTORY_PER_INSTANCE, 0),
        "events": events,
        "unknowns": unknowns,
    }


def _event_history(dag_id: str, run_id: str, audit_scope: str) -> dict[str, Any]:
    """Every event-log row for one run, or why there are none to read.

    Never raises, exactly like ``_attempt_history``: an unreadable history
    downgrades what the diagnosis can conclude and must not take the diagnosis
    down with it. ``TypeError`` is in the net because ``_api`` returns ``None``
    for an empty body.

    FAILS CLOSED. The read happens only when the caller's permissions said so;
    every other value — "denied", "", a missing argument, a plugin too old to
    inject one — performs zero HTTP calls. A permission gap can therefore only
    ever produce less information, never more.

    An EMPTY scope is reported as ``not_scoped``, not as ``not_permitted``: no
    argument arrived, so no permission was refused, and saying one was is a false
    statement about the caller.
    """
    payload: dict[str, Any] = {
        "status": "not_permitted",
        "events_scanned": 0,
        "total_entries": 0,
        "events_omitted": 0,
        "oldest_scanned_when": None,
        "rows_rejected": 0,
        "attribution_payload_bytes": 0,
        "attribution_payload_limit": ATTRIBUTION_PAYLOAD_LIMIT_CHARS,
        "attribution_reduced_for_size": 0,
        "attribution_payload_over_limit": False,
        "clear_with_include_flags": False,
        "run_scoped_events": [],
        "run_scoped_events_omitted": 0,
        "error": _AUDIT_NOT_PERMITTED,
        "query": _EVENT_QUERY,
        "limits": [_L1, _L2, _L3, _L4, _L5, _L6, _L7],
        "unknowns_legend": _UNKNOWNS,
        # The rows AND how much of the scan they are. Everything downstream that
        # concludes an absence from them reads this and nothing else.
        "reading": reading.failed_read(_EVENT_QUERY, _AUDIT_NOT_PERMITTED),
    }
    if audit_scope == "":
        payload["status"] = "not_scoped"
        payload["error"] = _AUDIT_NOT_SCOPED
        payload["reading"] = reading.failed_read(_EVENT_QUERY, _AUDIT_NOT_SCOPED)
        return payload
    if audit_scope != "granted":
        return payload
    fetched: list[dict[str, Any]] = []
    total = 0
    pages = 0
    exhausted = False
    # Bounded by construction rather than by the server's arithmetic: the break
    # below already ends the scan, and this makes an ``offset`` the API ignores
    # or a total that never comes down cost a fixed number of calls, not a spin.
    max_pages = max(1, -(-reading.EVENT_SCAN_LIMIT // max(reading.EVENT_SCAN_PAGE, 1)))
    try:
        for _ in range(max_pages):
            resp = transport._api(
                "GET",
                "/eventLogs",
                # Through ``params=``, never interpolated: a run_id carries
                # ``+00:00``, whose ``+`` decodes to a space when it is written
                # into the URL raw — which returned 0 rows for a run that has 27.
                # It also stops a crafted run_id smuggling extra query parameters
                # that would widen the scope past this Dag.
                params={
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "order_by": "-when",
                    "limit": reading.EVENT_SCAN_PAGE,
                    "offset": len(fetched),
                },
            )
            page = resp["event_logs"]
            claimed = resp.get("total_entries")
            fetched += page
            pages += 1
            if isinstance(claimed, int) and not isinstance(claimed, bool):
                total = claimed
            else:
                # A route that omits its count does not get to end the scan and
                # certify it whole; a FULL page is evidence of at least one more
                # row. Same sentinel as the run-instance scan and the backfill
                # run list, for the same reason.
                total = len(fetched) + (1 if len(page) >= reading.EVENT_SCAN_PAGE else 0)
            # An empty page ends it whatever the count says.
            if not page:
                exhausted = True
                break
            if len(fetched) >= min(total, reading.EVENT_SCAN_LIMIT):
                exhausted = len(fetched) >= total
                break
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError) as e:
        payload["status"] = "unavailable"
        payload["error"] = _explain_error(e)
        payload["reading"] = reading.failed_read(_EVENT_QUERY, _explain_error(e))
        return payload

    # Defence in depth: the query is already dag_id-scoped, so a row for another
    # Dag can only be a server-side filter regression. Dropped and counted.
    # Copied rather than mutated, and the parse happens exactly once per row.
    scan = reading.paged_read(
        [{**row, _PARSED_EXTRA_KEY: _parsed_extra(row.get("extra"))} for row in fetched],
        _EVENT_QUERY,
        claimed=total,
        pages=pages,
        exhausted=exhausted,
    )
    # A dropped row is a row this reading did not look at, exactly like a page it
    # never fetched. Counting it only into ``rows_rejected`` let a read that
    # discarded rows report itself as having covered the run.
    scanned = scan.filter(lambda row: isinstance(row, dict) and row.get("dag_id") == dag_id)
    kept = list(scanned.rows)
    run_scoped = [row for row in kept if row.get("task_id") is None]
    # A clear with an include_* flag names only its seed task ids, so the further
    # instances it swept in are named nowhere. Carried on the history rather than
    # on a row, because the instances that need the caveat are exactly the ones
    # with no row of their own.
    swept = any(any(bool(_parsed_extra_of(row).get(key)) for key in _EXTRA_INCLUDE_KEYS) for row in kept)
    payload.update(
        {
            "status": "checked" if scanned.complete else "partial",
            "events_scanned": len(kept),
            "total_entries": total,
            "events_omitted": scanned.omitted,
            "oldest_scanned_when": kept[-1].get("when") if kept else None,
            "rows_rejected": len(fetched) - len(kept),
            "clear_with_include_flags": swept,
            "run_scoped_events": [_compact_event(row) for row in run_scoped[:RUN_SCOPED_EVENT_LIMIT]],
            "run_scoped_events_omitted": max(len(run_scoped) - RUN_SCOPED_EVENT_LIMIT, 0),
            "error": None,
            "reading": scanned,
        }
    )
    return payload


def _attribution_bytes(attribution: dict[str, Any]) -> int:
    """The serialized size of one attribution object, measured not estimated."""
    return len(json.dumps(attribution, default=str))


def _enforce_attribution_ceiling(history: dict[str, Any], task_instances: list[dict[str, Any]]) -> None:
    """Bound the serialized attribution payload, and say so when it bites.

    The numbers land on the EVENT HISTORY and not on the diagnosis, which looks
    at first like the shape D14 forbids and is not: every ``last_state_change``
    they measure is this same read's projection onto one instance, built by
    ``_event_history`` out of the rows it read. D14 bars one read's completeness
    being written onto a DIFFERENT read's payload; this is the same read
    accounting for the size of its own projection.

    The per-field clamps bound a row; nothing bounded the SUM, and the levers a
    caller controls (a relayed ``extra.task_ids`` list, withheld key names at
    ``EXTRA_KEY_CLAMP_CHARS``, both owner fields, the context rows) multiply by
    the number of detailed instances. Measured worst case was ~997 KB.

    Reduction is in place, so the object the projection carries and the object a
    finding carries stay the same object and cannot disagree. It never touches
    ``attribution``, ``classification`` or ``unknowns``: the claims are what the
    reading is for, and the bulk is what it can afford to lose. Oldest instances
    first, so the head of the list keeps its detail.
    """
    carriers = [
        attribution for ti in task_instances if isinstance(attribution := ti.get("last_state_change"), dict)
    ]
    total = sum(_attribution_bytes(attribution) for attribution in carriers)
    reduced: set[int] = set()
    # Two passes, cheapest content first: the context rows are a convenience,
    # while ``extra`` is the only projected content that came from the caller.
    for strip in (_strip_context_rows, _strip_extra_projection):
        for attribution in reversed(carriers):
            if total <= ATTRIBUTION_PAYLOAD_LIMIT_CHARS:
                break
            before = _attribution_bytes(attribution)
            if not strip(attribution):
                continue
            attribution["size_reduced"] = True
            reduced.add(id(attribution))
            total -= before - _attribution_bytes(attribution)
    history["attribution_payload_bytes"] = total
    history["attribution_payload_limit"] = ATTRIBUTION_PAYLOAD_LIMIT_CHARS
    history["attribution_reduced_for_size"] = len(reduced)
    # Reduction takes bulk, never claims, so there is a floor it cannot go under.
    # A ceiling that was missed is reported as missed rather than asserted away.
    history["attribution_payload_over_limit"] = total > ATTRIBUTION_PAYLOAD_LIMIT_CHARS


def _strip_context_rows(attribution: dict[str, Any]) -> bool:
    """Drop the per-instance context rows, counting them as omitted."""
    if not attribution.get("events"):
        return False
    attribution["events_omitted_for_instance"] = attribution.get("events_recorded", 0)
    attribution["events"] = []
    return True


def _strip_extra_projection(attribution: dict[str, Any]) -> bool:
    """Drop the relayed ``extra`` and the withheld key names, counting them."""
    if not attribution.get("extra") and not attribution.get("extra_keys"):
        return False
    attribution["extra_keys_omitted"] = attribution.get("extra_keys_omitted", 0) + len(
        attribution.get("extra_keys") or []
    )
    attribution["extra"] = {}
    attribution["extra_keys"] = []
    attribution["extra_truncated"] = True
    return True


def _prune_unknowns_legend(history: dict[str, Any], *carriers: list[dict[str, Any]]) -> None:
    """Keep only the caveats this result actually cites.

    The legend is what replaced repeating six constant paragraphs on every
    detailed row; shipping all fifteen of them on a diagnosis that cites one is
    the same waste in a smaller costume.
    """
    used = {
        code
        for carrier in carriers
        for item in carrier
        for code in (item.get("last_state_change") or {}).get("unknowns", [])
    }
    history["unknowns_legend"] = {
        code: text for code, text in _UNKNOWNS.items() if code in used and code in _UNKNOWNS
    }


def _attribution_reader(history: dict[str, Any]) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """One attribution per instance, computed once and shared.

    The projection and the findings read the same object, so the row the caller
    sees and the row the prose was built from cannot drift apart.
    """
    cache: dict[tuple[str, int], dict[str, Any]] = {}

    def read(ti: dict[str, Any]) -> dict[str, Any]:
        key = _ti_key(ti)
        if key not in cache:
            cache[key] = _last_state_change(ti, history)
        return cache[key]

    return read


_DISPATCH_FINDING_KIND = "success_without_attempt_dispatch_fields"
_DISPATCH_TRUNCATED_KIND = "dispatch_findings_folded"

# The sentence a dispatch finding always ends on. Anything added to the finding
# later goes BEFORE it: the restraint is the last thing read, not the first
# thing buried.
_NOT_ESTABLISHED = "What wrote this state is not established by this diagnosis"


def _dispatch_finding(
    ti: dict[str, Any], history: reading.Reading | None, attribution: dict[str, Any] | None = None
) -> dict[str, Any] | None:
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
    status = reading.history_status(history)
    rows = history.rows if history is not None else ()
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
    # The live row is in ``rows`` exactly once, so dropping one of them leaves
    # the attempts OTHER than the one recorded success.
    # A page that carried no rows at all is the absence of an answer, not an
    # answer of absence, so it settles nothing here either.
    source = (
        history
        if history is not None and history.rows
        else reading.failed_read(reading._TRIES_ROUTE, _HISTORY_NOT_CHECKED)
    )
    others = reading.selection_of(source, executing_rows[1:] if live_has_execution else executing_rows)
    # "A different attempt" is a claim about try_number, so it is read off
    # try_number. The archived-not-redispatched disjunct fires on a DUPLICATE
    # try_number, and ``record_ti`` dedupes by try_number
    # (models/taskinstancehistory.py:196-206), so those two rows are one attempt
    # and its archive — saying "a different attempt" there would be false in
    # exactly the case that produced the finding.
    other_attempt_executed = any(row.get("try_number") != try_number for row in executing_rows)
    earlier_attempt_executed = reading.find(
        others,
        lambda row: True,
        f"no attempt of {_ti_where(ti)} other than the one recorded success carries execution fields",
    ).as_field()

    where = _fenced(_ti_where(ti))
    history_error = _HISTORY_NOT_CHECKED if history is None else reading.attempt_error(history)
    attempts_recorded = None if history is None or history.read_failed else history.universe
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
            f"Its attempt history could not be read ({_quoted(history_error or status, 240)}), "
            f"so whether an earlier attempt executed is not established."
        )

    attribution = attribution or _bare_attribution(_ATTR_UNKNOWN, [])
    # Exactly one sentence, chosen by attribution state and built only from
    # values this tool wrote or ``_quoted``/``_fenced`` neutralised. ``extra`` is
    # never spliced into it — it is structured-only.
    attribution_text = _attribution_sentence(attribution["attribution"], attribution.get("classification"))
    if attribution["attribution"] in (_ATTR_AUDITED_PATCH, _ATTR_AUDITED_OTHER):
        principal = (
            "the operating-system user name" if attribution["interface"] == "cli" else "the principal name"
        )
        attribution_text += (
            f", event {_quoted(attribution['event'])} at {_quoted(attribution['when'])}, recorded "
            f"against {principal} {_quoted(attribution['event_owner'])}; the row is written before "
            f"the request is handled, so it does not establish that this request wrote this state, "
            f"and it does not establish that a person acted"
        )
    elif attribution["attribution"] == _ATTR_PLATFORM:
        attribution_text += (
            f", event {_quoted(attribution['event'])} at {_quoted(attribution['when'])}, whose "
            f"owner field {_quoted(attribution['event_owner'])} is the task's owner attribute from "
            f"the Dag file and not an actor"
        )
    elif attribution["attribution"] == _ATTR_OTHER:
        # NOTHING is claimed about ``owner`` here. This branch is the one where
        # the event was NOT classified, so the row may well be an audited one
        # whose owner IS an authenticated principal - and the platform sentence
        # would have converted that principal into "not an actor".
        attribution_text += f", event {_quoted(attribution['event'])} at {_quoted(attribution['when'])}"
        if attribution.get("classification"):
            # A DEMOTED row is one whose name this tool does recognise as a
            # state-changing action, so what the row does establish is worth
            # saying in the same breath as what it does not - otherwise the only
            # place it is said is the legend, which is not what gets read out.
            attribution_text += (
                "; a row like that establishes only that a request naming that action was received "
                "and logged, and never that the request succeeded, cleared authorization, or "
                "wrote this state"
            )

    finding: dict[str, Any] = {
        "kind": _DISPATCH_FINDING_KIND,
        "detail": " ".join(
            [
                core,
                history_text,
                f"{attribution_text}.",
                _NOT_ESTABLISHED,
            ]
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
        "attribution": attribution["attribution"],
        "attribution_detail": attribution["attribution_detail"],
        "last_state_change": attribution,
    }
    if attempts_recorded is not None:
        finding["attempts_recorded"] = attempts_recorded
    if status != reading.NOT_CHECKED:
        finding["earlier_attempt_executed"] = earlier_attempt_executed
    if history_error:
        finding["tries_error"] = history_error
    return finding


def _check_dispatch_evidence(
    dag_id: str,
    run_path: str,
    tis: list[dict[str, Any]],
    attribution_of: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
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
    probed: dict[tuple[str, int], reading.Reading | None] = {}
    checked = unchecked = 0
    for position, ti in enumerate(tiers["A"] + tiers["B"]):
        history = _attempt_history(dag_id, run_path, ti) if position < TRIES_PROBE_LIMIT else None
        probed[_ti_key(ti)] = history
        # "Checked" is the reading's own completeness, not a second comparison:
        # a probe that came back short covers nothing it did not look at.
        if history is not None and history.complete and history.rows:
            checked += 1
        else:
            unchecked += 1

    checks = []
    for ti in successes:
        finding = _dispatch_finding(
            ti,
            probed.get(_ti_key(ti)),
            attribution_of(ti) if attribution_of else None,
        )
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
    attribution_of: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
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
    census: dict[str, int] = {}
    corroborated: dict[str, int] = {}
    row_claimed: dict[str, int] = {}
    unpinned = 0
    event_log_ids: set[Any] = set()
    seen: set[tuple[str, int]] = set()
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
        if attribution_of is not None:
            attribution = attribution_of(ti)
            entry["last_state_change"] = attribution
            if key not in seen:
                state = attribution["attribution"]
                census[state] = census.get(state, 0) + 1
                if attribution["event_log_id"] is not None:
                    if attribution["corroborated_association"]:
                        corroborated[state] = corroborated.get(state, 0) + 1
                    else:
                        row_claimed[state] = row_claimed.get(state, 0) + 1
                        unpinned += 1
                    event_log_ids.add(attribution["event_log_id"])
        seen.add(key)
        projected.append(entry)
    attribution_census = {
        "by_attribution": dict(sorted(census.items())),
        # Corroborated means the ROW's own map_index column agrees with the
        # instance. Row-claimed means the row named a task_id (or listed one in
        # its body) and nothing else — which a rejected request can do for any
        # task in any Dag.
        "corroborated": dict(sorted(corroborated.items())),
        "row_claimed": dict(sorted(row_claimed.items())),
        "instances_attributed_by_unpinned_row": unpinned,
        # One row that pins no map_index attaches to every instance of a mapped
        # task, so the per-instance counts above are NOT a count of state
        # changes. This says how many rows they actually stand on.
        "distinct_event_log_ids": len(event_log_ids),
    }
    return projected, len(tis) - len(detailed), attribution_census


_NO_CENSUS: dict[str, Any] = {
    "by_attribution": {},
    "corroborated": {},
    "row_claimed": {},
    "instances_attributed_by_unpinned_row": 0,
    "distinct_event_log_ids": 0,
}


def _run_health(
    run: dict[str, Any],
    tis: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    checks: list[dict[str, Any]],
    omitted: int,
    coverage: dict[str, Any],
    attribution_census: dict[str, Any] | None = None,
    event_history: dict[str, Any] | None = None,
    run_history: dict[str, Any] | None = None,
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
    # An unread event history is the absence of a measurement, not a measurement
    # of absence — and the strong sentence must not be earned off one. A
    # ``no_event_found`` is deliberately NOT a blocker: it is the ordinary state
    # for an EmptyOperator success and for any run whose rows aged out, and
    # ``successes_without_worker_fields`` already covers that population.
    if event_history and event_history["status"] in ("unavailable", "not_permitted"):
        blockers.append("event_history_unavailable")
    # An argument that never arrived is not a permission that was refused, and
    # the reason the strong sentence is withheld has to say which it was.
    if event_history and event_history["status"] == "not_scoped":
        blockers.append("event_history_not_scoped")
    if event_history and event_history["status"] == "partial":
        blockers.append("event_history_truncated")
    if run_history and run_history.get("error"):
        blockers.append("run_history_unavailable")
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
        "state_change_attribution": attribution_census if attribution_census is not None else _NO_CENSUS,
        "clean": not blockers,
        "clean_blockers": blockers,
    }
