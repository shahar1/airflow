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
Pure functions over one row or one string.

Neutralise author-controlled text, key and describe a single task instance, and
answer single-row predicates - with no I/O and no notion of a list.  Nothing
here reaches the network, holds state, or sees more than one row, so there is
no completeness for it to derive and nothing for it to refuse.

Imported by ``server.py`` and re-exported from it, so every ``server.X``
reference keeps resolving.  This module imports nothing from its siblings.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

# ``TaskInstance.operator`` is a free String(1000) the Dag author picks, and it is
# copied once per detailed row AND once per finding, so a 1000-char operator over
# 500 flagged rows put the size of the result in the author's hands.
OPERATOR_CLAMP_CHARS = 120


# ``Log.owner`` and ``Log.owner_display_name`` are String(500) and ``Log.extra``
# is unbounded Text, and ``extra`` is copied once per detailed instance — so
# without these the size of the result is chosen by whoever wrote the rows.
EVENT_OWNER_CLAMP_CHARS = 120
# One RELAYED ``extra`` value: an HTTP verb, a task-instance state, a map_index
# or a task id. Anything past this is off-vocabulary junk that is reported
# through ``_quoted`` at 60 characters anyway.
EVENT_EXTRA_CLAMP_CHARS = 120
# ``Log.event`` is String(60); clamped anyway, the same reflex as
# ``_clamped_operator``.
EVENT_NAME_CLAMP_CHARS = 120


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


_ATTR_AUDITED_PATCH = "audited_single_instance_patch"
_ATTR_AUDITED_OTHER = "audited_other_state_action"
_ATTR_PLATFORM = "platform_execution_event"
_ATTR_OTHER = "other_recorded_event"
_ATTR_NONE = "no_event_found"
_ATTR_TRUNCATED = "event_history_truncated"
_ATTR_UNAVAILABLE = "event_history_unavailable"
# An argument that was never supplied is not a permission that was refused.
_ATTR_NOT_SCOPED = "event_history_not_scoped"
_ATTR_UNKNOWN = "unknown"

_ATTRIBUTION_DETAIL = {
    _ATTR_AUDITED_PATCH: (
        "The event log holds a row recording that a single-task-instance PATCH request naming this "
        "task in this run was RECEIVED AND LOGGED. It records the principal name the request "
        "authenticated as, the time, the HTTP verb, and the state the request asked for. The row is "
        "committed before the request is validated and before it is authorized, so it does not "
        "establish that the request was accepted, that it was authorized, that it wrote the state "
        "now on the row, or that a person acted."
    ),
    _ATTR_AUDITED_OTHER: (
        "The event log holds a row for a different recorded action that can change this task "
        "instance's state - a task-group PATCH, a clear, a Dag-run PATCH, a task-instance delete, "
        "or a `cli_*` command. It establishes that such a request was received and logged, not that "
        "it was accepted, authorized, or that it wrote this state."
    ),
    _ATTR_PLATFORM: (
        "The event log holds a row the platform wrote for a state this task instance reached - the "
        "worker's execution-API report, or a scheduler-written state event. The recorded event "
        "does not distinguish which of those two wrote it, and `owner` on such a row is the task's "
        "`owner` attribute from the Dag file, not an actor."
    ),
    _ATTR_OTHER: (
        "A row exists for this task instance but it does not record a state change, or its event "
        "name is not one this tool recognises. The row is reported in full; no state transition is "
        "claimed from it."
    ),
    _ATTR_NONE: (
        "The scan covered every row this run-scoped query can reach and holds none for this task "
        "instance. That is NOT the same as covering this run completely, and it is not evidence of "
        "a direct database write: it is equally consistent with the bulk PATCH route (which changes "
        "state and records nothing), a direct database UPDATE, an in-process TaskInstance.set_state "
        "from a plugin or listener, the scheduler's EmptyOperator fast path, a task instance that "
        "completed inside the triggerer, a clear that swept this instance in through an include_* "
        "flag while naming only its seed task, event-log retention having deleted the row, and any "
        "`cli_*` command - `cli_dag_test` included - whose row carries a null run_id and is "
        "therefore unreachable by a run-scoped query at all."
    ),
    _ATTR_TRUNCATED: (
        "The event scan hit its cap before it could cover this run completely, and no row for this "
        "task instance appeared inside the scanned window. An older row may exist. Nothing is "
        "established."
    ),
    _ATTR_UNAVAILABLE: (
        "The event history was not read: the caller was not authorized for audit-log access on this "
        "Dag, or the query failed. Nothing about a recorded event is established either way - "
        "neither presence nor absence."
    ),
    _ATTR_NOT_SCOPED: (
        "The event history was not read because the call carried no audit scope at all - no "
        "permission was refused and none was asked for. Nothing about a recorded event is "
        "established either way - neither presence nor absence."
    ),
    _ATTR_UNKNOWN: (
        "The event history was not consulted for this task instance at all - it fell outside the "
        "detailed projection - or classification could not be decided."
    ),
}

# The one sentence each state contributes to a dispatch finding's prose. Never
# built from ``extra``, which is structured-only, and never phrased as an actor.
_ATTRIBUTION_SENTENCE = {
    _ATTR_AUDITED_PATCH: (
        "The event log records that a single task-instance state-change request naming this task in "
        "this run was RECEIVED AND LOGGED"
    ),
    _ATTR_AUDITED_OTHER: (
        "The event log records that a state-changing request naming this task in this run was "
        "RECEIVED AND LOGGED"
    ),
    _ATTR_PLATFORM: (
        "The newest event-log row for this task instance is one the platform wrote for a state it reached"
    ),
    _ATTR_OTHER: (
        "The newest event-log row for this task instance is one this diagnosis does not classify "
        "as a state change"
    ),
    _ATTR_NONE: (
        "The event log holds no row this run-scoped query can reach for this task instance, which "
        "is not evidence of a direct database write - a bulk state change, the scheduler's "
        "EmptyOperator path, a completion inside the triggerer, a `cli_*` command (whose row "
        "carries a null run_id and is unreachable here) and event-log retention all leave no row"
    ),
    _ATTR_TRUNCATED: (
        "The event scan was cut off before it covered this run, and no row for this task instance "
        "was inside the scanned window, so nothing about a recorded event is established"
    ),
    _ATTR_UNAVAILABLE: (
        "The event history was not read for this run, so nothing about a recorded event is "
        "established either way"
    ),
    _ATTR_NOT_SCOPED: (
        "The event history was not read for this run because the call carried no audit scope, so "
        "nothing about a recorded event is established either way"
    ),
    _ATTR_UNKNOWN: "The event history was not consulted for this task instance",
}

# A row DEMOTED into ``other_recorded_event`` is not the ``unrecognised`` case
# the prose above was written for, and must not borrow its words: this tool DOES
# recognise ``patch_task_instance``, ``post_clear_task_instances`` and
# ``cli_task_clear`` by name, so telling the reader it "does not classify [it]
# as a state change" or that the name is "not one this tool recognises" is false
# about the row - and that text reaches the summary the model reads aloud. Keyed
# by ``classification``, which names WHICH of the two demotions happened.
_DEMOTED_DETAIL = {
    "request_settable_targeting_fields": (
        "The event log holds a row whose event name this tool DOES recognise as an action that can "
        "change a task instance's state. It is not attributed to this task instance, because the "
        "`dag_id`, `run_id` and `task_id` columns it recorded are settable by the request itself, "
        "so the row cannot establish WHICH task instance it concerned. The row is reported in "
        "full; no state transition is claimed from it."
    ),
    "uncorroborated_association": (
        "The event log holds a row whose event name this tool DOES recognise as an action that can "
        "change a task instance's state. It is not attributed to this task instance, because "
        "nothing on the row corroborates that it concerned this one: the association rests on what "
        "the row claims about itself rather than on the row's own `map_index` column. The row is "
        "reported in full; no state transition is claimed from it."
    ),
}
_DEMOTED_SENTENCE = {
    "request_settable_targeting_fields": (
        "The newest event-log row for this task instance is one this diagnosis recognises as a "
        "state-changing action but does not attribute, because the `dag_id`, `run_id` and "
        "`task_id` it recorded are settable by the request and so cannot establish which task "
        "instance it concerned"
    ),
    "uncorroborated_association": (
        "The newest event-log row for this task instance is one this diagnosis recognises as a "
        "state-changing action but does not attribute, because the row's own `map_index` column "
        "does not corroborate that it concerned this task instance"
    ),
}


def _attribution_detail(state: str, classification: str | None = None) -> str:
    """The paragraph for one attribution — demoted rows get their own, not the unrecognised one."""
    return _DEMOTED_DETAIL.get(classification or "", _ATTRIBUTION_DETAIL[state])


def _attribution_sentence(state: str, classification: str | None = None) -> str:
    """The one summary sentence for one attribution, chosen the same way."""
    return _DEMOTED_SENTENCE.get(classification or "", _ATTRIBUTION_SENTENCE[state])


def _clamped_event_text(value: Any, limit: int) -> tuple[Any, bool]:
    """A value out of the event log, cut to a size this tool chooses."""
    if isinstance(value, str) and len(value) > limit:
        return value[: limit - 1] + "…", True
    return value, False


def _parsed_extra(raw: Any) -> dict[str, Any]:
    """``Log.extra`` as a dict, or an empty one.

    ``json.loads`` inside a try, never ``eval``; a non-dict result is ignored and
    only the raw string is reported.
    """
    if not isinstance(raw, str):
        return {}
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


# ``extra`` is unbounded Text, and the association check runs it once per (row,
# task instance) pair — 300 rows against 200 instances is 60000 parses of a
# string whose size the requester chose. Parsed once, memoised on the row, and
# never emitted: the rows are dropped from the payload before it is returned.
_PARSED_EXTRA_KEY = "__airy_parsed_extra"


def _parsed_extra_of(row: dict[str, Any]) -> dict[str, Any]:
    """``_parsed_extra`` for one event-log row, parsed at most once.

    Reads the copy ``_event_history`` made; it never WRITES the key, because the
    row it is handed is not this function's to mutate.
    """
    cached = row.get(_PARSED_EXTRA_KEY)
    return cached if cached is not None else _parsed_extra(row.get("extra"))


def _is_json_object(raw: Any) -> bool:
    """Whether ``Log.extra`` was a JSON object at all — reported, never guessed at."""
    if not isinstance(raw, str):
        return False
    try:
        return isinstance(json.loads(raw), dict)
    except (ValueError, TypeError):
        return False


def _carries_worker_field(row: dict[str, Any]) -> bool:
    """Whether one compared row carries a field only a worker writes."""
    return bool(row.get("hostname")) or row.get("pid") is not None


def _run_version(run: dict[str, Any]) -> int | None:
    """The Dag version a run executed with — the last entry is the one in effect."""
    versions = run.get("dag_versions") or []
    return versions[-1].get("version_number") if versions else None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _carries_execution_fields(row: dict[str, Any]) -> bool:
    return bool(row.get("hostname")) or row.get("pid") is not None


# Bytes the Dag author's own code wrote, echoed back through this tool. Tagged
# where they are carried so a reader cannot mistake the task's narration of its
# work for this tool's finding about the world — a log line saying "filing
# transmitted" is a claim by the task, not evidence that it happened.
_TASK_LOG_SOURCE = "dag_authored_task_log"


def _tagged_log(log: dict[str, Any]) -> dict[str, Any]:
    """The same log reading with its tail labelled as the task's own text."""
    if "tail" not in log:
        return log
    return {**log, "tail": {"source": _TASK_LOG_SOURCE, "text": log["tail"]}}


def _check(name: str, passed: bool | None, detail: str) -> dict[str, Any]:
    """One verification leg. ``None`` is "could not be established" and never counts as a pass."""
    return {"check": name, "passed": passed, "detail": detail}


def _later_than(when: Any, reference: str) -> bool | None:
    """Whether an API timestamp is after a reference one, or ``None`` if unanswerable."""
    if not reference or not isinstance(when, str) or not when:
        return None
    try:
        left = datetime.fromisoformat(when.replace("Z", "+00:00"))
        right = datetime.fromisoformat(reference.replace("Z", "+00:00"))
    except ValueError:
        return None
    if left.tzinfo is None or right.tzinfo is None:
        return None
    return left > right


def _clip_at_word(text: str, limit: int) -> str:
    """Clip to the limit at a word boundary, marking the cut with an ellipsis."""
    if len(text) <= limit:
        return text
    cut = text[:limit]
    head, _, _ = cut.rpartition(" ")
    return f"{head or cut}…"
