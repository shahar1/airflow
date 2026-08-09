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
What the user was shown, and the single-use right it grants.

Issue, peek at, redeem and evict the plan tokens and the approved-instance-set
baselines, and decide whether two run lists are the same list.  Nothing here
speaks HTTP, knows what a task instance is, or derives the completeness of a
read: it is a bounded in-memory store and the identity rule two run lists are
compared under.

Wave 3 of the move-only extraction in ``docs/extraction-plan.md``.

``_TOKEN_TTL_S``, ``_TOKEN_MAX`` and ``_APPROVED_SET_MAX`` bound the two stores,
and the suite rebinds the TTL to expire a token on demand.  A rebound number is
only seen by the module that reads it, so they are deliberately NOT re-exported
from ``server.py`` - a re-export there would be a display symbol a
``monkeypatch.setattr(server, "_TOKEN_TTL_S", ...)`` could change while
``_peek_token`` went on reading the real one.  The two stores themselves ARE
re-exported: they are mutated in place, never rebound, so one dict object serves
every reader.
"""

from __future__ import annotations

import secrets
import time
from datetime import datetime, timezone
from typing import Any

from primitives import _now_iso


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


# What the user was shown before consenting, keyed by token: the reviewed
# backfill plan, and the paused-Dag warning that has to precede an unpause.
# In-memory and per-process, like the plugin's pending approvals.
_issued_tokens: dict[str, dict[str, Any]] = {}
_TOKEN_TTL_S = 900.0
_TOKEN_MAX = 20

# Both stores are bounded reads of themselves, and the bound has no signal: a
# lookup that misses is "no such plan" AND "a plan that was pushed out by
# twenty newer ones", with nothing to tell them apart. Counted here so a miss
# can say which of the two it might be.
_evictions = {"tokens": 0, "approved_sets": 0}


def evicted() -> dict[str, int]:
    """How many entries each store has silently dropped for capacity."""
    return dict(_evictions)


def eviction_note(store: str) -> str:
    """The sentence a lookup miss carries when the store has dropped entries."""
    count = _evictions.get(store, 0)
    if not count:
        return ""
    return (
        f" This server has dropped {count} older entr(y/ies) from that store for capacity since it "
        f"started, so a record that existed may no longer be on it — the miss is not evidence that "
        f"nothing was ever recorded."
    )


def _issue_token(kind: str, payload: dict[str, Any]) -> str:
    now = time.monotonic()
    for token in [t for t, p in _issued_tokens.items() if now - p["created_at"] > _TOKEN_TTL_S]:
        del _issued_tokens[token]
    while len(_issued_tokens) >= _TOKEN_MAX:
        del _issued_tokens[next(iter(_issued_tokens))]
        _evictions["tokens"] += 1
    token = secrets.token_urlsafe(12)
    _issued_tokens[token] = {**payload, "kind": kind, "created_at": now}
    return token


def _peek_token(kind: str, token: str) -> dict[str, Any] | None:
    """Read a live plan without spending it.

    For refusals that happen *before* the write and change nothing. Spending the
    token there would destroy the plan the user already approved because the
    call carried the wrong arguments — the caller would fix the arguments and be
    told there is no plan, which is a worse answer than the real one.
    """
    payload = _issued_tokens.get(token)
    if payload is None or payload["kind"] != kind:
        return None
    if time.monotonic() - payload["created_at"] > _TOKEN_TTL_S:
        return None
    return payload


def _redeem_token(kind: str, token: str) -> dict[str, Any] | None:
    """Single-use by construction: a token can only ever be redeemed once."""
    payload = _peek_token(kind, token)
    _issued_tokens.pop(token, None)
    return payload


# What an approval actually authorized, keyed by the run it was for and written
# by the server at the moment the plan token is spent. The verification's set
# comparison reads THIS, not the list its caller handed it: a baseline the caller
# supplies can be replaced with the post-clear reality, which turned the one leg
# that catches an unenumerated instance into a green claim about what an operator
# approved. In-memory and per-process, like the tokens themselves.
_approved_clear_sets: dict[tuple[str, str], dict[str, Any]] = {}
_APPROVED_SET_MAX = 20


def _record_approved_set(dag_id: str, dag_run_id: str, approved: list[Any]) -> None:
    while len(_approved_clear_sets) >= _APPROVED_SET_MAX:
        del _approved_clear_sets[next(iter(_approved_clear_sets))]
        _evictions["approved_sets"] += 1
    _approved_clear_sets[(dag_id, dag_run_id)] = {
        "approved": sorted({(str(task), int(index)) for task, index in approved}),
        "recorded_at": _now_iso(),
    }


def _approved_set_record(dag_id: str, dag_run_id: str) -> dict[str, Any] | None:
    """The approved set this server recorded for a run, or ``None`` if it has none."""
    return _approved_clear_sets.get((dag_id, dag_run_id))


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


# ---------------------------------------------------------------------------
# What stands between an approval and a write.
#
# Every call in this tree that could change something is classified HERE, at one
# call site's granularity, so a write added later is either gated or is a
# deliberate decision somebody wrote down. It used to be neither: the gate
# covered one of seven writes, five gated on identity or on a digest, and one —
# the run this server creates — passed through nothing at all while the server's
# own module docstring said every mutation is planned first.
# ---------------------------------------------------------------------------

# The functions a write may pass through. A write inside a function that calls
# one of these, before the write, is gated by it.
_WRITE_GATES = ("_redeem_token", "_containment_gate")

# Every mutating call site, by module, function and the request it makes, with
# the gate that dominates it.
_GATED_WRITES = {
    ("recovery", "apply_task_instance_clear", "POST /clearTaskInstances"): "_containment_gate",
    ("codechange", "run_backfill", "POST /backfills"): "_redeem_token",
    ("codechange", "rerun_dag", "PATCH /dags/<dag>"): "_redeem_token",
    ("codechange", "apply_dag_code_changes", "WRITE the Dag file"): "_redeem_token",
    ("codechange", "revert_dag_code", "WRITE the Dag file"): "_redeem_token",
    # The backup is a write of its own, and it was covered only by the entry
    # beside it — a classification that was per FUNCTION rather than per
    # request, so any further write inside either of these was auto-classified
    # by an entry written for a different one.
    ("codechange", "apply_dag_code_changes", "WRITE the Dag file's backup"): "_redeem_token",
    ("codechange", "revert_dag_code", "DELETE the Dag file's backup"): "_redeem_token",
}

# Writes this server makes WITHOUT a reviewed plan, each with the reason. A
# write here is a decision, not an oversight, and the reason has to be a
# property of the write rather than of the effort of gating it.
_UNGATED_WRITES = {
    ("codechange", "rerun_dag", "POST /dags/<dag>/dagRuns"): (
        "creating a run is fully described by this call's own arguments — the dag_id, the conf and "
        "the note — so the tool call the user confirms IS the approval card, and a plan would show "
        "them the same three values a second time. It is also additive: it creates a new run and "
        "changes no existing instance, unlike a clear, a source write or a backfill range. The "
        "lasting part of it, unpausing, is separately gated and separately warned."
    ),
    ("codechange", "_abandon_backfill", "PUT /backfills/<id>/cancel"): (
        "the compensating action inside run_backfill, reached only after that tool has redeemed its "
        "token and only when what was created does not match what was approved; gating it again "
        "would leave the mismatched backfill running"
    ),
    ("dagsource", "_write_if_unchanged", "os.unlink of its own temp file"): (
        "the atomic-replace helper cleaning up the temporary file it just created and nothing else; "
        "the Dag file itself is written by the two tools above, each of which has redeemed its token "
        "before it reaches here"
    ),
    # The five steps of the atomic replace, each its own call and each its own
    # entry. They were unclassified while the registry above said every call
    # that could change something is classified here — including the ``os.replace``
    # that IS the Dag-file write, which a four-name scan could not see.
    ("dagsource", "_write_if_unchanged", "CREATE its own temp file"): (
        "mkstemp beside the target so the move stays on one filesystem; the file it creates is this "
        "helper's own scratch space, named with a leading dot and a .tmp suffix, and no Dag file "
        "exists at that path for it to overwrite"
    ),
    ("dagsource", "_write_if_unchanged", "OPEN its own temp file for writing"): (
        "wrapping the descriptor mkstemp just returned, so that the bytes can be written through a "
        "text handle with an explicit encoding; it opens no path of its own and reaches nothing the "
        "line above did not already create"
    ),
    ("dagsource", "_write_if_unchanged", "WRITE its own temp file"): (
        "the reviewed bytes going into this helper's own scratch file, not into the Dag file; what "
        "makes them the Dag's source is the replace below, and both are reached only from the two "
        "tools above, each of which has redeemed its token first"
    ),
    ("dagsource", "_write_if_unchanged", "CHMOD its own temp file"): (
        "carrying the target's existing mode onto the scratch file so the replace does not change "
        "the Dag file's permissions; it is the temp file's mode that is set, and the mode it is set "
        "to is the one already on disk"
    ),
    ("dagsource", "_write_if_unchanged", "REPLACE the Dag file with its own temp file"): (
        "THE Dag-file write, and the reason this helper exists: an atomic rename over the target so "
        "the Dag processor, which takes no lock, can never read a torn file. It is gated at its "
        "CALL SITE — the two tools that call this helper each redeem a token first, and each carries "
        "the 'WRITE the Dag file' classification for that call — so gating it a second time here "
        "would be a second approval for one reviewed diff"
    ),
    ("dagsource", "_force_reparse", "PUT /parseDagFile/<token>"): (
        "a post-write call, reached only from a tool that has already redeemed its token, that asks "
        "Airflow to re-read the file that write just changed"
    ),
}

# Calls that carry a mutator's SPELLING and change nothing. The census of
# writes is closed by these two directions together: every call spelled like a
# mutation is either classified above or declared here, so a real ``os.replace``
# cannot pass for the ``str.replace`` beside it. Each reason is about THIS call.
_NOT_A_WRITE = {
    ("approvals", "_run_identity", "str(raw).replace('Z', '+00:00')"): (
        "str.replace over a timestamp string, normalising the Zulu spelling before it is parsed"
    ),
    ("dagsource", "_exclusive", "path.open('r+', encoding='utf-8')"): (
        "opened only to hold an flock over the whole read-check-write; not one byte is written "
        "through this handle, and the mode is r+ because flock needs a writable descriptor"
    ),
    ("dagsource", "_patch", "patched.replace(old, new)"): (
        "str.replace over the source buffer in memory; these bytes reach the file only through "
        "_write_if_unchanged, which is classified above"
    ),
    ("primitives", "_parse_moment", "when.replace('Z', '+00:00')"): (
        "str.replace over a timestamp string, normalising the Zulu spelling before it is parsed"
    ),
}

# The two POSTs that read. Neither changes anything, and both are named here
# rather than inferred from the verb.
_READ_SEARCHES = (
    "/backfills/dry_run",
    "/dags/~/dagRuns/~/taskInstances/list",
    "dry_run=True",
    "'dry_run': True",
)
