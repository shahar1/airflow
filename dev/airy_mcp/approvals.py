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


def _issue_token(kind: str, payload: dict[str, Any]) -> str:
    now = time.monotonic()
    for token in [t for t, p in _issued_tokens.items() if now - p["created_at"] > _TOKEN_TTL_S]:
        del _issued_tokens[token]
    while len(_issued_tokens) >= _TOKEN_MAX:
        del _issued_tokens[next(iter(_issued_tokens))]
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
