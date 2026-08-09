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
Changes that are not a clear, and the check on the run one of them starts.

The tools that repair a Dag's source, revert it, start a fresh run, or create a
backfill - together with the conf validation that stands in front of a trigger
and the read that says whether the run it started did the work.

``plan_dag_code_changes``/``apply_dag_code_changes`` are one atomic set of edits
against the exact bytes the plan was made from.  ``plan_revert_dag_code``/
``revert_dag_code`` restore the original Airy backed up, discarding every change
Airy applied.  ``rerun_dag`` starts a fresh run on the latest code under an
identity the caller chooses, validating ``conf`` against the Dag's own params
schema and refusing to unpause without a second, separately-warned token.
``verify_replacement_run`` reads back, for THAT run and no other, whether the
task whose work was missing recorded its output; it writes nothing.
``plan_backfill``/``run_backfill`` create a range of runs - both WITHDRAWN from
the registered surface, kept here as the record of what was built.

The two mutations a user asks for are approved separately and neither approval
reaches the other: a source write redeems a token of its own kind, and the
trigger redeems none at all and re-establishes its own preconditions instead.

Wave 8 of the move-only extraction in ``docs/extraction-plan.md``.

It knows nothing about event-log epistemics, dispatch evidence, or diagnosis
prose.  It reaches ``transport`` directly for the four point reads issued inline
inside a tool body (``GET /dags/<id>`` three times and ``GET /dags/<id>/details``
once) and for the four writes (the unpause ``PATCH``, the run ``POST``, the
backfill ``POST`` and the cancel ``PUT``).  Those are the enumerated exceptions
this module carries in ``docs/extraction-plan.md`` §6 N2; relocating them into
``reading`` is a reshaping of a tool body rather than a move of a definition, so
it is left to the typed redesign.  This wave keeps every body byte-identical.

The bound the suite rebinds here is ``MAX_BACKFILL_RUNS``, which lives in
``reading`` and is read through the module object at both of its sites, so the
module that owns the number stays the one place to patch.  ``_read_reviewed_file``
is reached through ``dagsource`` for the same reason.  The seven tools are never
patched by the suite, so re-exporting them from ``server.py`` is the same object
either way.
"""

from __future__ import annotations

import difflib
from hashlib import md5
from typing import Any
from urllib.parse import quote

# ``_read_reviewed_file`` and ``MAX_BACKFILL_RUNS`` are rebound by the suite, so the
# sites that use them go through the module object and are never imported by name;
# ``_api`` is rebound too, for the same reason.
import dagsource
import httpx
import reading
import transport
from approvals import (
    _discard_same_source_plans,
    _issue_token,
    _redeem_token,
    _run_identity,
    _same_runs,
    eviction_note,
)
from dagsource import (
    DagFileDriftError,
    DagFileError,
    _backup_path,
    _build_revert_diff,
    _change_impact,
    _dag_path,
    _definition_updates,
    _exclusive,
    _find_unaddressed_findings,
    _force_reparse,
    _normalized_changes,
    _parsed_source,
    _patch,
    _write_if_unchanged,
)
from primitives import _quoted, _ti_key
from reading import (
    _backfill_runs,
    _build_asset_note,
    _dry_run_backfill,
    _latest_version,
)
from transport import _dag_url, _explain_error, _explain_unknown_dag


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
            "error": (
                "no reviewed plan for this revert; call plan_revert_dag_code and show the user the diff"
                + eviction_note("tokens")
            ),
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
            return _approval_spent_before_the_write("reverted", "the Dag record", e)
        return {"reverted": False, "mutation_applied": False, "error": message + _APPROVAL_IS_GONE}
    except _READ_FAILURES as e:
        return _approval_spent_before_the_write("reverted", "the Dag record", e)
    try:
        path = _dag_path(dag_id, dag)
        backup = _backup_path(path)
    except DagFileError as e:
        return {"reverted": False, "mutation_applied": False, "error": str(e)}
    if not backup.exists():
        return {"reverted": False, "mutation_applied": False, "error": f"no backup for {path.name}"}
    try:
        version_before_write = _latest_version(dag_id)
    except _READ_FAILURES as e:
        return _approval_spent_before_the_write("reverted", "the Dag's version list", e)
    with _exclusive(path):
        try:
            current = dagsource._read_reviewed_file(dag_id, path, source_digest)
        except DagFileDriftError as e:
            return {"reverted": False, "mutation_applied": False, "error": str(e)}
        except _READ_FAILURES as e:
            return _approval_spent_before_the_write("reverted", "the reviewed source", e)
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
# Every live read between redeeming the approval and the write. A blip on one of
# them used to raise out of the tool with the approval already spent, and the
# retry with the same token answered "no reviewed plan for this change" — which
# is false, and reads to the user as their own mistake. The clear path was
# repaired for exactly this; both code paths were left with it.
_READ_FAILURES = (httpx.HTTPStatusError, httpx.RequestError, KeyError, TypeError, ValueError)


# What a post-redemption refusal has to add, whatever else it says. A message
# that names only the Dag reads as a lookup problem, and the retry it invites
# answers "no reviewed plan for this change" — which is not what happened.
_APPROVAL_IS_GONE = (
    " Nothing was written. The approval was already spent by this attempt and is gone — re-plan "
    "and show the user again."
)


def _approval_spent_before_the_write(outcome_key: str, which: str, error: Exception) -> dict[str, Any]:
    """The answer when a read between the approval and the write did not come back.

    Pre-mutation, so "not applied" is a fact rather than an inference: the file
    has not been touched and no request has gone out.
    """
    return {
        outcome_key: False,
        "mutation_applied": False,
        "error": (
            f"this change was NOT made: {which} could not be read "
            f"({_quoted(_explain_error(error), 240)}), so the facts this write rests on were not "
            f"re-established. Nothing was written. The approval was already spent by this attempt "
            f"and is gone — re-plan and show the user again."
        ),
    }


_UNPAUSE_APPROVAL_IS_GONE = (
    "The unpause approval was already spent by this attempt and cannot be redeemed again — put the "
    "question to the user afresh and call rerun_dag without unpause to get a new unpause_token."
)


def _unpause_did_not_come_back(dag_id: str, error: Exception) -> dict[str, Any]:
    """The answer when the unpause write itself did not come back.

    ``_redeem_token`` burns the approval, and the PATCH behind it was the one
    write in this tree with nothing between it and the caller: a transient
    ``RequestError`` raised out of the tool with the approval spent and nothing
    reported, and the retry with the same token answered "unpausing NAME needs
    the unpause_token from its paused-Dag warning" — the user's own mistake, in
    this tool's own words, over a token this tool had already destroyed.

    A 4xx is a refusal the route CHOSE, so it changed nothing and says so.
    Anything else leaves the paused state unestablished, because an httpx error
    can be raised after the request has already reached Airflow.
    """
    refused = isinstance(error, httpx.HTTPStatusError) and 400 <= error.response.status_code < 500
    if refused:
        return {
            "triggered": False,
            "mutation_applied": False,
            "unpaused": False,
            "dag_id": dag_id,
            "error": (
                f"{dag_id} was NOT unpaused and no run was triggered: Airflow refused the unpause "
                f"({_quoted(_explain_error(error), 240)}). Nothing was changed. "
                f"{_UNPAUSE_APPROVAL_IS_GONE}"
            ),
        }
    # ``mutation_applied`` is absent, not false: the request may have landed.
    return {
        "triggered": False,
        "unpaused": None,
        "dag_id": dag_id,
        "error": (
            f"the unpause of {dag_id} did not come back ({_quoted(_explain_error(error), 240)}), so "
            f"whether it is still paused is NOT established and no run was triggered. Check the Dag's "
            f"paused state in Airflow before doing anything else. {_UNPAUSE_APPROVAL_IS_GONE}"
        ),
    }


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
            "error": (
                "no reviewed plan for this change; call plan_dag_code_changes and show the user the diff"
                + eviction_note("tokens")
            ),
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
            return _approval_spent_before_the_write("applied", "the Dag record", e)
        return {"applied": False, "mutation_applied": False, "error": message + _APPROVAL_IS_GONE}
    except _READ_FAILURES as e:
        return _approval_spent_before_the_write("applied", "the Dag record", e)
    try:
        # The write jail, re-asked after the approval and not only before it: the
        # Dag record names the file, and a record that now names a path outside
        # the bundle is a write this tool must refuse in words. Raising here sent
        # the jail refusal out as an exception, which is the one shape a caller
        # cannot tell from "the tool crashed, maybe after writing".
        path = _dag_path(dag_id, dag)
    except DagFileError as e:
        return {
            "applied": False,
            "mutation_applied": False,
            "error": f"this change was NOT applied: {e}. Nothing was written." + _APPROVAL_IS_GONE,
        }
    try:
        version_before_write = _latest_version(dag_id)
    except _READ_FAILURES as e:
        return _approval_spent_before_the_write("applied", "the Dag's version list", e)
    with _exclusive(path):
        try:
            source = dagsource._read_reviewed_file(dag_id, path, source_digest)
        except DagFileDriftError as e:
            return {"applied": False, "mutation_applied": False, "error": f"{e} — {_REPLAN_STEER}"}
        except _READ_FAILURES as e:
            return _approval_spent_before_the_write("applied", "the reviewed source", e)
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
        # Re-asked here, immediately before the write. The digest pins the FILE,
        # and the impact check does not rest on the file alone: it rests on the
        # live task graph, which moves on its own and which the plan read at a
        # different moment. A graph that has since changed — or that now comes
        # back short — must refuse rather than ride the plan's answer in.
        impact = _change_impact(dag_id, source, patched)
        if impact["blocking"]:
            return {
                "applied": False,
                "mutation_applied": False,
                "impact": impact,
                "error": (
                    f"this change was NOT applied: the task graph no longer supports it at the "
                    f"moment of the write ({impact['blocking']}). Nothing was written. Re-plan and "
                    f"show the user."
                ),
            }

        try:
            backup = _backup_path(path)
        except DagFileError as e:
            return {"applied": False, "mutation_applied": False, "error": str(e)}
        # Read here, under the same lock and immediately before the write, so
        # the graph the post-write check compares against is the one this write
        # actually replaced rather than one read at some earlier moment.
        graph_before = reading._tasks(dag_id)
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
    # Only NOW is there anything to check: a compile in memory says the bytes are
    # Python, and it says nothing about whether Airflow can import them or about
    # the Dag they produce. Both questions have to be asked of the Dag processor,
    # which means after the write and after the reparse.
    checks = _post_write_checks(dag_id, dag, graph_before, impact)
    if checks["problem"] is not None:
        return _put_the_original_back(
            dag_id,
            dag,
            path,
            written=patched,
            original=source,
            version_before_write=version_before_write,
            reparse=reparse,
            checks=checks,
        )
    return {
        "applied": True,
        "mutation_applied": True,
        "file": str(path),
        "change_count": len(pairs),
        "diff": diff,
        "reparse": reparse,
        "post_write_checks": checks["checks"],
        "post_write_checks_unread": checks["unread"],
        **({"warning": backup_failure} if backup_failure else {}),
        **_definition_updates(dag_id, version_before_write, version_after),
    }


def _post_write_checks(
    dag_id: str, dag: dict[str, Any], graph_before: reading.Reading, impact: dict[str, Any]
) -> dict[str, Any]:
    """What the Dag processor makes of the bytes that were just written.

    Two questions, and a third answer for each of them. Does the file still
    import, and is the Dag it produces the one the reviewed diff predicted? An
    unreadable or short check is NOT a failure: rolling a reviewed change back
    because a lookup timed out is an unforced revert of the user's own decision.
    A check that could not be established says so, names the read that fell
    short, and the change stands.
    """
    checks: dict[str, Any] = {}
    unread: list[str] = []
    problem: str | None = None

    errors = reading._find_import_errors(dag)
    if errors.read_failed:
        checks["imports"] = f"not established ({errors.error})"
    elif errors.rows:
        checks["imports"] = "FAILED"
        problem = (
            f"the file no longer imports after the change: "
            f"{_quoted(str(next(iter(errors.rows)).get('detail')), 400)}"
        )
    elif not errors.complete:
        # An empty page of a read that did not finish is not an absence of
        # import errors, and "clean" over it is the claim this must not make.
        checks["imports"] = "not established"
        unread.append(errors.reason)
    else:
        checks["imports"] = "clean"

    graph_after = reading._tasks(dag_id)
    removed = impact.get("removed_task_ids")
    for graph in (graph_before, graph_after):
        if not graph.complete:
            unread.append(graph.reason)
    if removed is None or not graph_before.complete or not graph_after.complete:
        checks["task_graph"] = "not established"
        return {"problem": problem, "checks": checks, "unread": unread}
    before = {task["task_id"] for task in graph_before.rows}
    now = {task["task_id"] for task in graph_after.rows}
    # The static scan is a lower bound on what the patch declares, so it can
    # under-predict an addition; what it may never do is leave a task the change
    # did not say it would remove missing from the parsed Dag.
    lost = sorted(before - set(removed) - now)
    if lost:
        checks["task_graph"] = "FAILED"
        problem = (problem + "; " if problem else "") + (
            f"the reparsed Dag no longer defines {lost}, which this change did not say it would remove"
        )
    else:
        checks["task_graph"] = "as predicted"
    return {"problem": problem, "checks": checks, "unread": unread}


def _put_the_original_back(
    dag_id: str,
    dag: dict[str, Any],
    path: Any,
    *,
    written: str,
    original: str,
    version_before_write: int | None,
    reparse: str,
    checks: dict[str, Any],
) -> dict[str, Any]:
    """Undo the write, and say plainly what state the file is in either way.

    A rollback that fails is the case this exists for. Reporting it as a plain
    refusal would leave a person believing their Dag is as it was while a broken
    file sits on disk being parsed, so the failure is the loudest thing in the
    result and ``applied`` is not the field that carries it.
    """
    restored = False
    restore_error = None
    try:
        with _exclusive(path):
            # The same request as the write above, under the same redeemed
            # approval, putting back the exact bytes that approval replaced.
            _write_if_unchanged(path, written, original)
        restored = True
    except Exception as e:
        restore_error = _explain_error(e)
    reparse_back = "not attempted"
    if restored:
        try:
            reparse_back, _ = _force_reparse(dag_id, dag["file_token"], None)
        except Exception as e:
            reparse_back = f"the file was restored, but the reparse request failed: {_explain_error(e)}"
    if restored:
        error = (
            f"this change was applied and then PUT BACK: {checks['problem']}. The file on disk is "
            f"byte-for-byte the original again. Nothing of the change survives; work out what it "
            f"broke before proposing it a second time."
        )
    else:
        error = (
            f"this change was applied, it {checks['problem']}, AND PUTTING IT BACK FAILED "
            f"({_quoted(str(restore_error), 240)}). {path} is left holding the changed bytes, in a "
            f"state known to be bad. This needs a person NOW: restore the file by hand from "
            f"{_backup_path(path).name} if it is there."
        )
    return {
        "applied": False,
        # Writes went out — the patch, and the attempt to put it back — and the
        # Dag was reparsed in between. "Nothing was applied" would be false.
        "mutation_applied": True,
        "rolled_back": restored,
        "file": str(path),
        "post_write_checks": checks["checks"],
        "post_write_checks_unread": checks["unread"],
        "reparse": reparse,
        "reparse_after_rollback": reparse_back,
        "error": error,
        "ui_updates": [{"kind": "dag_definition", "dag_id": dag_id}],
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


# What the trigger's own approval rests on, and the only claim it may make when
# one of these cannot be re-established: nothing was created. Both checks run
# BEFORE any write goes out, so "not applied" here is a fact and not a guess.
_TRIGGER_PRECONDITION_UNREAD = (
    "so the facts this trigger rests on were not re-established. No run was created and nothing was changed."
)

# The non-claim that travels with every idempotent trigger. A caller that reads
# "no duplicate" as "this work has not been done" will stop looking, so the
# limit of what the key buys is said in the payload rather than only here.
_IDENTITY_SCOPE = (
    "This prevents a duplicate under this exact run identity only. It establishes nothing about "
    "whether a semantically equivalent run exists under a different id, in this Dag or elsewhere."
)

# The non-claim that travels with a version-pinned trigger. Refusing a version
# that MOVED reads as a promise that a version that matched is the approved
# code, and it is not one: a Dag version is a number, not a set of bytes.
_VERSION_PIN_SCOPE = (
    "The Dag was still on the version this run was agreed against, which is why the trigger was "
    "not refused. That pins the version NUMBER and nothing else. It establishes nothing about the "
    "code that will run: under an unversioned bundle the worker imports the Dag file as it stands "
    "on disk when the task starts, and a version's recorded source is rewritten in place when the "
    "file changes, so identical version numbers can be different bytes."
)


def _version_precondition(dag_id: str, expected: int) -> dict[str, Any] | None:
    """Refuse unless the Dag is still on the version this run was agreed against."""
    try:
        current = reading._latest_version(dag_id)
    except _READ_FAILURES as e:
        return {
            "triggered": False,
            "mutation_applied": False,
            "dag_id": dag_id,
            "error": (
                f"no run was triggered: the Dag's version could not be read "
                f"({_quoted(_explain_error(e), 240)}), {_TRIGGER_PRECONDITION_UNREAD}"
            ),
        }
    if current != expected:
        return {
            "triggered": False,
            "mutation_applied": False,
            "dag_id": dag_id,
            "expected_dag_version": expected,
            "current_dag_version": current,
            "error": (
                f"no run was triggered: {dag_id} was on Dag version {expected} when this run was "
                f"agreed and is on {current} now, so the code that would run is not the code that "
                f"was approved. Nothing was created. Re-check the Dag and ask the user again."
            ),
        }
    return None


def _existing_run_with_this_identity(dag_id: str, run_id: str) -> dict[str, Any] | None:
    """The run already carrying this identity, a refusal, or ``None`` to go ahead.

    Three answers, never two. A run that is there is returned and nothing is
    created. A 404 is a real absence and the trigger proceeds. Anything else —
    a permission refusal, a timeout, a 5xx — leaves the absence UNESTABLISHED,
    and creating a run on an unestablished absence is exactly the duplicate the
    identity exists to prevent, so it refuses instead.
    """
    try:
        run, error = reading._resolve_run(dag_id, run_id)
    except Exception as e:
        return {
            "triggered": False,
            "mutation_applied": False,
            "dag_id": dag_id,
            "dag_run_id": run_id,
            "error": (
                f"no run was triggered: whether {run_id!r} already exists could not be read "
                f"({_quoted(_explain_error(e), 240)}), {_TRIGGER_PRECONDITION_UNREAD} Retry with "
                f"this same run_id — that is what keeps the retry from creating a second run."
            ),
        }
    if run is not None:
        return {
            "triggered": False,
            # Nothing was written on this call: the run was already there.
            "mutation_applied": False,
            "already_existed": True,
            "dag_id": dag_id,
            "dag_run_id": run.get("dag_run_id", run_id),
            "state": run.get("state"),
            "idempotency": _IDENTITY_SCOPE,
            "next_step": (
                f"run {run.get('dag_run_id', run_id)!r} already exists in state "
                f"{run.get('state')!r} and NO new run was created; check that run's outcome "
                f"rather than triggering again"
            ),
        }
    # ``_resolve_run`` turns a 404 into a sentence and a 403 into a different
    # one. Only the 404 is an absence; the 403 says the run could not be read.
    if error and "permission refusal" in error:
        return {
            "triggered": False,
            "mutation_applied": False,
            "dag_id": dag_id,
            "dag_run_id": run_id,
            "error": (
                f"no run was triggered: {error}, {_TRIGGER_PRECONDITION_UNREAD} Whether "
                f"{run_id!r} already exists is NOT established."
            ),
        }
    return None


def rerun_dag(
    dag_id: str,
    conf: dict[str, Any] | None = None,
    note: str = "",
    unpause: bool = False,
    unpause_token: str = "",
    run_id: str = "",
    expected_dag_version: int | None = None,
) -> dict[str, Any]:
    """
    Trigger a fresh run of a Dag on the latest code, once.

    ``conf`` sets the Dag's trigger parameters and is validated against the
    Dag's own params schema — unknown keys and wrong types are refused with the
    list of what the Dag accepts. Leave it out for a Dag without parameters.
    ``note`` is attached to the created run.

    ``run_id`` is the exact identity to create the run under, chosen by YOU, and
    it is what makes a retry safe: before triggering, this tool asks Airflow
    whether a run with that identity already exists, and if one does it returns
    that run and creates nothing. Pass the same ``run_id`` on every retry of the
    same intended run. **Idempotency here prevents duplication under THIS
    identity and nothing more** — it does not establish that no semantically
    equivalent run exists under some other id, and no field in the result says
    otherwise. Without ``run_id`` Airflow mints the identity and a second call
    creates a second run.

    ``expected_dag_version`` is the Dag version the run was agreed against. When
    given, it is re-read immediately before the trigger and a Dag that has moved
    since is refused rather than run. **A version that still matches pins the
    version number and not the code** — the result says so in ``version_pin``,
    and the converse of the refusal is not something to tell the user.

    Triggering is approved on its own. It redeems no plan token from a source
    change, and an approved source change is not an approval to run.

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
    # The trigger's OWN preconditions, re-established here and nowhere else.
    # Before the pause branch for the same reason the conf check is: neither is
    # a reason to have unpaused a Dag, and an unpause spent on a trigger this
    # then refuses is a lasting change bought for nothing.
    if expected_dag_version is not None:
        drift = _version_precondition(dag_id, expected_dag_version)
        if drift is not None:
            return drift
    if run_id:
        existing = _existing_run_with_this_identity(dag_id, run_id)
        if existing is not None:
            return existing
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
        try:
            transport._api("PATCH", _dag_url(dag_id), json={"is_paused": False})
        except Exception as e:
            return _unpause_did_not_come_back(dag_id, e)
        unpaused = True
    else:
        unpaused = False
    body: dict[str, Any] = {
        "logical_date": None,
        "conf": conf or {},
        "note": note or "Triggered via Airy",
    }
    if run_id:
        # The identity goes out WITH the create, so Airflow's own uniqueness
        # constraint is the last line: two racing calls carrying the same key
        # cannot both land, whatever the pre-check saw.
        body["dag_run_id"] = run_id
    try:
        run = transport._api("POST", _dag_url(dag_id, "/dagRuns"), json=body)
    except Exception as e:
        return _trigger_did_not_come_back(dag_id, run_id, unpaused, e)
    created_id = run["dag_run_id"]
    return {
        "triggered": True,
        "mutation_applied": True,
        "dag_id": dag_id,
        "dag_run_id": created_id,
        "state": run["state"],
        "unpaused": unpaused,
        **({"idempotency": _IDENTITY_SCOPE} if run_id else {"idempotency": _NO_IDENTITY_SUPPLIED}),
        **(
            {"expected_dag_version": expected_dag_version, "version_pin": _VERSION_PIN_SCOPE}
            if expected_dag_version is not None
            else {}
        ),
        # A model that diagnoses right after triggering gets served the *old*
        # failed run by the fallback and reports "it failed again"; the result
        # itself has to say the outcome is not in yet.
        "next_step": (
            f"created run {created_id} in state {run['state']} — its outcome is not known "
            f"yet; check it after it completes (verify_replacement_run, or diagnose_dag with "
            f"dag_run_id={created_id!r}) and never assume success or failure"
        ),
        "ui_updates": [{"kind": "dag_run", "dag_id": dag_id, "dag_run_id": created_id}],
    }


_NO_IDENTITY_SUPPLIED = (
    "No run_id was supplied, so Airflow minted this run's identity and nothing here is idempotent: "
    "calling again with the same arguments creates a SECOND run."
)


def _trigger_did_not_come_back(dag_id: str, run_id: str, unpaused: bool, error: Exception) -> dict[str, Any]:
    """The answer when the create did not come back — refused, or unsettled.

    A 4xx is a refusal the route CHOSE, so nothing was created and the result
    says so. Anything else — a timeout, a connect failure, a 5xx — can be raised
    after the request has already reached Airflow, so whether a run exists is
    genuinely UNKNOWN. Reporting that as "not triggered" was a false negative
    that sent the operator to trigger a second time.
    """
    refused = isinstance(error, httpx.HTTPStatusError) and 400 <= error.response.status_code < 500
    conflict = isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 409
    # The unpause already committed if it happened, and it is a lasting change
    # whatever became of the run.
    aftermath = (
        f" {dag_id} WAS unpaused first and is still unpaused, so it is scheduling again — tell "
        f"the user, and pause it again if that is not what they wanted."
        if unpaused
        else ""
    )
    stale = {"ui_updates": [{"kind": "dag_definition", "dag_id": dag_id}]} if unpaused else {}
    if refused:
        taken = (
            f" Airflow refused the identity {run_id!r} as already taken, so a run under it exists "
            f"and this call created nothing further."
            if conflict and run_id
            else ""
        )
        return {
            "triggered": False,
            # The create was refused, so the only thing that may have been
            # written is the unpause.
            "mutation_applied": unpaused,
            "dag_id": dag_id,
            "unpaused": unpaused,
            **({"already_existed": True} if conflict and run_id else {}),
            "error": (
                f"NO run was created: Airflow refused the trigger "
                f"({_quoted(_explain_error(error), 240)}).{taken}{aftermath}"
            ),
            **stale,
        }
    # ``mutation_applied`` is deliberately absent unless the unpause landed: the
    # create may have landed too, and false would be a claim this cannot make.
    return {
        "triggered": None,
        "mutation_outcome": "unknown",
        "dag_id": dag_id,
        "unpaused": unpaused,
        **({"dag_run_id": run_id} if run_id else {}),
        **({"mutation_applied": True} if unpaused else {}),
        "error": (
            f"whether a run was created is NOT established: the trigger did not come back "
            f"({_quoted(_explain_error(error), 240)}), and a request can fail after Airflow has "
            f"already accepted it. Do NOT report this as triggered and do NOT report it as not "
            f"triggered."
            + (
                f" Retry with run_id={run_id!r} — the same identity — which will find the run if "
                f"it landed instead of creating a second one."
                if run_id
                else " No run_id was supplied, so a retry would create a second run if the first "
                "one landed; check the Dag's runs by hand before triggering again."
            )
            + aftermath
        ),
        **stale,
    }


def plan_backfill(dag_id: str, from_date: str, to_date: str) -> dict[str, Any]:
    """
    Preview the runs a backfill would create, without creating anything.

    Read-only. Show the user every run listed in ``planned_runs``, then pass the
    ``plan_token`` back to run_backfill — that is what proves the backfill you
    create is the one they reviewed.
    """
    try:
        preview_reading = _dry_run_backfill(dag_id, from_date, to_date)
    except httpx.HTTPStatusError as e:
        message = _explain_unknown_dag(dag_id, e)
        if message is None:
            raise
        return {"dag_id": dag_id, "from_date": from_date, "to_date": to_date, "error": message}
    entries = list(preview_reading.rows)
    preview = {
        "dag_id": dag_id,
        "from_date": from_date,
        "to_date": to_date,
        "planned_run_count": len(entries),
        "planned_runs_read_whole": preview_reading.complete,
        # Every run, and both halves of its identity: a partitioned Dag has no
        # logical_date, so a dates-only list would show the user nothing at all.
        "planned_runs": [
            {"logical_date": entry.get("logical_date"), "partition_key": entry.get("partition_key")}
            for entry in entries
        ],
    }
    if not preview_reading.complete:
        # No token. The preview IS what the user is asked to approve, and a
        # preview that did not list every run it would create cannot be shown.
        return {
            **preview,
            "error": (
                f"the backfill preview was not read whole ({preview_reading.reason}), so the runs "
                f"listed here are not all of the runs this backfill would create; nothing is "
                f"proposed until it can be shown in full"
            ),
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
            "error": (
                "no reviewed plan for this backfill; call plan_backfill and show the user the result"
                + eviction_note("tokens")
            ),
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
        preview_reading = _dry_run_backfill(dag_id, from_date, to_date)
    except httpx.HTTPStatusError as e:
        message = _explain_unknown_dag(dag_id, e)
        if message is None:
            return _approval_spent_before_the_write("created", "the backfill preview", e)
        return {"created": False, "mutation_applied": False, "error": message + _APPROVAL_IS_GONE}
    except _READ_FAILURES as e:
        # The token is redeemed above, so a preview that does not come back must
        # not raise past the caller: the retry then answers "no reviewed plan
        # for this backfill", which is false and reads as the user's mistake.
        return _approval_spent_before_the_write("created", "the backfill preview", e)
    if not preview_reading.complete:
        # Pre-mutation, so "not applied" is a fact rather than an inference. The
        # identity compare below is what authorizes the write, and a compare
        # against a list that was cut short authorizes runs nobody reviewed.
        return {
            "created": False,
            "mutation_applied": False,
            "error": (
                f"this backfill was NOT created: the dry run taken immediately before the write was "
                f"not read whole ({preview_reading.reason}), so the runs it would create cannot be "
                f"compared against the ones the user approved. Nothing was created."
            ),
        }
    entries = list(preview_reading.rows)
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
    created_reading = _backfill_runs(resp["id"])
    created = list(created_reading.rows)
    # A slot Airflow could not fill still comes back with the planned identity, and
    # carries no dag_run_id plus a reason. Matching on identity alone would call
    # that a success, so the run has to have actually been created.
    landed = [entry for entry in created if entry.get("dag_run_id") and not entry.get("exception_reason")]
    # Identity, not arity: the same number of runs can still be different runs.
    # A read that did not list every run the backfill holds cannot support either
    # answer — a missing run reads as one that was never created.
    if not created_reading.complete or not _same_runs([_run_identity(entry) for entry in landed], planned):
        return _abandon_backfill(
            resp["id"],
            dag_id=dag_id,
            planned=planned,
            created=created,
            read_whole=created_reading.complete,
        )
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
    backfill_id: int,
    *,
    dag_id: str,
    planned: list[tuple[Any, Any]],
    created: list[dict[str, Any]],
    read_whole: bool = True,
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
    # The reason this list may be short of a survivor, kept OUT of the list: a
    # sentinel row inside it was counted as a run, so a cancel that killed all 51
    # created runs reported "1 run(s) were already past queued and are still
    # going" over a survivor set of exactly zero.
    unread: str | None = None
    try:
        recheck = _backfill_runs(backfill_id)
        survivors = [
            {"dag_run_id": entry.get("dag_run_id"), "state": entry.get("dag_run_state")}
            for entry in recheck.rows
            if entry.get("dag_run_state") not in (None, "failed")
        ]
        if not recheck.complete:
            # An empty or short survivor list is a claim that nothing outlived
            # the cancel, and this read cannot support one.
            unread = f"the backfill's runs were not read whole ({recheck.reason})"
    except Exception:
        unread = "the backfill's runs could not be re-read"
    aftermath = "cancelled" if cancelled else "CANCELLING IT FAILED"
    if not read_whole:
        aftermath += ", and the created runs were not read whole, so what it created is not established"
    if survivors:
        aftermath += f", but {len(survivors)} run(s) were already past queued and are still going"
    if unread:
        aftermath += f", and {unread}, so whether any further run outlived the cancel is NOT established"
    result = {
        "created": False,
        # Two writes went out on this path — the backfill POST and the cancel PUT
        # — and a run the scheduler had already picked up is still running. This
        # field is what a caller reads to decide whether anything changed, and
        # it contradicted the prose beside it, which says in as many words that
        # cancelling is a compensating action and not a rollback.
        "mutation_applied": True,
        "backfill_id": backfill_id,
        "planned_run_count": len(planned),
        "created_run_count": len(created),
        "cancelled": cancelled,
        "surviving_runs": survivors,
        # Three-valued, like everything else drawn over a bounded read: null says
        # the re-read could not settle whether the survivor list is the whole of
        # it, which an empty list on its own would have read as "none".
        "surviving_runs_read_whole": unread is None,
        "error": (
            f"the backfill did not match the {len(planned)} runs the user approved; {aftermath}. "
            f"Tell the user to check backfill {backfill_id}."
        ),
        # The runs exist and some of them may still be running, so the views the
        # user is looking at are stale. Emitting nothing here left them showing a
        # Dag with no backfill while its runs executed.
        "ui_updates": [{"kind": "dag_run", "dag_id": dag_id}],
    }
    if unread:
        result["surviving_runs_unread"] = unread
    return result


# The states a run can still leave. An absence read while any of these is the
# run's state is an absence in a run that has not finished producing evidence,
# which is not an absence at all.
_RUN_STILL_GOING = ("queued", "running", "restarting", "scheduled", None)

# What each answer here IS, said in the payload because that is where a small
# model meets it. The XCom record is the task's own report of its work - the
# closest thing to the external artefact that stays inside the API this server
# speaks - and it is not an observation of the external system. ``false`` is
# defined here too: the field is named for the operation, but what it is
# measured over is the RECORD, and a task can finish successfully having written
# nothing down.
_VERIFY_SCOPE = (
    "`occurred: true` means this run's own record shows the task executed and recorded the "
    "expected output. Nothing here observed the external system directly. `occurred: false` means "
    "this run's own record holds no such output. That is a statement about the RECORD, not about "
    "the world: a task whose own `task_state` is success DID run, so read false beside it as "
    '"it recorded nothing", never as "the work did not happen". `occurred: null` means '
    "UNKNOWN - the run is unfinished, a read did not come back, or the evidence covered less than "
    'the answer needs - and is NEVER to be relayed as "it did not happen".'
)


def _verification(
    occurred: bool | None,
    reason: str,
    *readings: reading.Reading,
    **fields: Any,
) -> dict[str, Any]:
    """One shape for every answer, so the caveats cannot be dropped by a branch.

    Every reading this answer rests on is passed in, and each one that came back
    short says so in its own words. A tool that answers over a list it did not
    read whole has to put that in the payload, not only in the verdict.
    """
    # Always present, empty when nothing was short. A key that only appears
    # under truncation is a claim that arrives with the shortfall, and the
    # reader has no way to tell its absence from a whole read.
    return {
        "occurred": occurred,
        "reason": reason,
        "external_system_checked": False,
        "scope": _VERIFY_SCOPE,
        "unread": [read.reason for read in readings if read is not None and not read.complete],
        **fields,
    }


def verify_replacement_run(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int = -1,
    output_key: str = "",
    xcom_scope: str = "",
) -> dict[str, Any]:
    """
    Say whether ONE named run's named task actually did its work — or that it is unknown.

    Read-only. ``dag_run_id`` must be the exact id of the run you are asking
    about; ``latest`` and ``previous`` are refused, because the whole value of
    this check is that it is about the run you triggered and not about whichever
    run is newest by the time you ask.

    ``occurred`` is THREE-valued and null is not false:

    * ``true`` — the instance recorded its output, so this run's own record
      shows the work happened. Positive evidence, so a read that was cut short
      does not weaken it.
    * ``false`` — the run has finished, every relevant record was read whole,
      and the output is not there. Only ever returned on a complete read. It is
      an absence of the RECORD, not of the work: a task whose own ``task_state``
      is success ran, and relaying false there as "it did not happen" is the
      overclaim the field's name invites.
    * ``null`` — UNKNOWN. The run is still going, a read did not come back or
      timed out, permission for the output records was not granted, or the
      evidence was not read whole. Say "not established", never "it did not
      happen".

    ``output_key`` names the record to look for; without it any output record
    counts. ``xcom_scope`` is set by the caller's permissions, not by you, and
    it degrades rather than gates: without it the answer is ``null``.

    ``external_system_checked`` is always false. A ``true`` here is Airflow's
    own record of the task's output, not a look at the system the task talks to.
    """
    if dag_run_id in ("", "latest", "previous"):
        return _verification(
            None,
            (
                "name the exact dag_run_id of the run you are asking about — this check is only "
                "worth something when it is pinned to that one run, and 'latest' is whichever run "
                "is newest by the time it is asked"
            ),
            dag_id=dag_id,
        )
    try:
        run, error = reading._resolve_run(dag_id, dag_run_id)
    except Exception as e:
        return _verification(
            None,
            f"the run could not be read ({_quoted(_explain_error(e), 240)}), so nothing about it "
            f"is established",
            dag_id=dag_id,
            dag_run_id=dag_run_id,
        )
    if run is None:
        return _verification(None, str(error), dag_id=dag_id, dag_run_id=dag_run_id)
    resolved = run["dag_run_id"]
    run_state = run.get("state")
    finished = run_state not in _RUN_STILL_GOING
    run_path = f"/dagRuns/{quote(resolved, safe='')}"
    common: dict[str, Any] = {
        "dag_id": dag_id,
        "dag_run_id": resolved,
        "task_id": task_id,
        "map_index": map_index,
        "run_state": run_state,
        "run_finished": finished,
    }

    try:
        scan = reading._run_task_instances(dag_id, run_path)
    except Exception as e:
        return _verification(
            None,
            f"the run's task instances could not be read ({_quoted(_explain_error(e), 240)}), so "
            f"whether {task_id} ran in it is not established",
            **common,
        )
    located = reading.find(
        scan,
        lambda ti: _ti_key(ti) == (task_id, map_index),
        f"no instance of {task_id} was among the instances this reading read",
    )
    if located.is_unknown():
        return _verification(
            None,
            f"{scan.kept} of {scan.universe} of the run's task instances were read, so "
            f"{task_id} not being among them is not evidence that it is not there",
            scan,
            evidence_read_whole=False,
            **common,
        )
    if located.is_absent():
        # A complete read of a finished run that holds no such instance IS an
        # absence of the work. A complete read of a run still going is not.
        return _verification(
            # False, not None: a finished run whose instances were all read and
            # which holds no instance of this task is an absence of the work.
            False if finished else None,
            (
                f"the run holds no instance of {task_id} at map_index {map_index}, and every "
                f"instance it holds was read"
            )
            if finished
            else (
                f"the run holds no instance of {task_id} yet, and it is still {run_state} — the "
                f"instance may not have been created"
            ),
            scan,
            evidence_read_whole=True,
            **common,
        )
    ti = next(row for row in scan.rows if _ti_key(row) == (task_id, map_index))
    common["task_state"] = ti.get("state")

    output = reading._recorded_output(dag_id, run_path, ti, xcom_scope)
    if output.read_failed:
        return _verification(
            None,
            f"the instance's output records could not be read ({_quoted(output.error, 200)}), so "
            f"whether it recorded its work is not established",
            scan,
            evidence_read_whole=False,
            **common,
        )
    recorded = reading.find(
        output,
        (lambda row: row.get("key") == output_key) if output_key else (lambda row: True),
        f"no output record{f' keyed {output_key!r}' if output_key else ''} was among the records "
        f"this reading read",
    )
    seen = [row.get("key") for row in output.rows]
    whole = scan.complete and output.complete
    if recorded.is_present():
        # Positive evidence. A short read cannot take it away: the record that
        # was found was found.
        return _verification(
            True,
            f"{task_id} recorded output in this run"
            + (f" keyed {output_key!r}" if output_key else "")
            + f". Records seen: {seen}.",
            scan,
            output,
            evidence_read_whole=whole,
            **common,
        )
    if recorded.is_unknown():
        return _verification(
            None,
            f"{output.kept} of {output.universe} of the instance's output records were read, "
            f"so an absence among them is not an absence. Records seen: {seen}.",
            scan,
            output,
            evidence_read_whole=False,
            **common,
        )
    if not finished:
        return _verification(
            None,
            f"the instance has recorded no matching output yet and the run is still {run_state}, "
            f"so this is not established either way. Records seen: {seen}.",
            scan,
            output,
            evidence_read_whole=whole,
            **common,
        )
    # ``whole`` is asked once more here rather than assumed: the instance list
    # can be short even when the output records are not, and an absence drawn
    # over either is not an absence.
    if not whole:
        return _verification(
            None,
            f"the run has finished and {task_id} recorded no matching output, but the evidence "
            f"was not all read, so this is not established. Records seen: {seen}.",
            scan,
            output,
            evidence_read_whole=False,
            **common,
        )
    # The one place ``occurred: false`` and the task's own state disagree, said
    # in the answer rather than left to the reader: the field is named for the
    # operation, and a model handed a bare false beside a task that succeeded
    # will relay it as work that did not happen.
    ran = common.get("task_state") == "success"
    return _verification(
        False,
        f"the run has finished, every output record was read, and {task_id} recorded no "
        f"output"
        + (f" keyed {output_key!r}" if output_key else "")
        + f". Records seen: {seen}."
        + (
            f" {task_id} itself finished in state 'success', so it DID run: what is absent is the "
            f"output RECORD, not necessarily the work. Say it recorded nothing, not that it did "
            f"not happen."
            if ran
            else ""
        ),
        scan,
        output,
        evidence_read_whole=True,
        **common,
    )
