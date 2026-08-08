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
Changes that are not a clear.

The seven tools that repair a Dag's source, revert it, start a fresh run, or
create a backfill - together with the conf validation that stands in front of a
trigger.  Five of them write; every one of the five is reached only through a
plan that was shown to the user first, and refuses without the single-use token
that plan handed back.

``plan_dag_code_changes``/``apply_dag_code_changes`` are one atomic set of edits
against the exact bytes the plan was made from.  ``plan_revert_dag_code``/
``revert_dag_code`` restore the original Airy backed up, discarding every change
Airy applied.  ``rerun_dag`` starts a fresh run on the latest code, validating
``conf`` against the Dag's own params schema and refusing to unpause without a
second, separately-warned token.  ``plan_backfill``/``run_backfill`` create a
range of runs, re-checking at execution time that the range is still the one the
user approved and cancelling what landed if it is not.

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
            raise
        return {"created": False, "mutation_applied": False, "error": message}
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
            resp["id"], planned=planned, created=created, read_whole=created_reading.complete
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
        "mutation_applied": False,
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
    }
    if unread:
        result["surviving_runs_unread"] = unread
    return result
