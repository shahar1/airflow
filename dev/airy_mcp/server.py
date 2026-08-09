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

Write-capable tools on top of the Airflow REST API, scoped to ONE workflow:
``diagnose_dag`` finds what is wrong with a named run,
``plan_dag_code_changes``/``apply_dag_code_changes`` repair that Dag's source as
one atomic change, ``plan_revert_dag_code``/``revert_dag_code`` put it back, and
``rerun_dag`` triggers a specifically identified replacement run whose external
result ``verify_replacement_run`` then reads back.  Deliberately goes beyond
AIP-91 phase 1 (read-only) to show where the value ends up.

What this server is certified to do is exactly that workflow and nothing wider:
diagnose a green run whose expected work never executed, propose an exact source
correction, obtain explicit human approval, apply it safely, trigger a
specifically identified replacement run, and check what that run actually
recorded.  Nothing here observes the external system: ``occurred: true`` is the
presence of an output record in Airflow, and ``external_system_checked`` is
always false, so the claim stops at the record.  Broad automatic discovery and
clearing of arbitrary historical task instances is OUT OF SCOPE and
uncertified; the tools that did it
(``plan_task_instance_clear``, ``apply_task_instance_clear``,
``verify_task_instance_recovery``, ``plan_backfill``, ``run_backfill``) are
WITHDRAWN from the registered surface below and are not reachable from the demo.
Their implementations and their tests are kept in the tree deliberately, as the
evidence of what was built and of why it is not exposed.

Each of the two mutations a user asks for is approved on its own, and one
approval never authorizes the other.  A source write is planned first: the
planning tool is read-only and hands back a single-use token of ITS OWN kind,
and the writing tool refuses without it.  Triggering redeems no source token at
all - it is approved by its own tool call, whose arguments describe it
completely - and it re-establishes its own preconditions immediately before it
acts.

Three writes are outside the plan-first rule, and ``approvals._UNGATED_WRITES``
is the authority on which - this sentence is not.  One is asked for:
``rerun_dag`` only ADDS a run and the call's own arguments describe it
completely (the lasting part of it, unpausing, has a token and a warning of its
own).  The others are steps INSIDE a write the user already approved - the
atomic replace itself, and re-parsing a file that was just written - and neither
is a change the user could have been asked about separately, because neither
exists until the approved write has already happened.

Runs as a second MCP sidecar next to the read-only ``astro-airflow-mcp``.

Env:
    AIRFLOW_API_URL      Airflow API base (default http://localhost:8080)
    AIRFLOW_USERNAME     simple-auth-manager user (default admin)
    AIRFLOW_PASSWORD     simple-auth-manager password (default admin)
    AIRY_MCP_DAGS_DIR    bundle root; also the write jail (default /files/dags)
"""

from __future__ import annotations

import argparse
import os  # noqa: F401 - re-exported for ``server.os``, which the suite patches ``replace`` on

# Wave 3 of the move-only extraction: the plan tokens, the approved-instance-set
# baselines and the run-identity rule live in ``approvals.py`` now.  The functions and
# the two stores are re-exported here so every ``server.X`` reference keeps resolving -
# the stores are mutated in place, never rebound, so this is the one dict object.  The
# three bounds (``_TOKEN_TTL_S``, ``_TOKEN_MAX``, ``_APPROVED_SET_MAX``) are NOT
# re-exported: the suite rebinds the TTL, and a rebound number is only ever seen by the
# module that reads it.
from approvals import (
    _approved_clear_sets,  # noqa: F401 - re-exported for ``server._approved_clear_sets``
    _approved_set_record,  # noqa: F401 - re-exported for ``server._approved_set_record``
    _issue_token,  # noqa: F401 - re-exported for ``server._issue_token``
    _issued_tokens,  # noqa: F401 - re-exported for ``server._issued_tokens``
    _peek_token,  # noqa: F401 - re-exported for ``server._peek_token``
    _record_approved_set,  # noqa: F401 - re-exported for ``server._record_approved_set``
    _redeem_token,  # noqa: F401 - re-exported for ``server._redeem_token``
)

# Wave 8 of the move-only extraction: the changes that are not a clear - repairing a
# Dag's source, reverting it, starting a fresh run, and creating a backfill - live in
# ``codechange.py`` now.  Nothing here is rebound by the suite, so a re-export is the
# same object the call sites use; the one bound those tools read, ``MAX_BACKFILL_RUNS``,
# lives in ``reading`` and is reached through that module object, so it stays patchable
# in exactly one place.
from codechange import (
    _JSON_TYPE_CHECKS,  # noqa: F401 - re-exported for ``server._JSON_TYPE_CHECKS``
    _REPLAN_STEER,  # noqa: F401 - re-exported for ``server._REPLAN_STEER``
    _abandon_backfill,  # noqa: F401 - re-exported for ``server._abandon_backfill``
    _describe_bounds,  # noqa: F401 - re-exported for ``server._describe_bounds``
    _describe_params,  # noqa: F401 - re-exported for ``server._describe_params``
    _validate_conf,  # noqa: F401 - re-exported for ``server._validate_conf``
    apply_dag_code_changes,
    plan_backfill,  # noqa: F401 - WITHDRAWN from the registered surface; kept as ``server.plan_backfill``
    plan_dag_code_changes,
    plan_revert_dag_code,
    rerun_dag,
    revert_dag_code,
    run_backfill,  # noqa: F401 - WITHDRAWN from the registered surface; kept as ``server.run_backfill``
    verify_replacement_run,
)

# Wave 5 of the move-only extraction: the Dag file - where it is, the jail around
# it, the lock, the atomic replace, the reparse, and the scans over the text it
# holds - lives in ``dagsource.py`` now.  ``DAGS_DIR``, ``REPARSE_TIMEOUT_S`` and
# ``_read_reviewed_file`` are rebound by the suite, so they are deliberately not
# re-exported here; the rest are, where a re-export is the same object because
# nothing rebinds them.
from dagsource import (
    STATIC_CHECK_LIMIT,  # noqa: F401 - re-exported for ``server.STATIC_CHECK_LIMIT``
    DagFileError,  # noqa: F401 - re-exported for ``server.DagFileError``
    _dag_path,  # noqa: F401 - re-exported for ``server._dag_path``
    _display_order,  # noqa: F401 - re-exported for ``server._display_order``
    _resolve_task,  # noqa: F401 - re-exported for ``server._resolve_task``
    _write_if_unchanged,  # noqa: F401 - re-exported for ``server._write_if_unchanged``
)

# Wave 7 of the move-only extraction: what is wrong with a run, and the prose that
# says so - the four read-only tools and the narrative layer they share - lives in
# ``diagnosis.py`` now.  ``DIAGNOSIS_LOG_BUDGET_CHARS``,
# ``DIAGNOSIS_SUMMARY_BUDGET_CHARS``, ``DISPATCH_CONTRAST_RUN_LIMIT`` and
# ``DISPATCH_IMPACT_TASK_LIMIT`` are rebound by the suite and are read only inside
# that module, so they are deliberately not re-exported here; the rest are, where a
# re-export is the same object because nothing ever rebinds them.
from diagnosis import (
    _CHECK_LABELS,  # noqa: F401 - re-exported for ``server._CHECK_LABELS``
    _FOLD_KINDS,  # noqa: F401 - re-exported for ``server._FOLD_KINDS``
    _RUN_HISTORY_KEYS,  # noqa: F401 - re-exported for ``server._RUN_HISTORY_KEYS``
    _augment_dispatch_findings,  # noqa: F401 - re-exported for ``server._augment_dispatch_findings``
    _build_diagnosis_summary,  # noqa: F401 - re-exported for ``server._build_diagnosis_summary``
    _census_clause,  # noqa: F401 - re-exported for ``server._census_clause``
    _compared_rows_by_run,  # noqa: F401 - re-exported for ``server._compared_rows_by_run``
    _comparison_task_ids,  # noqa: F401 - re-exported for ``server._comparison_task_ids``
    _contrast_clause,  # noqa: F401 - re-exported for ``server._contrast_clause``
    _coverage_clauses,  # noqa: F401 - re-exported for ``server._coverage_clauses``
    _error_signature,  # noqa: F401 - re-exported for ``server._error_signature``
    _extract_error_line,  # noqa: F401 - re-exported for ``server._extract_error_line``
    _impact_clause,  # noqa: F401 - re-exported for ``server._impact_clause``
    _recurrence_clause,  # noqa: F401 - re-exported for ``server._recurrence_clause``
    _run_history,  # noqa: F401 - re-exported for ``server._run_history``
    _summarize_failure,  # noqa: F401 - re-exported for ``server._summarize_failure``
    compare_dag_runs,
    diagnose_dag,
    find_failure_clusters,
    get_blast_radius,
)

# Wave 6 of the move-only extraction: what the event log and the dispatch fields
# establish about an attempt - and what they cannot - lives in ``evidence.py`` now,
# together with the legend of caveats it ships as data.  ``TASK_INSTANCE_DETAIL_LIMIT``,
# ``TRIES_PROBE_LIMIT`` and ``DISPATCH_FINDING_LIMIT`` are rebound by the suite and are
# read only inside that module, so they are deliberately not re-exported here; the rest
# are, where a re-export is the same object because nothing ever rebinds them.
from evidence import (
    _AUDITED_TI_STATE_ACTIONS,  # noqa: F401 - re-exported for ``server._AUDITED_TI_STATE_ACTIONS``
    _DISPATCH_EVIDENCE_KEYS,  # noqa: F401 - re-exported for ``server._DISPATCH_EVIDENCE_KEYS``
    _L1,  # noqa: F401 - re-exported for ``server._L1``
    _L2,  # noqa: F401 - re-exported for ``server._L2``
    _L3,  # noqa: F401 - re-exported for ``server._L3``
    _L4,  # noqa: F401 - re-exported for ``server._L4``
    _L5,  # noqa: F401 - re-exported for ``server._L5``
    _L6,  # noqa: F401 - re-exported for ``server._L6``
    _L7,  # noqa: F401 - re-exported for ``server._L7``
    _NOT_ESTABLISHED,  # noqa: F401 - re-exported for ``server._NOT_ESTABLISHED``
    _R1,  # noqa: F401 - re-exported for ``server._R1``
    _R2,  # noqa: F401 - re-exported for ``server._R2``
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
    _classify_event,  # noqa: F401 - re-exported for ``server._classify_event``
    _event_history,  # noqa: F401 - re-exported for ``server._event_history``
    _is_never_dispatched_attempt,  # noqa: F401 - re-exported for ``server._is_never_dispatched_attempt``
    _is_request_settable_targeting,  # noqa: F401 - re-exported for ``server._is_request_settable_targeting``
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
    _carries_execution_fields,  # noqa: F401 - re-exported for ``server._carries_execution_fields``
    _check,  # noqa: F401 - re-exported for ``server._check``
    _later_than,  # noqa: F401 - re-exported for ``server._later_than``
    _now_iso,  # noqa: F401 - re-exported for ``server._now_iso``
    _quoted,  # noqa: F401 - re-exported for ``server._quoted``
    _tagged_log,  # noqa: F401 - re-exported for ``server._tagged_log``
    _ti_key,  # noqa: F401 - re-exported for ``server._ti_key``
    _ti_where,  # noqa: F401 - re-exported for ``server._ti_where``
)

# Wave 4 of the move-only extraction: every bounded read, and every scan, page and
# clamp bound that bounds one, lives in ``reading.py`` now.  The bounds the suite
# rebinds - ``TASK_INSTANCE_PAGE``, ``TASK_INSTANCE_SCAN_LIMIT``, ``EVENT_SCAN_PAGE``,
# ``EVENT_SCAN_LIMIT``, ``FAILURE_SCAN_LIMIT``, ``TASK_COMPARISON_LIMIT``,
# ``MAX_BACKFILL_RUNS`` - and the three reads it rebinds are deliberately not
# re-exported here; the rest are, where a re-export is the same object because nothing
# ever rebinds them.
from reading import (
    _HISTORY_CLAMPED,  # noqa: F401 - re-exported for ``server._HISTORY_CLAMPED``
    LOG_TAIL_CHARS,  # noqa: F401 - re-exported for ``server.LOG_TAIL_CHARS``
    RUN_HISTORY_LIMIT,  # noqa: F401 - re-exported for ``server.RUN_HISTORY_LIMIT``
    _attempt_history,  # noqa: F401 - re-exported for ``server._attempt_history``
    _attempt_log,  # noqa: F401 - re-exported for ``server._attempt_log``
    _attempt_reading,  # noqa: F401 - re-exported for ``server._attempt_reading``
    _attempt_rows,  # noqa: F401 - re-exported for ``server._attempt_rows``
    _audit_transitions,  # noqa: F401 - re-exported for ``server._audit_transitions``
    _duration_baseline,  # noqa: F401 - re-exported for ``server._duration_baseline``
    _recorded_output,  # noqa: F401 - re-exported for ``server._recorded_output``
    _resolve_run,  # noqa: F401 - re-exported for ``server._resolve_run``
    _tail,  # noqa: F401 - re-exported for ``server._tail``
    _tasks_reading,  # noqa: F401 - re-exported for ``server._tasks_reading``
    _version_context,  # noqa: F401 - re-exported for ``server._version_context``
)

# Wave 9 of the move-only extraction, and the last: the plan -> containment gate ->
# apply -> verify pipeline for a task-instance clear lives in ``recovery.py`` now.
# ``_mapped_in_closure`` is rebound by the suite and is read only inside that module,
# so it is deliberately not re-exported here; the rest are, where a re-export is the
# same object because nothing ever rebinds them.
from recovery import (
    _APPROVED_SET_NOT_ESTABLISHED,  # noqa: F401 - re-exported for ``server._APPROVED_SET_NOT_ESTABLISHED``
    _DOWNSTREAM_DATING,  # noqa: F401 - re-exported for ``server._DOWNSTREAM_DATING``
    _IN_REQUEST_CREATION_NOT_ESTABLISHED,  # noqa: F401 - re-exported for ``server._IN_REQUEST_CREATION_NOT_ESTABLISHED``
    _MAPPED_CREATION_NOT_ESTABLISHED,  # noqa: F401 - re-exported for ``server._MAPPED_CREATION_NOT_ESTABLISHED``
    _NO_OUTPUT_AT_ALL,  # noqa: F401 - re-exported for ``server._NO_OUTPUT_AT_ALL``
    _NOTHING_CLEARED,  # noqa: F401 - re-exported for ``server._NOTHING_CLEARED``
    _PARTIAL_UNSETTLED_BY_HISTORY,  # noqa: F401 - re-exported for ``server._PARTIAL_UNSETTLED_BY_HISTORY``
    _STALE_ARTEFACT,  # noqa: F401 - re-exported for ``server._STALE_ARTEFACT``
    _clear_flags,  # noqa: F401 - re-exported for ``server._clear_flags``
    _recovery_evidence,  # noqa: F401 - re-exported for ``server._recovery_evidence``
    apply_task_instance_clear,  # noqa: F401 - WITHDRAWN; kept as ``server.apply_task_instance_clear``
    plan_task_instance_clear,  # noqa: F401 - WITHDRAWN; kept as ``server.plan_task_instance_clear``
    verify_task_instance_recovery,  # noqa: F401 - WITHDRAWN; kept as ``server.verify_task_instance_recovery``
)

# The three transport helpers the suite only ever reads, never patches, so a re-export
# here is the same object the call sites use.
from transport import (
    _api_detail,  # noqa: F401 - re-exported for ``server._api_detail``
    _dag_url,  # noqa: F401 - re-exported for ``server._dag_url``
    _explain_error,  # noqa: F401 - re-exported for ``server._explain_error``
)

mcp: FastMCP = FastMCP("airy-selfheal")


# Registered here rather than with @mcp.tool so the module keeps exporting plain
# functions — directly callable from tests.
#
# This tuple IS the demo-visible surface: a tool absent from it is never handed
# to FastMCP, so no MCP client is ever told it exists and no model can call it.
# Five tools that used to sit here — ``plan_backfill``, ``run_backfill``,
# ``plan_task_instance_clear``, ``apply_task_instance_clear`` and
# ``verify_task_instance_recovery`` — were withdrawn because they are the broad
# recovery surface this server no longer claims to have got right. They are
# still imported above, so their code and their tests stay exercisable in the
# tree; what changed is that nothing exposes them.
for _tool in (
    diagnose_dag,
    compare_dag_runs,
    find_failure_clusters,
    get_blast_radius,
    plan_dag_code_changes,
    apply_dag_code_changes,
    plan_revert_dag_code,
    revert_dag_code,
    rerun_dag,
    verify_replacement_run,
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
