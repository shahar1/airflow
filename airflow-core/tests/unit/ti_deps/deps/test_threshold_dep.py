#
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
from __future__ import annotations

from unittest import mock

import pytest
from sqlalchemy import select

from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.log import Log
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Threshold
from airflow.sdk.definitions.threshold import Atom
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils.state import TaskInstanceState as S

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

THRESHOLD_VERSION = {("core", "serialized_dag_max_write_version"): "5"}
DONE_ALL = Atom(frozenset({"success", "skipped", "failed", "upstream_failed", "removed"}), "==", "all", None)
NONE_FAILED_MIN_ONE_SUCCESS = Threshold(
    (
        Atom(frozenset({"failed", "upstream_failed"}), "==", 0, "upstream_failed"),
        Atom(frozenset({"success"}), ">=", 1, "upstream_failed"),
        DONE_ALL,
    )
)
ONE_FAILED = Threshold((Atom(frozenset({"failed", "upstream_failed"}), ">=", 1, "skipped"),))
ALL_SUCCESS = Threshold(
    (
        Atom(frozenset({"failed", "upstream_failed"}), "==", 0, "upstream_failed"),
        Atom(frozenset({"skipped"}), "==", 0, "skipped"),
        DONE_ALL,
    ),
    shrink_route=True,
)
UNMET_PREFIX = (
    "Task's trigger rule 'threshold/2:failed+upstream_failed==0->upstream_failed;success>=1->upstream_failed;"
    "failed+removed+skipped+success+upstream_failed==all->none:shrink_route=false' is not met: "
)


def _identity(x):
    return x


def _make(dag_maker, session, upstream_count, **consumers):
    with dag_maker("threshold_dep", serialized=True, session=session):
        upstreams = [EmptyOperator(task_id=f"u{i}") for i in range(upstream_count)]
        for task_id, rule in consumers.items():
            upstreams >> EmptyOperator(task_id=task_id, trigger_rule=rule)
    return dag_maker.create_dagrun()


def _set_upstream_states(dr, session, states):
    tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
    for i, state in enumerate(states):
        tis[f"u{i}"].state = state
    session.flush()
    return tis


def _evaluate(dag_maker, ti, session, **context):
    ti.state = None
    ti.refresh_from_task(dag_maker.serialized_dag.get_task(ti.task_id))
    statuses = list(
        TriggerRuleDep()._get_dep_statuses(
            ti, dep_context=DepContext(flag_upstream_failed=True, **context), session=session
        )
    )
    return [s for s in statuses if not s.passed], ti.state


@conf_vars(THRESHOLD_VERSION)
def test_undecided_threshold_reason(dag_maker, session):
    dr = _make(dag_maker, session, 3, c=NONE_FAILED_MIN_ONE_SUCCESS)
    tis = _set_upstream_states(dr, session, [S.SUCCESS, None, None])

    [failing], state = _evaluate(dag_maker, tis["c"], session)

    prefix, _, rest = failing.reason.partition("upstream_states=")
    assert prefix == (
        UNMET_PREFIX + "failed+upstream_failed==0: 0 now, 2 pending; "
        "failed+removed+skipped+success+upstream_failed==all: 1 now, 2 pending; "
    )
    assert rest.startswith("_UpstreamTIStates(")
    assert "upstream_task_ids=" in rest
    assert state is None


@conf_vars(THRESHOLD_VERSION)
def test_failed_atom_routes_upstream_failed(dag_maker, session):
    dr = _make(dag_maker, session, 3, c=NONE_FAILED_MIN_ONE_SUCCESS)
    tis = _set_upstream_states(dr, session, [S.FAILED, S.SUCCESS, S.SUCCESS])

    [failing], state = _evaluate(dag_maker, tis["c"], session)

    assert failing.reason.startswith(UNMET_PREFIX + "failed+upstream_failed==0: 1 now; ")
    assert "pending" not in failing.reason.partition("upstream_states=")[0]
    assert state == S.UPSTREAM_FAILED


@pytest.mark.parametrize(
    ("threshold", "states"),
    [
        pytest.param(NONE_FAILED_MIN_ONE_SUCCESS, [S.SUCCESS, S.SKIPPED, S.SUCCESS], id="met"),
        pytest.param(Threshold(()), [None, None, None], id="no-atoms"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_passing_threshold_has_no_failing_status(dag_maker, session, threshold, states):
    dr = _make(dag_maker, session, 3, c=threshold)
    tis = _set_upstream_states(dr, session, states)

    failing, state = _evaluate(dag_maker, tis["c"], session)

    assert failing == []
    assert state is None


@conf_vars(THRESHOLD_VERSION)
def test_enum_reason_text_is_unchanged(dag_maker, session):
    dr = _make(dag_maker, session, 3, c="all_success")
    tis = _set_upstream_states(dr, session, [S.FAILED, S.SUCCESS, S.SUCCESS])

    [failing], _ = _evaluate(dag_maker, tis["c"], session)

    assert failing.reason.startswith(
        "Task's trigger rule 'all_success' requires all upstream tasks to have succeeded, "
        "but found 1 non-success(es). upstream_states="
    )


@mock.patch("airflow.ti_deps.deps.trigger_rule_dep.is_past_depends_met", autospec=True)
@pytest.mark.parametrize(
    ("past_depends_met", "expected_state", "reason_prefix"),
    [
        pytest.param(
            False, None, "Task should be skipped but the past depends are not met", id="past-depends-not-met"
        ),
        pytest.param(True, S.SKIPPED, "Task's trigger rule 'threshold/", id="past-depends-met"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_wait_for_past_depends_before_skipping(
    mock_past_depends, dag_maker, session, past_depends_met, expected_state, reason_prefix
):
    mock_past_depends.return_value = past_depends_met
    dr = _make(dag_maker, session, 3, c=ONE_FAILED)
    tis = _set_upstream_states(dr, session, [S.SUCCESS] * 3)

    failing, state = _evaluate(dag_maker, tis["c"], session, wait_for_past_depends_before_skipping=True)

    assert state == expected_state
    [status] = failing
    assert status.reason.startswith(reason_prefix)


@conf_vars(THRESHOLD_VERSION)
def test_threshold_routing_without_fold_writes_no_audit_row(dag_maker, session):
    # The audit records fold decisions only: trigger_rule_dep appends to threshold_routed under `fold`.
    dr = _make(dag_maker, session, 3, c=NONE_FAILED_MIN_ONE_SUCCESS)
    _set_upstream_states(dr, session, [S.FAILED, S.SUCCESS, S.SUCCESS])

    SchedulerJobRunner(job=Job())._schedule_dag_run(dr, session)

    ti = next(ti for ti in dr.get_task_instances(session=session) if ti.task_id == "c")
    assert ti.state == S.UPSTREAM_FAILED
    assert session.scalars(select(Log).where(Log.event == "success_threshold_decision")).all() == []


@conf_vars(THRESHOLD_VERSION)
def test_shrink_route_matches_all_success_preset(dag_maker, session):
    with dag_maker("threshold_shrink", serialized=True, session=session):
        upstream = PythonOperator.partial(task_id="u", python_callable=_identity).expand(
            op_args=[[1], [2], [3]]
        )
        for prefix, rule in (("p", "all_success"), ("t", ALL_SUCCESS)):
            upstream >> PythonOperator.partial(
                task_id=f"{prefix}c", python_callable=_identity, trigger_rule=rule
            ).expand(op_args=[[1], [2], [3]])
    dr = dag_maker.create_dagrun()
    tis = {(ti.task_id, ti.map_index): ti for ti in dr.get_task_instances(session=session)}
    for i, state in enumerate([S.SUCCESS, S.SUCCESS, S.REMOVED]):
        tis["u", i].state = state
    session.flush()

    results = {
        prefix: [_evaluate(dag_maker, tis[f"{prefix}c", i], session) for i in range(3)] for prefix in "pt"
    }

    assert [state for _, state in results["p"]] == [None, None, S.REMOVED]
    assert [state for _, state in results["t"]] == [state for _, state in results["p"]]
    assert [bool(failing) for failing, _ in results["t"]] == [bool(failing) for failing, _ in results["p"]]


@conf_vars(THRESHOLD_VERSION)
def test_atom_less_threshold_matches_always_after_a_failed_setup(dag_maker, session):
    with dag_maker("threshold_setup", serialized=True, session=session):
        s = EmptyOperator(task_id="s").as_setup()
        w = EmptyOperator(task_id="w")
        s >> w >> EmptyOperator(task_id="c", trigger_rule=Threshold(()))
        w >> EmptyOperator(task_id="a", trigger_rule="always")
    dr = dag_maker.create_dagrun()
    tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
    tis["s"].state = S.FAILED
    session.flush()

    results = {task_id: _evaluate(dag_maker, tis[task_id], session) for task_id in ("c", "a")}

    assert results["c"] == results["a"] == ([], None)
    statuses = list(
        TriggerRuleDep()._get_dep_statuses(
            tis["c"], dep_context=DepContext(flag_upstream_failed=True), session=session
        )
    )
    assert [s.reason for s in statuses if s.passed] == ["The task's Threshold trigger rule has no atoms."]
