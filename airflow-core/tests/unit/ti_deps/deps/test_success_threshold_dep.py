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
from sqlalchemy import delete

from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task, task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.task.trigger_rule import TriggerRule as TR
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

S = TaskInstanceState.SUCCESS
F = TaskInstanceState.FAILED
UF = TaskInstanceState.UPSTREAM_FAILED
SK = TaskInstanceState.SKIPPED
RM = TaskInstanceState.REMOVED

RUN = (True, None)
WAIT = (False, None)
TO_UF = (False, UF)
TO_SK = (False, SK)

# A tuple vector leaves T unexpanded (expanding over an upstream's output) with its placeholder in that state.
NOT_DONE = [S, S, S, None]
SUCCESS = [S, S, S, F]
FAILED = [S, S, F, F]
UPSTREAM_FAILED = (UF,)
SKIPPED = [SK] * 4
REMOVED = [RM] * 4

THRESHOLD_VERSION = {("core", "serialized_dag_max_write_version"): "4"}


def _identity(x):
    return x


def _mapped_t(states, threshold, task_id="t"):
    if isinstance(states, tuple):
        source = EmptyOperator(task_id=f"{task_id}_source").output
    else:
        source = [[i] for i in range(len(states))]
    t = PythonOperator.partial(task_id=task_id, python_callable=_identity).expand(op_args=source)
    if threshold is not None:
        t.with_success_threshold(threshold)
    return t


def _set_states(dr, session, task_id, states):
    tis = sorted(
        (ti for ti in dr.get_task_instances(session=session) if ti.task_id == task_id),
        key=lambda ti: ti.map_index,
    )
    for ti, state in zip(tis, states, strict=False):
        ti.state = state
    session.flush()


def _evaluate(dag_maker, dr, session, task_id, map_index=-1, **context):
    ti = next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == task_id and ti.map_index == map_index
    )
    ti.refresh_from_task(dag_maker.serialized_dag.get_task(task_id))
    statuses = list(
        TriggerRuleDep().get_dep_statuses(
            ti, session=session, dep_context=DepContext(flag_upstream_failed=True, **context)
        )
    )
    return all(s.passed for s in statuses), ti.state, [s.reason for s in statuses if not s.passed]


def _consumer(rule):
    if rule == TR.ALL_DONE_SETUP_SUCCESS:
        return EmptyOperator(task_id="c").as_teardown()
    return EmptyOperator(task_id="c", trigger_rule=rule)


COLUMNS = (NOT_DONE, SUCCESS, FAILED, UPSTREAM_FAILED, SKIPPED, REMOVED)
COLUMN_IDS = ("not_done", "success", "failed", "upstream_failed", "skipped", "removed")
MATRIX = {
    TR.ALL_SUCCESS: (WAIT, RUN, TO_UF, TO_UF, TO_SK, RUN),
    TR.ALL_FAILED: (WAIT, TO_SK, RUN, RUN, TO_SK, RUN),
    TR.ALL_DONE: (WAIT, RUN, RUN, RUN, RUN, RUN),
    TR.ALL_DONE_MIN_ONE_SUCCESS: (WAIT, RUN, TO_UF, TO_UF, TO_SK, TO_UF),
    TR.ONE_SUCCESS: (WAIT, RUN, TO_UF, TO_UF, TO_SK, TO_UF),
    TR.ONE_FAILED: (WAIT, TO_SK, RUN, RUN, TO_SK, TO_SK),
    TR.ONE_DONE: (WAIT, RUN, RUN, TO_SK, TO_SK, TO_SK),
    TR.NONE_FAILED: (WAIT, RUN, TO_UF, TO_UF, RUN, RUN),
    TR.NONE_FAILED_MIN_ONE_SUCCESS: (WAIT, RUN, TO_UF, TO_UF, TO_SK, TO_UF),
    TR.NONE_SKIPPED: (WAIT, RUN, RUN, RUN, TO_SK, RUN),
    TR.ALL_SKIPPED: (WAIT, TO_SK, TO_SK, TO_SK, RUN, WAIT),
    TR.ALWAYS: (RUN, RUN, RUN, RUN, RUN, RUN),
    TR.ALL_DONE_SETUP_SUCCESS: (WAIT, RUN, RUN, RUN, RUN, RUN),
}


@pytest.mark.parametrize(
    ("rule", "t_states", "expected"),
    [
        pytest.param(rule, states, outcomes[i], id=f"{rule.value}-{COLUMN_IDS[i]}")
        for rule, outcomes in MATRIX.items()
        for i, states in enumerate(COLUMNS)
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_consumer_matrix(dag_maker, session, rule, t_states, expected):
    with dag_maker("matrix", serialized=True, session=session):
        _mapped_t(t_states, 3) >> _consumer(rule)
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", t_states)

    passed, state, reasons = _evaluate(dag_maker, dr, session, "c")

    assert (passed, state) == expected
    if t_states is NOT_DONE and expected == WAIT:
        assert reasons[0] == (
            "Task's upstream 't' has a success threshold and is not done: "
            "success=3, live=4, skipped=0, pending=1, need=3"
        )


@pytest.mark.parametrize(
    ("t_states", "threshold", "expected"),
    [
        pytest.param([S] * 36 + [F] * 3 + [SK], 35, RUN, id="36s-3f-1sk"),
        pytest.param([S] * 30 + [RM] * 10, "90%", RUN, id="30s-10rm-pct"),
        pytest.param([S] * 30 + [RM] * 10, 35, TO_UF, id="30s-10rm-row8"),
        pytest.param([S] * 20, 35, TO_UF, id="20s-of-20-row8"),
        pytest.param([S] * 7 + [F] * 93, "7%", RUN, id="7pct-of-100"),
        pytest.param([S] * 2 + [SK] * 38, "90%", RUN, id="2s-38sk-pct"),
        pytest.param([S] * 30 + [SK] * 4 + [None] * 6, 35, WAIT, id="not-done"),
        pytest.param([SK] * 40, 35, TO_SK, id="all-skipped-row3"),
        pytest.param([SK] * 20 + [RM] * 20, 35, TO_SK, id="skipped-and-removed-row3"),
        pytest.param([RM] * 40, 35, RUN, id="all-removed-row3"),
        pytest.param((UF,), 35, TO_UF, id="placeholder-upstream-failed-row5"),
        pytest.param((None,), 35, WAIT, id="placeholder-unfinished"),
        pytest.param([], 35, TO_SK, id="zero-length-row3"),
        pytest.param([S] * 20 + [F] * 15 + [SK] * 5, 35, TO_UF, id="row6-before-row7"),
        pytest.param([S] * 30 + [SK] * 10, 35, TO_SK, id="row7"),
        pytest.param([S] * 30 + [UF] * 10, 35, TO_UF, id="upstream-failed-with-success-row6"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_worked_vectors(dag_maker, session, t_states, threshold, expected):
    with dag_maker("vectors", serialized=True, session=session):
        _mapped_t(t_states, threshold) >> EmptyOperator(task_id="c")
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", t_states)

    passed, state, _ = _evaluate(dag_maker, dr, session, "c")

    assert (passed, state) == expected


@conf_vars(THRESHOLD_VERSION)
def test_upstream_without_instances_contributes_nothing(dag_maker, session):
    with dag_maker("absent", serialized=True, session=session):
        _mapped_t([S] * 4, 3) >> EmptyOperator(task_id="c")
    dr = dag_maker.create_dagrun()
    session.execute(delete(TaskInstance).where(TaskInstance.run_id == dr.run_id, TaskInstance.task_id == "t"))

    assert _evaluate(dag_maker, dr, session, "c")[:2] == RUN


@conf_vars(THRESHOLD_VERSION)
def test_waiting_reason_precedes_the_rule_status(dag_maker, session):
    with dag_maker("reason", serialized=True, session=session):
        _mapped_t([S] * 30 + [None] * 10, 35) >> EmptyOperator(task_id="c")
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", [S] * 30 + [None] * 10)

    passed, state, reasons = _evaluate(dag_maker, dr, session, "c")

    assert (passed, state) == WAIT
    assert reasons[0] == (
        "Task's upstream 't' has a success threshold and is not done: "
        "success=30, live=40, skipped=0, pending=10, need=35"
    )
    assert reasons[1].startswith("Task's trigger rule 'all_success' requires all upstream tasks")
    assert len(reasons) == 2


@pytest.mark.parametrize(
    ("t_states", "without_threshold", "with_threshold"),
    [
        pytest.param(SUCCESS, TO_UF, RUN, id="3s-1f"),
        pytest.param(NOT_DONE, WAIT, WAIT, id="3s-1pending"),
        pytest.param([S, S, S, SK], TO_SK, RUN, id="3s-1sk"),
        pytest.param([S, S, None, SK], TO_SK, WAIT, id="skip-held-while-undecided"),
        pytest.param(FAILED, TO_UF, TO_UF, id="2s-2f"),
    ],
)
def test_threshold_changes_only_thresholded_outcome(
    dag_maker, session, t_states, without_threshold, with_threshold
):
    with dag_maker("plain", serialized=True, session=session):
        _mapped_t(t_states, None) >> EmptyOperator(task_id="c")
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", t_states)
    assert _evaluate(dag_maker, dr, session, "c")[:2] == without_threshold

    with conf_vars(THRESHOLD_VERSION):
        with dag_maker("thresholded", serialized=True, session=session):
            _mapped_t(t_states, 3) >> EmptyOperator(task_id="c")
        dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", t_states)
    assert _evaluate(dag_maker, dr, session, "c")[:2] == with_threshold


@pytest.mark.parametrize(
    ("t_states", "expected"),
    [
        pytest.param(NOT_DONE, WAIT, id="t-undecided"),
        pytest.param(FAILED, TO_UF, id="t-failed"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_all_success_holds_skip_of_other_upstream_while_undecided(dag_maker, session, t_states, expected):
    with dag_maker("held_skip", serialized=True, session=session):
        [_mapped_t(t_states, 3), EmptyOperator(task_id="u")] >> EmptyOperator(task_id="c")
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", t_states)
    _set_states(dr, session, "u", [SK])

    assert _evaluate(dag_maker, dr, session, "c")[:2] == expected


@pytest.mark.parametrize(
    ("threshold", "t_states", "p_states", "consumer_length", "first_removed"),
    [
        pytest.param("90%", [S] * 36 + [F] * 4, [S] * 30 + [RM] * 10, 100, 70, id="tolerated-failures"),
        pytest.param(None, [S] * 40, [S] * 30 + [RM] * 10, 100, 70, id="tolerated-failures-baseline"),
        pytest.param("90%", [S] * 30 + [RM] * 10, None, 40, 30, id="re-expanded"),
        pytest.param(None, [S] * 30 + [RM] * 10, None, 40, 30, id="re-expanded-baseline"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_removed_route_bound(
    dag_maker, session, threshold, t_states, p_states, consumer_length, first_removed
):
    with dag_maker("removed_route", serialized=True, session=session):
        upstreams = [_mapped_t(t_states, threshold)]
        if p_states:
            upstreams.append(_mapped_t(p_states, None, task_id="p"))
        upstreams >> PythonOperator.partial(task_id="c", python_callable=_identity).expand(
            op_args=[[i] for i in range(consumer_length)]
        )
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", t_states)
    if p_states:
        _set_states(dr, session, "p", p_states)

    outcomes = [_evaluate(dag_maker, dr, session, "c", map_index=i)[:2] for i in range(consumer_length)]

    assert outcomes == [RUN] * first_removed + [(True, RM)] * (consumer_length - first_removed)


@conf_vars(THRESHOLD_VERSION)
def test_all_success_holds_removed_route_while_undecided(dag_maker, session):
    with dag_maker("held_removed", serialized=True, session=session):
        [_mapped_t(NOT_DONE, 3), _mapped_t([S, RM], None, task_id="p")] >> PythonOperator.partial(
            task_id="c", python_callable=_identity
        ).expand(op_args=[[0], [1]])
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", NOT_DONE)
    _set_states(dr, session, "p", [S, RM])

    assert _evaluate(dag_maker, dr, session, "c", map_index=1)[:2] == WAIT


@mock.patch("airflow.ti_deps.deps.trigger_rule_dep.is_past_depends_met", autospec=True, return_value=False)
@conf_vars(THRESHOLD_VERSION)
def test_fold_skip_waits_for_past_depends(mock_past_depends, dag_maker, session):
    with dag_maker("past_depends", serialized=True, session=session):
        _mapped_t(SKIPPED, 3) >> EmptyOperator(task_id="c")
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", SKIPPED)

    passed, state, reasons = _evaluate(
        dag_maker, dr, session, "c", wait_for_past_depends_before_skipping=True
    )

    assert (passed, state) == WAIT
    assert reasons == ["Task should be skipped but the past depends are not met"]
    mock_past_depends.assert_called_once()


@pytest.mark.parametrize(
    ("t_states", "expected", "fold_reason"),
    [
        pytest.param([S] * 36 + [F] * 4, RUN, False, id="quorum-met"),
        pytest.param([S] * 30 + [F] * 10, TO_UF, False, id="quorum-missed"),
        pytest.param([S] * 36 + [None] * 4, WAIT, True, id="undecided"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_consumer_in_mapped_task_group_over_t_output(dag_maker, session, t_states, expected, fold_reason):
    with dag_maker("mapped_group", serialized=True, session=session):
        t = _mapped_t(t_states, 35)

        @task_group
        def group(x):
            PythonOperator(task_id="c", python_callable=_identity, op_args=[x])

        group.expand(x=t.output)
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", t_states)

    passed, state, reasons = _evaluate(dag_maker, dr, session, "group.c")

    assert (passed, state) == expected
    if fold_reason:
        assert reasons[0] == (
            "Task's upstream 't' has a success threshold and is not done: "
            "success=36, live=40, skipped=0, pending=4, need=35"
        )


@pytest.mark.parametrize(
    ("t_states", "expected"),
    [
        pytest.param([S] * 36 + [F] * 4, RUN, id="quorum-met"),
        pytest.param([S] * 30 + [F] * 10, TO_UF, id="quorum-missed"),
        pytest.param([S] * 36 + [None] * 4, WAIT, id="undecided"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_loop_entry_over_top_level_t(dag_maker, session, t_states, expected):
    @task_group
    def body():
        EmptyOperator(task_id="entry")

    with dag_maker("loop", serialized=True, session=session):
        _mapped_t(t_states, 35) >> create_loop(body, max_iterations=3)
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", t_states)

    assert _evaluate(dag_maker, dr, session, "body.entry", map_index=1)[:2] == expected


@pytest.mark.parametrize(
    ("t_states", "expected"),
    [
        pytest.param([S] * 36 + [F] * 4, RUN, id="quorum-met"),
        pytest.param([S] * 30 + [F] * 10, TO_UF, id="quorum-missed"),
        pytest.param([S] * 36 + [None] * 4, WAIT, id="undecided"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_mapped_task_group_with_outside_thresholded_upstream(dag_maker, session, t_states, expected):
    with dag_maker("issue_50210_shape", serialized=True, session=session):

        @task
        def divide(i):
            return 30 / i

        @task(trigger_rule=TR.ONE_FAILED)
        def report_failure(i):
            pass

        @task
        def report_success(i):
            pass

        @task_group
        def divide_and_report(i):
            divide(i) >> [report_success(i), report_failure(i)]

        _mapped_t(t_states, 35) >> divide_and_report.expand(i=[0, 1, 2, 3])
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, "t", t_states)

    outcomes = [
        _evaluate(dag_maker, dr, session, "divide_and_report.divide", map_index=i)[:2] for i in range(4)
    ]

    assert outcomes == [expected] * 4
