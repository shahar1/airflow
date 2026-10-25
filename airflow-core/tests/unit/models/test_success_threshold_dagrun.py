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

from collections import Counter

import pytest

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task, task_group
from airflow.utils.state import DagRunState, TaskInstanceState

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

S = TaskInstanceState.SUCCESS
F = TaskInstanceState.FAILED
SK = TaskInstanceState.SKIPPED

THRESHOLD_VERSION = {("core", "serialized_dag_max_write_version"): "4"}


def _identity(x):
    return x


def _mapped_t(length=40, threshold=35):
    return (
        PythonOperator.partial(task_id="t", python_callable=_identity)
        .expand(op_args=[[i] for i in range(length)])
        .with_success_threshold(threshold)
    )


def _set_states(dr, session, states):
    for task_id, task_states in states.items():
        tis = sorted(
            (ti for ti in dr.get_task_instances(session=session) if ti.task_id == task_id),
            key=lambda ti: ti.map_index,
        )
        for ti, state in zip(tis, task_states, strict=True):
            ti.state = state
    session.flush()


@pytest.mark.parametrize(
    "t_states",
    [
        pytest.param([S] * 36 + [F] * 4, id="36s-4f"),
        pytest.param([S] * 36 + [F] * 3 + [SK], id="36s-3f-1sk"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_run_succeeds_once_consumer_of_met_threshold_succeeds(dag_maker, session, t_states):
    with dag_maker("run_outcome", serialized=True, session=session):
        _mapped_t() >> EmptyOperator(task_id="aggregate")
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, {"t": t_states})

    schedulable, _ = dr.update_state(session=session)
    assert [ti.task_id for ti in schedulable] == ["aggregate"]

    _set_states(dr, session, {"aggregate": [S]})
    dr.update_state(session=session)

    assert dr.state == DagRunState.SUCCESS
    assert Counter(ti.state for ti in dr.get_task_instances(session=session) if ti.task_id == "t") == (
        Counter(t_states)
    )


@conf_vars(THRESHOLD_VERSION)
def test_run_fails_when_thresholded_task_is_the_effective_leaf(dag_maker, session):
    with dag_maker("effective_leaf", serialized=True, session=session):
        setup = EmptyOperator(task_id="setup").as_setup()
        setup >> _mapped_t() >> EmptyOperator(task_id="teardown").as_teardown(setups=setup)
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, {"setup": [S], "t": [S] * 36 + [F] * 4, "teardown": [S]})

    dr.update_state(session=session)

    assert dr.state == DagRunState.FAILED


def _build_aggregate_consumer():
    @task
    def aggregate(values):
        return values

    aggregate(_mapped_t().output)


def _build_mapped_consumer():
    PythonOperator.partial(task_id="c", python_callable=_identity).expand(op_args=_mapped_t().output)


def _build_mapped_group_consumer():
    @task_group
    def group(x):
        PythonOperator(task_id="c", python_callable=_identity, op_args=[x])

    group.expand(x=_mapped_t().output)


def _build_teardown_consumer():
    setup = EmptyOperator(task_id="setup").as_setup()
    setup >> _mapped_t() >> EmptyOperator(task_id="teardown").as_teardown(setups=setup)


@pytest.mark.parametrize(
    ("build", "consumer_id"),
    [
        pytest.param(_build_aggregate_consumer, "aggregate", id="task-over-output"),
        pytest.param(_build_mapped_consumer, "c", id="mapped-over-output"),
        pytest.param(_build_mapped_group_consumer, "group.c", id="mapped-group-over-output"),
        pytest.param(_build_teardown_consumer, "teardown", id="setup-t-teardown"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_consumer_waits_for_completion_after_quorum_without_deadlock(dag_maker, session, build, consumer_id):
    with dag_maker("completion_only", serialized=True, session=session):
        build()
    dr = dag_maker.create_dagrun()
    _set_states(dr, session, {"t": [S] * 36 + [SK] + [None] * 3})
    if consumer_id == "teardown":
        _set_states(dr, session, {"setup": [S]})

    decision = dr.task_instance_scheduling_decisions(session=session)
    dr.update_state(session=session)

    assert consumer_id not in {ti.task_id for ti in decision.schedulable_tis}
    assert {ti.state for ti in dr.get_task_instances(session=session) if ti.task_id == consumer_id} == {None}
    assert dr.state == DagRunState.RUNNING
