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

import json
import logging
import re
from datetime import timedelta
from unittest import mock

import pytest
from sqlalchemy import event, select

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.triggers.base import StartTriggerArgs
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

S = TaskInstanceState.SUCCESS
F = TaskInstanceState.FAILED
SK = TaskInstanceState.SKIPPED
RM = TaskInstanceState.REMOVED

THRESHOLD_VERSION = {("core", "serialized_dag_max_write_version"): "4"}
QUORUM_MET = [S, S, S, F]
QUORUM_MISSED = [S, S, F, F]


def _identity(x):
    return x


class _StartFromTrigger(BaseOperator):
    start_trigger_args = StartTriggerArgs(
        trigger_cls="airflow.triggers.testing.SuccessTrigger",
        trigger_kwargs={},
        next_method="execute_complete",
        timeout=None,
    )
    start_from_trigger = True

    def execute_complete(self):
        pass


def _python_consumer():
    return PythonOperator(task_id="c", python_callable=_identity, op_args=[0])


def _mapped_consumer(length):
    return lambda: PythonOperator.partial(task_id="c", python_callable=_identity).expand(
        op_args=[[i] for i in range(length)]
    )


def _make(dag_maker, session, t_states, threshold=3, consumer=_python_consumer):
    with dag_maker("audit", serialized=True, session=session):
        t = (
            PythonOperator.partial(task_id="t", python_callable=_identity)
            .expand(op_args=[[i] for i in range(len(t_states))])
            .with_success_threshold(threshold)
        )
        t >> consumer()
    dr = dag_maker.create_dagrun()
    tis = sorted(
        (ti for ti in dr.get_task_instances(session=session) if ti.task_id == "t"),
        key=lambda ti: ti.map_index,
    )
    for ti, state in zip(tis, t_states, strict=True):
        ti.state = state
    session.flush()
    return dr


def _rows(session):
    return session.scalars(select(Log).where(Log.event == "success_threshold_decision")).all()


def _consumer_ti(dr, session):
    return next(
        ti for ti in dr.get_task_instances(session=session) if ti.task_id == "c" and ti.map_index <= 0
    )


@pytest.mark.parametrize(
    ("t_states", "threshold", "consumer", "outcomes"),
    [
        pytest.param(QUORUM_MET, 3, _python_consumer, {"scheduled": [[-1, -1]]}, id="scheduled"),
        pytest.param(
            QUORUM_MET, 3, lambda: EmptyOperator(task_id="c"), {"success": [[-1, -1]]}, id="empty-success"
        ),
        pytest.param(
            QUORUM_MET, 3, lambda: _StartFromTrigger(task_id="c"), {"deferred": [[-1, -1]]}, id="deferred"
        ),
        pytest.param(QUORUM_MISSED, 3, _python_consumer, {"upstream_failed": [[-1, -1]]}, id="routed-uf"),
        pytest.param([SK] * 4, 3, _python_consumer, {"skipped": [[-1, -1]]}, id="routed-skipped"),
        pytest.param(
            [S] * 30 + [RM] * 10,
            "90%",
            _mapped_consumer(40),
            {"scheduled": [[0, 29]], "removed": [[30, 39]]},
            id="routed-removed",
        ),
        pytest.param(QUORUM_MET, 3, _mapped_consumer(1000), {"scheduled": [[0, 999]]}, id="1000-wide"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_one_core_insert_per_batch(dag_maker, session, t_states, threshold, consumer, outcomes):
    dr = _make(dag_maker, session, t_states, threshold, consumer)
    engine = session.get_bind()
    statements = []

    def _record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", _record)
    try:
        SchedulerJobRunner(job=Job())._schedule_dag_run(dr, session)
    finally:
        event.remove(engine, "before_cursor_execute", _record)

    [row] = _rows(session)
    assert (row.dag_id, row.run_id, row.task_id, row.owner) == ("audit", dr.run_id, "c", "scheduler")
    assert row.task_instance_id is None
    assert row.map_index is None
    assert len(row.extra) < 65000
    extra = json.loads(row.extra)
    assert extra["region"] is None
    assert extra["outcomes"] == outcomes
    assert "truncated" not in extra
    start = next(i for i, s in enumerate(statements) if s.startswith("SAVEPOINT"))
    window = statements[start : start + 3]
    assert window[1].startswith("INSERT INTO log")
    assert window[2].startswith("RELEASE SAVEPOINT")
    assert not any("SELECT" in s.upper() or "RETURNING" in s.upper() for s in window)


@conf_vars(THRESHOLD_VERSION)
def test_upstream_fold_counts(dag_maker, session):
    dr = _make(dag_maker, session, QUORUM_MET)

    SchedulerJobRunner(job=Job())._schedule_dag_run(dr, session)

    [row] = _rows(session)
    assert json.loads(row.extra)["upstreams"] == {
        "t": {"s": 3, "f": 1, "uf": 0, "sk": 0, "rm": 0, "expected": 4, "need": 3, "state": "success"}
    }


@conf_vars(THRESHOLD_VERSION)
def test_scheduled_task_without_fold_is_not_audited(dag_maker, session):
    with dag_maker("audit", serialized=True, session=session):
        t = (
            PythonOperator.partial(task_id="t", python_callable=_identity)
            .expand(op_args=[[i] for i in range(4)])
            .with_success_threshold(3)
        )
        t >> _python_consumer()
        PythonOperator(task_id="other", python_callable=_identity, op_args=[0])
    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances(session=session):
        if ti.task_id == "t":
            ti.state = S
    session.flush()

    SchedulerJobRunner(job=Job())._schedule_dag_run(dr, session)

    states = dict(session.execute(select(TaskInstance.task_id, TaskInstance.state)).all())
    assert states["c"] == states["other"] == TaskInstanceState.SCHEDULED
    assert [row.task_id for row in _rows(session)] == ["c"]


@pytest.mark.parametrize(
    ("t_states", "c_state", "end_date_offset", "outcomes"),
    [
        pytest.param(
            QUORUM_MISSED,
            TaskInstanceState.SCHEDULED,
            None,
            {"upstream_failed": [[-1, -1]]},
            id="probe-routes",
        ),
        pytest.param(QUORUM_MET, TaskInstanceState.SCHEDULED, None, None, id="probe-passes-pool-wait"),
        pytest.param(
            QUORUM_MET, TaskInstanceState.UP_FOR_RETRY, timedelta(0), None, id="probe-passes-retry-wait"
        ),
        pytest.param(
            QUORUM_MET,
            TaskInstanceState.UP_FOR_RETRY,
            timedelta(days=1),
            {"scheduled": [[-1, -1]]},
            id="retry",
        ),
        pytest.param(QUORUM_MET, TaskInstanceState.UP_FOR_RESCHEDULE, None, None, id="reschedule-re-entry"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_consumer_re_entry(dag_maker, session, t_states, c_state, end_date_offset, outcomes):
    dr = _make(dag_maker, session, t_states)
    c = _consumer_ti(dr, session)
    c.state = c_state
    if end_date_offset is not None:
        c.try_number = 1
        c.end_date = timezone.utcnow() - end_date_offset
    session.flush()

    SchedulerJobRunner(job=Job())._schedule_dag_run(dr, session)

    assert [json.loads(row.extra)["outcomes"] for row in _rows(session)] == ([outcomes] if outcomes else [])


@conf_vars(THRESHOLD_VERSION)
def test_rescheduling_consumer_routed_is_audited(dag_maker, session, caplog):
    dr = _make(dag_maker, session, QUORUM_MISSED)
    _consumer_ti(dr, session).state = TaskInstanceState.UP_FOR_RESCHEDULE
    session.flush()

    with caplog.at_level(logging.ERROR):
        SchedulerJobRunner(job=Job())._schedule_dag_run(dr, session)

    assert session.scalar(select(TaskInstance.state).where(TaskInstance.task_id == "c")) == (
        TaskInstanceState.UPSTREAM_FAILED
    )
    [row] = _rows(session)
    assert row.task_id == "c"
    assert json.loads(row.extra)["outcomes"] == {"upstream_failed": [[-1, -1]]}
    assert {
        "log_level": "error",
        "event": re.compile("Failed to write the success threshold audit record"),
    } not in caplog


@conf_vars(THRESHOLD_VERSION)
def test_update_state_without_flag_keeps_nothing(dag_maker, session):
    dr = _make(dag_maker, session, QUORUM_MISSED)

    dr.update_state(session=session, execute_callbacks=False)

    assert _consumer_ti(dr, session).state == TaskInstanceState.UPSTREAM_FAILED
    assert dr._threshold_routed == []
    assert _rows(session) == []


@conf_vars(THRESHOLD_VERSION)
def test_dependency_view_keeps_no_fold(dag_maker, session):
    dr = _make(dag_maker, session, QUORUM_MET)
    c = _consumer_ti(dr, session)
    c.task = dag_maker.serialized_dag.get_task("c")

    assert list(c.get_failed_dep_statuses(dep_context=DepContext(), session=session)) == []
    assert not hasattr(c, "_threshold_fold")
    assert _rows(session) == []


@conf_vars(THRESHOLD_VERSION)
def test_scheduling_decisions_without_update_state_initialise_the_routed_list(dag_maker, session):
    with dag_maker("plain", serialized=True, session=session):
        EmptyOperator(task_id="a") >> EmptyOperator(task_id="b")
    dr = dag_maker.create_dagrun()

    dr.task_instance_scheduling_decisions(session=session, record_threshold_decisions=True)

    assert dr._threshold_routed == []


@conf_vars(THRESHOLD_VERSION)
def test_oversized_payload_is_truncated(dag_maker, session):
    task_ids = [f"{i:03d}" + "u" * 227 for i in range(220)]
    with dag_maker("audit", serialized=True, session=session):
        upstreams = [
            PythonOperator.partial(task_id=task_id, python_callable=_identity)
            .expand(op_args=[[0]])
            .with_success_threshold(1)
            for task_id in task_ids
        ]
        upstreams >> _python_consumer()
    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances(session=session):
        if ti.task_id != "c":
            ti.state = S
    session.flush()

    SchedulerJobRunner(job=Job())._schedule_dag_run(dr, session)

    [row] = _rows(session)
    extra = json.loads(row.extra)
    assert extra["truncated"] is True
    assert sorted(extra["upstreams"]) == task_ids[:50]
    assert extra["outcomes"] == {"scheduled": [[-1, -1]]}


@mock.patch("airflow.jobs.scheduler_job_runner.insert", autospec=True, side_effect=RuntimeError)
@conf_vars(THRESHOLD_VERSION)
def test_failed_write_does_not_fail_scheduling(mock_insert, dag_maker, session, caplog):
    dr = _make(dag_maker, session, QUORUM_MET)

    with caplog.at_level(logging.ERROR):
        SchedulerJobRunner(job=Job())._schedule_dag_run(dr, session)
    session.commit()

    assert _rows(session) == []
    assert session.scalar(select(TaskInstance.state).where(TaskInstance.task_id == "c")) == (
        TaskInstanceState.SCHEDULED
    )
    assert {
        "log_level": "error",
        "logger_name": "airflow.jobs.scheduler_job_runner.SchedulerJobRunner",
        "event": re.compile("Failed to write the success threshold audit record"),
    } in caplog
