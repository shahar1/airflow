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
"""
AIP-106 layer 2: statements issued by one ``_schedule_dag_run`` pass, counted per statement type.

The unthresholded shapes must issue the same statements as ``upstream/main``; the thresholded shapes may add
only the audit row (one INSERT inside a SAVEPOINT) and never a SELECT.

A local listener is used instead of ``assert_queries_count`` because that helper only totals statements and
cannot group them by type.
"""

from __future__ import annotations

from collections import Counter

import pytest
from sqlalchemy import event, select

from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

THRESHOLD_VERSION = {("core", "serialized_dag_max_write_version"): "5"}
AUDIT_STATEMENTS = {"INSERT": 1, "SAVEPOINT": 1, "RELEASE": 1}


def _identity(x):
    return x


def _mapped_upstream(length, thresholded):
    mapped = PythonOperator.partial(task_id="up", python_callable=_identity).expand(
        op_args=[[i] for i in range(length)]
    )
    return mapped.with_success_threshold(length // 2) if thresholded else mapped


def _build_plain_fan_in(width):
    def build(thresholded):
        down = EmptyOperator(task_id="down")
        for i in range(width):
            EmptyOperator(task_id=f"up_{i}") >> down
        return [f"up_{i}" for i in range(width)]

    return build


def _build_mapped_fan_in(thresholded):
    _mapped_upstream(20, thresholded) >> EmptyOperator(task_id="down")
    return ["up"]


def _build_mapped_consumer(thresholded):
    consumer = PythonOperator.partial(task_id="c", python_callable=_identity).expand(
        op_args=[[i] for i in range(20)]
    )
    _mapped_upstream(20, thresholded) >> consumer
    return ["up"]


def _build_shared_upstream_set(thresholded):
    ups = [EmptyOperator(task_id=f"up_{i}") for i in range(50)]
    for j in range(100):
        down = EmptyOperator(task_id=f"down_{j}")
        for up in ups:
            up >> down
    return [f"up_{i}" for i in range(50)]


def _build_mapped_task_group(thresholded):
    @task_group
    def group(x):
        a = EmptyOperator(task_id="a")
        b = EmptyOperator(task_id="b")
        c = EmptyOperator(task_id="c")
        a >> b >> c

    group.expand(x=list(range(100)))
    return ["group.a"]


def _build_loop_task_group(thresholded):
    @task_group
    def body():
        plain = PythonOperator(task_id="plain", python_callable=list)
        mapped = PythonOperator.partial(task_id="mapped", python_callable=list).expand(op_kwargs=[{}, {}])
        plain >> mapped

    outside = EmptyOperator(task_id="outside")
    outside >> create_loop(body, max_iterations=3)
    return ["outside"]


def _count_statements(dag_maker, session, build, thresholded=False):
    with dag_maker("layer2", serialized=True, session=session):
        upstream_ids = build(thresholded)
    dr = dag_maker.create_dagrun()
    for ti in session.scalars(select(TaskInstance).where(TaskInstance.dag_id == dr.dag_id)):
        if ti.task_id in upstream_ids:
            ti.state = TaskInstanceState.SUCCESS
    session.flush()

    statements = []

    def _record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    engine = session.get_bind()
    event.listen(engine, "before_cursor_execute", _record)
    try:
        SchedulerJobRunner(job=Job())._schedule_dag_run(dr, session)
    finally:
        event.remove(engine, "before_cursor_execute", _record)
    return statements, dict(Counter(s.lstrip().split(None, 1)[0].upper() for s in statements))


# Counts are identical on upstream/main.
BASELINE_SHAPES = [
    pytest.param(_build_plain_fan_in(40), {"SELECT": 4, "UPDATE": 1}, id="40-upstream-to-1"),
    pytest.param(_build_plain_fan_in(1000), {"SELECT": 4, "UPDATE": 1}, id="1000-upstream-to-1"),
    pytest.param(_build_mapped_fan_in, {"SELECT": 5, "UPDATE": 1}, id="20-mapped-to-1"),
    pytest.param(_build_mapped_consumer, {"SELECT": 6, "UPDATE": 2}, id="mapped-consumer-over-20-mapped"),
    pytest.param(_build_shared_upstream_set, {"SELECT": 4, "UPDATE": 7}, id="50-upstream-100-downstream"),
    pytest.param(_build_mapped_task_group, {"SELECT": 206, "UPDATE": 7}, id="mapped-task-group-100x3"),
    pytest.param(_build_loop_task_group, {"SELECT": 12, "UPDATE": 1}, id="loop-task-group"),
]


@pytest.mark.parametrize(("build", "expected"), BASELINE_SHAPES)
def test_baseline_statement_counts(dag_maker, session, build, expected):
    _, counts = _count_statements(dag_maker, session, build)

    assert counts == expected


@pytest.mark.parametrize(
    "build",
    [
        pytest.param(_build_mapped_fan_in, id="20-mapped-to-1"),
        pytest.param(_build_mapped_consumer, id="mapped-consumer-over-20-mapped"),
    ],
)
@conf_vars(THRESHOLD_VERSION)
def test_success_threshold_adds_only_the_audit_row(dag_maker, session, build):
    expected = dict(next(p.values[1] for p in BASELINE_SHAPES if p.values[0] is build))
    for statement_type, extra in AUDIT_STATEMENTS.items():
        expected[statement_type] = expected.get(statement_type, 0) + extra

    statements, counts = _count_statements(dag_maker, session, build, thresholded=True)

    assert counts == expected
    assert not any("RETURNING" in s.upper() for s in statements)


@conf_vars(THRESHOLD_VERSION)
def test_threshold_consumer_statement_counts(dag_maker, session):
    from airflow.sdk import Atom, Threshold

    failed = frozenset({"failed", "upstream_failed"})
    done = frozenset({"success", "failed", "upstream_failed", "skipped", "removed"})

    def build(thresholded):
        down = EmptyOperator(
            task_id="down",
            trigger_rule=Threshold(
                (Atom(failed, "==", 0), Atom({"success"}, ">=", 1), Atom(done, "==", "all", None))
            ),
        )
        for i in range(40):
            EmptyOperator(task_id=f"up_{i}") >> down
        return [f"up_{i}" for i in range(40)]

    statements, counts = _count_statements(dag_maker, session, build)

    assert counts == {"SELECT": 4, "UPDATE": 1}
    assert not any("RETURNING" in s.upper() for s in statements)
