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

from pathlib import Path

import pytest
from sqlalchemy import select

from airflow._shared.module_loading import import_string
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

EXAMPLES_FOLDER = Path(__file__).parents[2] / "system" / "core"


@pytest.mark.parametrize("example", ["example_success_threshold"])
@conf_vars({("core", "serialized_dag_max_write_version"): "4"})
def test_example_runs_with_a_failed_partition(example, configure_dag_bundles, session):
    dag = import_string(f"system.core.{example}.dag")

    with configure_dag_bundles({"examples": EXAMPLES_FOLDER}):
        dag.test()

    dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag.dag_id))
    tis = session.scalars(select(TaskInstance).where(TaskInstance.dag_id == dag.dag_id)).all()
    mapped = [ti for ti in tis if ti.task_id == "process"]
    states = sorted(ti.state for ti in mapped)
    assert dag_run.state == DagRunState.SUCCESS
    assert {ti.task_id: ti.state for ti in tis if ti.task_id == "aggregate"} == {
        "aggregate": TaskInstanceState.SUCCESS
    }
    assert states == sorted([TaskInstanceState.FAILED] + [TaskInstanceState.SUCCESS] * 3)
