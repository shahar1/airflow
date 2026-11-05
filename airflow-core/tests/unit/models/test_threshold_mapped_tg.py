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
Mapped task group differential: every preset against its ``Threshold`` equivalent, end to end.

The consumers sit in a mapped task group, directly or inside a nested plain task group, fed per index by an
in-group mapped upstream and optionally by an outside upstream. One Dag run by ``dag.test()`` holds a preset
consumer and its ``Threshold`` twin for every preset, all on the same upstreams; each twin must end with the
same state as its preset for every map index.
"""

from __future__ import annotations

import pytest

from airflow.exceptions import AirflowSkipException
from airflow.sdk import task, task_group

from tests_common.test_utils.config import conf_vars
from unit.ti_deps.deps.test_threshold_differential import EQUIVALENTS

pytestmark = pytest.mark.db_test

OUTSIDE = ("none", "success", "skipped", "failed")
INNER = ("success", "fail_one", "skip_one")


def _build(dag_maker, nested, outside, inner):
    @task
    def init():
        return [0, 1, 2]

    @task
    def outside_task():
        if outside == "failed":
            raise ValueError("boom")
        if outside == "skipped":
            raise AirflowSkipException

    @task
    def imsleepy(i):
        if inner == "fail_one" and i == 1:
            raise ValueError("boom")
        if inner == "skip_one" and i == 1:
            raise AirflowSkipException
        return i

    with dag_maker(dag_id=f"tg_{int(nested)}_{outside}_{inner}") as dag:
        outside_upstream = outside_task() if outside != "none" else None

        def add_consumers(x):
            for rule, threshold in EQUIVALENTS.items():
                for arm, trigger_rule in (("preset", rule), ("threshold", threshold)):

                    @task(task_id=f"{arm}_{rule}", trigger_rule=trigger_rule, retries=0)
                    def imawake(i):
                        return i

                    awake = imawake(x)
                    if outside_upstream is not None:
                        outside_upstream >> awake

        @task_group
        def tg(i):
            upstream = imsleepy(i)
            if nested:

                @task_group(group_id="inner")
                def inner_group(x):
                    add_consumers(x)

                inner_group(upstream)
            else:
                add_consumers(upstream)

        tg.expand(i=init())
    return dag


def _get_consumer_states(dr, name):
    return {
        ti.map_index: str(ti.state) for ti in dr.get_task_instances() if ti.task_id.rsplit(".", 1)[-1] == name
    }


@pytest.mark.parametrize("inner", INNER)
@pytest.mark.parametrize("outside", OUTSIDE)
@pytest.mark.parametrize("nested", [False, True], ids=["direct", "nested"])
@conf_vars({("core", "serialized_dag_max_write_version"): "5"})
def test_mapped_task_group_preset_vs_threshold(dag_maker, nested, outside, inner):
    dr = _build(dag_maker, nested, outside, inner).test()
    for rule in EQUIVALENTS:
        preset = _get_consumer_states(dr, f"preset_{rule}")
        assert preset, rule
        assert _get_consumer_states(dr, f"threshold_{rule}") == preset, rule
