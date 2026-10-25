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

The consumer sits in a mapped task group, directly or inside a nested plain task group, fed per index by an
in-group mapped upstream and optionally by an outside upstream. Both arms live side by side in one Dag run by ``dag.test()``; the
Threshold arm must end with the same state for every task instance as the preset arm.
"""

from __future__ import annotations

import pytest

from airflow.exceptions import AirflowSkipException
from airflow.sdk import task, task_group

from tests_common.test_utils.config import conf_vars
from unit.ti_deps.deps.test_threshold_differential import EQUIVALENTS

pytestmark = pytest.mark.db_test

PRESETS = list(EQUIVALENTS)
OUTSIDE = ("none", "success", "skipped", "failed")
INNER = ("success", "fail_one", "skip_one")


def _add_arm(arm, trigger_rule, nested, outside, inner):

    @task(task_id=f"{arm}_init")
    def init():
        return [0, 1, 2]

    @task(task_id=f"{arm}_outside")
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

    @task(trigger_rule=trigger_rule, retries=0)
    def imawake(i):
        return i

    outside_upstream = outside_task() if outside != "none" else None

    @task_group(group_id=f"{arm}_tg")
    def tg(i):
        upstream = imsleepy(i)
        if nested:

            @task_group(group_id="inner")
            def inner_group(x):
                awake = imawake(x)
                if outside_upstream is not None:
                    outside_upstream >> awake

            inner_group(upstream)
        else:
            awake = imawake(upstream)
            if outside_upstream is not None:
                outside_upstream >> awake

    tg.expand(i=init())


def _build(dag_maker, rule, nested, outside, inner):
    with dag_maker(dag_id=f"tg_{rule}_{int(nested)}_{outside}_{inner}") as dag:
        _add_arm("preset", rule, nested, outside, inner)
        _add_arm("threshold", EQUIVALENTS[rule], nested, outside, inner)
    return dag


def _get_arm_states(dr, arm):
    prefix = f"{arm}_"
    return {
        (ti.task_id.removeprefix(prefix).replace(f".{prefix}", "."), ti.map_index): str(ti.state)
        for ti in dr.get_task_instances()
        if ti.task_id.startswith(prefix)
    }


@pytest.mark.parametrize("inner", INNER)
@pytest.mark.parametrize("outside", OUTSIDE)
@pytest.mark.parametrize("nested", [False, True], ids=["direct", "nested"])
@pytest.mark.parametrize("rule", PRESETS)
@conf_vars({("core", "serialized_dag_max_write_version"): "5"})
def test_mapped_task_group_preset_vs_threshold(dag_maker, rule, nested, outside, inner):
    dr = _build(dag_maker, rule, nested, outside, inner).test()
    preset = _get_arm_states(dr, "preset")
    assert preset
    assert _get_arm_states(dr, "threshold") == preset
