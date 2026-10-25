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

import copy

import pytest

from airflow.exceptions import DeserializationError
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, BaseOperator, task, task_group
from airflow.serialization.dag_version_diff import SUPPORTED_SERIALIZED_DAG_SCHEMA_VERSIONS
from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator
from airflow.serialization.serialized_objects import DagSerialization

from tests_common.test_utils.config import conf_vars

CEILING = ("core", "serialized_dag_max_write_version")


def _build_dag(threshold=2, *, fail_fast=False):
    with DAG("thresholded", schedule=None, fail_fast=fail_fast) as dag:
        mapped = BashOperator.partial(task_id="t").expand(bash_command=["a", "b", "c"])
        if threshold is not None:
            mapped.with_success_threshold(threshold)
        mapped >> BaseOperator(task_id="consumer")
        BaseOperator(task_id="plain")
    return dag


def _task_var(data, task_id):
    return next(t["__var"] for t in data["dag"]["tasks"] if t["__var"]["task_id"] == task_id)


@pytest.fixture
def v4_payload():
    with conf_vars({CEILING: "4"}):
        return DagSerialization.to_dict(_build_dag())


class TestWriteSide:
    def test_constants(self):
        assert DagSerialization.SERIALIZER_VERSION == 3
        assert DagSerialization.MAX_SERIALIZER_VERSION == 4
        assert frozenset(range(1, 5)) == SUPPORTED_SERIALIZED_DAG_SCHEMA_VERSIONS

    @pytest.mark.parametrize("ceiling", ["4", "5"])
    def test_thresholded_dag_is_version_4(self, ceiling, v4_payload):
        with conf_vars({CEILING: ceiling}):
            data = DagSerialization.to_dict(_build_dag())

        assert data["__version"] == 4
        assert _task_var(data, "t")["success_threshold"] == 2
        assert "success_threshold" not in _task_var(data, "t")["partial_kwargs"]
        assert "success_threshold" not in _task_var(data, "consumer")
        assert "success_threshold" not in _task_var(data, "plain")

    def test_default_ceiling_rejects_thresholded_dag(self):
        with pytest.raises(ValueError, match="serialized_dag_max_write_version is 3"):
            DagSerialization.to_dict(_build_dag())

    @pytest.mark.parametrize("ceiling", ["2", "6"])
    def test_out_of_range_ceiling_is_rejected(self, ceiling):
        with (
            conf_vars({CEILING: ceiling}),
            pytest.raises(ValueError, match="serialized_dag_max_write_version"),
        ):
            DagSerialization.to_dict(_build_dag())

    def test_unthresholded_dag_payload_and_hash_are_unchanged_by_the_feature(self):
        before = DagSerialization.to_dict(_build_dag(None))
        with conf_vars({CEILING: "4"}):
            DagSerialization.to_dict(_build_dag())
        after = DagSerialization.to_dict(_build_dag(None))

        assert before["__version"] == 3
        assert all("success_threshold" not in t["__var"] for t in before["dag"]["tasks"])
        assert after == before
        assert SerializedDagModel.hash(after) == SerializedDagModel.hash(before)


class TestReadSide:
    def test_round_trip(self, v4_payload):
        dag = DagSerialization.from_dict(v4_payload)

        assert isinstance(dag.task_dict["t"], SerializedMappedOperator)
        assert dag.task_dict["t"].success_threshold == 2
        assert dag.task_dict["consumer"].thresholded_upstream_ids == {"t"}
        assert dag.task_dict["t"].thresholded_upstream_ids == frozenset()
        assert dag.task_dict["plain"].thresholded_upstream_ids == frozenset()

    def test_version_3_payload_with_the_field_ignores_it(self, v4_payload, caplog):
        v4_payload["__version"] = 3
        before = copy.deepcopy(v4_payload)

        dag = DagSerialization.from_dict(v4_payload)

        assert {"log_level": "warning", "logger": "airflow.serialization.serialized_objects"} in caplog

        assert v4_payload == before
        assert dag.task_dict["t"].success_threshold is None
        assert dag.task_dict["consumer"].thresholded_upstream_ids == frozenset()

    def test_version_5_is_rejected(self, v4_payload):
        v4_payload["__version"] = 5

        with pytest.raises(ValueError, match="Unsure how to deserialize version 5"):
            DagSerialization.from_dict(v4_payload)

    @pytest.mark.parametrize(
        ("keep", "expected"),
        [(["consumer"], frozenset()), (["t", "consumer"], frozenset({"t"}))],
    )
    def test_partial_subset_recomputes_thresholded_upstream_ids(self, v4_payload, keep, expected):
        subset = DagSerialization.from_dict(v4_payload).partial_subset(
            keep, include_upstream=False, include_downstream=False
        )

        assert subset.task_dict["consumer"].thresholded_upstream_ids == expected


@pytest.fixture
def ceiling_4():
    with conf_vars({CEILING: "4"}):
        yield


@pytest.mark.usefixtures("ceiling_4")
class TestValidateSerializedDag:
    def test_accepts_a_valid_threshold(self, v4_payload):
        dag = DagSerialization.validate_serialized_dag(v4_payload)

        assert dag.task_dict["t"].success_threshold == 2

    def test_accepts_a_percent_over_an_xcom_fed_expand(self):
        with DAG("fed", schedule=None) as dag:

            @task
            def make():
                return [1, 2]

            @task
            def work(x):
                return x

            work.expand(x=make()).with_success_threshold("50%")

        assert DagSerialization.validate_serialized_dag(DagSerialization.to_dict(dag))

    @pytest.mark.parametrize(
        ("edit", "reason"),
        [
            pytest.param({"version": 5}, "above [core] serialized_dag_max_write_version", id="above-ceiling"),
            pytest.param({"version": 3}, "format version 3", id="v3-with-field"),
            pytest.param({"drop": True}, "version 4 but no success_threshold", id="v4-without-field"),
            pytest.param({"partial": {"is_setup": True}}, "setup or teardown", id="setup"),
            pytest.param({"partial": {"is_teardown": True}}, "setup or teardown", id="teardown"),
            pytest.param({"dag": {"fail_fast": True}}, "fail_fast", id="fail-fast"),
            pytest.param({"var": {"success_threshold": 4}}, "only 3 instances", id="above-count"),
            pytest.param({"var": {"success_threshold": 0}}, "is not an int", id="zero"),
            pytest.param({"var": {"success_threshold": "101%"}}, "is not an int", id="percent-over-100"),
            pytest.param({"var": {"success_threshold": "x"}}, "is not an int", id="not-a-percent"),
            pytest.param({"var": {"success_threshold": True}}, "is not an int", id="bool"),
        ],
    )
    def test_rejects(self, v4_payload, edit, reason):
        if "version" in edit:
            v4_payload["__version"] = edit["version"]
        if edit.get("drop"):
            del _task_var(v4_payload, "t")["success_threshold"]
        _task_var(v4_payload, "t").update(edit.get("var", {}))
        _task_var(v4_payload, "t")["partial_kwargs"].update(edit.get("partial", {}))
        v4_payload["dag"].update(edit.get("dag", {}))

        with pytest.raises(DeserializationError, match=reason.replace("[", r"\[").replace("]", r"\]")):
            DagSerialization.validate_serialized_dag(v4_payload)

    def test_rejects_a_threshold_on_an_unmapped_task(self, v4_payload):
        var = _task_var(v4_payload, "consumer")
        var["success_threshold"] = 1

        with pytest.raises(DeserializationError, match="it is not mapped"):
            DagSerialization.validate_serialized_dag(v4_payload)

    def test_rejects_a_threshold_inside_a_mapped_task_group(self, v4_payload):
        with DAG("grouped", schedule=None) as dag:

            @task_group
            def grp(x):
                BashOperator(task_id="w", bash_command="x")

            grp.expand(x=[1, 2])
        payload = DagSerialization.to_dict(dag)
        mapped_var = {**_task_var(v4_payload, "t"), "task_id": "grp.w", "downstream_task_ids": []}
        payload["dag"]["tasks"] = [{"__type": "operator", "__var": mapped_var}]
        payload["__version"] = 4

        with pytest.raises(DeserializationError, match="mapped task group"):
            DagSerialization.validate_serialized_dag(payload)
