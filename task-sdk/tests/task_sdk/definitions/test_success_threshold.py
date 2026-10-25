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

import pytest

from airflow.sdk import DAG, task, task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.sdk.definitions.xcom_arg import PlainXComArg

from tests_common.test_utils.mock_operators import MockOperator


@task
def _produce():
    return [1, 2, 3]


@task
def _consume(x):
    return x


def test_classic_operator_returns_receiver_and_records_value():
    with DAG("d"):
        mapped = MockOperator.partial(task_id="m").expand(arg2=[1, 2, 3])
        assert mapped.with_success_threshold(35) is mapped
    assert mapped.success_threshold == 35
    assert "success_threshold" not in mapped.partial_kwargs


def test_classic_operator_expand_kwargs():
    with DAG("d"):
        mapped = MockOperator.partial(task_id="m").expand_kwargs([{"arg2": 1}, {"arg2": 2}])
        assert mapped.with_success_threshold(2) is mapped
    assert mapped.success_threshold == 2


@pytest.mark.parametrize(
    "build",
    [
        pytest.param(lambda: _consume.expand(x=[1, 2]), id="expand"),
        pytest.param(lambda: _consume.expand_kwargs([{"x": 1}, {"x": 2}]), id="expand_kwargs"),
    ],
)
def test_task_decorator_returns_receiver_and_records_value(build):
    with DAG("d"):
        xcom_arg = build()
        assert xcom_arg.with_success_threshold("90%") is xcom_arg
    assert xcom_arg.operator.success_threshold == "90%"


@pytest.mark.parametrize("value", [1, 35, "1%", "90%", "100%"])
def test_valid_values(value):
    with DAG("d"):
        mapped = MockOperator.partial(task_id="m").expand(arg2=[1])
        mapped.with_success_threshold(value)
    assert mapped.success_threshold == value


@pytest.mark.parametrize("value", [0, -1, True, False, "0%", "101%", "5", "5.5%", 1.0, None, "%", ""])
def test_invalid_values(value):
    with DAG("d"):
        mapped = MockOperator.partial(task_id="m").expand(arg2=[1])
        with pytest.raises(ValueError, match="success threshold must be"):
            mapped.with_success_threshold(value)
    assert mapped.success_threshold is None


def test_unmapped_xcom_arg_rejected():
    with DAG("d"):
        plain = _produce()
        classic_output = MockOperator(task_id="c", arg1="a").output
        with pytest.raises(ValueError, match="'_produce' is not mapped"):
            plain.with_success_threshold(1)
        with pytest.raises(ValueError, match="'c' is not mapped"):
            classic_output.with_success_threshold(1)


def test_derived_xcom_args_have_no_method():
    with DAG("d"):
        mapped = _consume.expand(x=_produce())
        derived = [mapped.map(str), mapped.zip(mapped), mapped.concat(mapped)]
    assert all(not hasattr(arg, "with_success_threshold") for arg in derived)
    assert not any(isinstance(arg, PlainXComArg) for arg in derived)


def test_validate_rejects_setup():
    with DAG("d") as dag:
        _consume.override(task_id="s").expand(x=[1, 2]).with_success_threshold(1).as_setup()
    with pytest.raises(ValueError, match="'s'.*setup or teardown"):
        dag.validate()


def test_validate_rejects_teardown_marked_after_threshold():
    with DAG("d") as dag:
        _consume.override(task_id="t").expand(x=[1, 2]).with_success_threshold(1).as_teardown()
    with pytest.raises(ValueError, match="'t'.*setup or teardown"):
        dag.validate()


def test_validate_rejects_loop_task_group():
    @task_group
    def body():
        _consume.override(task_id="m").expand(x=[1, 2]).with_success_threshold(1)

    with DAG("d") as dag:
        create_loop(body, max_iterations=2)
    with pytest.raises(ValueError, match="'body.m'.*loop task group"):
        dag.validate()


def test_validate_rejects_fail_fast():
    with DAG("d", fail_fast=True) as dag:
        _consume.override(task_id="m").expand(x=[1, 2]).with_success_threshold(1)
    with pytest.raises(ValueError, match="'m'.*fail_fast"):
        dag.validate()


@pytest.mark.parametrize(
    ("build", "threshold"),
    [
        pytest.param(lambda: MockOperator.partial(task_id="m").expand(arg2=[1, 2]), 3, id="expand"),
        pytest.param(
            lambda: MockOperator.partial(task_id="m").expand_kwargs([{"arg2": 1}, {"arg2": 2}]),
            3,
            id="expand_kwargs",
        ),
        pytest.param(
            lambda: MockOperator.partial(task_id="m").expand(arg1=[1, 2], arg2=[1, 2]), 5, id="product"
        ),
    ],
)
def test_validate_rejects_threshold_above_literal_length(build, threshold):
    with DAG("d") as dag:
        build().with_success_threshold(threshold)
    with pytest.raises(ValueError, match="'m'.*only"):
        dag.validate()


@pytest.mark.parametrize(
    ("build", "threshold"),
    [
        pytest.param(lambda: _consume.override(task_id="m").expand(x=_produce()), 100, id="unknown-length"),
        pytest.param(
            lambda: MockOperator.partial(task_id="m").expand(arg1=[1, 2], arg2=[1, 2]), 4, id="product"
        ),
        pytest.param(lambda: _consume.override(task_id="m").expand(x=[1, 2]), "90%", id="percent"),
    ],
)
def test_validate_accepts(build, threshold):
    with DAG("d") as dag:
        build().with_success_threshold(threshold)
    dag.validate()
