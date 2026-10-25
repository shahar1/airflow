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

import airflow.sdk
from airflow.sdk import DAG, Atom, TaskInstanceState, Threshold, task, task_group
from airflow.sdk.bases.operator import BASEOPERATOR_ARGS_EXPECTED_TYPES
from airflow.sdk.definitions._internal.contextmanager import TaskGroupContext
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.exceptions import FailFastDagInvalidTriggerRule

from tests_common.test_utils.mock_operators import MockOperator

ONE_SUCCESS = Atom({"success"}, ">=", 1)


@task
def _produce():
    return [1, 2, 3]


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        pytest.param(({"success"}, ">=", 1), {"success"}, id="string-state"),
        pytest.param(([TaskInstanceState.SUCCESS, "failed"], "<", 3), {"success", "failed"}, id="enum-state"),
        pytest.param(({"skipped"}, ">", 0), {"skipped"}, id="gt"),
        pytest.param(({"removed"}, "<=", 4), {"removed"}, id="le"),
        pytest.param(({"upstream_failed"}, "==", 0), {"upstream_failed"}, id="eq"),
        pytest.param(({"success"}, "==", "all"), {"success"}, id="eq-all"),
        pytest.param(({"success"}, "<", "all"), {"success"}, id="lt-all"),
        pytest.param(({"success"}, ">=", 10000), {"success"}, id="max-n"),
    ],
)
def test_atom_valid(args, expected):
    atom = Atom(*args)
    assert atom.count == frozenset(expected)
    assert all(isinstance(state, str) and not hasattr(state, "value") for state in atom.count)
    assert atom.route == "upstream_failed"


@pytest.mark.parametrize("route", ["upstream_failed", "skipped", None])
def test_atom_routes(route):
    assert Atom({"success"}, ">=", 1, route=route).route == route


@pytest.mark.parametrize(
    "args",
    [
        pytest.param((set(), ">=", 1), id="empty-count"),
        pytest.param(({"bogus"}, ">=", 1), id="unknown-state"),
        pytest.param(({"success"}, "!=", 1), id="unknown-op"),
        pytest.param(({"success"}, ">=", True), id="n-bool"),
        pytest.param(({"success"}, ">=", 1.0), id="n-float"),
        pytest.param(({"success"}, ">=", -1), id="n-negative"),
        pytest.param(({"success"}, ">=", 10001), id="n-too-big"),
        pytest.param(({"success"}, ">", "all"), id="all-gt"),
        pytest.param(({"success"}, "<", 0), id="lt-zero"),
        pytest.param(({"success"}, ">=", 1, "bogus"), id="bad-route"),
    ],
)
def test_atom_invalid(args):
    with pytest.raises(ValueError, match="Atom"):
        Atom(*args)


def test_atom_repr_sorts_states_and_equality():
    a = Atom({"success", "failed"}, ">=", 1)
    b = Atom(["failed", "success"], ">=", 1)
    assert repr(a) == repr(b)
    assert "frozenset(['failed', 'success'])" in repr(a)
    assert a == b
    assert len({a, b}) == 1


@pytest.mark.parametrize("container", [tuple, list])
def test_threshold_accepts_atoms_as_tuple(container):
    thr = Threshold(container([ONE_SUCCESS]))
    assert thr.atoms == (ONE_SUCCESS,)
    assert isinstance(thr.atoms, tuple)
    assert thr.shrink_route is False


def test_threshold_empty_allowed():
    assert Threshold(()).atoms == ()


@pytest.mark.parametrize(
    "atoms",
    [
        pytest.param([Atom({"success"}, ">=", i) for i in range(7)], id="seven-atoms"),
        pytest.param([Atom({"success"}, "==", 0), Atom({"success"}, ">=", 1)], id="eq0-ge1"),
        pytest.param([Atom({"success"}, "<=", 2), Atom({"success"}, ">", 5)], id="le2-gt5"),
        pytest.param([Atom({"success"}, "<", "all"), Atom({"success"}, "==", "all")], id="lt-all-eq-all"),
        pytest.param(["not-an-atom"], id="not-an-atom"),
    ],
)
def test_threshold_invalid(atoms):
    with pytest.raises(ValueError, match="Threshold|Atom|never both hold"):
        Threshold(atoms)


@pytest.mark.parametrize(
    "atoms",
    [
        pytest.param([Atom({"success"}, ">=", 1), Atom({"success"}, "<=", 3)], id="ge1-le3"),
        pytest.param([Atom({"success"}, ">=", 1), Atom({"failed"}, "==", 0)], id="different-states"),
        pytest.param([Atom({"success"}, ">=", 1), Atom({"success"}, "==", "all")], id="n-and-all"),
    ],
)
def test_threshold_compatible_atoms(atoms):
    assert Threshold(atoms).atoms == tuple(atoms)


def test_operator_keeps_threshold_object():
    thr = Threshold([ONE_SUCCESS])
    with DAG("d", default_args={"trigger_rule": thr}):
        assert MockOperator(task_id="a", arg1="x", arg2="y", trigger_rule=thr).trigger_rule is thr
        assert MockOperator(task_id="b", arg1="x", arg2="y").trigger_rule is thr
        mapped = MockOperator.partial(task_id="m", arg1="x", trigger_rule=thr).expand(arg2=[1, 2])
        assert isinstance(mapped, MappedOperator)
        assert mapped.trigger_rule is thr

        @task(trigger_rule=thr)
        def decorated():
            pass

        assert decorated().operator.trigger_rule is thr


def test_operator_bogus_trigger_rule_still_raises():
    with pytest.raises(ValueError, match="The trigger_rule must be one of"):
        MockOperator(task_id="a", arg1="x", arg2="y", trigger_rule="bogus")


def test_expected_types_include_threshold():
    assert Threshold in BASEOPERATOR_ARGS_EXPECTED_TYPES["trigger_rule"]


def test_as_teardown_rejected_for_operator():
    op = MockOperator(task_id="a", arg1="x", arg2="y", trigger_rule=Threshold(()))
    with pytest.raises(ValueError, match="cannot be a teardown"):
        op.as_teardown()


def test_as_teardown_rejected_for_xcom_arg():
    @task(trigger_rule=Threshold(()))
    def thr_task():
        pass

    with DAG("d"):
        with pytest.raises(ValueError, match="cannot be a teardown"):
            thr_task().as_teardown()


def test_setup_followed_by_threshold_rejected():
    with DAG("d") as dag:
        setup = MockOperator(task_id="s", arg1="x", arg2="y").as_setup()
        setup >> MockOperator(task_id="t", arg1="x", arg2="y", trigger_rule=Threshold(()))
    with pytest.raises(ValueError, match="Setup tasks must be followed"):
        dag.validate()


def test_fail_fast_rejects_threshold():
    dag = DAG("d", fail_fast=True)
    with pytest.raises(FailFastDagInvalidTriggerRule):
        MockOperator(task_id="t", arg1="x", arg2="y", trigger_rule=Threshold(()), dag=dag)


def _build_dag(atom, *, upstreams=2, mapped=False):
    with DAG("d") as dag:
        consumer = MockOperator(task_id="consumer", arg1="x", arg2="y", trigger_rule=Threshold([atom]))
        for i in range(upstreams):
            consumer << MockOperator(task_id=f"up{i}", arg1="x", arg2="y")
        if mapped:
            consumer << MockOperator.partial(task_id="mapped", arg1="x").expand(arg2=[1, 2])
    return dag


@pytest.mark.parametrize(
    "atom",
    [
        pytest.param(Atom({"success"}, ">=", 3), id="ge3"),
        pytest.param(Atom({"success"}, "==", 3), id="eq3"),
        pytest.param(Atom({"success"}, ">", 2), id="gt2"),
    ],
)
def test_validate_rejects_unsatisfiable_atom(atom):
    with pytest.raises(ValueError, match="only 2 upstream tasks"):
        _build_dag(atom).validate()


@pytest.mark.parametrize(
    ("atom", "mapped"),
    [
        pytest.param(Atom({"success"}, "<=", 5), False, id="le5"),
        pytest.param(Atom({"success"}, ">=", 2), False, id="ge2"),
        pytest.param(Atom({"success"}, "==", "all"), False, id="all"),
        pytest.param(Atom({"success"}, ">=", 3), True, id="ge3-mapped-upstream"),
    ],
)
def test_validate_accepts_satisfiable_atom(atom, mapped):
    _build_dag(atom, mapped=mapped).validate()


@pytest.mark.parametrize(
    ("rule", "threshold_set", "raises"),
    [
        pytest.param(Threshold([ONE_SUCCESS]), True, True, id="threshold-on-thresholded-mapped"),
        pytest.param("all_success", True, False, id="all-success-on-thresholded-mapped"),
        pytest.param(Threshold([ONE_SUCCESS]), False, False, id="threshold-on-plain-mapped"),
    ],
)
def test_validate_threshold_consumer_of_mapped(rule, threshold_set, raises):
    with DAG("d") as dag:
        mapped = MockOperator.partial(task_id="mapped", arg1="x").expand(arg2=[1, 2])
        if threshold_set:
            mapped.with_success_threshold(1)
        mapped >> MockOperator(task_id="consumer", arg1="x", arg2="y", trigger_rule=rule)
    if raises:
        with pytest.raises(ValueError, match="'consumer'.*'mapped'"):
            dag.validate()
    else:
        dag.validate()


def _expand_group_with_child(rule):
    @task(trigger_rule=rule)
    def child(x):
        return x

    with DAG("d"):

        @task_group
        def grp(x):
            child(x)

        return grp.expand(x=_produce())


def test_mapped_task_group_rejects_empty_threshold_child():
    try:
        with pytest.raises(ValueError, match="empty Threshold"):
            _expand_group_with_child(Threshold(()))
    finally:
        TaskGroupContext.pop()


def test_mapped_task_group_allows_threshold_child_with_atoms():
    assert list(_expand_group_with_child(Threshold([ONE_SUCCESS])))


@pytest.mark.parametrize(
    "expand",
    [
        pytest.param(lambda t: t.expand(x=_produce()), id="expand"),
        pytest.param(lambda t: t.expand_kwargs([{"x": _produce()}]), id="expand_kwargs"),
    ],
)
def test_decorator_empty_threshold_rejects_task_generated_mapping(expand):
    @task(trigger_rule=Threshold(()))
    def consume(x):
        return x

    with DAG("d"):
        with pytest.raises(ValueError, match="empty Threshold trigger rule"):
            expand(consume)


@pytest.mark.parametrize(
    "expand",
    [
        pytest.param(lambda t: t.expand(x=[1, 2]), id="expand"),
        pytest.param(lambda t: t.expand_kwargs([{"x": 1}]), id="expand_kwargs"),
    ],
)
@pytest.mark.parametrize("rule", [Threshold(()), Threshold([ONE_SUCCESS])], ids=["empty", "atoms"])
def test_decorator_allows_literal_mapping(rule, expand):
    @task(trigger_rule=rule)
    def consume(x):
        return x

    with DAG("d"):
        assert expand(consume)


@pytest.mark.parametrize(
    "expand",
    [
        pytest.param(lambda t: t.expand(x=_produce()), id="expand"),
        pytest.param(lambda t: t.expand_kwargs([{"x": _produce()}]), id="expand_kwargs"),
    ],
)
def test_decorator_threshold_with_atoms_allows_task_generated_mapping(expand):
    @task(trigger_rule=Threshold([ONE_SUCCESS]))
    def consume(x):
        return x

    with DAG("d"):
        assert expand(consume)


def test_exported_from_sdk():
    assert {"Threshold", "Atom"} <= set(airflow.sdk.__all__)
    assert airflow.sdk.Threshold is Threshold
    assert airflow.sdk.Atom is Atom
