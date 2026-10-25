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

from airflow.exceptions import DeserializationError
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Atom, BaseOperator, Threshold, task_group
from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator
from airflow.serialization.serialized_objects import DagSerialization, create_scheduler_operator
from airflow.task.threshold import encode_threshold, parse_threshold
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars

CEILING = ("core", "serialized_dag_max_write_version")
DONE = frozenset({"success", "failed", "upstream_failed", "skipped", "removed"})
FAILED_ANY = frozenset({"failed", "upstream_failed"})
DONE_ALL = Atom(DONE, "==", "all", None)
NONE_FAILED_MIN_ONE_SUCCESS = Threshold((Atom(FAILED_ANY, "==", 0), Atom({"success"}, ">=", 1), DONE_ALL))
NONE_FAILED_MIN_ONE_SUCCESS_RULE = (
    "threshold/2:failed+upstream_failed==0->upstream_failed;success>=1->upstream_failed;"
    "failed+removed+skipped+success+upstream_failed==all->none:shrink_route=false"
)
UPSTREAM_FAILED = TaskInstanceState.UPSTREAM_FAILED
SKIPPED = TaskInstanceState.SKIPPED
SINGLE_ATOM_RULE = "threshold/2:success>=1->upstream_failed:shrink_route=false"

EQUIVALENTS = {
    "all_done": Threshold((DONE_ALL,)),
    "one_success": Threshold((Atom({"success"}, ">=", 1),)),
    "one_failed": Threshold((Atom(FAILED_ANY, ">=", 1, "skipped"),)),
    "one_done": Threshold((Atom({"success", "failed"}, ">=", 1, "skipped"),)),
    "none_failed": Threshold((Atom(FAILED_ANY, "==", 0), DONE_ALL)),
    "none_failed_min_one_success": NONE_FAILED_MIN_ONE_SUCCESS,
    "none_skipped": Threshold((Atom({"skipped"}, "==", 0, "skipped"), DONE_ALL)),
    "all_failed": Threshold(
        (Atom({"success"}, "==", 0, "skipped"), Atom({"skipped"}, "==", 0, "skipped"), DONE_ALL)
    ),
    "all_skipped": Threshold(
        (Atom({"success"}, "==", 0, "skipped"), Atom(FAILED_ANY, "==", 0, "skipped"), DONE_ALL)
    ),
    "all_success": Threshold(
        (Atom(FAILED_ANY, "==", 0), Atom({"skipped"}, "==", 0, "skipped"), DONE_ALL), shrink_route=True
    ),
    "all_done_min_one_success": Threshold(
        (Atom({"skipped"}, "==", 0, "skipped"), Atom({"success"}, ">=", 1), DONE_ALL)
    ),
    "always": Threshold(()),
}


def _counts(**counts):
    return {state: counts.get(state, 0) for state in sorted(DONE)}


def _task_var(data, task_id):
    return next(t["__var"] for t in data["dag"]["tasks"] if t["__var"]["task_id"] == task_id)


def _serialize(dag, ceiling="5"):
    with conf_vars({CEILING: ceiling}):
        return DagSerialization.to_dict(dag)


def _build_plain_dag(threshold):
    with DAG("plain_thr", schedule=None) as dag:
        BaseOperator(task_id="consumer", trigger_rule=threshold)
    return dag


def _build_mapped_upstream_dag(threshold=NONE_FAILED_MIN_ONE_SUCCESS):
    with DAG("mapped_thr", schedule=None) as dag:
        mapped = BashOperator.partial(task_id="t").expand(bash_command=["a", "b", "c"])
        mapped >> BaseOperator(task_id="consumer", trigger_rule=threshold)
        BaseOperator(task_id="plain")
    return dag


def _build_plain_upstreams_dag():
    with DAG("plain_upstreams", schedule=None) as dag:
        [BaseOperator(task_id="a"), BaseOperator(task_id="b")] >> BaseOperator(task_id="c")
    return dag


@pytest.fixture
def v5_payload():
    return _serialize(_build_mapped_upstream_dag())


class TestWireFormat:
    @pytest.mark.parametrize(
        ("threshold", "expected", "atoms", "shrink_route"),
        [
            pytest.param(
                NONE_FAILED_MIN_ONE_SUCCESS,
                NONE_FAILED_MIN_ONE_SUCCESS_RULE,
                (
                    (("failed", "upstream_failed"), "==", 0, UPSTREAM_FAILED),
                    (("success",), ">=", 1, UPSTREAM_FAILED),
                    (tuple(sorted(DONE)), "==", None, None),
                ),
                False,
                id="none-failed-min-one-success",
            ),
            pytest.param(Threshold(()), "threshold/2::shrink_route=false", (), False, id="atom-less"),
            pytest.param(
                Threshold((Atom({"success"}, ">=", 1),), shrink_route=True),
                "threshold/2:success>=1->upstream_failed:shrink_route=true",
                ((("success",), ">=", 1, UPSTREAM_FAILED),),
                True,
                id="shrink-route",
            ),
            pytest.param(
                Threshold((Atom({"success", "skipped"}, "<", 5, "skipped"),)),
                "threshold/2:skipped+success<5->skipped:shrink_route=false",
                ((("skipped", "success"), "<", 5, SKIPPED),),
                False,
                id="skipped-route-int-n",
            ),
        ],
    )
    def test_round_trip(self, threshold, expected, atoms, shrink_route):
        data = _serialize(_build_plain_dag(threshold))
        dag = DagSerialization.from_dict(data)
        compiled = dag.task_dict["consumer"].trigger_rule_compiled

        assert _task_var(data, "consumer")["trigger_rule"] == expected
        assert encode_threshold(threshold) == expected
        assert dag.task_dict["consumer"].trigger_rule == expected
        assert compiled == parse_threshold(expected)
        assert compiled.atoms == atoms
        assert compiled.shrink_route is shrink_route

    def test_atom_order_changes_the_string_and_the_hash(self):
        first = Atom(FAILED_ANY, "==", 0)
        second = Atom({"success"}, ">=", 1)
        forward = _serialize(_build_plain_dag(Threshold((first, second))))
        backward = _serialize(_build_plain_dag(Threshold((second, first))))

        assert (
            _task_var(forward, "consumer")["trigger_rule"] != _task_var(backward, "consumer")["trigger_rule"]
        )
        assert SerializedDagModel.hash(forward) != SerializedDagModel.hash(backward)


class TestVersionStamping:
    @pytest.mark.parametrize(
        ("threshold", "success_threshold", "expected"),
        [
            pytest.param(True, False, 5, id="threshold"),
            pytest.param(True, True, 5, id="threshold-and-success-threshold"),
            pytest.param(False, True, 4, id="success-threshold"),
            pytest.param(False, False, 3, id="neither"),
        ],
    )
    def test_version(self, threshold, success_threshold, expected):
        with DAG("stamped", schedule=None) as dag:
            mapped = BashOperator.partial(task_id="t").expand(bash_command=["a", "b", "c"])
            if success_threshold:
                mapped.with_success_threshold(2)
            BaseOperator(
                task_id="consumer", trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS if threshold else "all_success"
            )

        assert _serialize(dag)["__version"] == expected

    def test_dag_without_threshold_is_unchanged_by_the_feature(self):
        def build():
            with DAG("untouched", schedule=None) as dag:
                BaseOperator(task_id="consumer")
            return dag

        before = DagSerialization.to_dict(build())
        _serialize(_build_plain_dag(NONE_FAILED_MIN_ONE_SUCCESS))
        after = DagSerialization.to_dict(build())

        assert before["__version"] == 3
        assert after == before

    def test_ceiling_below_5_rejects_a_threshold(self):
        with pytest.raises(ValueError, match=r"version 5.*serialized_dag_max_write_version is 4"):
            _serialize(_build_plain_dag(NONE_FAILED_MIN_ONE_SUCCESS), ceiling="4")


def _edit_rule(rule, task_id="consumer"):
    return lambda payload: _task_var(payload, task_id).update(trigger_rule=rule)


def _set_version(version):
    return lambda payload: payload.update(__version=version)


def _edit_v4_with_success_threshold(payload):
    payload["__version"] = 4
    _task_var(payload, "t")["success_threshold"] = 2


def _edit_teardown(payload):
    _task_var(payload, "consumer")["is_teardown"] = True


def _edit_fail_fast(payload):
    payload["dag"]["fail_fast"] = True


def _edit_success_threshold_on_mapped_upstream(payload):
    _task_var(payload, "t")["success_threshold"] = 2


@pytest.fixture
def ceiling_5():
    with conf_vars({CEILING: "5"}):
        yield


@pytest.mark.usefixtures("ceiling_5")
class TestValidateSerializedDag:
    def test_accepts_a_valid_threshold(self, v5_payload):
        dag = DagSerialization.validate_serialized_dag(v5_payload)

        assert dag.task_dict["consumer"].trigger_rule_compiled == parse_threshold(
            NONE_FAILED_MIN_ONE_SUCCESS_RULE
        )

    def test_accepts_an_int_n_above_the_upstream_count_for_a_mapped_upstream(self, v5_payload):
        _edit_rule("threshold/2:success>=3->upstream_failed:shrink_route=false")(v5_payload)

        assert DagSerialization.validate_serialized_dag(v5_payload)

    @pytest.mark.parametrize(
        ("op", "n", "accepted"),
        [
            pytest.param(">=", 3, False, id="ge-above"),
            pytest.param(">", 2, False, id="gt-at"),
            pytest.param("==", 3, False, id="eq-above"),
            pytest.param(">=", 2, True, id="ge-at"),
            pytest.param(">", 1, True, id="gt-below"),
            pytest.param("<=", 9, True, id="le-above"),
            pytest.param("<", 9, True, id="lt-above"),
        ],
    )
    def test_int_n_against_unmapped_upstreams(self, op, n, accepted):
        payload = _serialize(_build_plain_upstreams_dag())
        _task_var(payload, "c")["trigger_rule"] = (
            f"threshold/2:success{op}{n}->upstream_failed:shrink_route=false"
        )
        payload["__version"] = 5

        if accepted:
            assert DagSerialization.validate_serialized_dag(payload)
        else:
            with pytest.raises(DeserializationError, match="an atom needs more than its 2 upstream tasks"):
                DagSerialization.validate_serialized_dag(payload)

    @pytest.mark.parametrize(
        ("edit", "reason"),
        [
            pytest.param(
                _edit_v4_with_success_threshold,
                "Threshold trigger rule in serialized Dag format version 4",
                id="v4-with-success-threshold",
            ),
            pytest.param(
                _set_version(4), "version 4 but no success_threshold", id="v4-without-success-threshold"
            ),
            pytest.param(
                _edit_rule("all_success"),
                "version 5 but no Threshold trigger rule",
                id="v5-without-threshold",
            ),
            pytest.param(
                _edit_rule("threshold/9:success>=1->none:shrink_route=false"),
                "cannot be deserialized",
                id="unknown-version",
            ),
            pytest.param(
                _edit_rule("threshold/2:success+failed>=1->none:shrink_route=false"),
                "sorted order",
                id="unsorted-states",
            ),
            pytest.param(
                _edit_rule("threshold/2:success>=1->none"), "Malformed Threshold", id="missing-shrink-route"
            ),
            pytest.param(
                _edit_rule("threshold/2:" + ";".join(["success>=1->none"] * 7) + ":shrink_route=false"),
                "more than 6 atoms",
                id="seven-atoms",
            ),
            pytest.param(
                _edit_rule(
                    "threshold/2:"
                    + ";".join(["success>=3->none", "success<2->none"] + ["success>=1->none"] * 5)
                    + ":shrink_route=false"
                ),
                "more than 6 atoms",
                id="seven-atoms-with-contradiction",
            ),
            pytest.param(
                _edit_rule("threshold/2:success>=01->none:shrink_route=false"),
                "Malformed atom",
                id="malformed-atom",
            ),
            pytest.param(
                _edit_rule("threshold/2:success>=10001->none:shrink_route=false"),
                "between 0 and 10000",
                id="n-too-large",
            ),
            pytest.param(_edit_teardown, "it is a teardown task", id="teardown"),
            pytest.param(_edit_fail_fast, "the Dag has fail_fast=True", id="fail-fast"),
            pytest.param(
                _edit_success_threshold_on_mapped_upstream,
                "cannot consume a mapped task with a success threshold",
                id="consumes-success-threshold",
            ),
        ],
    )
    def test_rejects(self, v5_payload, edit, reason):
        edit(v5_payload)

        with pytest.raises(DeserializationError, match=reason):
            DagSerialization.validate_serialized_dag(v5_payload)

    def test_rejects_an_atom_less_threshold_in_a_mapped_task_group(self):
        with DAG("grouped", schedule=None) as dag:

            @task_group
            def grp(x):
                BaseOperator(task_id="w")

            grp.expand(x=[1, 2])
        payload = _serialize(dag)
        _task_var(payload, "grp.w")["trigger_rule"] = "threshold/2::shrink_route=false"
        payload["__version"] = 5

        with pytest.raises(DeserializationError, match="no atoms and is in a mapped task group"):
            DagSerialization.validate_serialized_dag(payload)


class TestCompiledPlan:
    def test_plain_and_mapped_operators_are_compiled(self):
        dag = DagSerialization.from_dict(
            _serialize(
                _build_mapped_upstream_dag(),
            )
        )
        with DAG("both", schedule=None) as sdk_dag:
            BashOperator.partial(task_id="m", trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS).expand(
                bash_command=["a", "b"]
            )
        mapped = DagSerialization.from_dict(_serialize(sdk_dag)).task_dict["m"]
        expected = parse_threshold(NONE_FAILED_MIN_ONE_SUCCESS_RULE)

        assert dag.task_dict["consumer"].trigger_rule_compiled == expected
        assert isinstance(mapped, SerializedMappedOperator)
        assert mapped.trigger_rule_compiled == expected
        assert dag.task_dict["plain"].trigger_rule_compiled is None
        assert dag.task_dict["t"].trigger_rule_compiled is None

    @pytest.mark.parametrize(
        ("name", "is_fast_triggered", "defers_to_expanded_tis"),
        [
            pytest.param(
                name,
                name in {"one_success", "one_failed", "one_done"},
                name in {"none_failed_min_one_success", "all_done_min_one_success"},
                id=name,
            )
            for name in EQUIVALENTS
        ],
    )
    def test_flags(self, name, is_fast_triggered, defers_to_expanded_tis):
        compiled = (
            DagSerialization.from_dict(_serialize(_build_plain_dag(EQUIVALENTS[name])))
            .task_dict["consumer"]
            .trigger_rule_compiled
        )

        assert compiled.is_fast_triggered is is_fast_triggered
        assert compiled.defers_to_expanded_tis is defers_to_expanded_tis

    def test_preset_operators_keep_no_compiled_plan(self):
        plain = create_scheduler_operator(BaseOperator(task_id="p", trigger_rule="one_success"))
        mapped = create_scheduler_operator(
            BashOperator.partial(task_id="m", trigger_rule="one_success").expand(bash_command=["a"])
        )

        assert plain.trigger_rule_compiled is None
        assert mapped.trigger_rule_compiled is None


def _evaluate(rule, counts, *, done, upstream, flag_upstream_failed=True, map_index=-1):
    return parse_threshold(rule).evaluate(
        counts, done=done, upstream=upstream, flag_upstream_failed=flag_upstream_failed, map_index=map_index
    )


class TestEvaluate:
    @pytest.mark.parametrize(
        ("op", "now", "pending", "verdict"),
        [
            (">=", 2, 0, True),
            (">=", 1, 0, False),
            (">=", 1, 1, None),
            (">", 3, 0, True),
            (">", 2, 0, False),
            (">", 2, 1, None),
            ("<=", 1, 1, True),
            ("<=", 3, 0, False),
            ("<=", 2, 1, None),
            ("<", 1, 0, True),
            ("<", 2, 0, False),
            ("<", 1, 1, None),
            ("==", 2, 0, True),
            ("==", 1, 0, False),
            ("==", 1, 1, None),
        ],
    )
    def test_three_valued_decision_per_op(self, op, now, pending, verdict):
        passed, state = _evaluate(
            f"threshold/2:success{op}2->upstream_failed:shrink_route=false",
            _counts(success=now),
            done=now,
            upstream=now + pending,
        )

        assert passed is (verdict is True)
        assert state is (UPSTREAM_FAILED if verdict is False else None)

    @pytest.mark.parametrize(
        ("rule", "expected"),
        [
            pytest.param(
                "threshold/2:success>=3->skipped;failed==0->upstream_failed:shrink_route=false",
                SKIPPED,
                id="first-atom-first",
            ),
            pytest.param(
                "threshold/2:failed==0->upstream_failed;success>=3->skipped:shrink_route=false",
                UPSTREAM_FAILED,
                id="swapped",
            ),
            pytest.param(
                "threshold/2:success>=3->none;failed==0->upstream_failed:shrink_route=false",
                None,
                id="route-none-blocks-later-routes",
            ),
        ],
    )
    def test_first_false_atom_routes_in_authored_order(self, rule, expected):
        passed, state = _evaluate(rule, _counts(success=1, failed=1), done=2, upstream=2)

        assert passed is False
        assert state is expected

    @pytest.mark.parametrize(
        ("counts", "done", "upstream", "expected"),
        [
            pytest.param(_counts(skipped=2), 2, 2, SKIPPED, id="all-skipped"),
            pytest.param(_counts(skipped=1, failed=1), 2, 2, UPSTREAM_FAILED, id="mixed"),
            pytest.param(_counts(skipped=1), 1, 2, None, id="pending"),
        ],
    )
    def test_all_skipped_convention(self, counts, done, upstream, expected):
        _, state = _evaluate(SINGLE_ATOM_RULE, counts, done=done, upstream=upstream)

        assert state is expected

    @pytest.mark.parametrize(
        ("rule", "counts", "map_index", "flag_upstream_failed", "expected"),
        [
            pytest.param(
                "threshold/2::shrink_route=true",
                _counts(success=2, removed=1),
                2,
                True,
                TaskInstanceState.REMOVED,
                id="beyond-success",
            ),
            pytest.param(
                "threshold/2::shrink_route=true",
                _counts(success=2, removed=1),
                1,
                True,
                None,
                id="within-success",
            ),
            pytest.param(
                "threshold/2::shrink_route=true",
                _counts(success=2, removed=1),
                -1,
                True,
                None,
                id="not-expanded",
            ),
            pytest.param(
                "threshold/2::shrink_route=true", _counts(removed=1), 0, True, None, id="no-success"
            ),
            pytest.param(
                "threshold/2::shrink_route=true", _counts(success=2), 2, True, None, id="no-removed"
            ),
            pytest.param(
                "threshold/2::shrink_route=false",
                _counts(success=2, removed=1),
                2,
                True,
                None,
                id="shrink-off",
            ),
            pytest.param(
                "threshold/2::shrink_route=true", _counts(success=2, removed=1), 2, False, None, id="flag-off"
            ),
        ],
    )
    def test_shrink_route(self, rule, counts, map_index, flag_upstream_failed, expected):
        passed, state = _evaluate(
            rule, counts, done=3, upstream=3, flag_upstream_failed=flag_upstream_failed, map_index=map_index
        )

        assert passed is True
        assert state is expected

    def test_without_flag_upstream_failed_nothing_routes(self):
        passed, state = _evaluate(
            SINGLE_ATOM_RULE, _counts(failed=1), done=1, upstream=1, flag_upstream_failed=False
        )

        assert (passed, state) == (False, None)


class TestDescribeUnmet:
    @pytest.mark.parametrize(
        ("rule", "counts", "done", "upstream", "expected"),
        [
            pytest.param(SINGLE_ATOM_RULE, _counts(), 0, 3, "success>=1: 0 now, 3 pending", id="pending"),
            pytest.param(
                "threshold/2:failed+upstream_failed==0->none;success>=2->none:shrink_route=false",
                _counts(failed=1),
                1,
                1,
                "failed+upstream_failed==0: 1 now; success>=2: 0 now",
                id="authored-order-no-pending",
            ),
            pytest.param(
                "threshold/2:success==all->none:shrink_route=false",
                _counts(success=1),
                1,
                2,
                "success==all: 1 now, 1 pending",
                id="all",
            ),
            pytest.param(
                "threshold/2:success>=1->none;failed==0->none:shrink_route=false",
                _counts(success=1),
                1,
                1,
                "",
                id="all-met",
            ),
        ],
    )
    def test_describe_unmet(self, rule, counts, done, upstream, expected):
        assert parse_threshold(rule).describe_unmet(counts, done=done, upstream=upstream) == expected
