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
Differential test: every trigger rule preset against its ``Threshold`` equivalent.

Both consumers run through the real ``TriggerRuleDep``; the expected ``(passed, new_state)`` is whatever the
enum evaluator returns at test time. Vectors are the upstream state multisets ``calculate`` can see.

The thresholded-upstream arm builds the Threshold consumer scheduler-side because ingestion rejects a
Threshold consumer of a thresholded mapped upstream. It compares the two evaluators while one upstream is
a mapped task of three instances with a success threshold of two; a vector with an instance still pending
leaves that upstream undecided, and only those vectors may differ.
"""

from __future__ import annotations

import itertools
import time
from collections import Counter

import pytest

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Threshold
from airflow.sdk.definitions.threshold import Atom
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils.state import TaskInstanceState as S

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

FAILED_ANY = {"failed", "upstream_failed"}
DONE_ALL = Atom(frozenset({"success", "skipped", "failed", "upstream_failed", "removed"}), "==", "all", None)
STATES = (S.SUCCESS, S.SKIPPED, S.FAILED, S.UPSTREAM_FAILED, S.REMOVED, None)
MAP_INDEXES = (-1, 0, 1)
THRESHOLD_VERSION = {("core", "serialized_dag_max_write_version"): "5"}


def _atom(states, op, n, route="upstream_failed"):
    return Atom(frozenset(states), op, n, route)


EQUIVALENTS = {
    "all_done": Threshold((DONE_ALL,)),
    "one_success": Threshold((_atom({"success"}, ">=", 1, "upstream_failed"),)),
    "one_failed": Threshold((_atom(FAILED_ANY, ">=", 1, "skipped"),)),
    "one_done": Threshold((_atom({"success", "failed"}, ">=", 1, "skipped"),)),
    "none_failed": Threshold((_atom(FAILED_ANY, "==", 0, "upstream_failed"), DONE_ALL)),
    "none_failed_min_one_success": Threshold(
        (_atom(FAILED_ANY, "==", 0), _atom({"success"}, ">=", 1), DONE_ALL)
    ),
    "none_skipped": Threshold((_atom({"skipped"}, "==", 0, "skipped"), DONE_ALL)),
    "all_failed": Threshold(
        (_atom({"success"}, "==", 0, "skipped"), _atom({"skipped"}, "==", 0, "skipped"), DONE_ALL)
    ),
    "all_skipped": Threshold(
        (_atom({"success"}, "==", 0, "skipped"), _atom(FAILED_ANY, "==", 0, "skipped"), DONE_ALL)
    ),
    "all_success": Threshold(
        (_atom(FAILED_ANY, "==", 0), _atom({"skipped"}, "==", 0, "skipped"), DONE_ALL), shrink_route=True
    ),
    "all_done_min_one_success": Threshold(
        (_atom({"skipped"}, "==", 0, "skipped"), _atom({"success"}, ">=", 1), DONE_ALL)
    ),
}


def _identity(x):
    return x


def _add_consumers(upstreams):
    """Add a plain and a mapped consumer per preset and per Threshold, all fed by ``upstreams``."""
    for rule, threshold in EQUIVALENTS.items():
        for prefix, trigger_rule in (("p", rule), ("t", threshold)):
            plain = EmptyOperator(task_id=f"{prefix}_{rule}", trigger_rule=trigger_rule)
            mapped = PythonOperator.partial(
                task_id=f"{prefix}m_{rule}", python_callable=_identity, trigger_rule=trigger_rule
            ).expand(op_args=[[1], [2]])
            upstreams >> plain
            upstreams >> mapped


def _multisets(n):
    return itertools.combinations_with_replacement(range(len(STATES)), n)


def _evaluate(ti, task, session, *, flag):
    ti.state = None
    ti.refresh_from_task(task)
    statuses = list(
        TriggerRuleDep()._get_dep_statuses(
            ti, dep_context=DepContext(flag_upstream_failed=flag), session=session
        )
    )
    return all(s.passed for s in statuses), ti.state


def _index_tis(dr, session):
    return {(ti.task_id, ti.map_index): ti for ti in dr.get_task_instances(session=session)}


def _compare(rule, enum, threshold, states):
    """Return a failure description, or None when the pair behaves as the equivalence requires."""
    # Q1: ALL_SKIPPED waits on REMOVED upstreams; its Threshold passes.
    if rule == "all_skipped" and S.REMOVED in states and enum != threshold:
        if enum == (False, None) and threshold == (True, None):
            return None
    return None if enum == threshold else f"enum={enum} threshold={threshold}"


@pytest.mark.parametrize("n", [1, 2, 3, 4])
@conf_vars(THRESHOLD_VERSION)
def test_presets_equal_their_threshold(dag_maker, session, n):
    with dag_maker("differential", serialized=True, session=session):
        _add_consumers([EmptyOperator(task_id=f"u{i}") for i in range(n)])
    dr = dag_maker.create_dagrun()
    sdag = dag_maker.serialized_dag
    tis = _index_tis(dr, session)

    compared = 0
    q1 = 0
    failures = []
    started = time.monotonic()
    for combo in _multisets(n):
        states = [STATES[i] for i in combo]
        for i, state in enumerate(states):
            tis[f"u{i}", -1].state = state
        session.flush()
        for rule in EQUIVALENTS:
            for flag, map_index in itertools.product((True, False), MAP_INDEXES):
                infix = "" if map_index == -1 else "m"
                results = []
                for prefix in ("p", "t"):
                    task_id = f"{prefix}{infix}_{rule}"
                    results.append(
                        _evaluate(tis[task_id, map_index], sdag.get_task(task_id), session, flag=flag)
                    )
                compared += 1
                q1 += results[0] != results[1]
                if (failure := _compare(rule, *results, states)) is not None:
                    failures.append((rule, states, flag, map_index, failure))
    print(f"\nN={n} compared={compared} q1={q1} seconds={time.monotonic() - started:.1f}")

    assert compared == len(list(_multisets(n))) * len(EQUIVALENTS) * 2 * len(MAP_INDEXES)
    assert not failures, failures[:10]
    removal_only = [
        c
        for c in _multisets(n)
        if S.REMOVED in map(STATES.__getitem__, c) and {STATES[i] for i in c} <= {S.SKIPPED, S.REMOVED}
    ]
    assert q1 == len(removal_only) * 2 * len(MAP_INDEXES)


@conf_vars(THRESHOLD_VERSION)
def test_thresholded_upstream_arm(dag_maker, session):
    with dag_maker("thresholded", serialized=True, session=session):
        mapped = PythonOperator.partial(task_id="t", python_callable=_identity).expand(
            op_args=[[1], [2], [3]]
        )
        _add_consumers_plain([mapped, EmptyOperator(task_id="u")])
    dr = dag_maker.create_dagrun()
    sdag = dag_maker.serialized_dag
    # Scheduler-side construction: what ``with_success_threshold(2)`` serializes to, minus the ingestion check.
    sdag.get_task("t").success_threshold = 2
    for task in sdag.tasks:
        if task.task_id.startswith(("p_", "t_")):
            task.thresholded_upstream_ids = frozenset({"t"})
    tis = _index_tis(dr, session)

    compared = 0
    differing = []
    undecided_vectors = 0
    for combo in _multisets(3):
        t_states = [STATES[i] for i in combo]
        for u_state in STATES:
            for i, state in enumerate(t_states):
                tis["t", i].state = state
            tis["u", -1].state = u_state
            session.flush()
            undecided = None in t_states
            undecided_vectors += undecided
            for rule in EQUIVALENTS:
                for flag in (True, False):
                    results = [
                        _evaluate(
                            tis[f"{prefix}_{rule}", -1], sdag.get_task(f"{prefix}_{rule}"), session, flag=flag
                        )
                        for prefix in ("p", "t")
                    ]
                    compared += 1
                    all_states = [*t_states, u_state]
                    if _compare(rule, *results, all_states) is not None:
                        differing.append(
                            (
                                rule,
                                flag,
                                tuple(s and s.value for s in t_states),
                                u_state and u_state.value,
                                *results,
                                undecided,
                            )
                        )
    table = Counter((rule, undecided) for rule, *_, undecided in differing)
    print(
        f"\nthresholded arm compared={compared} undecided_vectors={undecided_vectors} differing={len(differing)}"
    )
    for row in differing:
        print("DIFF", row)
    print("by (rule, undecided):", sorted(table.items()))

    assert compared == len(list(_multisets(3))) * len(STATES) * len(EQUIVALENTS) * 2
    assert all(undecided for *_, undecided in differing), [row for row in differing if not row[-1]][:10]


def _add_consumers_plain(upstreams):
    for rule, threshold in EQUIVALENTS.items():
        for prefix, trigger_rule in (("p", rule), ("t", threshold)):
            upstreams >> EmptyOperator(task_id=f"{prefix}_{rule}", trigger_rule=trigger_rule)
