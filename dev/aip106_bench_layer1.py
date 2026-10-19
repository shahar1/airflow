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
AIP-106 layer-1 micro-benchmark: TriggerRuleDep.get_dep_statuses with no database.

Builds a fan-in Dag (F upstream EmptyOperators -> one downstream with the rule under test), serializes
and deserializes it so the scheduler-side operator classes are used, fakes the finished task instances,
stubs the two DB touch points (region check, expected-count query), and times one dependency evaluation.

Usage: uv run --project airflow-core python dev/aip106_bench_layer1.py [--json out.json]
"""

from __future__ import annotations

import argparse
import gc
import json
import statistics
import sys
import time
import uuid
from types import SimpleNamespace

from airflow.serialization.serialized_objects import DagSerialization
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils.state import TaskInstanceState

FAN_INS = (10, 100, 1000, 5000)
RULES = ("all_success", "all_done", "none_failed_min_one_success")
REPEATS = 7


class FakeSession:
    """Answers the per-task count query with pre-computed rows; records how often it was asked."""

    def __init__(self, counts):
        self.counts = counts
        self.calls = 0

    def execute(self, *_a, **_k):
        self.calls += 1
        return list(self.counts.items())

    def scalar(self, *_a, **_k):
        self.calls += 1
        return sum(self.counts.values())


def build_dag(fan_in: int, rule: str, *, mapped: bool, threshold=None):
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import DAG, task

    with DAG(dag_id=f"bench_{fan_in}_{rule}_{int(mapped)}", schedule=None) as dag:
        down = EmptyOperator(task_id="down", trigger_rule=rule)
        if mapped:

            @task
            def up(x):
                return x

            m = up.expand(x=list(range(fan_in)))
            if threshold is not None:
                m.operator.success_threshold = threshold
            m.operator >> down
        else:
            for i in range(fan_in):
                EmptyOperator(task_id=f"up_{i}") >> down
    ser = DagSerialization.to_dict(dag)
    if mapped and threshold is not None:
        for op in ser["dag"]["tasks"]:
            if op["__var"]["task_id"] == "up":
                op["__var"]["success_threshold"] = threshold
    return DagSerialization.from_dict(ser)


def fake_tis(sdag, fan_in: int, *, mapped: bool, failures: int = 0):
    tis = []
    run_id = "bench"
    if mapped:
        up_task = sdag.get_task("up")
        for i in range(fan_in):
            state = TaskInstanceState.FAILED if i < failures else TaskInstanceState.SUCCESS
            tis.append(
                SimpleNamespace(
                    task=up_task,
                    task_id="up",
                    state=state,
                    region_index=i,
                    map_index=i,
                    id=uuid.uuid4(),
                    dag_id=sdag.dag_id,
                    run_id=run_id,
                    dag_version_id=None,
                )
            )
    else:
        for i in range(fan_in):
            state = TaskInstanceState.FAILED if i < failures else TaskInstanceState.SUCCESS
            tis.append(
                SimpleNamespace(
                    task=sdag.get_task(f"up_{i}"),
                    task_id=f"up_{i}",
                    state=state,
                    region_index=-1,
                    map_index=-1,
                    id=uuid.uuid4(),
                    dag_id=sdag.dag_id,
                    run_id=run_id,
                    dag_version_id=None,
                )
            )
    return tis


def make_consumer(sdag, finished):
    dr = SimpleNamespace(dag=sdag, run_id="bench", dag_id=sdag.dag_id)
    ti = SimpleNamespace(
        task=sdag.get_task("down"),
        task_id="down",
        state=None,
        region_index=-1,
        map_index=-1,
        id=uuid.uuid4(),
        dag_id=sdag.dag_id,
        run_id="bench",
        dag_version_id=None,
        get_dagrun=lambda session=None: dr,
        set_state=lambda state, session=None: True,
        xcom_pull=lambda **_k: False,
    )
    return ti, dr


def time_eval(ti, finished, session, *, flag: bool, loops: int) -> float:
    dep = TriggerRuleDep()
    best = []
    for _ in range(REPEATS):
        gc.collect()
        t0 = time.perf_counter()
        for _ in range(loops):
            ctx = DepContext(flag_upstream_failed=flag, finished_tis=finished)
            for _status in dep.get_dep_statuses(ti, ctx, session=session):
                pass
        best.append((time.perf_counter() - t0) / loops)
    return statistics.median(best)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json")
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    DepContext.has_regions = lambda self, ti, *, session: False  # type: ignore[method-assign]

    results = []
    print(f"{'shape':<34}{'rule':<30}{'F':>6}{'us/eval':>12}{'q':>3}")
    for fan_in in FAN_INS:
        loops = max(3, 20000 // fan_in)
        for rule in RULES:
            sdag = build_dag(fan_in, rule, mapped=False)
            finished = fake_tis(sdag, fan_in, mapped=False)
            ti, _ = make_consumer(sdag, finished)
            sess = FakeSession({})
            us = time_eval(ti, finished, sess, flag=True, loops=loops) * 1e6
            results.append(
                dict(
                    shape="plain fan-in", rule=rule, F=fan_in, us=us, queries=sess.calls // (loops * REPEATS)
                )
            )
            print(f"{'plain fan-in':<34}{rule:<30}{fan_in:>6}{us:>12.1f}{sess.calls // (loops * REPEATS):>3}")
        # mapped upstream, all_success consumer: query branch (stubbed), with and without threshold
        for threshold, label in (
            (None, "mapped fan-in (no threshold)"),
            (max(1, int(fan_in * 0.9)), "mapped fan-in (threshold 90%)"),
        ):
            try:
                sdag = build_dag(fan_in, "all_success", mapped=True, threshold=threshold)
            except Exception as e:
                print(f"{label:<34}{'all_success':<30}{fan_in:>6}  skipped: {type(e).__name__}: {e}")
                continue
            finished = fake_tis(sdag, fan_in, mapped=True)
            ti, _ = make_consumer(sdag, finished)
            sess = FakeSession({"up": fan_in})
            try:
                us = time_eval(ti, finished, sess, flag=True, loops=loops) * 1e6
            except Exception as e:
                print(f"{label:<34}{'all_success':<30}{fan_in:>6}  error: {type(e).__name__}: {e}")
                continue
            results.append(
                dict(
                    shape=label, rule="all_success", F=fan_in, us=us, queries=sess.calls // (loops * REPEATS)
                )
            )
            print(f"{label:<34}{'all_success':<30}{fan_in:>6}{us:>12.1f}{sess.calls // (loops * REPEATS):>3}")
    if args.json:
        with open(args.json, "w") as fh:
            json.dump({"label": args.label, "python": sys.version, "results": results}, fh, indent=1)


if __name__ == "__main__":
    main()
