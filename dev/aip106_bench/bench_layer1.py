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
AIP-106 layer-1 benchmark: ``TriggerRuleDep._get_dep_statuses`` per trigger rule, no database.

One fan-in Dag per F: F upstream tasks (half FAILED, half SUCCESS) feeding one consumer per preset and,
when ``airflow.sdk.Threshold`` exists, one consumer per equivalent Threshold. No upstream needs expansion,
so the count query is never reached (the session is a MagicMock that is asserted untouched). The Phase 1
fold needs that query and is measured at layer 2. Runs unchanged on upstream/main (Threshold rows skipped).

Usage: AIRFLOW__CORE__SERIALIZED_DAG_MAX_WRITE_VERSION=5 uv run --project airflow-core python dev/aip106_bench/bench_layer1.py [--smoke] [--json out.json]
(the ceiling is read at import time, so it has to be set in the environment; bench_series.sh does it.)
"""

from __future__ import annotations

import argparse
import gc
import importlib.util
import json
import os
import statistics
import subprocess
import sys
import time
import uuid
from types import SimpleNamespace
from unittest import mock

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.serialization.serialized_objects import DagSerialization
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep, _UpstreamTIStates
from airflow.utils.state import TaskInstanceState

THRESHOLD_AVAILABLE = importlib.util.find_spec("airflow.sdk.definitions.threshold") is not None
PRESETS = (
    "all_success",
    "all_failed",
    "all_done",
    "all_done_min_one_success",
    "one_success",
    "one_failed",
    "one_done",
    "none_failed",
    "none_failed_min_one_success",
    "none_skipped",
    "all_skipped",
)
DEFAULT_INNER = {10: 2000, 100: 200, 1000: 20, 5000: 4}

EQUIVALENTS: dict = {}
if THRESHOLD_AVAILABLE:
    from airflow.sdk import Threshold
    from airflow.sdk.definitions.threshold import Atom

    FAILED_ANY = {"failed", "upstream_failed"}
    DONE_ALL = Atom(
        frozenset({"success", "skipped", "failed", "upstream_failed", "removed"}), "==", "all", None
    )

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
            (_atom(FAILED_ANY, "==", 0), _atom({"skipped"}, "==", 0, "skipped"), DONE_ALL),
            shrink_route=True,
        ),
        "all_done_min_one_success": Threshold(
            (_atom({"skipped"}, "==", 0, "skipped"), _atom({"success"}, ">=", 1), DONE_ALL)
        ),
    }


def build_dag(fan_in: int):
    """Return the deserialized fan-in Dag and the consumer task ids by rule label."""
    consumers = {}
    with DAG(dag_id=f"bench_{fan_in}", schedule=None) as dag:
        ups = [EmptyOperator(task_id=f"up_{i}") for i in range(fan_in)]
        for rule in PRESETS:
            consumers[rule] = EmptyOperator(task_id=f"p_{rule}", trigger_rule=rule)
        for rule, threshold in EQUIVALENTS.items():
            consumers[f"threshold:{rule}"] = EmptyOperator(task_id=f"t_{rule}", trigger_rule=threshold)
        for consumer in consumers.values():
            ups >> consumer
    sdag = DagSerialization.from_dict(DagSerialization.to_dict(dag))
    return sdag, {label: task.task_id for label, task in consumers.items()}


def make_finished(sdag, fan_in: int):
    return [
        SimpleNamespace(
            task=sdag.get_task(f"up_{i}"),
            task_id=f"up_{i}",
            state=TaskInstanceState.FAILED if i < fan_in // 2 else TaskInstanceState.SUCCESS,
            region_index=-1,
            map_index=-1,
            id=uuid.uuid4(),
            dag_id=sdag.dag_id,
            run_id="bench",
            dag_version_id=None,
        )
        for i in range(fan_in)
    ]


def make_consumer(sdag, task_id: str):
    dr = SimpleNamespace(dag=sdag, run_id="bench", dag_id=sdag.dag_id)
    return SimpleNamespace(
        task=sdag.get_task(task_id),
        task_id=task_id,
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


def make_runners(sdag, task_ids, finished, session):
    dep = TriggerRuleDep()

    def make_eval(task_id):
        ti = make_consumer(sdag, task_id)

        def run():
            ctx = DepContext(flag_upstream_failed=True, finished_tis=finished)
            for _ in dep._get_dep_statuses(ti, ctx, session=session):
                pass

        return run

    runners = {label: make_eval(tid) for label, tid in task_ids.items()}
    runners["control"] = lambda: _UpstreamTIStates.calculate(iter(finished))
    return runners


def time_rule(fn, inner: int) -> float:
    """Return microseconds per call of ``fn``."""
    gc.collect()
    gc.disable()
    t0 = time.perf_counter_ns()
    for _ in range(inner):
        fn()
    elapsed = time.perf_counter_ns() - t0
    gc.enable()
    return elapsed / inner / 1000


def get_git_head() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="10,100,1000,5000")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--inner", type=int, help="iterations per measurement (default scales with F)")
    ap.add_argument("--json")
    ap.add_argument("--smoke", action="store_true", help="sizes=10, repeats=1, inner=10")
    args = ap.parse_args()
    sizes = [int(s) for s in args.sizes.split(",")]
    repeats, inner_override = args.repeats, args.inner
    if args.smoke:
        sizes, repeats, inner_override = [10], 1, 10

    # No upstream is expanded, so the region lookup would only touch the mocked session.
    mock.patch.object(DepContext, "has_regions", return_value=False).start()
    if not THRESHOLD_AVAILABLE:
        print("airflow.sdk.Threshold not importable: skipping all threshold:<preset> rows")

    results: dict = {}
    for fan_in in sizes:
        inner = inner_override or DEFAULT_INNER.get(fan_in, max(2, 20000 // fan_in))
        # Longer samples shrink the share of a preemption in each measurement; BENCH_INNER_SCALE=4 quarters it.
        inner = int(inner * float(os.environ.get("BENCH_INNER_SCALE", "1")))
        sdag, task_ids = build_dag(fan_in)
        finished = make_finished(sdag, fan_in)
        session = mock.MagicMock()

        runners = make_runners(sdag, task_ids, finished, session)
        for run in runners.values():  # warm up
            run()
        if session.method_calls or session.mock_calls:
            raise SystemExit(f"session was touched at F={fan_in}: {session.mock_calls[:3]}")
        gc.freeze()

        samples: dict[str, list[float]] = {label: [] for label in runners}
        for _ in range(repeats):
            for label, run in runners.items():
                samples[label].append(time_rule(run, inner))
        results[str(fan_in)] = {
            label: {"median_us": statistics.median(v), "min_us": min(v), "all_us": v}
            for label, v in samples.items()
        }

    print(f"{'rule':<42}" + "".join(f"{'F=' + str(s) + ' med/min us':>26}" for s in sizes))
    for label in results[str(sizes[0])]:
        row = "".join(
            f"{results[str(s)][label]['median_us']:>14.1f}{results[str(s)][label]['min_us']:>12.1f}"
            for s in sizes
        )
        print(f"{label:<42}{row}")

    if args.json:
        with open(args.json, "w") as fh:
            json.dump(
                {
                    "python": sys.version,
                    "git_head": get_git_head(),
                    "sizes": sizes,
                    "results": results,
                    "threshold_available": THRESHOLD_AVAILABLE,
                },
                fh,
                indent=1,
            )


if __name__ == "__main__":
    main()
