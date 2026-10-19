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
AIP-106 layer-2 benchmark: DagRun.update_state over real task instances in a database.

Not a regression test. Prints per-shape timing and per-statement-type query counts and writes them to
the file named by AIP106_BENCH_JSON. Run with:

  AIRFLOW_HOME=<isolated> AIP106_BENCH_JSON=out.json uv run --project airflow-core pytest \\
      airflow-core/tests/unit/models/test_aip106_layer2_bench.py -s -p no:cacheprovider
"""

from __future__ import annotations

import json
import os
import statistics
import time
from collections import Counter

import pytest
from sqlalchemy import event, select

from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task
from airflow.utils.state import TaskInstanceState

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]

PASSES = int(os.environ.get("AIP106_BENCH_PASSES", "30"))
RESULTS: list[dict] = []


def _statement_counter(engine):
    counts: Counter[str] = Counter()

    def before(conn, cursor, statement, parameters, context, executemany):
        counts[statement.lstrip().split(None, 1)[0].upper()] += 1

    event.listen(engine, "before_cursor_execute", before)
    return counts, lambda: event.remove(engine, "before_cursor_execute", before)


def _time_update_state(dr, session, label, fan_in, extra=None):
    import airflow.settings

    durations = []
    totals: Counter[str] = Counter()
    for _ in range(PASSES):
        counts, stop = _statement_counter(airflow.settings.engine)
        t0 = time.perf_counter()
        dr.update_state(session=session)
        durations.append(time.perf_counter() - t0)
        stop()
        totals.update(counts)
        session.expire_all()
    per_pass = {k: v / PASSES for k, v in totals.items()}
    row = dict(
        shape=label,
        F=fan_in,
        ms_median=statistics.median(durations) * 1e3,
        ms_min=min(durations) * 1e3,
        queries_per_pass=per_pass,
        **(extra or {}),
    )
    RESULTS.append(row)
    print(
        f"\n[aip106-bench] {label:<40} F={fan_in:<5} median {row['ms_median']:8.2f} ms  min {row['ms_min']:8.2f} ms  "
        f"queries/pass {dict(sorted(per_pass.items()))}"
    )
    return row


def _set_states(session, dr, task_id, state, limit=None):
    q = select(TI).where(TI.dag_id == dr.dag_id, TI.run_id == dr.run_id, TI.task_id == task_id)
    tis = session.scalars(q).all()
    for i, ti in enumerate(tis):
        if limit is not None and i >= limit:
            break
        ti.state = state
    session.flush()
    return len(tis)


@pytest.mark.parametrize("fan_in", [40, 1000])
def test_plain_fan_in(dag_maker, session, fan_in):
    with dag_maker(dag_id=f"aip106_plain_{fan_in}", serialized=True, session=session):
        down = EmptyOperator(task_id="down")
        for i in range(fan_in):
            EmptyOperator(task_id=f"up_{i}") >> down
    dr = dag_maker.create_dagrun(session=session)
    for i in range(fan_in):
        _set_states(session, dr, f"up_{i}", TaskInstanceState.SUCCESS)
    session.commit()
    _time_update_state(dr, session, "plain fan-in -> all_success", fan_in)


@pytest.mark.parametrize("fan_in", [20, 200])
@pytest.mark.parametrize("threshold", [None, "90%"])
def test_mapped_fan_in(dag_maker, session, fan_in, threshold):
    with dag_maker(
        dag_id=f"aip106_mapped_{fan_in}_{'thr' if threshold else 'plain'}", serialized=True, session=session
    ):

        @task
        def up(x):
            return x

        m = up.expand(x=list(range(fan_in)))
        down = EmptyOperator(task_id="down")
        m.operator >> down
    dr = dag_maker.create_dagrun(session=session)
    sdag = dr.get_dag()
    if threshold is not None:
        # Prototype: set the field on the deserialized operators the scheduler side evaluates, which is
        # what the deserialization post-pass would produce from a serialized ``success_threshold``.
        sdag.get_task("up").success_threshold = threshold
        sdag.get_task("down").thresholded_upstream_ids = frozenset({"up"})
    # Expand the mapped task so instances exist, then mark them finished.
    dr.update_state(session=session)
    session.commit()
    n = _set_states(session, dr, "up", TaskInstanceState.SUCCESS)
    session.commit()
    thr = getattr(sdag.get_task("up"), "success_threshold", None)
    label = f"mapped fan-in -> all_success ({'threshold ' + str(threshold) if threshold else 'no threshold'})"
    _time_update_state(dr, session, label, n, extra={"deserialized_threshold": thr})


def test_shared_upstream_set(dag_maker, session):
    ups, downs = 50, 100
    with dag_maker(dag_id="aip106_shared", serialized=True, session=session):
        up_ops = [EmptyOperator(task_id=f"up_{i}") for i in range(ups)]
        for j in range(downs):
            d = EmptyOperator(task_id=f"down_{j}")
            for u in up_ops:
                u >> d
    dr = dag_maker.create_dagrun(session=session)
    for i in range(ups):
        _set_states(session, dr, f"up_{i}", TaskInstanceState.SUCCESS)
    session.commit()
    _time_update_state(dr, session, "50 upstream -> 100 downstream (shared set)", ups)


def test_zz_write_results():
    path = os.environ.get("AIP106_BENCH_JSON")
    if path:
        with open(path, "w") as fh:
            json.dump({"passes": PASSES, "results": RESULTS}, fh, indent=1)
    assert RESULTS
