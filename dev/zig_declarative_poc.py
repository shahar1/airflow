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
Proof of concept: a DECLARATIVE Dag spec -> serialized-DAG dict, with NO DAG construction
and NO operator import. This is exactly what a Zig fast-path parser would do.

Proves two things the throughput case depends on:
  1. Fidelity  -- the mechanical transform produces output identical to the real
                  DagSerialization.to_dict (same dict => same dag_hash).
  2. Speedup   -- emitting the blob from a spec is orders of magnitude cheaper than
                  constructing the DAG + operators and serializing it (the work Zig deletes).

Run (host venv, no DB needed):
  uv run --project airflow-core python dev/zig_declarative_poc.py
"""

from __future__ import annotations

import json
import statistics
import time

import pendulum

# ---------------------------------------------------------------------------
# Operator manifest: the per-operator constants a Zig parser would read from a
# JSON file generated once per Airflow release (introspected from the classes).
# Hand-written here for EmptyOperator only -- enough to prove the mechanism.
# ---------------------------------------------------------------------------
OPERATOR_MANIFEST = {
    "EmptyOperator": {
        "_task_module": "airflow.providers.standard.operators.empty",
        "task_type": "EmptyOperator",
        "ui_color": "#e8f7e4",
        "_is_empty": True,
        "template_fields": [],
        "retry_delay": 300.0,
        "has_retry_policy": False,
    },
}

SERIALIZER_VERSION = 3


def emit_task(task_id: str, operator: str, downstream: list[str]) -> dict:
    """Build the serialized form of one task -- mechanical, from the manifest + graph."""
    m = OPERATOR_MANIFEST[operator]
    var = {
        "_is_empty": m["_is_empty"],
        "_needs_expansion": False,
        "has_retry_policy": m["has_retry_policy"],
        "retry_delay": m["retry_delay"],
        "task_id": task_id,
        "task_type": m["task_type"],
        "template_fields": m["template_fields"],
        "ui_color": m["ui_color"],
        "_task_module": m["_task_module"],
    }
    if downstream:
        var["downstream_task_ids"] = downstream
    return {"__type": "operator", "__var": var}


def emit_serialized(spec: dict, fileloc: str, dags_folder: str) -> dict:
    """Declarative spec -> serialized-DAG dict. The Zig fast path, written in Python."""
    edges: dict[str, list[str]] = {t["task_id"]: [] for t in spec["tasks"]}
    for up, down in spec.get("dependencies", []):
        edges[up].append(down)

    tasks = [emit_task(t["task_id"], t["operator"], edges[t["task_id"]]) for t in spec["tasks"]]

    if spec.get("schedule"):
        timetable = {
            "__type": "airflow.timetables.trigger.CronTriggerTimetable",
            "__var": {
                "expression": spec["schedule"],
                "interval": 0.0,
                "run_immediately": False,
                "timezone": "UTC",
            },
        }
    else:
        timetable = {"__type": "airflow.timetables.simple.NullTimetable", "__var": {}}

    start_epoch = pendulum.parse(spec["start_date"]).timestamp()

    dag = {
        "_processor_dags_folder": dags_folder,
        "allowed_run_types": None,
        "catchup": False,
        "dag_dependencies": [],
        "dag_id": spec["dag_id"],
        "deadline": None,
        "disable_bundle_versioning": False,
        "edge_info": {},
        "fileloc": fileloc,
        "max_active_runs": 16,
        "max_active_tasks": 16,
        "max_consecutive_failed_dag_runs": 0,
        "params": [],
        "start_date": start_epoch,
        "task_group": {
            "_group_id": None,
            "children": {t["task_id"]: ["operator", t["task_id"]] for t in spec["tasks"]},
            "downstream_group_ids": [],
            "downstream_task_ids": [],
            "group_display_name": "",
            "prefix_group_id": True,
            "tooltip": "",
            "ui_color": "CornflowerBlue",
            "ui_fgcolor": "#000",
            "upstream_group_ids": [],
            "upstream_task_ids": [],
        },
        "tasks": tasks,
        "timetable": timetable,
        "timezone": "UTC",
    }
    return {"__version": SERIALIZER_VERSION, "dag": dag}


def build_real(spec: dict):
    """Construct the actual DAG + operators (the expensive path Zig deletes)."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import DAG

    with DAG(
        spec["dag_id"],
        schedule=spec.get("schedule"),
        start_date=pendulum.parse(spec["start_date"]),
    ) as dag:
        ops = {t["task_id"]: EmptyOperator(task_id=t["task_id"]) for t in spec["tasks"]}
        for up, down in spec.get("dependencies", []):
            ops[up] >> ops[down]
    return dag


def diff(a, b, path="") -> list[str]:
    out = []
    if type(a) is not type(b):
        return [f"{path}: type {type(a).__name__} != {type(b).__name__} ({a!r} vs {b!r})"]
    if isinstance(a, dict):
        for k in set(a) | set(b):
            if k not in a:
                out.append(f"{path}.{k}: missing in MINE (real={b[k]!r})")
            elif k not in b:
                out.append(f"{path}.{k}: extra in MINE ({a[k]!r})")
            else:
                out += diff(a[k], b[k], f"{path}.{k}")
    elif isinstance(a, list):
        if len(a) != len(b):
            out.append(f"{path}: len {len(a)} != {len(b)}")
        else:
            for i, (x, y) in enumerate(zip(a, b)):
                out += diff(x, y, f"{path}[{i}]")
    elif a != b:
        out.append(f"{path}: {a!r} != {b!r}")
    return out


def chain_spec(dag_id: str, n: int) -> dict:
    tasks = [{"task_id": f"t{i}", "operator": "EmptyOperator"} for i in range(n)]
    deps = [[f"t{i}", f"t{i + 1}"] for i in range(n - 1)]
    return {
        "dag_id": dag_id,
        "schedule": "0 0 * * *",
        "start_date": "2024-01-01",
        "tasks": tasks,
        "dependencies": deps,
    }


def main() -> None:
    from airflow.configuration import conf
    from airflow.serialization.serialized_objects import DagSerialization

    dags_folder = conf.get("core", "dags_folder")
    fileloc = __file__

    print("=== FIDELITY: declarative transform vs real DagSerialization.to_dict ===")
    ok = True
    for n in (1, 3, 10, 50):
        spec = chain_spec(f"poc_{n}", n)
        mine = emit_serialized(spec, fileloc=fileloc, dags_folder=dags_folder)
        real = DagSerialization.to_dict(build_real(spec))
        # Normalize the two volatile, environment-derived fields out of scope for this PoC:
        for d in (mine, real):
            d["dag"]["fileloc"] = fileloc
            d["dag"]["_processor_dags_folder"] = dags_folder
        # Compare what actually gets STORED: the JSON projection (str-enums -> str, tuples -> arrays).
        mine_j = json.loads(json.dumps(mine, default=str, sort_keys=True))
        real_j = json.loads(json.dumps(real, default=str, sort_keys=True))
        diffs = diff(mine_j["dag"], real_j["dag"], "dag")
        same_hash = json.dumps(mine_j, sort_keys=True) == json.dumps(real_j, sort_keys=True)
        status = "OK" if not diffs else f"{len(diffs)} DIFFS"
        print(f"  n={n:<4} {status}  hash-identical={same_hash}")
        if diffs:
            ok = False
            for d in diffs[:12]:
                print(f"      {d}")

    print("\n=== SPEEDUP: per-dag cost, declarative emit vs real construct+serialize ===")
    for n in (10, 50, 200):
        spec = chain_spec(f"bench_{n}", n)
        mine_t, real_t = [], []
        for _ in range(20):
            t0 = time.monotonic()
            emit_serialized(spec, fileloc=fileloc, dags_folder=dags_folder)
            mine_t.append(time.monotonic() - t0)
        for _ in range(20):
            t0 = time.monotonic()
            DagSerialization.to_dict(build_real(spec))
            real_t.append(time.monotonic() - t0)
        mt, rt = statistics.median(mine_t), statistics.median(real_t)
        print(
            f"  n={n:<4} declarative={mt * 1e6:8.1f}us  real_construct+serialize={rt * 1e3:7.2f}ms  speedup={rt / mt:6.0f}x"
        )

    print(
        "\nNote: the real path ALSO pays ~2125ms/file `import airflow` in a fresh process "
        "(Phase-0). The Zig binary pays it zero times. The speedups above are ON TOP of that."
    )
    if not ok:
        print("\nFidelity gaps above show exactly what a Zig emitter must match (differential-test target).")


if __name__ == "__main__":
    main()
