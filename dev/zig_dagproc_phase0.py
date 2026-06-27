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
Phase-0 measurement gate for the "Zig Dag processor" investigation.

Question this answers: of the per-file Dag-processing wall-clock, how much is
work a Zig rewrite could remove (process fork/exec + IPC framing) vs work that
stays in CPython no matter what (import user code, build DAGs, serialize, DB write)?

Buckets measured per file:
  bare_fork_exec    : spawn a trivial `python -c pass`            -> Zig-removable
  import_airflow    : extra cost of `import ...processor` in a    -> warm-interpreter
                      fresh subprocess over bare_fork_exec           amortizable (embed/forkserver)
  parse             : BundleDagBag(file): import user code + build -> CPython only
  serialize         : DagSerialization.to_dict per dag            -> CPython only
  ipc_roundtrip     : msgpack pack+unpack of the serialized data  -> Zig-removable
  db_write          : sync_bag_to_db against configured DB        -> CPython/DB, keep

Run inside breeze (needs airflow importable; postgres for the db_write bucket):
  breeze run python dev/zig_dagproc_phase0.py
  breeze run python dev/zig_dagproc_phase0.py --small 20 --medium 10 --large 3 --reps 3
"""

from __future__ import annotations

import argparse
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

DAG_TEMPLATE = """\
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum
{heavy}
with DAG("{dag_id}", schedule=None, start_date=pendulum.datetime(2024, 1, 1)):
    prev = None
    for i in range({n_tasks}):
        op = EmptyOperator(task_id=f"t{{i}}")
        if prev is not None:
            prev >> op
        prev = op
"""

# Simulates expensive top-level work (slow import / network call at module import time)
# that real-world fleets routinely have. ponytail: a sleep stands in for a heavy import
# without depending on numpy/pandas being installed.
HEAVY = "import time as _t; _t.sleep(0.5)"


def _timer():
    return time.monotonic()


def build_corpus(root: Path, small: int, medium: int, large: int, heavy: int) -> list[Path]:
    files: list[Path] = []
    specs = (
        [("small", 3)] * small
        + [("medium", 30)] * medium
        + [("large", 200)] * large
        + [("heavy", 10)] * heavy
    )
    for idx, (kind, n_tasks) in enumerate(specs):
        f = root / f"dag_{kind}_{idx}.py"
        f.write_text(
            DAG_TEMPLATE.format(
                dag_id=f"bench_{kind}_{idx}",
                n_tasks=n_tasks,
                heavy=HEAVY if kind == "heavy" else "",
            )
        )
        files.append(f)
    return files


def measure_subprocess(code: str, reps: int) -> float:
    samples = []
    for _ in range(reps):
        t0 = _timer()
        subprocess.run([sys.executable, "-c", code], check=True, capture_output=True)
        samples.append(_timer() - t0)
    return statistics.median(samples)


def measure_parse_serialize(files: list[Path], root: Path) -> tuple[float, float, list]:
    """Warm, in-process: import+build (parse) and DagSerialization.to_dict (serialize)."""
    from airflow.dag_processing.dagbag import BundleDagBag
    from airflow.serialization.serialized_objects import DagSerialization

    parse_total = 0.0
    serialize_total = 0.0
    last_bag = None
    for f in files:
        t0 = _timer()
        bag = BundleDagBag(
            dag_folder=str(f),
            bundle_path=root,
            bundle_name="bench",
            load_op_links=False,
        )
        parse_total += _timer() - t0

        for dag in bag.dags.values():
            t0 = _timer()
            DagSerialization.to_dict(dag)
            serialize_total += _timer() - t0
        last_bag = bag
    n = len(files)
    return parse_total / n, serialize_total / n, last_bag


def measure_ipc(sample_bag, reps: int) -> float:
    """msgpack pack+unpack of one serialized dag's data dict — the framing a Zig manager removes."""
    try:
        import msgpack
    except ImportError:
        return float("nan")
    if not sample_bag or not sample_bag.dags:
        return float("nan")
    from airflow.serialization.serialized_objects import DagSerialization

    data = DagSerialization.to_dict(next(iter(sample_bag.dags.values())))
    samples = []
    for _ in range(reps):
        t0 = _timer()
        packed = msgpack.packb(data, default=str)
        msgpack.unpackb(packed, raw=False)
        samples.append(_timer() - t0)
    return statistics.median(samples)


def measure_db_write(root: Path) -> float:
    """Full corpus through sync_bag_to_db against the configured DB. N/A if DB unreachable."""
    try:
        from airflow.dag_processing.dagbag import BundleDagBag, sync_bag_to_db

        bag = BundleDagBag(
            dag_folder=str(root),
            bundle_path=root,
            bundle_name="bench",
            load_op_links=False,
        )
        n = max(len(bag.dags), 1)
        t0 = _timer()
        sync_bag_to_db(bag, bundle_name="bench", bundle_version=None)
        return (_timer() - t0) / n
    except Exception as e:
        print(f"  db_write: N/A ({type(e).__name__}: {e})")
        return float("nan")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--small", type=int, default=20)
    p.add_argument("--medium", type=int, default=10)
    p.add_argument("--large", type=int, default=3)
    p.add_argument("--heavy", type=int, default=2)
    p.add_argument("--reps", type=int, default=3)
    args = p.parse_args()

    with tempfile.TemporaryDirectory(prefix="zig_dagproc_") as tmp:
        root = Path(tmp)
        files = build_corpus(root, args.small, args.medium, args.large, args.heavy)
        print(f"Corpus: {len(files)} files in {root}")
        print("Measuring (per-file medians/means)...\n")

        bare = measure_subprocess("pass", args.reps)
        with_airflow = measure_subprocess("import airflow.dag_processing.processor", args.reps)
        import_airflow = max(with_airflow - bare, 0.0)

        parse, serialize, sample_bag = measure_parse_serialize(files, root)
        ipc = measure_ipc(sample_bag, max(args.reps * 100, 100))
        db_write = measure_db_write(root)

    rows = [
        ("bare_fork_exec", bare, "Zig-removable"),
        ("import_airflow", import_airflow, "warm-amortizable"),
        ("parse", parse, "CPython only"),
        ("serialize", serialize, "CPython only"),
        ("ipc_roundtrip", ipc, "Zig-removable"),
        ("db_write", db_write, "CPython/DB keep"),
    ]
    measured = [(n, v, c) for n, v, c in rows if v == v]  # drop NaN
    total = sum(v for _, v, _ in measured)

    print(f"{'bucket':<16}{'ms/file':>12}{'% total':>10}  category")
    print("-" * 60)
    for name, val, cat in rows:
        if val != val:
            print(f"{name:<16}{'N/A':>12}{'':>10}  {cat}")
            continue
        print(f"{name:<16}{val * 1000:>12.2f}{val / total * 100:>9.1f}%  {cat}")
    print("-" * 60)

    zig_removable = sum(v for n, v, _ in measured if n in ("bare_fork_exec", "ipc_roundtrip"))
    warm = sum(v for n, v, _ in measured if n == "import_airflow")
    print(f"\nZig-removable orchestration (fork/exec + IPC): {zig_removable / total * 100:.1f}% of total")
    print(f"Warm-interpreter amortizable (import airflow):  {warm / total * 100:.1f}% of total")
    print(
        "\nGate: if 'Zig-removable' is single-digit %, a Zig orchestration rewrite "
        "cannot move throughput. The only lever is 'warm-amortizable', which Python "
        "forkserver/preimport already targets."
    )


if __name__ == "__main__":
    main()
