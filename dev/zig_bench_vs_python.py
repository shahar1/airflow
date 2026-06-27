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
Benchmark: native-Zig Dag processor vs the Python equivalent, on the same corpus.

Generates N runnable Dags (EmptyOperator chains + TaskFlow) in --dags-dir, then times:
  * Python: import each file via BundleDagBag (executes it) + DagSerialization.to_dict
  * Zig:    zigdagproc --bench (static tree-sitter parse + serialize, no Python)
Both measure parse+serialize (the differentiating cost); DB write is the same Postgres for
both and is excluded. The Python side ALSO pays one cold `import airflow` the Zig side never does.

Run (from repo root):
  uv run --project airflow-core python dev/zig_bench_vs_python.py --n 200 --dags-dir files/dags
"""

from __future__ import annotations

import argparse
import shutil
import statistics
import subprocess
import time
from pathlib import Path

ZIG_BIN = Path(__file__).resolve().parent / "zigdagproc" / "zig-out" / "bin" / "zigdagproc"

CHAIN = """from datetime import datetime
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG(dag_id="zigbench_chain_{i}", schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False):
    prev = None
    tasks = [EmptyOperator(task_id=f"t{{j}}") for j in range({n})]
    for a, b in zip(tasks, tasks[1:]):
        a >> b
"""

# A statically-written chain (no python loop) so the Zig static parser sees explicit tasks/edges.
CHAIN_STATIC_HEAD = """from datetime import datetime
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG(dag_id="zigbench_chain_{i}", schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False):
"""

TASKFLOW = """from datetime import datetime
from airflow.sdk import dag, task

@dag(dag_id="zigbench_tf_{i}", schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def zigbench_tf_{i}():
    @task
    def extract():
        return 1

    @task
    def transform(x):
        return x + 1

    @task
    def load(y):
        print(y)

    load(transform(extract()))

zigbench_tf_{i}()
"""


def write_chain(path: Path, i: int, n: int) -> None:
    lines = [CHAIN_STATIC_HEAD.format(i=i)]
    for j in range(n):
        lines.append(f'    t{j} = EmptyOperator(task_id="t{j}")\n')
    for j in range(n - 1):
        lines.append(f"    t{j} >> t{j + 1}\n")
    path.write_text("".join(lines))


def gen_corpus(dags_dir: Path, n: int) -> list[Path]:
    files = []
    for i in range(n):
        if i % 4 == 0:
            p = dags_dir / f"zigbench_tf_{i}.py"
            p.write_text(TASKFLOW.format(i=i))
        else:
            p = dags_dir / f"zigbench_chain_{i}.py"
            write_chain(p, i, n=5 + (i % 20))  # 5..24 tasks
        files.append(p)
    return files


def bench_python(files: list[Path]) -> tuple[float, int]:
    from airflow.dag_processing.dagbag import BundleDagBag
    from airflow.serialization.serialized_objects import DagSerialization

    parent = files[0].parent
    t0 = time.monotonic()
    n = 0
    for f in files:
        bag = BundleDagBag(dag_folder=str(f), bundle_path=parent, bundle_name="bench", load_op_links=False)
        for d in bag.dags.values():
            DagSerialization.to_dict(d)
            n += 1
    return time.monotonic() - t0, n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--dags-dir", default="files/dags")
    ap.add_argument("--keep", action="store_true", help="keep the generated corpus")
    args = ap.parse_args()

    if not ZIG_BIN.exists():
        raise SystemExit(f"build first: (cd {ZIG_BIN.parents[2]} && zig build)")

    dags_dir = Path(args.dags_dir).resolve()
    bench_dir = dags_dir / "_zigbench"
    if bench_dir.exists():
        shutil.rmtree(bench_dir)
    bench_dir.mkdir(parents=True)
    files = gen_corpus(bench_dir, args.n)
    print(f"corpus: {len(files)} Dags in {bench_dir}")

    # Python (cold import included — that is the real per-process cost).
    py_t, py_n = bench_python(files)

    # Zig: --bench over the same dir (median of 3, all warm; first run primes the page cache).
    zig_samples = []
    out = ""
    for _ in range(3):
        r = subprocess.run(
            [str(ZIG_BIN), "--bench", "--dags-dir", str(bench_dir)], capture_output=True, text=True
        )
        out = r.stderr.strip()
        # parse "zig: N dags in T ms = ..."
        ms = float(out.split(" in ")[1].split(" ms")[0])
        zig_samples.append(ms)
    zig_ms = statistics.median(zig_samples)

    py_ms = py_t * 1000
    print(f"\n  {'':10}{'dags':>6}{'total ms':>12}{'ms/dag':>10}{'dags/sec':>12}")
    print("  " + "-" * 50)
    print(f"  {'python':10}{py_n:>6}{py_ms:>12.1f}{py_ms / py_n:>10.3f}{py_n / (py_ms / 1000):>12.0f}")
    print(f"  {'zig':10}{args.n:>6}{zig_ms:>12.1f}{zig_ms / args.n:>10.3f}{args.n / (zig_ms / 1000):>12.0f}")
    print(f"\n  speedup: {py_ms / zig_ms:.0f}x  ({out})")
    print("  note: python pays a one-time `import airflow`; zig pays it zero times (the point).")

    if not args.keep:
        shutil.rmtree(bench_dir)


if __name__ == "__main__":
    main()
