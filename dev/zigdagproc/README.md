<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# zigdagproc — a native-Zig Dag processor (proof of concept)

A minimal Dag processor written in **Zig 0.17**, with **zero Python at parse time**. It
watches a directory for **real, runnable Python Dag files**, parses each **statically** over
a **tree-sitter-python AST** (it never executes the file), turns it into Airflow's exact v3
serialized-DAG blob, and writes the rows straight into the Postgres metadata DB via **libpq**.

The Python front-end uses [`zig-tree-sitter`](https://github.com/tree-sitter/zig-tree-sitter)
+ the tree-sitter-python grammar, both **vendored** under `vendor/` and compiled directly
(their `build.zig` scripts use the removed `b.build_root` and don't build on zig-0.17-dev).

## Why this exists

Profiling the real Python processor (`dev/zig_dagproc_phase0.py`) showed **96.7%** of
per-file wall-clock is `import airflow` — a cost that exists *only because parsing a Dag
means executing arbitrary Python*. Constrain Dags to a **declarative, non-executing** form
and that cost disappears entirely: no interpreter, no `import airflow`, no GIL. What's left
is a mechanical transform that the Python PoC measured at ~230× cheaper than
construct+serialize. This program is the native-Zig realisation of that fast path.

## Scope (v1 — deliberately small)

- Input: real `.py` Dag files in the **declarative subset** (see below) — `with DAG(...)`,
  **EmptyOperator** tasks, `>>`/`<<` dependencies. These are genuine, runnable Airflow Dags.
- Output: `dag`, `dag_version`, `serialized_dag`, `dag_code` rows (+ a `dag_bundle` row),
  written transactionally. Mirrors Airflow's real behaviour: **upsert** the dag, **version-bump**
  on content change, **no-op** when the `dag_hash` is unchanged. Never deletes (a `task_instance`
  may reference an older `dag_version`).
- Watcher: polls the dags dir and processes **new** files (edit-detection via mtime is a
  follow-up; re-runs are idempotent regardless).

Because parsing is a real tree-sitter AST walk (not token matching), it handles arbitrary
Python *syntax* — multi-line calls, inline comments, whitespace, nesting. What's bounded is
the *semantic* subset it extracts. **Covered:** classic `with DAG()` + `EmptyOperator`,
chains/fan-out/`<<` deps, cron preset aliases, and **TaskFlow `@dag`/`@task`** (the call graph
`load(transform(extract()))` — nested-call and assignment data-flow — resolves to deps + the
`op_args` xcom-pull templates). Not yet covered (some **fundamentally** un-coverable statically
— these route to the Python fallback): non-Empty/non-`@task` operators, `op_kwargs` and literal
task args, `range()`-loop unrolling, `.expand()` mapping, assets/deadlines/params,
`@once`/`@continuous`, and any task set that depends on runtime I/O.

## Supported Python subset

Recognised statically (no Python executed):

```python
with DAG(
    dag_id="zig_example_fanout",
    schedule="@daily",             # cron string or preset alias; None -> NullTimetable
    start_date=datetime(2024, 1, 1),
):
    start = EmptyOperator(task_id="start")
    a = EmptyOperator(task_id="branch_a")
    b = EmptyOperator(task_id="branch_b")
    end = EmptyOperator(task_id="end")
    start >> [a, b] >> end          # chains, fan-out lists, and << all work
```

Anything outside the subset is ignored. See `dags/*.py`.

## Build & test

```bash
cd dev/zigdagproc
zig build            # produces zig-out/bin/zigdagproc
zig build test       # unit tests (epoch math, AST parse, canonical serialization)
```

Needs `libpq` (`/usr/include/postgresql`). The tree-sitter runtime + Python grammar are
vendored under `vendor/` and compiled by the build (`-std=gnu11`) — no network fetch needed.

## Run it inside breeze (the `--use-zig-dag-processor` flag)

```bash
cd dev/zigdagproc && zig build                 # build the host binary (ABI-compatible with the container)
breeze start-airflow -b postgres --use-zig-dag-processor
```

The flag makes breeze run the native-Zig processor **in place of** `airflow dag-processor` (in its
own tmux/mprocs pane), watching `/files/dags` and writing to the metadata DB the scheduler/UI read.
The host-built binary is mounted into the container and runs there directly (glibc/libpq compatible —
no in-container build). `AIRFLOW__CORE__DAGS_FOLDER=/files/dags` is set so the `dags-folder` bundle
matches. If the binary isn't built, the launcher prints a hint and falls back to the Python processor.

Wiring: `--use-zig-dag-processor` (breeze) -> `USE_ZIG_DAG_PROCESSOR` env -> `run_tmux` /
`generate_mprocs_config.py` -> `scripts/in_container/bin/run_zig_dag_processor` -> the binary.

### Hybrid fallback — every Dag still loads

Zig only serialises its subset (EmptyOperator + TaskFlow `@task`). To avoid dropping everything
else, pass `--fallback <script>`: files Zig can't parse are **batched and handed to the real
Python processor** (`python3 <fallback> <file...>`), which writes them into the same `dags-folder`
bundle. `import airflow` is paid once per batch, and Zig keeps fast-pathing the dags it knows —
so a CloudRun/Bigtable/`@task_group` Dag loads via Python while an EmptyOperator chain loads via
Zig. The breeze launcher sets `--fallback dev/zigdagproc/fallback.py` automatically.

### Or replace it manually (host)

The scheduler/UI read serialized Dags **from the DB** — they don't care who wrote them. So you can
also stop the Python `dag-processor` and run this from the host against the published Postgres:

```bash
breeze start-airflow --backend postgres --load-example-dags   # terminal 1 (DB on host :25433)
# in the breeze tmux, Ctrl-C the "dag-processor" pane, then:
cd dev/zigdagproc && ./breeze-processor.sh                    # terminal 2
```

`breeze-processor.sh` runs the host binary (zig is on the host; libpq talks to the published
Postgres on 25433) over `files/dags` (which breeze mounts at `/files/dags`), writing with the
`dags-folder` bundle the scheduler reads. Point that bundle at `files/dags` by setting
`AIRFLOW__CORE__DAGS_FOLDER=/files/dags` (added to `files/airflow-breeze-config/init.sh`).

**Verified fully compatible as the replacement processor.** Against the breeze Postgres, the
Dags Zig writes are first-class to Airflow's own tooling: `airflow dags list` (reads
`serialized_dag`), `dags details`, `tasks list` (enumerates the operators), and `dags trigger`
(creates a queued dag run) all work. The rows Zig writes — `dag` / `dag_version` /
`serialized_dag` / `dag_code` / `dag_bundle` — are accepted unchanged.

**Fully interchangeable with the Python dag-processor.** Zig computes a **byte-identical
`dag_hash`** to Airflow's `SerializedDagModel.hash` (verified by the oracle: `hash_identical=True`
for all sample Dags). It does this by re-serialising its own blob through a canonical writer that
mirrors `_sort_serialized_dag_dict` (sort keys, sort tasks by `task_id`, sort string lists) +
`json.dumps` defaults (`", "` / `": "` separators). So Zig and the Python processor agree on the
hash — neither re-versions the other's Dags, and you can run them interchangeably or swap one for
the other. The one deployment-coupled input is `_processor_dags_folder` (part of the hash): pass
`--processor-dags-folder /files/dags` (the container's `core.dags_folder`), which
`breeze-processor.sh` does. (Subdir filelocs use `basename` for `relative_fileloc`; flat folders
match exactly — nested bundles are a follow-up.)

## Benchmark vs the Python processor

`dev/zig_bench_vs_python.py` generates a corpus of supported Dags and times Zig (`--bench`,
static parse+serialize) against the Python equivalent (`BundleDagBag` execute + serialize):

```bash
uv run --project airflow-core python dev/zig_bench_vs_python.py --n 200 --dags-dir files/dags
# python (warm) 3.83 ms/dag ; zig 1.36 ms/dag -> ~3x  (warm-vs-warm, single-threaded)
```

This is the conservative number: Python is warm (one process, `import airflow` amortized).
Zig's larger wins — zero cold-start import, no GIL (thread-per-file), tiny RSS — are not in
this single-threaded parse+serialize figure.

## Run via breeze

`zigdagproc` runs on the host and talks to the **breeze-published Postgres** (host port
25433). Start breeze so the DB is up and migrated, then run the processor:

```bash
# terminal 1 — brings up Postgres and applies migrations (schema must exist)
breeze start-airflow --backend postgres

# terminal 2
cd dev/zigdagproc
./run.sh --once      # parse dags/*.json and write them into the breeze metadata DB
# or ./run.sh        # watch mode
```

Then the Dags appear in the Airflow UI / `breeze run airflow dags list`. Override the
connection with `ZIGDAGPROC_DSN=...` or `--dsn "host=... port=... user=... dbname=..."`.

## Verify serialization fidelity (no DB needed)

`--emit` prints the serialized blob + hash for one `.py`. The differential oracle parses
each Dag **two ways** — Zig's static parse vs Airflow's real `DagBag` (which *executes* the
file like the production processor) — and compares them:

```bash
cd dev/zigdagproc && zig build
cd ../.. && uv run --project airflow-core python dev/zig_verify_emit.py
# -> example_chain.py   OK   struct_identical=True ...
```

`struct_identical=True` means the data Zig stores from a *static* parse is identical to what
Airflow produces by *executing* the same file. The `dag_hash` is currently self-consistent but **not byte-identical**
to Python's (Python's `json.dumps` uses `", "`/`": "` separators and the deployment's
`_processor_dags_folder`); matching it exactly is a tracked follow-up and is only needed if
the Python processor and this one ever serialize the same Dag.

## Notes on Zig 0.17

This targets the `0.17.0-dev` series, which moved a lot of stdlib surface:
`@cImport` → `b.addTranslateC`; filesystem/clock/sleep are now threaded through `std.Io`
(`std.Io.Dir.cwd().readFileAlloc(io, ...)`, `std.Io.Timestamp.now(io, .real)`,
`std.Io.sleep`); `main` receives `std.process.Init`; `GeneralPurposeAllocator` →
`DebugAllocator`; `Allocator.dupeZ` → `allocSentinel`. libpq is linked via
`mod.linkSystemLibrary("pq", .{})` and bound with translate-c (`src/c.h`).

## Layout

```
build.zig                  # build: libpq translate-c, vendored tree-sitter C + binding, exe/test
src/main.zig               # entry: mode dispatch, per-file glue, inotify watch loop
src/config.zig             # CLI flags -> Config
src/dag.zig                # the declarative Dag data model (Spec/TaskSpec/Dep)
src/parser.zig             # tree-sitter-python AST -> dag.Spec (no Python executed)
src/serializer.zig         # dag.Spec -> v3 blob + byte-identical dag_hash
src/db.zig                 # libpq: upsert / version-bump / mark-stale, uuid7
src/watcher.zig            # inotify directory watcher + SIGTERM/SIGINT graceful shutdown
src/c.h                    # #include <libpq-fe.h>  (translate-c root)
dags/*.py                  # sample real, runnable Dags (declarative subset)
vendor/                    # tree-sitter runtime + python grammar + zig binding (compiled in)
breeze-processor.sh        # run on the host against the breeze Postgres
```

## Watcher & process management

`watch` mode (default) does an initial full scan, then uses **inotify** (`std.os.linux`, the
Linux primitive — std has no high-level file watcher) to react to changes: a created/modified
`.py` is re-parsed and written; a removed one marks its Dags stale. The inotify fd is polled with
a 1 s timeout so **SIGTERM/SIGINT** is observed promptly for a clean shutdown
([src/watcher.zig](src/watcher.zig)). Each file is processed in its own arena, so memory is bounded.
