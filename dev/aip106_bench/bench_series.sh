#!/usr/bin/env bash
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
# Layer-1 series: alternate base (upstream/main) and HEAD, BENCH_RUNS (15) runs each, sizes 10/100/1000, pinned to BENCH_CORE (15).
set -u
ROOT=$(git rev-parse --show-toplevel)
OUT=${BENCH_OUT:-$ROOT/files/aip106-bench}
CORE=${BENCH_CORE:-$(( $(nproc) - 1 ))}
export PYTHONHASHSEED=0
# The write ceiling is read at import time; 5 lets the head arm serialize Threshold Dags, main ignores it.
export AIRFLOW__CORE__SERIALIZED_DAG_MAX_WRITE_VERSION=5
export BENCH_INNER_SCALE=${BENCH_INNER_SCALE:-4}
BASE=${BENCH_BASE_WT:?set BENCH_BASE_WT to a worktree checked out at the base commit}
HEAD=$ROOT
for i in $(seq 1 "${BENCH_RUNS:-15}"); do
  mkdir -p "$OUT"
  for arm in base head; do
    wt=$([[ $arm == base ]] && echo "$BASE" || echo "$HEAD")
    # The script is read from this checkout so the base arm needs no copy of it; the base arm's own airflow is used.
    ( cd "$wt" && AIRFLOW_HOME=$(mktemp -d) taskset -c "$CORE" uv run --project airflow-core python "$ROOT/dev/aip106_bench/bench_layer1.py" --repeats "${BENCH_REPEATS:-5}" --sizes 10,100,1000 --json "$OUT/layer1-$arm-$i.json" ) > "$OUT/layer1-$arm-$i.log" 2>&1
    rc=$?
    echo "$(date -u +%T) run $i $arm exit=$rc"
    if [[ $rc != 0 ]]; then echo "stopping: see $OUT/layer1-$arm-$i.log"; exit "$rc"; fi
  done
done
