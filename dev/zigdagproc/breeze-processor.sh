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
#
# Run the native-Zig Dag processor as a DROP-IN REPLACEMENT for Airflow's
# `airflow dag-processor`, against a running breeze stack.
#
# The scheduler/UI read serialized Dags from the metadata DB — they don't care
# which process wrote them. So "replacing the processor" is just: stop the Python
# dag-processor and run this instead, pointed at the same dags folder + DB.
#
# It runs ON THE HOST and talks to the breeze-published Postgres (port 25433) and
# the host-side files/dags (which breeze mounts into the container at /files/dags).
#
# Usage (with `breeze start-airflow` running in another terminal):
#   1. stop the Python processor inside breeze:   (in the breeze tmux) Ctrl-C the
#      "dag-processor" pane, OR run breeze without it.
#   2. ./breeze-processor.sh            # watch files/dags, write Dags into the breeze DB
#      ./breeze-processor.sh --once     # one shot
set -euo pipefail
cd "$(dirname "$0")"

zig build

# Host path of the breeze dags folder (mounted into the container at /files/dags).
REPO_ROOT="$(git -C "$PWD" rev-parse --show-toplevel)"
DAGS_DIR="${ZIGDAGPROC_DAGS:-$REPO_ROOT/files/dags}"
DSN="${ZIGDAGPROC_DSN:-host=127.0.0.1 port=25433 user=postgres password=airflow dbname=airflow}"

# --processor-dags-folder is the CONTAINER path breeze configures (core.dags_folder), so the
# dag_hash is byte-identical to what the Python dag-processor would compute -> fully interchangeable.
echo "zigdagproc: dags=$DAGS_DIR"
exec ./zig-out/bin/zigdagproc \
    --dags-dir "$DAGS_DIR" \
    --dsn "$DSN" \
    --bundle dags-folder \
    --processor-dags-folder /files/dags "$@"
