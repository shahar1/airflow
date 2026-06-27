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
# Build and run the native-Zig Dag processor against the breeze-published Postgres.
#
# Prerequisites: a breeze environment with the metadata DB up and migrated, e.g.
#   breeze start-airflow --backend postgres        # (in another terminal)
# That publishes Postgres on host port 25433 (POSTGRES_HOST_PORT) with the schema created.
#
# Usage:
#   ./run.sh                 # watch ./dags, write new specs into the breeze DB
#   ./run.sh --once          # process everything once and exit
#   ZIGDAGPROC_DSN=... ./run.sh --once   # override the connection string
set -euo pipefail
cd "$(dirname "$0")"

zig build

DSN="${ZIGDAGPROC_DSN:-host=127.0.0.1 port=25433 user=postgres password=airflow dbname=airflow}"
exec ./zig-out/bin/zigdagproc --dsn "$DSN" --dags-dir dags "$@"
