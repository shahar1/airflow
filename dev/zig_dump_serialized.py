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
"""Dump the serialized-DAG JSON a Zig parser would have to emit for a trivial declarative Dag."""

from __future__ import annotations

import json

import pendulum

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.serialization.serialized_objects import DagSerialization

with DAG("declarative_demo", schedule="@daily", start_date=pendulum.datetime(2024, 1, 1)) as dag:
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")
    c = EmptyOperator(task_id="c")
    a >> b >> c

data = DagSerialization.to_dict(dag)
text = json.dumps(data, indent=2, default=str, sort_keys=True)
print(text)
print("\n==== STATS ====")
print(f"bytes: {len(text)}")
print(f"top-level keys: {sorted(data.keys())}")
print(f"dag keys: {sorted(data['dag'].keys())}")
print(f"task count: {len(data['dag'].get('tasks', []))}")
if data["dag"].get("tasks"):
    print(
        f"per-task keys: {sorted(data['dag']['tasks'][0].keys()) if isinstance(data['dag']['tasks'][0], dict) else type(data['dag']['tasks'][0])}"
    )
