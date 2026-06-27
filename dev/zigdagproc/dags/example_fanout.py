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
"""Fan-out/fan-in Dag — exercises list dependencies and messy formatting."""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG(
    dag_id="zig_example_fanout",
    schedule="@daily",  # a preset-looking cron alias as a plain string
    start_date=datetime(2024, 3, 15),
    catchup=False,
):
    start = EmptyOperator(task_id="start")
    a = EmptyOperator(task_id="branch_a")
    b = EmptyOperator(
        task_id="branch_b",  # multi-line call with a trailing comment
    )
    end = EmptyOperator(task_id="end")

    start >> [a, b] >> end
