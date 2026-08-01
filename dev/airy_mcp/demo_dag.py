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
"""Daily sales summary. Copy to the Dag bundle to run the Airy self-healing demo."""

from __future__ import annotations

from typing import Any

import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

SALES: list[dict[str, Any]] = [
    {"region": "EMEA", "amount": 120},
    {"region": "AMER", "amount": 340},
    {"region": "APAC", "amount": 90},
]


def extract_sales() -> int:
    print(f"Extracted {len(SALES)} rows")
    return len(SALES)


def summarize_sales(column: str) -> int:
    total = sum(row[column] for row in SALES)
    print(f"Total {column}: {total}")
    return total


def report(total: str) -> None:
    print(f"Reporting total of {int(total):,}")


with DAG(
    dag_id="sales_summary",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
):
    extract = PythonOperator(task_id="extract", python_callable=extract_sales)
    summarize = PythonOperator(
        task_id="summarize",
        python_callable=summarize_sales,
        op_kwargs={"column": "ammount"},
    )
    load = PythonOperator(
        task_id="report",
        python_callable=report,
        op_kwargs={"total": "{{ ti.xcom_pull(task_ids='summarise') }}"},
    )

    extract >> summarize >> load
