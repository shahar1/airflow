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
"""A real, runnable TaskFlow Dag — @dag/@task, parsed statically by zigdagproc."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(dag_id="zig_taskflow", schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def zig_taskflow():
    @task
    def extract():
        return 1

    @task
    def transform(value):
        return value + 1

    @task
    def load(value):
        print(value)

    # nested-call form and assignment form both resolve to extract >> transform >> load
    load(transform(extract()))


zig_taskflow()
