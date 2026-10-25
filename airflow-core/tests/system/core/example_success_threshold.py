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
from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, task

from tests_common.test_utils.watcher import watcher

DAG_ID = "example_success_threshold"

# [START howto_success_threshold]
with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    partitions = [1, 2, 3, 4]

    @task(retries=0)
    def process(partition: int) -> int:
        if partition == 3:
            raise ValueError(f"partition {partition} failed")
        return partition * 10

    @task
    def aggregate(results) -> int:
        return sum(results)

    aggregate_task = aggregate(process.expand(partition=partitions).with_success_threshold(3))
    # [END howto_success_threshold]

    # The mapped task fails by design, so only the consumer is watched.
    aggregate_task >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
