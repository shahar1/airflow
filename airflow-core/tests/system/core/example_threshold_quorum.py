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

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, DONE, FAILED_ANY, Atom, Threshold, task
from airflow.sdk.exceptions import AirflowSkipException

from tests_common.test_utils.watcher import watcher

DAG_ID = "example_threshold_quorum"

# [START howto_threshold_quorum]
with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:

    @task(retries=0)
    def copy_region(region: str) -> str:
        if region == "ap-south":
            raise AirflowSkipException(f"{region} has nothing to copy")
        return region

    # No failures and at most one skipped copy, which no TriggerRule enum value expresses.
    notify = EmptyOperator(
        task_id="notify",
        trigger_rule=Threshold(
            (
                Atom(FAILED_ANY, "==", 0, "upstream_failed"),
                Atom({"skipped"}, "<=", 1, "skipped"),
                Atom(DONE, "==", "all", None),
            )
        ),
    )
    copies = [
        copy_region.override(task_id=f"copy_{name}")(region)
        for name, region in [("eu", "eu-west"), ("us", "us-east"), ("ap", "ap-south"), ("sa", "sa-east")]
    ]
    copies >> notify
    # [END howto_threshold_quorum]

    # Only the consumer is watched: one upstream is skipped by design.
    notify >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
