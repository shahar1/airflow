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

DAG_ID = "example_threshold_min_one_success"

# [START howto_threshold_min_one_success]
with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:

    @task
    def check_schema() -> None:
        pass

    @task
    def check_freshness() -> None:
        pass

    @task(retries=0)
    def check_optional_source() -> None:
        raise AirflowSkipException("optional source not configured")

    # The atoms are evaluated in order and the first one that does not pass decides the routing.
    publish = EmptyOperator(
        task_id="publish",
        trigger_rule=Threshold(
            (
                # Any failed or upstream_failed upstream: the consumer becomes upstream_failed.
                Atom(FAILED_ANY, "==", 0, "upstream_failed"),
                # No success at all: skipped when every upstream was skipped, else upstream_failed.
                Atom({"success"}, ">=", 1, "upstream_failed"),
                # Wait until every upstream is done; otherwise keep waiting (no routing).
                Atom(DONE, "==", "all", None),
            )
        ),
    )
    [check_schema(), check_freshness(), check_optional_source()] >> publish
    # [END howto_threshold_min_one_success]

    # Only the consumer is watched: one upstream is skipped by design.
    publish >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
