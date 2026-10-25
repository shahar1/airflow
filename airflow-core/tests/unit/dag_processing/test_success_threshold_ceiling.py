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

import textwrap

import pytest
import structlog

from airflow.dag_processing.dagbag import DagBag
from airflow.dag_processing.processor import _serialize_dags

from tests_common.test_utils.config import conf_vars

CEILING = ("core", "serialized_dag_max_write_version")

DAG_FILE = textwrap.dedent(
    """
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG

    with DAG("thresholded", schedule=None):
        BashOperator.partial(task_id="t").expand(bash_command=["a", "b"]).with_success_threshold(1)
    """
)


@pytest.fixture
def dag_bag(tmp_path):
    (tmp_path / "thresholded.py").write_text(DAG_FILE)
    return DagBag(dag_folder=tmp_path / "thresholded.py", bundle_path=tmp_path, bundle_name="testing")


def test_default_ceiling_is_an_import_error_for_the_file(dag_bag):
    serialized_dags, import_errors = _serialize_dags(dag_bag, structlog.get_logger())

    assert serialized_dags == []
    assert "serialized_dag_max_write_version" in import_errors["thresholded.py"]


@conf_vars({CEILING: "4"})
def test_raised_ceiling_serializes_at_version_4(dag_bag):
    serialized_dags, import_errors = _serialize_dags(dag_bag, structlog.get_logger())

    assert import_errors == {}
    assert serialized_dags[0].data["__version"] == 4
