#
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

from unittest import mock

import pytest
from google.api_core.exceptions import InvalidArgument
from google.api_core.operation import Operation

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.hooks.operation_helpers import OperationHelper


@pytest.fixture
def operation():
    return mock.create_autospec(Operation, instance=True)


class TestWaitForOperationResult:
    def test_wrapped_google_api_call_error_keeps_the_original_message(self, operation):
        api_error = InvalidArgument("the resource is not ready")
        operation.result.side_effect = api_error

        with pytest.raises(AirflowException, match="the resource is not ready") as exc_info:
            OperationHelper.wait_for_operation_result(operation=operation)

        assert exc_info.value.__cause__ is api_error
        operation.exception.assert_not_called()

    def test_wraps_other_errors_without_polling_again(self, operation):
        timeout_error = TimeoutError("operation did not complete in time")
        operation.result.side_effect = timeout_error

        with pytest.raises(AirflowException, match="operation did not complete in time") as exc_info:
            OperationHelper.wait_for_operation_result(operation=operation, timeout=15)

        assert exc_info.value.__cause__ is timeout_error
        operation.exception.assert_not_called()
