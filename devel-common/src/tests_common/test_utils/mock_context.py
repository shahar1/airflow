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

import inspect
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any
from unittest import mock

from airflow.utils.context import Context

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def mock_context(task) -> Context:
    from airflow.models import TaskInstance
    from airflow.utils.session import NEW_SESSION
    from airflow.utils.state import TaskInstanceState
    from airflow.utils.xcom import XCOM_RETURN_KEY

    values: dict[str, Any] = {}

    class MockedTaskInstance(TaskInstance):
        def __init__(
            self,
            task,
            run_id: str | None = "run_id",
            state: str | None = TaskInstanceState.RUNNING,
            map_index: int = -1,
        ):
            # Inspect the parameters of TaskInstance.__init__
            init_sig = inspect.signature(super().__init__)
            if "dag_version_id" in init_sig.parameters:
                super().__init__(
                    task=task,
                    run_id=run_id,
                    state=state,
                    map_index=map_index,
                    dag_version_id=mock.MagicMock(),
                )
            else:
                super().__init__(
                    task=task,
                    run_id=run_id,
                    state=state,
                    map_index=map_index,
                )  # type: ignore[call-arg]
            self.values: dict[str, Any] = {}

        def xcom_pull(
            self,
            task_ids: str | Iterable[str] | None = None,
            dag_id: str | None = None,
            key: str = XCOM_RETURN_KEY,
            include_prior_dates: bool = False,
            session: Session = NEW_SESSION,
            *,
            map_indexes: int | Iterable[int] | None = None,
            default: Any = None,
            run_id: str | None = None,
        ) -> Any:
            key = f"{self.task_id}_{self.dag_id}_{key}"
            if map_indexes is not None and (not isinstance(map_indexes, int) or map_indexes >= 0):
                key += f"_{map_indexes}"
            return values.get(key, default)

        def xcom_push(self, key: str, value: Any, session: Session = NEW_SESSION, **kwargs) -> None:
            key = f"{self.task_id}_{self.dag_id}_{key}"
            if self.map_index is not None and self.map_index >= 0:
                key += f"_{self.map_index}"
            values[key] = value

    values["ti"] = MockedTaskInstance(task=task)

    # See https://github.com/python/mypy/issues/8890 - mypy does not support passing typed dict to TypedDict
    return Context(values)  # type: ignore[misc]
