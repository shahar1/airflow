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

import contextlib
import importlib
import os
import sys
import traceback
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, BinaryIO, ClassVar, Literal

import attrs
from pydantic import BaseModel, Field, TypeAdapter

from airflow.callbacks.callback_requests import (
    CallbackRequest,
    DagCallbackRequest,
    TaskCallbackRequest,
)
from airflow.configuration import conf
from airflow.models.dagbag import DagBag
from airflow.sdk.execution_time.comms import (
    ConnectionResult,
    DeleteVariable,
    ErrorResponse,
    GetConnection,
    GetVariable,
    OKResponse,
    PutVariable,
    VariableResult,
)
from airflow.sdk.execution_time.supervisor import WatchedSubprocess
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.serialization.serialized_objects import LazyDeserializedDAG, SerializedDAG
from airflow.stats import Stats
from airflow.utils.file import iter_airflow_imports
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
    from airflow.sdk.api.client import Client
    from airflow.sdk.definitions.context import Context
    from airflow.typing_compat import Self


class DagFileParseRequest(BaseModel):
    """
    Request for DAG File Parsing.

    This is the request that the manager will send to the DAG parser with the dag file and
    any other necessary metadata.
    """

    file: str

    bundle_path: Path
    """Passing bundle path around lets us figure out relative file path."""

    callback_requests: list[CallbackRequest] = Field(default_factory=list)
    type: Literal["DagFileParseRequest"] = "DagFileParseRequest"


class DagFileParsingResult(BaseModel):
    """
    Result of DAG File Parsing.

    This is the result of a successful DAG parse, in this class, we gather all serialized DAGs,
    import errors and warnings to send back to the scheduler to store in the DB.
    """

    fileloc: str
    serialized_dags: list[LazyDeserializedDAG]
    warnings: list | None = None
    import_errors: dict[str, str] | None = None
    type: Literal["DagFileParsingResult"] = "DagFileParsingResult"


ToManager = Annotated[
    DagFileParsingResult | GetConnection | GetVariable | PutVariable | DeleteVariable,
    Field(discriminator="type"),
]

ToDagProcessor = Annotated[
    DagFileParseRequest | ConnectionResult | VariableResult | ErrorResponse | OKResponse,
    Field(discriminator="type"),
]


def _pre_import_airflow_modules(file_path: str, log: FilteringBoundLogger) -> None:
    """
    Pre-import Airflow modules found in the given file.

    This prevents modules from being re-imported in each processing process,
    saving CPU time and memory.
    (The default value of "parsing_pre_import_modules" is set to True)

    :param file_path: Path to the file to scan for imports
    :param log: Logger instance to use for warnings
    """
    if not conf.getboolean("dag_processor", "parsing_pre_import_modules", fallback=True):
        return

    for module in iter_airflow_imports(file_path):
        try:
            importlib.import_module(module)
        except ModuleNotFoundError as e:
            log.warning("Error when trying to pre-import module '%s' found in %s: %s", module, file_path, e)


def _parse_file_entrypoint():
    import structlog

    from airflow.sdk.execution_time import comms, task_runner

    # Parse DAG file, send JSON back up!
    comms_decoder = comms.CommsDecoder[ToDagProcessor, ToManager](
        body_decoder=TypeAdapter[ToDagProcessor](ToDagProcessor),
    )

    msg = comms_decoder._get_response()
    if not isinstance(msg, DagFileParseRequest):
        raise RuntimeError(f"Required first message to be a DagFileParseRequest, it was {msg}")

    task_runner.SUPERVISOR_COMMS = comms_decoder
    log = structlog.get_logger(logger_name="task")

    # Put bundle root on sys.path if needed. This allows the dag bundle to add
    # code in util modules to be shared between files within the same bundle.
    if (bundle_root := os.fspath(msg.bundle_path)) not in sys.path:
        sys.path.append(bundle_root)

    result = _parse_file(msg, log)
    if result is not None:
        comms_decoder.send(result)


def _parse_file(msg: DagFileParseRequest, log: FilteringBoundLogger) -> DagFileParsingResult | None:
    # TODO: Set known_pool names on DagBag!

    bag = DagBag(
        dag_folder=msg.file,
        bundle_path=msg.bundle_path,
        include_examples=False,
        safe_mode=True,
        load_op_links=False,
    )
    if msg.callback_requests:
        # If the request is for callback, we shouldn't serialize the DAGs
        _execute_callbacks(bag, msg.callback_requests, log)
        return None

    serialized_dags, serialization_import_errors = _serialize_dags(bag, log)
    bag.import_errors.update(serialization_import_errors)
    dags = [LazyDeserializedDAG(data=serdag) for serdag in serialized_dags]
    result = DagFileParsingResult(
        fileloc=msg.file,
        serialized_dags=dags,
        import_errors=bag.import_errors,
        # TODO: Make `bag.dag_warnings` not return SQLA model objects
        warnings=[],
    )
    return result


def _serialize_dags(bag: DagBag, log: FilteringBoundLogger) -> tuple[list[dict], dict[str, str]]:
    serialization_import_errors = {}
    serialized_dags = []
    for dag in bag.dags.values():
        try:
            serialized_dag = SerializedDAG.to_dict(dag)
            serialized_dags.append(serialized_dag)
        except Exception:
            log.exception("Failed to serialize DAG: %s", dag.fileloc)
            dagbag_import_error_traceback_depth = conf.getint(
                "core", "dagbag_import_error_traceback_depth", fallback=None
            )
            serialization_import_errors[dag.fileloc] = traceback.format_exc(
                limit=-dagbag_import_error_traceback_depth
            )
    return serialized_dags, serialization_import_errors


def _execute_callbacks(
    dagbag: DagBag, callback_requests: list[CallbackRequest], log: FilteringBoundLogger
) -> None:
    for request in callback_requests:
        log.debug("Processing Callback Request", request=request.to_json())
        if isinstance(request, TaskCallbackRequest):
            _execute_task_callbacks(dagbag, request, log)
        if isinstance(request, DagCallbackRequest):
            _execute_dag_callbacks(dagbag, request, log)


def _execute_dag_callbacks(dagbag: DagBag, request: DagCallbackRequest, log: FilteringBoundLogger) -> None:
    dag = dagbag.dags[request.dag_id]

    callbacks = dag.on_failure_callback if request.is_failure_callback else dag.on_success_callback
    if not callbacks:
        log.warning("Callback requested, but dag didn't have any", dag_id=request.dag_id)
        return

    callbacks = callbacks if isinstance(callbacks, list) else [callbacks]
    # TODO:We need a proper context object!
    context: Context = {  # type: ignore[assignment]
        "dag": dag,
        "run_id": request.run_id,
        "reason": request.msg,
    }

    for callback in callbacks:
        log.info(
            "Executing on_%s dag callback",
            "failure" if request.is_failure_callback else "success",
            dag_id=request.dag_id,
        )
        try:
            callback(context)
        except Exception:
            log.exception("Callback failed", dag_id=request.dag_id)
            Stats.incr("dag.callback_exceptions", tags={"dag_id": request.dag_id})


def _execute_task_callbacks(dagbag: DagBag, request: TaskCallbackRequest, log: FilteringBoundLogger) -> None:
    if not request.is_failure_callback:
        log.warning(
            "Task callback requested but is not a failure callback",
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
        )
        return

    dag = dagbag.dags[request.ti.dag_id]
    task = dag.get_task(request.ti.task_id)

    if request.task_callback_type is TaskInstanceState.UP_FOR_RETRY:
        callbacks = task.on_retry_callback
    else:
        callbacks = task.on_failure_callback

    if not callbacks:
        log.warning(
            "Callback requested but no callback found",
            dag_id=request.ti.dag_id,
            task_id=request.ti.task_id,
            run_id=request.ti.run_id,
            ti_id=request.ti.id,
        )
        return

    callbacks = callbacks if isinstance(callbacks, Sequence) else [callbacks]
    ctx_from_server = request.context_from_server

    if ctx_from_server is not None:
        runtime_ti = RuntimeTaskInstance.model_construct(
            **request.ti.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=ctx_from_server,
            max_tries=ctx_from_server.max_tries,
        )
    else:
        runtime_ti = RuntimeTaskInstance.model_construct(
            **request.ti.model_dump(exclude_unset=True),
            task=task,
        )
    context = runtime_ti.get_template_context()

    def get_callback_representation(callback):
        with contextlib.suppress(AttributeError):
            return callback.__name__
        with contextlib.suppress(AttributeError):
            return callback.__class__.__name__
        return callback

    for idx, callback in enumerate(callbacks):
        callback_repr = get_callback_representation(callback)
        log.info("Executing Task callback at index %d: %s", idx, callback_repr)
        try:
            callback(context)
        except Exception:
            log.exception("Error in callback at index %d: %s", idx, callback_repr)


def in_process_api_server() -> InProcessExecutionAPI:
    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI

    api = InProcessExecutionAPI()
    return api


@attrs.define(kw_only=True)
class DagFileProcessorProcess(WatchedSubprocess):
    """
    Parses dags with Task SDK API.

    This class provides a wrapper and management around a subprocess to parse a specific DAG file.

    Since DAGs are written with the Task SDK, we need to parse them in a task SDK process such that
    we can use the Task SDK definitions when serializing. This prevents potential conflicts with classes
    in core Airflow.
    """

    logger_filehandle: BinaryIO
    parsing_result: DagFileParsingResult | None = None
    decoder: ClassVar[TypeAdapter[ToManager]] = TypeAdapter[ToManager](ToManager)

    client: Client
    """The HTTP client to use for communication with the API server."""

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        path: str | os.PathLike[str],
        bundle_path: Path,
        callbacks: list[CallbackRequest],
        target: Callable[[], None] = _parse_file_entrypoint,
        client: Client,
        **kwargs,
    ) -> Self:
        logger = kwargs["logger"]

        _pre_import_airflow_modules(os.fspath(path), logger)

        proc: Self = super().start(target=target, client=client, **kwargs)
        proc._on_child_started(callbacks, path, bundle_path)
        return proc

    def _on_child_started(
        self,
        callbacks: list[CallbackRequest],
        path: str | os.PathLike[str],
        bundle_path: Path,
    ) -> None:
        msg = DagFileParseRequest(
            file=os.fspath(path),
            bundle_path=bundle_path,
            callback_requests=callbacks,
        )
        self.send_msg(msg, request_id=0)

    def _handle_request(self, msg: ToManager, log: FilteringBoundLogger, req_id: int) -> None:  # type: ignore[override]
        from airflow.sdk.api.datamodels._generated import ConnectionResponse, VariableResponse

        resp: BaseModel | None = None
        dump_opts = {}
        if isinstance(msg, DagFileParsingResult):
            self.parsing_result = msg
        elif isinstance(msg, GetConnection):
            conn = self.client.connections.get(msg.conn_id)
            if isinstance(conn, ConnectionResponse):
                conn_result = ConnectionResult.from_conn_response(conn)
                resp = conn_result
                dump_opts = {"exclude_unset": True, "by_alias": True}
            else:
                resp = conn
        elif isinstance(msg, GetVariable):
            var = self.client.variables.get(msg.key)
            if isinstance(var, VariableResponse):
                var_result = VariableResult.from_variable_response(var)
                resp = var_result
                dump_opts = {"exclude_unset": True}
            else:
                resp = var
        elif isinstance(msg, PutVariable):
            self.client.variables.set(msg.key, msg.value, msg.description)
        elif isinstance(msg, DeleteVariable):
            resp = self.client.variables.delete(msg.key)
        else:
            log.error("Unhandled request", msg=msg)
            self.send_msg(
                None,
                request_id=req_id,
                error=ErrorResponse(
                    detail={"status_code": 400, "message": "Unhandled request"},
                ),
            )
            return

        self.send_msg(resp, request_id=req_id, error=None, **dump_opts)

    @property
    def is_ready(self) -> bool:
        if self._check_subprocess_exit() is None:
            # Process still alive, def can't be finished yet
            return False

        return not self._open_sockets

    def wait(self) -> int:
        raise NotImplementedError(f"Don't call wait on {type(self).__name__} objects")
