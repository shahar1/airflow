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

import sys
import types

import httpx
import pytest

# fastmcp only ships inside the Breeze image; stub it so the tool logic is
# testable anywhere.  The stub's ``tool`` is the identity, which is exactly how
# server.py registers its tools.
if "fastmcp" not in sys.modules:
    _stub = types.ModuleType("fastmcp")
    _stub.FastMCP = lambda *args, **kwargs: types.SimpleNamespace(tool=lambda fn: fn)  # type: ignore[attr-defined]
    sys.modules["fastmcp"] = _stub

import server

DAG_ID = "sales_summary"
SOURCE = 'op_kwargs={"column": "ammount"}\nprint("ammount is a typo")\n'


class FakeAirflow:
    """Minimal stand-in for the Airflow REST API."""

    def __init__(self, relative_fileloc: str = "sales_summary.py"):
        self.relative_fileloc = relative_fileloc
        self.is_paused = False
        self.version = 1
        self.runs: list[dict] = []
        self.task_instances: list[dict] = []
        self.log: object = ""
        self.calls: list[tuple[str, str]] = []
        self.payloads: list[object] = []
        self.bump_version_on_reparse = True
        self.fail_reparse: Exception | None = None
        self.reparse_status = 0

    def __call__(self, method: str, path: str, **kwargs):
        self.calls.append((method, path))
        self.payloads.append(kwargs.get("json"))
        if "headers" in kwargs:
            raise TypeError("_api() got multiple values for keyword argument 'headers'")
        if path == f"/dags/{DAG_ID}":
            if method == "PATCH":
                self.is_paused = kwargs["json"]["is_paused"]
                return {}
            return {
                "relative_fileloc": self.relative_fileloc,
                "file_token": "tok",
                "is_paused": self.is_paused,
            }
        if path == f"/dags/{DAG_ID}/dagVersions":
            return {"dag_versions": [{"version_number": self.version}]}
        if path.startswith("/parseDagFile/"):
            if self.reparse_status:
                raise httpx.HTTPStatusError(
                    "conflict",
                    request=httpx.Request("PUT", path),
                    response=httpx.Response(self.reparse_status),
                )
            if self.fail_reparse:
                raise self.fail_reparse
            if self.bump_version_on_reparse:
                self.version += 1
            return None
        if path == f"/dags/{DAG_ID}/dagRuns":
            if method == "POST":
                return {"dag_run_id": "manual__new", "state": "queued"}
            return {"dag_runs": self.runs}
        if path.endswith("/taskInstances"):
            return {"task_instances": self.task_instances}
        if "/logs/" in path:
            return {"content": self.log}
        raise AssertionError(f"unexpected API call {method} {path}")


@pytest.fixture
def airflow(monkeypatch, tmp_path):
    fake = FakeAirflow()
    monkeypatch.setattr(server, "_api", fake)
    monkeypatch.setattr(server, "DAGS_DIR", tmp_path)
    monkeypatch.setattr(server, "REPARSE_TIMEOUT_S", 2.0)
    (tmp_path / "sales_summary.py").write_text(SOURCE)
    return fake


@pytest.fixture
def escape_target(tmp_path):
    """A real file outside the jail, plus the ways of reaching it from inside."""
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir(exist_ok=True)
    target = outside / "escape.py"
    target.write_text("stolen = True\n")
    (tmp_path / "link.py").symlink_to(target)
    (tmp_path / "sales_summary.txt").write_text("not python\n")
    return target


@pytest.mark.parametrize(
    "relative_fileloc",
    ["../{outside}/escape.py", "{absolute}", "link.py", "sales_summary.txt"],
    ids=["traversal", "absolute", "symlink", "not-python"],
)
def test_dag_path_rejects_escapes_from_the_bundle(airflow, escape_target, relative_fileloc):
    # Every target exists, so only the jail check itself can reject them.
    airflow.relative_fileloc = relative_fileloc.format(
        outside=escape_target.parent.name, absolute=escape_target
    )
    with pytest.raises(server.DagFileError, match="outside the editable"):
        server._dag_path(DAG_ID)


def test_dag_path_rejects_a_missing_file(airflow):
    airflow.relative_fileloc = "missing.py"
    with pytest.raises(server.DagFileError, match="does not exist"):
        server._dag_path(DAG_ID)


def test_dag_path_requires_a_file_location(airflow):
    airflow.relative_fileloc = ""
    with pytest.raises(server.DagFileError, match="no file location"):
        server._dag_path(DAG_ID)


def test_dag_url_escapes_ids():
    assert server._dag_url("../variables/secret") == "/dags/..%2Fvariables%2Fsecret"
    assert server._dag_url("my.dag-1", "/dagRuns") == "/dags/my.dag-1/dagRuns"


def test_diagnose_dag_without_runs(airflow):
    assert server.diagnose_dag(DAG_ID) == {"dag_id": DAG_ID, "diagnosis": "this Dag has never run"}


def test_diagnose_dag_reports_failed_task_log_and_source(airflow):
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.task_instances = [{"task_id": "summarize", "try_number": 1}]
    airflow.log = [{"event": "KeyError: 'ammount'"}]

    result = server.diagnose_dag(DAG_ID)

    assert result["failed_task_id"] == "summarize"
    assert "KeyError" in result["log_tail"]
    assert result["source"] == SOURCE


def test_diagnose_dag_prefers_the_failed_run_over_the_newest(airflow):
    airflow.runs = [
        {"dag_run_id": "manual__2", "state": "running"},
        {"dag_run_id": "manual__1", "state": "failed"},
    ]
    assert server.diagnose_dag(DAG_ID)["dag_run_id"] == "manual__1"


def test_diagnose_dag_escapes_the_run_and_task_ids(airflow):
    # dag_run_id is user-settable at trigger time, so it is not a trusted value.
    airflow.runs = [{"dag_run_id": "manual__a/b", "state": "failed"}]
    airflow.task_instances = [{"task_id": "sum/marize", "try_number": 1}]

    server.diagnose_dag(DAG_ID)

    assert any("manual__a%2Fb" in path and "sum%2Fmarize" in path for _, path in airflow.calls)


def test_diagnose_dag_without_failed_task_instances(airflow):
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    assert "no failed task instances" in server.diagnose_dag(DAG_ID)["diagnosis"]


def test_diagnose_dag_still_reports_when_the_source_is_out_of_reach(airflow):
    airflow.relative_fileloc = "missing.py"
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.task_instances = [{"task_id": "summarize", "try_number": 1}]
    airflow.log = [{"event": "KeyError"}]

    result = server.diagnose_dag(DAG_ID)

    assert result["failed_task_id"] == "summarize"
    assert result["source"].startswith("unavailable:")


def test_fix_dag_code_patches_backs_up_and_reparses(airflow, tmp_path):
    result = server.fix_dag_code(DAG_ID, '"column": "ammount"', '"column": "amount"')

    assert result["applied"] is True
    assert (tmp_path / "sales_summary.py").read_text().startswith('op_kwargs={"column": "amount"}')
    assert (tmp_path / "sales_summary.py.airy-bak").read_text() == SOURCE
    assert "-op_kwargs" in result["diff"]
    assert "+op_kwargs" in result["diff"]
    assert result["reparse"] == "reparsed — Dag version 1 → 2"


def test_fix_dag_code_keeps_the_original_backup_across_two_fixes(airflow, tmp_path):
    server.fix_dag_code(DAG_ID, '"column": "ammount"', '"column": "amount"')
    server.fix_dag_code(DAG_ID, '"column": "amount"', '"column": "region"')
    assert (tmp_path / "sales_summary.py.airy-bak").read_text() == SOURCE


@pytest.mark.parametrize("old", ["not-in-the-file", "ammount"], ids=["absent", "not-unique"])
def test_fix_dag_code_refuses_ambiguous_snippets(airflow, tmp_path, old):
    result = server.fix_dag_code(DAG_ID, old, "amount")

    assert result["applied"] is False
    assert "exactly once" in result["error"]
    assert (tmp_path / "sales_summary.py").read_text() == SOURCE


def test_fix_dag_code_reports_a_reparse_that_never_lands(airflow):
    airflow.bump_version_on_reparse = False
    result = server.fix_dag_code(DAG_ID, '"column": "ammount"', '"column": "amount"')
    assert result["applied"] is True
    assert result["reparse"] == "reparse requested, but the Dag version did not change within 2s"


def test_fix_dag_code_refuses_a_patch_that_would_not_compile(airflow, tmp_path):
    result = server.fix_dag_code(DAG_ID, '"ammount"}', '"amount"')

    assert result["applied"] is False
    assert "would not compile" in result["error"]
    assert (tmp_path / "sales_summary.py").read_text() == SOURCE


def test_fix_dag_code_refuses_a_no_op_patch(airflow, tmp_path):
    result = server.fix_dag_code(DAG_ID, '"column": "ammount"', '"column": "ammount"')

    assert result["applied"] is False
    assert "identical" in result["error"]
    assert not (tmp_path / "sales_summary.py.airy-bak").exists()


@pytest.mark.parametrize(
    "error",
    [httpx.ConnectError("boom"), TypeError("'NoneType' object is not subscriptable")],
    ids=["http", "unexpected"],
)
def test_fix_dag_code_keeps_the_write_when_the_reparse_request_fails(airflow, tmp_path, error):
    # The write is the commit point: whatever goes wrong afterwards, the caller
    # must still learn that the file changed.
    airflow.fail_reparse = error
    result = server.fix_dag_code(DAG_ID, '"column": "ammount"', '"column": "amount"')

    assert result["applied"] is True
    assert "the reparse request failed" in result["reparse"]
    assert '"column": "amount"' in (tmp_path / "sales_summary.py").read_text()


def test_revert_dag_code_restores_the_backup(airflow, tmp_path):
    server.fix_dag_code(DAG_ID, '"column": "ammount"', '"column": "amount"')

    result = server.revert_dag_code(DAG_ID)

    assert result["reverted"] is True
    assert (tmp_path / "sales_summary.py").read_text() == SOURCE
    assert not (tmp_path / "sales_summary.py.airy-bak").exists()


def test_revert_dag_code_keeps_the_restore_when_the_reparse_request_fails(airflow, tmp_path):
    server.fix_dag_code(DAG_ID, '"column": "ammount"', '"column": "amount"')
    airflow.fail_reparse = TypeError("'NoneType' object is not subscriptable")

    result = server.revert_dag_code(DAG_ID)

    assert result["reverted"] is True
    assert "the reparse request failed" in result["reparse"]
    assert (tmp_path / "sales_summary.py").read_text() == SOURCE


def test_revert_dag_code_without_a_backup(airflow):
    result = server.revert_dag_code(DAG_ID)

    assert result["reverted"] is False
    assert "no backup" in result["error"]


def test_rerun_dag_unpauses_first(airflow):
    airflow.is_paused = True
    result = server.rerun_dag(DAG_ID)

    assert ("PATCH", f"/dags/{DAG_ID}") in airflow.calls
    assert result == {"dag_id": DAG_ID, "dag_run_id": "manual__new", "state": "queued"}


def test_rerun_dag_leaves_an_active_dag_alone(airflow):
    server.rerun_dag(DAG_ID)
    assert ("PATCH", f"/dags/{DAG_ID}") not in airflow.calls


def test_rerun_dag_sends_the_required_logical_date(airflow):
    # TriggerDAGRunPostBody.logical_date has no default: omitting it is a 422.
    server.rerun_dag(DAG_ID)
    assert {"logical_date": None, "conf": {}} in airflow.payloads


def test_force_reparse_tolerates_an_already_queued_request(airflow):
    airflow.reparse_status = 409
    result = server.fix_dag_code(DAG_ID, '"column": "ammount"', '"column": "amount"')
    assert result["applied"] is True
    assert "did not change" in result["reparse"]


def test_force_reparse_still_reports_other_http_errors(airflow):
    airflow.reparse_status = 500
    result = server.fix_dag_code(DAG_ID, '"column": "ammount"', '"column": "amount"')
    assert "the reparse request failed" in result["reparse"]


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("plain text", "plain text"),
        ([{"event": "boom"}], '{"event": "boom"}'),
        (["already a string"], "already a string"),
    ],
)
def test_tail_handles_every_log_shape(content, expected):
    assert server._tail(content) == expected


def test_tail_truncates_long_logs():
    assert len(server._tail("x" * 10_000)) == server.LOG_TAIL_CHARS
