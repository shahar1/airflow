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
from hashlib import md5
from pathlib import Path

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
        self.params: list[object] = []
        self.runs_by_id: dict[str, dict] = {}
        self.tis_by_run: dict[str, list] = {}
        self.sources_by_version: dict[int, str] = {}
        self.logs_by_task: dict[tuple[str, str], str] = {}
        self.parsed_source: str = SOURCE
        self.dags_dir: Path | None = None
        self.dry_run_dates: list[str] = []
        self.created_dates: list[str] | None = None
        self.skipped_dates: set[str] = set()
        self.created_run_state: str | None = "queued"
        self.cancelled = False
        self.fail_cancel: Exception | None = None
        self.fail_trigger: Exception | None = None
        self.assets: list[dict] = []
        self.bump_version_on_reparse = True
        self.fail_reparse: Exception | None = None
        self.reparse_status = 0
        self.tasks: list[dict] = []
        self.cleared: list[dict] = []
        self.fail_clear: Exception | None = None

    def __call__(self, method: str, path: str, **kwargs):
        self.calls.append((method, path))
        self.payloads.append(kwargs.get("json"))
        self.params.append(kwargs.get("params"))
        if "headers" in kwargs:
            raise TypeError("_api() got multiple values for keyword argument 'headers'")
        for run_id, run in self.runs_by_id.items():
            quoted = f"/dagRuns/{run_id}"
            if path == f"/dags/{DAG_ID}{quoted}":
                return run
            if path == f"/dags/{DAG_ID}{quoted}/taskInstances":
                return {"task_instances": self.tis_by_run.get(run_id, [])}
        if path == "/assets":
            return {"assets": self.assets}
        if path == f"/dags/{DAG_ID}/tasks":
            return {"tasks": self.tasks, "total_entries": len(self.tasks)}
        if path == f"/dags/{DAG_ID}/clearTaskInstances":
            return self._clear(kwargs["json"])
        if path == "/backfills/dry_run":
            return {
                "backfills": [{"logical_date": d} for d in self.dry_run_dates],
                "total_entries": len(self.dry_run_dates),
            }
        if path == "/backfills":
            body = kwargs["json"]
            return {"id": 7, "is_paused": False, **body}
        if path == "/backfills/7/dag_runs":
            dates = self.dry_run_dates if self.created_dates is None else self.created_dates
            return {
                "total_entries": len(dates),
                "backfill_dag_runs": [
                    {
                        "logical_date": d,
                        "partition_key": None,
                        "dag_run_id": None if d in self.skipped_dates else f"backfill__{d}",
                        "exception_reason": "already exists" if d in self.skipped_dates else None,
                        "dag_run_state": self.created_run_state,
                    }
                    for d in dates
                ],
            }
        if path == "/backfills/7/cancel":
            if self.fail_cancel:
                raise self.fail_cancel
            self.cancelled = True
            return {}
        if path == f"/dagSources/{DAG_ID}":
            version = (kwargs.get("params") or {}).get("version_number")
            if version is None:
                # What Airflow has parsed. Defaults to the file on disk — they
                # only differ when a test makes them.
                return {"content": self.parsed_source}
            return {"content": self.sources_by_version[version], "version_number": version}
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
                if self.dags_dir is not None:
                    self.parsed_source = (self.dags_dir / "sales_summary.py").read_text()
            return None
        if path == f"/dags/{DAG_ID}/dagRuns":
            if method == "POST":
                if self.fail_trigger:
                    raise self.fail_trigger
                return {"dag_run_id": "manual__new", "state": "queued"}
            return {"dag_runs": self.runs}
        if path == "/dags/~/dagRuns/~/taskInstances/list":
            wanted = (kwargs.get("json") or {}).get("dag_ids")
            tis = self.task_instances
            if wanted is not None:
                tis = [ti for ti in tis if ti["dag_id"] in set(wanted)]
            return {"task_instances": tis[: (kwargs["json"]).get("page_limit", 100)]}
        if path.endswith("/taskInstances"):
            return {"task_instances": self.task_instances}
        if "/logs/" in path:
            for (dag_id, task_id), content in self.logs_by_task.items():
                if f"/dags/{dag_id}/" in path and f"/taskInstances/{task_id}/" in path:
                    return {"content": content}
            return {"content": self.log}
        raise AssertionError(f"unexpected API call {method} {path}")

    def _clear(self, body: dict) -> dict:
        """What Airflow's clearTaskInstances does, to the depth the tools rely on."""
        if self.fail_clear and not body["dry_run"]:
            raise self.fail_clear
        names = {t if isinstance(t, str) else t[0] for t in body["task_ids"]}
        indexes = {tuple(t) for t in body["task_ids"] if not isinstance(t, str)}
        if body["include_downstream"]:
            names |= self._downstream_of(names)
            indexes = set()
        matches = [
            ti
            for ti in self.tis_by_run.get(body["dag_run_id"], [])
            if ti["task_id"] in names
            and (not indexes or (ti["task_id"], ti.get("map_index", -1)) in indexes)
            # only_failed covers upstream_failed too, which is the state a
            # downstream sits in after its parent failed.
            and (not body["only_failed"] or ti.get("state") in ("failed", "upstream_failed"))
        ]
        if not body["dry_run"]:
            self.cleared.append(body)
            for ti in matches:
                ti["state"] = None
        return {"task_instances": matches, "total_entries": len(matches)}

    def _downstream_of(self, names: set) -> set:
        """Everything reachable from those tasks, as `include_downstream` means."""
        edges = {task["task_id"]: task.get("downstream_task_ids") or [] for task in self.tasks}
        reached, queue = set(), list(names)
        while queue:
            for child in edges.get(queue.pop(), []):
                if child not in reached:
                    reached.add(child)
                    queue.append(child)
        return reached


@pytest.fixture
def airflow(monkeypatch, tmp_path):
    fake = FakeAirflow()
    monkeypatch.setattr(server, "_api", fake)
    monkeypatch.setattr(server, "DAGS_DIR", tmp_path)
    monkeypatch.setattr(server, "REPARSE_TIMEOUT_S", 2.0)
    (tmp_path / "sales_summary.py").write_text(SOURCE)
    fake.dags_dir = tmp_path
    return fake


def _parses(airflow, tmp_path):
    """Let Airflow catch up with the file, as the Dag processor would."""
    airflow.parsed_source = (tmp_path / "sales_summary.py").read_text()


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
    airflow.task_instances = [{"task_id": "summarize", "try_number": 1, "state": "failed"}]
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
    airflow.task_instances = [{"task_id": "sum/marize", "try_number": 1, "state": "failed"}]

    server.diagnose_dag(DAG_ID)

    assert any("manual__a%2Fb" in path and "sum%2Fmarize" in path for _, path in airflow.calls)


def test_diagnose_dag_without_failed_task_instances(airflow):
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    assert "no failed task instances" in server.diagnose_dag(DAG_ID)["diagnosis"]


def test_diagnose_dag_reads_the_parsed_source_not_the_file(airflow, tmp_path):
    """The file may already define a Dag nobody authorized; the parsed version cannot."""
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.task_instances = [{"task_id": "summarize", "try_number": 1, "state": "failed"}]
    (tmp_path / "sales_summary.py").write_text(SOURCE + "\nsecret_dag = DAG('secret')\n")

    result = server.diagnose_dag(DAG_ID)

    assert result["source"] == SOURCE
    assert "secret_dag" not in result["source"]


def test_diagnose_dag_still_reports_when_the_source_is_out_of_reach(airflow):
    """A Dag outside the writable bundle has no source_file, but still diagnoses."""
    airflow.relative_fileloc = "missing.py"
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.task_instances = [{"task_id": "summarize", "try_number": 1, "state": "failed"}]
    airflow.log = [{"event": "KeyError"}]

    result = server.diagnose_dag(DAG_ID)

    assert result["failed_task_id"] == "summarize"
    assert "source_file" not in result


DEMO_TASKS = [
    {"task_id": "extract", "downstream_task_ids": ["summarize"]},
    {"task_id": "summarize", "downstream_task_ids": ["report"]},
    {"task_id": "report", "downstream_task_ids": []},
]


def test_diagnose_dag_reports_every_task_state_and_every_failed_log(airflow):
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.task_instances = [
        {"task_id": "extract", "try_number": 1, "state": "success"},
        {"task_id": "summarize", "try_number": 1, "state": "failed"},
        {"task_id": "report", "try_number": 2, "state": "failed"},
    ]
    airflow.logs_by_task = {
        (DAG_ID, "summarize"): "KeyError: 'ammount'",
        (DAG_ID, "report"): "ValueError: invalid literal for int()",
    }

    result = server.diagnose_dag(DAG_ID)

    assert [ti["state"] for ti in result["task_instances"]] == ["success", "failed", "failed"]
    assert [f["task_id"] for f in result["failures"]] == ["summarize", "report"]
    assert "KeyError" in result["failures"][0]["log_tail"]
    assert "ValueError" in result["failures"][1]["log_tail"]
    # The first failure stays where every prompt and card already looks for it.
    assert result["failed_task_id"] == "summarize"


def test_diagnose_dag_caps_the_total_log_it_returns(airflow, monkeypatch):
    monkeypatch.setattr(server, "DIAGNOSIS_LOG_BUDGET_CHARS", 10)
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.task_instances = [{"task_id": f"t{i}", "try_number": 1, "state": "failed"} for i in range(3)]
    airflow.log = f"{'x' * 100}KeyError"

    result = server.diagnose_dag(DAG_ID)

    assert sum(len(f["log_tail"]) for f in result["failures"]) <= 10
    assert result["logs_omitted"] == 3 - len(result["failures"])
    # What is kept is the end of the log — where the exception is.
    assert result["failures"][0]["log_tail"].endswith("KeyError")


def test_diagnose_dag_names_the_unmatched_xcom_task_id_before_it_runs(airflow):
    """The demo's second bug is a Jinja string, which an AST walk cannot see."""
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.task_instances = [{"task_id": "summarize", "try_number": 1, "state": "failed"}]
    airflow.tasks = DEMO_TASKS
    airflow.parsed_source = "op_kwargs={'total': \"{{ ti.xcom_pull(task_ids='summarise') }}\"}\n"

    result = server.diagnose_dag(DAG_ID)

    assert result["checks"] == [
        {
            "kind": "unknown_xcom_task_id",
            "detail": (
                "an XCom pull references task_ids='summarise', which is not a task in this Dag, "
                "so the pull returns None"
            ),
        }
    ]
    assert result["tasks"]["order"] == ["extract", "summarize", "report"]
    assert result["tasks"]["ambiguous_positions"] == []


def test_diagnose_dag_asks_for_the_mapped_instance_own_log(airflow):
    """The log route defaults to map_index=-1 — a different instance entirely."""
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.task_instances = [{"task_id": "summarize", "try_number": 1, "state": "failed", "map_index": 3}]

    server.diagnose_dag(DAG_ID)

    log_params = [params for (_, path), params in zip(airflow.calls, airflow.params) if "/logs/" in path]
    assert log_params == [{"map_index": 3}]


def test_diagnose_dag_skips_reference_checks_for_a_file_with_another_dag(airflow):
    """A co-located Dag's own task ids are not this Dag's unknown references."""
    airflow.tasks = DEMO_TASKS
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.parsed_source = (
        "PythonOperator(task_id='summarize')\n"
        "PythonOperator(task_id='other_dag_task')\n"
        "x = \"{{ ti.xcom_pull(task_ids='other_dag_task') }}\"\n"
    )

    checks = server.diagnose_dag(DAG_ID)["checks"]

    assert [c["kind"] for c in checks] == ["source_graph_disagreement"]
    assert "other_dag_task" in checks[0]["detail"]


def test_diagnose_dag_skips_reference_checks_for_a_file_with_a_taskflow_dag(airflow):
    """A co-located TaskFlow Dag declares no task_id, so counting Dags is the signal."""
    airflow.tasks = DEMO_TASKS
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.parsed_source = (
        "with DAG('sales_summary'):\n"
        "    PythonOperator(task_id='summarize')\n"
        "\n"
        "@dag\n"
        "def other():\n"
        "    @task\n"
        "    def other_task():\n"
        "        pass\n"
        "    x = \"{{ ti.xcom_pull(task_ids='other_task') }}\"\n"
    )

    checks = server.diagnose_dag(DAG_ID)["checks"]

    assert [c["kind"] for c in checks] == ["source_graph_disagreement"]
    assert "defines more than this Dag" in checks[0]["detail"]


def _paged_task_instances(airflow, monkeypatch, total: int):
    """A run whose task instances only come back a page at a time."""
    tis = [{"task_id": f"t{i}", "try_number": 1, "state": "success"} for i in range(total)]
    monkeypatch.setattr(server, "TASK_INSTANCE_PAGE", 2)

    def paged(method, path, **kw):
        if path.endswith("/taskInstances"):
            offset = kw["params"]["offset"]
            return {
                "task_instances": tis[offset : offset + kw["params"]["limit"]],
                "total_entries": total,
            }
        return airflow(method, path, **kw)

    monkeypatch.setattr(server, "_api", paged)


def test_diagnose_dag_follows_the_pages_of_a_long_run(airflow, monkeypatch):
    """Asking for a bigger page does not work — maximum_page_limit clamps it."""
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    _paged_task_instances(airflow, monkeypatch, total=5)

    result = server.diagnose_dag(DAG_ID)

    assert [ti["task_id"] for ti in result["task_instances"]] == ["t0", "t1", "t2", "t3", "t4"]
    assert "task_instances_omitted" not in result


def test_diagnose_dag_says_when_a_run_is_longer_than_it_will_scan(airflow, monkeypatch):
    """A silently short list would hide a failure; say what was not looked at."""
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    _paged_task_instances(airflow, monkeypatch, total=9)
    monkeypatch.setattr(server, "TASK_INSTANCE_SCAN_LIMIT", 4)

    result = server.diagnose_dag(DAG_ID)

    assert len(result["task_instances"]) == 4
    assert result["task_instances_omitted"] == 5


def test_display_order_marks_only_the_positions_a_branch_leaves_open():
    """A diamond's middle two swap; the task they rejoin at cannot."""
    tasks = [
        {"task_id": "start", "downstream_task_ids": ["left", "right"]},
        {"task_id": "left", "downstream_task_ids": ["join"]},
        {"task_id": "right", "downstream_task_ids": ["join"]},
        {"task_id": "join", "downstream_task_ids": []},
    ]
    order, ambiguous = server._display_order(tasks)

    assert order == ["start", "left", "right", "join"]
    assert ambiguous == {1, 2}


def test_display_order_marks_a_branch_that_can_float_past_a_longer_one():
    """`b` can be listed anywhere after `start`, so no later position is fixed."""
    tasks = [
        {"task_id": "start", "downstream_task_ids": ["a", "b"]},
        {"task_id": "a", "downstream_task_ids": ["c"]},
        {"task_id": "c", "downstream_task_ids": ["d"]},
        {"task_id": "d", "downstream_task_ids": []},
        {"task_id": "b", "downstream_task_ids": []},
    ]
    order, ambiguous = server._display_order(tasks)

    assert order == ["start", "a", "b", "c", "d"]
    assert ambiguous == {1, 2, 3, 4}


def test_display_order_marks_a_fan_out_that_never_rejoins():
    tasks = [
        {"task_id": "extract", "downstream_task_ids": ["a", "b"]},
        {"task_id": "a", "downstream_task_ids": []},
        {"task_id": "b", "downstream_task_ids": []},
    ]
    order, ambiguous = server._display_order(tasks)

    assert order == ["extract", "a", "b"]
    assert ambiguous == {1, 2}


def _changes(*pairs):
    return [{"old": old, "new": new} for old, new in pairs]


def _apply(*pairs, dag_id=DAG_ID):
    """Plan then apply, the way the prompt requires every source change to go."""
    changes = _changes(*pairs)
    plan = server.plan_dag_code_changes(dag_id, changes)
    return server.apply_dag_code_changes(dag_id, changes, plan.get("plan_token", ""))


def test_apply_dag_code_changes_patches_backs_up_and_reparses(airflow, tmp_path):
    result = _apply(('"column": "ammount"', '"column": "amount"'))

    assert result["applied"] is True
    assert result["mutation_applied"] is True
    assert (tmp_path / "sales_summary.py").read_text().startswith('op_kwargs={"column": "amount"}')
    assert (tmp_path / "sales_summary.py.airy-bak").read_text() == SOURCE
    assert "-op_kwargs" in result["diff"]
    assert "+op_kwargs" in result["diff"]
    assert result["reparse"] == "reparsed — Dag version 1 → 2"
    assert result["ui_updates"] == [{"kind": "dag_definition", "dag_id": DAG_ID, "version_number": 2}]


def test_apply_dag_code_changes_writes_two_edits_as_one_version(airflow, tmp_path):
    """Two proposals against one source cannot both be right; one write can."""
    result = _apply(
        ('"column": "ammount"', '"column": "amount"'),
        ("ammount is a typo", "amount is correct"),
    )

    assert result["applied"] is True
    assert result["change_count"] == 2
    assert (tmp_path / "sales_summary.py").read_text() == (
        'op_kwargs={"column": "amount"}\nprint("amount is correct")\n'
    )
    # One backup, one write, one new version — not two of anything.
    assert (tmp_path / "sales_summary.py.airy-bak").read_text() == SOURCE
    assert airflow.version == 2


def test_apply_dag_code_changes_keeps_the_original_backup_across_two_fixes(airflow, tmp_path):
    _apply(('"column": "ammount"', '"column": "amount"'))
    _parses(airflow, tmp_path)
    _apply(('"column": "amount"', '"column": "region"'))
    assert (tmp_path / "sales_summary.py.airy-bak").read_text() == SOURCE


@pytest.mark.parametrize("old", ["not-in-the-file", "ammount"], ids=["absent", "not-unique"])
def test_plan_dag_code_changes_refuses_ambiguous_snippets(airflow, tmp_path, old):
    result = server.plan_dag_code_changes(DAG_ID, _changes((old, "amount")))

    assert result["planned"] is False
    assert "exactly once" in result["error"]
    assert "plan_token" not in result
    assert (tmp_path / "sales_summary.py").read_text() == SOURCE


def test_plan_dag_code_changes_checks_uniqueness_after_the_earlier_edits(airflow):
    """The second snippet meets the file the first edit left, not the original."""
    result = server.plan_dag_code_changes(
        DAG_ID, _changes(("ammount is a typo", "ammount ammount"), ("ammount", "amount"))
    )

    assert result["planned"] is False
    assert "at the point it is applied" in result["error"]


def test_apply_dag_code_changes_reports_a_reparse_that_never_lands(airflow):
    airflow.bump_version_on_reparse = False
    result = _apply(('"column": "ammount"', '"column": "amount"'))

    assert result["applied"] is True
    assert result["reparse"] == "reparse requested, but the Dag version did not change within 2s"
    # No new version means the Graph and Code views show exactly what they did
    # before; telling them to refresh would only promise otherwise.
    assert result["ui_updates"] == []


def test_plan_dag_code_changes_refuses_a_patch_that_would_not_compile(airflow, tmp_path):
    result = server.plan_dag_code_changes(DAG_ID, _changes(('"ammount"}', '"amount"')))

    assert result["planned"] is False
    assert "would not compile" in result["error"]
    assert (tmp_path / "sales_summary.py").read_text() == SOURCE


def test_plan_dag_code_changes_refuses_a_no_op_patch(airflow, tmp_path):
    result = server.plan_dag_code_changes(DAG_ID, _changes(('"column": "ammount"', '"column": "ammount"')))

    assert result["planned"] is False
    assert "exactly as it is" in result["error"]
    assert not (tmp_path / "sales_summary.py.airy-bak").exists()


@pytest.mark.parametrize(
    "changes",
    [[], "not a list", [{"old": "a"}], [{"old": "", "new": "b"}]],
    ids=["empty", "not-a-list", "no-new", "empty-old"],
)
def test_dag_code_tools_refuse_arguments_that_are_not_changes(airflow, changes):
    assert server.plan_dag_code_changes(DAG_ID, changes)["planned"] is False
    assert server.apply_dag_code_changes(DAG_ID, changes, "token")["applied"] is False


def test_apply_dag_code_changes_refuses_without_a_reviewed_plan(airflow, tmp_path):
    result = server.apply_dag_code_changes(DAG_ID, _changes(('"column": "ammount"', '"column": "amount"')))

    assert result["applied"] is False
    assert result["mutation_applied"] is False
    assert "no reviewed plan" in result["error"]
    assert (tmp_path / "sales_summary.py").read_text() == SOURCE


def test_apply_dag_code_changes_refuses_changes_the_user_did_not_review(airflow, tmp_path):
    """The token proves a diff was shown; it must be the diff being written."""
    token = server.plan_dag_code_changes(DAG_ID, _changes(('"column": "ammount"', '"column": "amount"')))[
        "plan_token"
    ]

    result = server.apply_dag_code_changes(DAG_ID, _changes(("print", "pass  # print")), token)

    assert result["applied"] is False
    assert "not the changes that were planned" in result["error"]
    assert (tmp_path / "sales_summary.py").read_text() == SOURCE


def test_apply_dag_code_changes_refuses_a_token_twice(airflow, tmp_path):
    change = ('"column": "ammount"', '"column": "amount"')
    token = server.plan_dag_code_changes(DAG_ID, _changes(change))["plan_token"]
    assert server.apply_dag_code_changes(DAG_ID, _changes(change), token)["applied"] is True
    _parses(airflow, tmp_path)

    again = server.apply_dag_code_changes(DAG_ID, _changes(change), token)

    assert again["applied"] is False
    assert "no reviewed plan" in again["error"]


def test_apply_dag_code_changes_refuses_when_the_source_moved_under_the_plan(airflow, tmp_path):
    """The impact findings were computed from bytes that are no longer there."""
    change = ("print", "pass  # print")
    token = server.plan_dag_code_changes(DAG_ID, _changes(change))["plan_token"]
    (tmp_path / "sales_summary.py").write_text(SOURCE + "extra = 1\n")
    _parses(airflow, tmp_path)

    result = server.apply_dag_code_changes(DAG_ID, _changes(change), token)

    assert result["applied"] is False
    assert "changed since it was planned" in result["error"]


@pytest.mark.parametrize("quote", ["'", '"'], ids=["single", "double"])
def test_plan_dag_code_changes_blocks_removing_a_task_others_still_need(airflow, quote):
    airflow.parsed_source = (
        "summarize = PythonOperator(task_id='summarize')\n"
        f"report = PythonOperator(task_id='report', op_kwargs={{'t': {quote}summarize{quote}}})\n"
    )
    (server.DAGS_DIR / "sales_summary.py").write_text(airflow.parsed_source)
    airflow.tasks = [
        {"task_id": "extract", "downstream_task_ids": ["summarize"]},
        {"task_id": "summarize", "downstream_task_ids": ["report"]},
        {"task_id": "report", "downstream_task_ids": []},
    ]

    result = server.plan_dag_code_changes(
        DAG_ID, _changes(("summarize = PythonOperator(task_id='summarize')\n", ""))
    )

    assert result["planned"] is False
    assert "plan_token" not in result
    edges = result["impact"]["removed_task_edges"]["summarize"]
    assert edges == {"upstream": ["extract"], "downstream": ["report"], "still_referenced": True}


def test_plan_dag_code_changes_blocks_removing_a_taskflow_task(airflow):
    """A @task function declares no task_id — a declaration diff would miss it."""
    airflow.parsed_source = "@task\ndef summarize():\n    pass\n\n\n@task\ndef report():\n    pass\n"
    (server.DAGS_DIR / "sales_summary.py").write_text(airflow.parsed_source)
    airflow.tasks = DEMO_TASKS

    result = server.plan_dag_code_changes(DAG_ID, _changes(("@task\ndef summarize():\n    pass\n\n\n", "")))

    assert result["planned"] is False
    assert result["impact"]["removed_task_ids"] == ["summarize"]
    assert result["impact"]["removed_task_edges"]["summarize"]["downstream"] == ["report"]


@pytest.mark.parametrize(
    "decorator",
    ["@task", "@task(retries=1)", "@task(\n    retries=1,\n)", "@task.branch", "@wraps\n@task"],
    ids=["bare", "called", "multiline", "attribute", "stacked"],
)
def test_plan_dag_code_changes_sees_a_taskflow_task_however_it_is_decorated(airflow, decorator):
    """The decorator's arguments can run over several lines; a regex loses that."""
    definition = f"{decorator}\ndef summarize():\n    pass\n"
    airflow.parsed_source = f"{definition}\n\n@task\ndef report():\n    summarize()\n"
    (server.DAGS_DIR / "sales_summary.py").write_text(airflow.parsed_source)
    airflow.tasks = DEMO_TASKS

    result = server.plan_dag_code_changes(DAG_ID, _changes((definition, "")))

    assert result["planned"] is False
    assert result["impact"]["removed_task_ids"] == ["summarize"]


@pytest.mark.parametrize(
    ("source", "removal"),
    [
        (
            'mine = PythonOperator(task_id="load", python_callable=here)\n'
            'theirs = PythonOperator(task_id="load", python_callable=there)\n',
            'mine = PythonOperator(task_id="load", python_callable=here)\n',
        ),
        (
            "@dag\ndef a():\n    @task\n    def load():\n        return 1\n\n\n"
            "@dag\ndef b():\n    @task\n    def load():\n        return 2\n",
            "@dag\ndef a():\n    @task\n    def load():\n        return 1\n\n\n",
        ),
    ],
    ids=["declared", "taskflow"],
)
def test_plan_dag_code_changes_blocks_a_removal_a_co_located_twin_would_hide(airflow, source, removal):
    """Two Dags in one file can each have a `load`; deleting one still removes one.

    "Is `load` still defined?" answers yes off the *other* Dag's copy, so the
    question has to be how many definitions there were and how many are left.
    """
    airflow.parsed_source = source
    (server.DAGS_DIR / "sales_summary.py").write_text(source)
    airflow.tasks = [
        {"task_id": "load", "downstream_task_ids": ["report"]},
        {"task_id": "report", "downstream_task_ids": []},
    ]

    result = server.plan_dag_code_changes(DAG_ID, _changes((removal, "")))

    assert result["planned"] is False
    assert result["impact"]["removed_task_ids"] == ["load"]
    assert result["impact"]["removed_task_edges"]["load"]["downstream"] == ["report"]


def test_plan_dag_code_changes_does_not_mistake_a_sensor_for_the_task_it_waits_on(airflow):
    """`external_task_id="load"` points at a task; it does not define one."""
    airflow.parsed_source = 'load = EmptyOperator(task_id="load")\n'
    (server.DAGS_DIR / "sales_summary.py").write_text(airflow.parsed_source)
    airflow.tasks = [
        {"task_id": "load", "downstream_task_ids": ["report"]},
        {"task_id": "report", "downstream_task_ids": []},
    ]

    result = server.plan_dag_code_changes(
        DAG_ID,
        _changes(
            (
                'load = EmptyOperator(task_id="load")\n',
                'ExternalTaskSensor(task_id="wait", external_task_id="load")\n',
            )
        ),
    )

    assert result["planned"] is False
    assert result["impact"]["removed_task_ids"] == ["load"]


def test_plan_dag_code_changes_refuses_when_it_cannot_read_the_task_graph(airflow, monkeypatch):
    """ "Found no problems" and "could not look" must not be the same answer."""
    monkeypatch.setattr(
        server,
        "_tasks",
        lambda dag_id: (_ for _ in ()).throw(
            httpx.HTTPStatusError(
                "boom",
                request=httpx.Request("GET", "/tasks"),
                response=httpx.Response(500),
            )
        ),
    )

    result = server.plan_dag_code_changes(DAG_ID, _changes(("ammount is a typo", "amount is right")))

    assert result["planned"] is False
    assert "plan_token" not in result
    assert "task graph could not be read" in result["impact"]["blocking"][0]


def test_plan_dag_code_changes_blocks_dropping_the_task_decorator(airflow):
    """The decorator is what makes it a task; losing it removes the task."""
    airflow.parsed_source = "@task\ndef summarize():\n    pass\n\n\n@task\ndef report():\n    pass\n"
    (server.DAGS_DIR / "sales_summary.py").write_text(airflow.parsed_source)
    airflow.tasks = DEMO_TASKS

    result = server.plan_dag_code_changes(DAG_ID, _changes(("@task\ndef summarize", "def summarize")))

    assert result["planned"] is False
    assert result["impact"]["removed_task_ids"] == ["summarize"]


def test_plan_dag_code_changes_blocks_a_removal_a_bare_name_still_points_at(airflow):
    """`registry = [orphan]` is a NameError the compile check cannot see."""
    airflow.parsed_source = "@task\ndef orphan():\n    pass\n\n\nregistry = [orphan]\n"
    (server.DAGS_DIR / "sales_summary.py").write_text(airflow.parsed_source)
    airflow.tasks = [{"task_id": "orphan", "downstream_task_ids": []}]

    result = server.plan_dag_code_changes(DAG_ID, _changes(("@task\ndef orphan():\n    pass\n\n\n", "")))

    assert result["planned"] is False
    assert result["impact"]["removed_task_edges"]["orphan"]["still_referenced"] is True


def test_apply_dag_code_changes_still_reports_the_write_when_the_backup_fails(airflow, tmp_path, monkeypatch):
    """The write is the commit point; a failure after it must not hide the edit."""
    change = ('"column": "ammount"', '"column": "amount"')
    token = server.plan_dag_code_changes(DAG_ID, _changes(change))["plan_token"]
    real_write = Path.write_text

    def refuse_the_backup(self, *args, **kwargs):
        if self.name.endswith(".airy-bak"):
            raise OSError("read-only file system")
        return real_write(self, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", refuse_the_backup)

    result = server.apply_dag_code_changes(DAG_ID, _changes(change), token)

    assert result["applied"] is True
    assert "could not be backed up" in result["warning"]
    assert '"column": "amount"' in (tmp_path / "sales_summary.py").read_text()


def test_apply_dag_code_changes_leaves_no_backup_behind_when_it_refuses(airflow, tmp_path, monkeypatch):
    """A backup kept for an edit that never landed would later revert over real work."""
    path = tmp_path / "sales_summary.py"
    change = ('"column": "ammount"', '"column": "amount"')
    token = server.plan_dag_code_changes(DAG_ID, _changes(change))["plan_token"]
    real_read = server._read_reviewed_file

    def read_then_someone_else_writes(*args, **kwargs):
        source = real_read(*args, **kwargs)
        path.write_text(SOURCE + "someone_else = 1\n")
        return source

    monkeypatch.setattr(server, "_read_reviewed_file", read_then_someone_else_writes)

    assert server.apply_dag_code_changes(DAG_ID, _changes(change), token)["applied"] is False
    assert not (tmp_path / "sales_summary.py.airy-bak").exists()


def test_plan_dag_code_changes_sends_an_asset_change_to_the_authorized_tool(airflow):
    """The neighbours are other Dags' ids; only get_blast_radius is checked for those."""
    airflow.parsed_source = "outlets=[Asset('sales')]\nprint('unrelated')\n"
    (server.DAGS_DIR / "sales_summary.py").write_text(airflow.parsed_source)

    touching = server.plan_dag_code_changes(DAG_ID, _changes(("Asset('sales')", "Asset('sales_v2')")))
    unrelated = server.plan_dag_code_changes(DAG_ID, _changes(("'unrelated'", "'still unrelated'")))

    assert "get_blast_radius" in touching["impact"]["asset_review_needed"]
    assert ("GET", "/assets") not in airflow.calls
    # A change elsewhere in a Dag that happens to mention assets is not one.
    assert "asset_review_needed" not in unrelated["impact"]


@pytest.mark.parametrize(
    "error",
    [httpx.ConnectError("boom"), TypeError("'NoneType' object is not subscriptable")],
    ids=["http", "unexpected"],
)
def test_apply_dag_code_changes_keeps_the_write_when_the_reparse_request_fails(airflow, tmp_path, error):
    # The write is the commit point: whatever goes wrong afterwards, the caller
    # must still learn that the file changed.
    airflow.fail_reparse = error
    result = _apply(('"column": "ammount"', '"column": "amount"'))

    assert result["applied"] is True
    assert "the reparse request failed" in result["reparse"]
    assert '"column": "amount"' in (tmp_path / "sales_summary.py").read_text()


def test_revert_dag_code_restores_the_backup(airflow, tmp_path):
    _apply(('"column": "ammount"', '"column": "amount"'))
    _parses(airflow, tmp_path)

    result = server.revert_dag_code(DAG_ID)

    assert result["reverted"] is True
    assert (tmp_path / "sales_summary.py").read_text() == SOURCE
    assert not (tmp_path / "sales_summary.py.airy-bak").exists()


def test_revert_dag_code_reports_every_fix_it_discards(airflow, tmp_path):
    _apply(('"column": "ammount"', '"column": "amount"'))
    _parses(airflow, tmp_path)
    _apply(("ammount is a typo", "amount is correct"))
    _parses(airflow, tmp_path)

    result = server.revert_dag_code(DAG_ID)

    assert (tmp_path / "sales_summary.py").read_text() == SOURCE
    # Both fixes are undone, so the diff has to show both coming back.
    assert '+op_kwargs={"column": "ammount"}' in result["diff"]
    assert '+print("ammount is a typo")' in result["diff"]


def test_revert_dag_code_keeps_the_restore_when_the_reparse_request_fails(airflow, tmp_path):
    _apply(('"column": "ammount"', '"column": "amount"'))
    _parses(airflow, tmp_path)
    airflow.fail_reparse = TypeError("'NoneType' object is not subscriptable")

    result = server.revert_dag_code(DAG_ID)

    assert result["reverted"] is True
    assert "the reparse request failed" in result["reparse"]
    assert (tmp_path / "sales_summary.py").read_text() == SOURCE


@pytest.mark.parametrize(
    ("tool", "call"),
    [
        (
            "apply_dag_code_changes",
            lambda token: server.apply_dag_code_changes(DAG_ID, _changes(("print", "pass  # print")), token),
        ),
        ("revert_dag_code", lambda token: server.revert_dag_code(DAG_ID)),
    ],
)
def test_writes_refuse_a_file_airflow_has_not_parsed(airflow, tmp_path, tool, call):
    """Access was authorized against the parsed Dags; unparsed bytes are not those."""
    _apply(('"column": "ammount"', '"column": "amount"'))
    _parses(airflow, tmp_path)
    token = server.plan_dag_code_changes(DAG_ID, _changes(("print", "pass  # print")))["plan_token"]
    # Someone edits the file directly; the Dag processor has not caught up.
    path = tmp_path / "sales_summary.py"
    path.write_text(path.read_text() + "\nother_dag = DAG('other')\n")
    before = path.read_text()

    result = call(token)

    assert "has not been reviewed" in result["error"]
    assert path.read_text() == before


def test_apply_dag_code_changes_refuses_when_the_file_changes_mid_patch(airflow, tmp_path, monkeypatch):
    """Validate-then-write would put back a buffer computed from bytes that moved."""
    path = tmp_path / "sales_summary.py"
    change = ('"column": "ammount"', '"column": "amount"')
    token = server.plan_dag_code_changes(DAG_ID, _changes(change))["plan_token"]
    real_read = server._read_reviewed_file

    def read_then_someone_else_writes(*args, **kwargs):
        source = real_read(*args, **kwargs)
        # Another writer lands between the check and the write. It does not take
        # our lock — an editor never would.
        path.write_text(SOURCE + "\nsomeone_else = 1\n")
        return source

    monkeypatch.setattr(server, "_read_reviewed_file", read_then_someone_else_writes)

    result = server.apply_dag_code_changes(DAG_ID, _changes(change), token)

    assert result["applied"] is False
    assert "changed while the patch was being prepared" in result["error"]
    assert path.read_text().endswith("someone_else = 1\n")


@pytest.fixture
def cleared_run(airflow):
    """The demo's state before a clear: two tasks done, the third failed."""
    airflow.tasks = DEMO_TASKS
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.runs_by_id = {"manual__1": {"dag_run_id": "manual__1", "state": "failed"}}
    airflow.tis_by_run = {
        "manual__1": [
            {"task_id": "extract", "state": "success", "map_index": -1, "try_number": 1},
            {"task_id": "summarize", "state": "success", "map_index": -1, "try_number": 2},
            {"task_id": "report", "state": "failed", "map_index": -1, "try_number": 1},
        ]
    }
    return airflow


def test_plan_task_instance_clear_resolves_latest_and_the_position(cleared_run):
    plan = server.plan_task_instance_clear(DAG_ID, position=3)

    assert plan["planned"] is True
    assert plan["dag_run_id"] == "manual__1"
    assert plan["task_ids"] == ["report"]
    assert plan["resolved_by"] == "position 3 in topological order ['extract', 'summarize', 'report']"
    assert plan["affected"] == [{"task_id": "report", "map_index": -1, "state": "failed", "try_number": 1}]
    assert plan["creates_dag_run"] is False


def test_plan_task_instance_clear_refuses_a_position_a_branch_leaves_open(cleared_run):
    cleared_run.tasks = [
        {"task_id": "extract", "downstream_task_ids": ["a", "b"]},
        {"task_id": "a", "downstream_task_ids": []},
        {"task_id": "b", "downstream_task_ids": []},
    ]
    cleared_run.tis_by_run["manual__1"] = [
        {"task_id": t, "state": "failed", "map_index": -1, "try_number": 1} for t in ("extract", "a", "b")
    ]

    plan = server.plan_task_instance_clear(DAG_ID, position=3)

    assert plan["planned"] is False
    assert "branches" in plan["error"]
    assert "plan_token" not in plan
    # The position before the branch is still the graph's own answer.
    assert server.plan_task_instance_clear(DAG_ID, position=1)["planned"] is True


def test_plan_task_instance_clear_refuses_an_unknown_task(cleared_run):
    plan = server.plan_task_instance_clear(DAG_ID, task_id="summarise")

    assert plan["planned"] is False
    assert "has no task 'summarise'" in plan["error"]


def test_plan_task_instance_clear_refuses_when_nothing_matches(cleared_run):
    """only_failed means a succeeded task is not a match — say so, don't clear."""
    plan = server.plan_task_instance_clear(DAG_ID, task_id="extract", include_downstream=False)

    assert plan["planned"] is False
    assert "nothing to clear" in plan["error"]
    assert "plan_token" not in plan


def test_plan_task_instance_clear_takes_the_downstream_with_it(cleared_run):
    """Airflow's own dialog defaults to downstream, and for good reason: a task
    left in upstream_failed is never rescheduled, so clearing alone strands it."""
    cleared_run.tis_by_run["manual__1"][2]["state"] = "upstream_failed"

    plan = server.plan_task_instance_clear(DAG_ID, task_id="summarize", only_failed=False)

    assert [ti["task_id"] for ti in plan["affected"]] == ["summarize", "report"]
    assert plan["include_downstream"] is True


def test_plan_task_instance_clear_can_be_held_to_the_one_task(cleared_run):
    plan = server.plan_task_instance_clear(
        DAG_ID, task_id="summarize", only_failed=False, include_downstream=False
    )

    assert [ti["task_id"] for ti in plan["affected"]] == ["summarize"]
    # The write has to be asked for the same narrowing, or it is not that plan.
    assert (
        server.apply_task_instance_clear(
            DAG_ID, plan["dag_run_id"], plan["task_ids"], plan["plan_token"], only_failed=False
        )["cleared"]
        is False
    )


@pytest.mark.parametrize("run_on_latest_version", [True, False])
def test_plan_task_instance_clear_refuses_a_version_that_would_move_the_task_set(
    cleared_run, run_on_latest_version
):
    """Clearing re-queues the run, and the scheduler reconciles it either way."""
    cleared_run.tasks = DEMO_TASKS + [{"task_id": "publish", "downstream_task_ids": []}]

    plan = server.plan_task_instance_clear(
        DAG_ID, task_id="report", run_on_latest_version=run_on_latest_version
    )

    assert plan["planned"] is False
    assert plan["migration"] == {"added": ["publish"], "removed": []}
    assert "plan_token" not in plan


def test_apply_task_instance_clear_re_checks_the_task_set_before_writing(cleared_run):
    """The Dag can gain a task between the approval card and the click."""
    plan = server.plan_task_instance_clear(DAG_ID, task_id="report")
    cleared_run.tasks = DEMO_TASKS + [{"task_id": "publish", "downstream_task_ids": []}]

    result = server.apply_task_instance_clear(
        DAG_ID, plan["dag_run_id"], plan["task_ids"], plan["plan_token"]
    )

    assert result["cleared"] is False
    assert result["migration"] == {"added": ["publish"], "removed": []}
    assert cleared_run.cleared == []


def test_apply_task_instance_clear_checks_the_task_set_after_the_repeated_dry_run(cleared_run, monkeypatch):
    """The check has to be the last thing before the write, not the first."""
    plan = server.plan_task_instance_clear(DAG_ID, task_id="report")
    real_clear = cleared_run._clear

    def gains_a_task_during_the_dry_run(body):
        result = real_clear(body)
        if body["dry_run"]:
            cleared_run.tasks = DEMO_TASKS + [{"task_id": "publish", "downstream_task_ids": []}]
        return result

    monkeypatch.setattr(cleared_run, "_clear", gains_a_task_during_the_dry_run)

    result = server.apply_task_instance_clear(
        DAG_ID, plan["dag_run_id"], plan["task_ids"], plan["plan_token"]
    )

    assert result["cleared"] is False
    assert result["migration"] == {"added": ["publish"], "removed": []}
    assert cleared_run.cleared == []


def test_plan_task_instance_clear_refuses_when_it_cannot_see_the_whole_run(cleared_run, monkeypatch):
    """Half a task list cannot answer whether the latest version moves the set."""
    real = server._run_task_instances
    monkeypatch.setattr(
        server, "_run_task_instances", lambda dag_id, run_path: (real(dag_id, run_path)[0], 4)
    )

    plan = server.plan_task_instance_clear(DAG_ID, task_id="report")

    assert plan["planned"] is False
    assert "4 not seen" in plan["error"]
    assert "plan_token" not in plan


def test_apply_task_instance_clear_clears_the_existing_instance(cleared_run):
    plan = server.plan_task_instance_clear(DAG_ID, position=3)

    result = server.apply_task_instance_clear(
        DAG_ID, plan["dag_run_id"], plan["task_ids"], plan["plan_token"]
    )

    assert result["cleared"] is True
    assert result["mutation_applied"] is True
    assert result["created_dag_run"] is False
    assert result["created_task_instances"] == []
    assert result["ui_updates"] == [
        {
            "kind": "task_instances",
            "dag_id": DAG_ID,
            "dag_run_id": "manual__1",
            "task_ids": ["report"],
        }
    ]
    # Exactly one clear, and it never reached for a new Dag run.
    assert len(cleared_run.cleared) == 1
    assert cleared_run.cleared[0]["dag_run_id"] == "manual__1"
    assert cleared_run.cleared[0]["prevent_running_task"] is True
    assert cleared_run.cleared[0]["run_on_latest_version"] is True
    assert ("POST", f"/dags/{DAG_ID}/dagRuns") not in cleared_run.calls


def test_apply_task_instance_clear_repeats_the_dry_run_before_writing(cleared_run):
    plan = server.plan_task_instance_clear(DAG_ID, position=3)
    # The task started running between the preview and the approval.
    cleared_run.tis_by_run["manual__1"][2]["state"] = "running"

    result = server.apply_task_instance_clear(
        DAG_ID, plan["dag_run_id"], plan["task_ids"], plan["plan_token"]
    )

    assert result["cleared"] is False
    assert result["mutation_applied"] is False
    assert "changed since the user reviewed it" in result["error"]
    assert cleared_run.cleared == []


def test_apply_task_instance_clear_refuses_without_a_reviewed_plan(cleared_run):
    result = server.apply_task_instance_clear(DAG_ID, "manual__1", ["report"])

    assert result["cleared"] is False
    assert "no reviewed plan" in result["error"]
    assert cleared_run.cleared == []


def test_apply_task_instance_clear_refuses_a_target_that_was_not_planned(cleared_run):
    plan = server.plan_task_instance_clear(DAG_ID, position=3)

    result = server.apply_task_instance_clear(DAG_ID, plan["dag_run_id"], ["summarize"], plan["plan_token"])

    assert result["cleared"] is False
    assert "not the task instances that were planned" in result["error"]
    assert cleared_run.cleared == []


def test_apply_task_instance_clear_reports_a_refused_clear_without_a_fallback(cleared_run):
    plan = server.plan_task_instance_clear(DAG_ID, position=3)
    cleared_run.fail_clear = httpx.HTTPStatusError(
        "conflict",
        request=httpx.Request("POST", "/clearTaskInstances"),
        response=httpx.Response(409, text="AirflowClearRunningTaskException"),
    )

    result = server.apply_task_instance_clear(
        DAG_ID, plan["dag_run_id"], plan["task_ids"], plan["plan_token"]
    )

    assert result["cleared"] is False
    assert result["mutation_applied"] is False
    assert "AirflowClearRunningTaskException" in result["error"]
    assert ("POST", f"/dags/{DAG_ID}/dagRuns") not in cleared_run.calls


def test_source_tools_refuse_bytes_they_were_not_authorized_for(airflow):
    """Airflow rewrites the latest version's source in place, so identity is the hash."""
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]
    airflow.parsed_source = "a Dag nobody checked\n"

    result = server.diagnose_dag(DAG_ID, source_digest=md5(SOURCE.encode()).hexdigest())

    assert "no longer the version this request was authorized against" in result["source"]


def test_source_tools_accept_the_bytes_they_were_authorized_for(airflow):
    airflow.runs = [{"dag_run_id": "manual__1", "state": "failed"}]

    result = server.diagnose_dag(DAG_ID, source_digest=md5(SOURCE.encode()).hexdigest())

    assert result["source"] == SOURCE


def test_rerun_dag_says_the_dag_is_still_unpaused_when_the_trigger_fails(airflow):
    """The unpause committed first; reporting only the failure would hide it."""
    airflow.is_paused = True
    token = server.rerun_dag(DAG_ID)["unpause_token"]
    airflow.fail_trigger = httpx.ConnectError("boom")

    result = server.rerun_dag(DAG_ID, unpause=True, unpause_token=token)

    assert result["triggered"] is False
    assert result["unpaused"] is True
    assert "is still unpaused" in result["error"]


def test_revert_dag_code_without_a_backup(airflow):
    result = server.revert_dag_code(DAG_ID)

    assert result["reverted"] is False
    assert "no backup" in result["error"]


def seed_two_runs(airflow):
    airflow.runs_by_id = {
        "old": {
            "state": "success",
            "duration": 30.0,
            "conf": {"column": "amount"},
            "dag_versions": [{"version_number": 1}],
        },
        "new": {
            "state": "failed",
            "duration": 45.0,
            "conf": {"column": "ammount", "retries": 2},
            "dag_versions": [{"version_number": 1}, {"version_number": 2}],
        },
    }
    airflow.tis_by_run = {
        "old": [
            {"task_id": "extract", "duration": 1.0},
            {"task_id": "summarize", "duration": 4.0},
        ],
        "new": [
            {"task_id": "extract", "duration": 1.5},
            {"task_id": "summarize", "duration": None},
        ],
    }
    airflow.sources_by_version = {
        1: 'op_kwargs={"column": "amount"}\n',
        2: 'op_kwargs={"column": "ammount"}\n',
    }


def test_compare_dag_runs_reports_duration_deltas(airflow):
    seed_two_runs(airflow)

    result = server.compare_dag_runs(DAG_ID, "old", "new")

    assert result["task_durations"] == [
        {"task_id": "extract", "run_a": 1.0, "run_b": 1.5, "delta": 0.5},
        {"task_id": "summarize", "run_a": 4.0, "run_b": None, "delta": None},
    ]
    assert result["run_a"]["state"] == "success"
    assert result["run_b"]["state"] == "failed"


def test_compare_dag_runs_reports_conf_changes_only(airflow):
    seed_two_runs(airflow)

    changes = server.compare_dag_runs(DAG_ID, "old", "new")["conf_changes"]

    assert changes == {
        "column": {"run_a": "amount", "run_b": "ammount"},
        "retries": {"run_a": None, "run_b": 2},
    }


def test_compare_dag_runs_refuses_outright_when_the_source_drifted(airflow):
    airflow.parsed_source = "something else\n"
    airflow.runs_by_id = {"r1": {"state": "success"}, "r2": {"state": "failed"}}

    result = server.compare_dag_runs(DAG_ID, "r1", "r2", md5(SOURCE.encode()).hexdigest())

    assert "no longer the version this request was authorized against" in result["error"]
    assert "task_durations" not in result


def test_compare_dag_runs_will_not_diff_versions_it_was_not_authorized_for(airflow):
    """An older version can hold a co-located Dag since removed and never checked."""
    seed_two_runs(airflow)

    result = server.compare_dag_runs(DAG_ID, "old", "new")

    assert "not authorized for" in result["source_diff"]
    # Notably it does not go and fetch either historical version.
    assert not any(params and "version_number" in params for params in airflow.params)


def test_compare_dag_runs_skips_the_diff_when_versions_match(airflow):
    seed_two_runs(airflow)
    airflow.runs_by_id["new"]["dag_versions"] = [{"version_number": 1}]

    result = server.compare_dag_runs(DAG_ID, "old", "new")

    assert result["source_diff"] is None
    assert not any(params and "version_number" in params for params in airflow.params)


@pytest.mark.parametrize(
    ("log_tail", "expected"),
    [
        ("KeyError: 'ammount'", "KeyError: '…'"),
        ("KeyError: 'region'", "KeyError: '…'"),
        (
            "ValueError: invalid literal for int() with base 10: 'None'",
            "ValueError: invalid literal for int() with base N: '…'",
        ),
        (
            "some INFO line\nTraceback (most recent call last):\n  boring frame",
            "Traceback (most recent call last):",
        ),
        ("", "unknown failure"),
    ],
    ids=["quoted-key", "same-shape-other-key", "digits", "prefers-error-line", "empty"],
)
def test_error_signature_normalises_equivalent_failures(log_tail, expected):
    assert server._error_signature(log_tail) == expected


def test_find_failure_clusters_groups_by_signature_biggest_first(airflow):
    airflow.task_instances = [
        {"dag_id": "etl", "task_id": "load", "dag_run_id": "r1", "try_number": 1},
        {"dag_id": "ml", "task_id": "train", "dag_run_id": "r2", "try_number": 1},
        {"dag_id": "etl", "task_id": "load", "dag_run_id": "r3", "try_number": 2},
    ]
    airflow.logs_by_task = {
        ("etl", "load"): "KeyError: 'ammount'",
        ("ml", "train"): "TimeoutError: deadline exceeded after 30 seconds",
    }

    result = server.find_failure_clusters(hours=6)

    assert result["failures_scanned"] == 3
    assert [c["count"] for c in result["clusters"]] == [2, 1]
    assert result["clusters"][0]["error"] == "KeyError: '…'"
    assert {e["dag_run_id"] for e in result["clusters"][0]["examples"]} == {"r1", "r3"}


def test_find_failure_clusters_scans_only_recent_failed_tis(airflow):
    server.find_failure_clusters(hours=6)

    listing = next(body for body in airflow.payloads if body and "state" in body)
    assert listing["state"] == ["failed"]
    assert listing["page_limit"] == server.FAILURE_SCAN_LIMIT
    assert "start_date_gte" in listing


def test_find_failure_clusters_drops_dags_outside_the_allowlist(airflow):
    """The allowlist is a filter we apply, not one we trust the API to have applied."""
    airflow.task_instances = [
        {"dag_id": "sales_summary", "task_id": "summarize", "dag_run_id": "r1", "try_number": 1},
        {"dag_id": "secret_dag", "task_id": "leak", "dag_run_id": "r2", "try_number": 1},
    ]
    airflow.log = "ValueError: boom"

    result = server.find_failure_clusters(24, dag_ids=["sales_summary"])

    assert airflow.payloads[0]["dag_ids"] == ["sales_summary"]
    seen = {ex["dag_id"] for cluster in result["clusters"] for ex in cluster["examples"]}
    assert seen == {"sales_summary"}
    assert result["failures_scanned"] == 1


def _approve(dag_id, from_date, to_date, plan=None):
    """plan_backfill, then run_backfill the way an honest caller would."""
    plan = plan if plan is not None else server.plan_backfill(dag_id, from_date, to_date)
    return server.run_backfill(dag_id, from_date, to_date, plan["plan_token"], plan["planned_runs"])


def test_plan_backfill_previews_without_creating_anything(airflow):
    airflow.dry_run_dates = [f"2026-07-{day:02}T00:00:00Z" for day in range(1, 26)]

    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-25")

    assert plan["planned_run_count"] == 25
    # Every run, not a first page: a token must not authorize what was never shown.
    assert len(plan["planned_runs"]) == 25
    assert plan["planned_runs"][0]["logical_date"] == "2026-07-01T00:00:00Z"
    # The whole point of the plan step: nothing may be written.
    assert ("POST", "/backfills") not in airflow.calls


def test_run_backfill_creates_the_backfill(airflow):
    airflow.dry_run_dates = [f"2026-07-{day:02}T00:00:00Z" for day in range(1, 9)]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-08")

    result = _approve(DAG_ID, "2026-07-01", "2026-07-08", plan)

    assert result == {
        "created": True,
        "mutation_applied": True,
        "backfill_id": 7,
        "dag_id": DAG_ID,
        "from_date": "2026-07-01",
        "to_date": "2026-07-08",
        "planned_run_count": 8,
        "is_paused": False,
        "ui_updates": [{"kind": "dag_run", "dag_id": DAG_ID}],
    }
    assert {"dag_id": DAG_ID, "from_date": "2026-07-01", "to_date": "2026-07-08"} in airflow.payloads


def test_plan_backfill_issues_no_token_for_a_plan_over_the_cap(airflow, monkeypatch):
    """It could never be created, and the preview is too long to have really been read."""
    monkeypatch.setattr(server, "MAX_BACKFILL_RUNS", 5)
    airflow.dry_run_dates = [f"2026-07-{day:02}T00:00:00Z" for day in range(1, 26)]

    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-25")

    assert "plan_token" not in plan
    assert "exceeds the 5-run limit" in plan["error"]


def test_run_backfill_refuses_more_runs_than_the_cap(airflow, monkeypatch):
    airflow.dry_run_dates = [f"2026-07-{day:02}T00:00:00Z" for day in range(1, 26)]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-25")
    # The cap tightens between the preview and the approval.
    monkeypatch.setattr(server, "MAX_BACKFILL_RUNS", 5)

    result = _approve(DAG_ID, "2026-07-01", "2026-07-25", plan)

    assert result["created"] is False
    assert result["planned_run_count"] == 25
    assert "exceeds the 5-run limit" in result["error"]
    assert ("POST", "/backfills") not in airflow.calls


def test_run_backfill_abandons_a_backfill_with_a_slot_it_could_not_fill(airflow):
    """A skipped slot keeps the planned identity but never became a run."""
    airflow.dry_run_dates = ["2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z"]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-02")
    airflow.skipped_dates = {"2026-07-02T00:00:00Z"}

    result = _approve(DAG_ID, "2026-07-01", "2026-07-02", plan)

    assert result["created"] is False
    assert airflow.cancelled is True


def test_run_backfill_refuses_without_a_reviewed_plan(airflow):
    airflow.dry_run_dates = ["2026-07-01T00:00:00Z"]

    result = server.run_backfill(DAG_ID, "2026-07-01", "2026-07-08")

    assert result["created"] is False
    assert "no reviewed plan" in result["error"]
    assert ("POST", "/backfills") not in airflow.calls


def test_run_backfill_refuses_a_range_the_user_did_not_review(airflow):
    """The token proves a plan was shown — not that it was a plan for *this* range."""
    airflow.dry_run_dates = ["2026-07-01T00:00:00Z"]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-02")

    result = _approve(DAG_ID, "2026-01-01", "2026-12-31", plan)

    assert result["created"] is False
    assert "not the ones planned" in result["error"]
    assert ("POST", "/backfills") not in airflow.calls


def test_run_backfill_refuses_when_the_plan_drifted_before_execution(airflow):
    airflow.dry_run_dates = [f"2026-07-{day:02}T00:00:00Z" for day in range(1, 4)]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-08")
    # A run appearing between preview and approval changes what gets created.
    airflow.dry_run_dates.append("2026-07-04T00:00:00Z")

    result = _approve(DAG_ID, "2026-07-01", "2026-07-08", plan)

    assert result["created"] is False
    assert "changed since the user reviewed it" in result["error"]
    assert ("POST", "/backfills") not in airflow.calls


def test_run_backfill_cancels_a_backfill_that_did_not_match_the_plan(airflow):
    """Preview and create are two calls, so what landed has to be checked."""
    airflow.dry_run_dates = ["2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z"]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-02")
    airflow.created_dates = ["2026-07-01T00:00:00Z"] * 9

    result = _approve(DAG_ID, "2026-07-01", "2026-07-02", plan)

    assert result["created"] is False
    assert result["created_run_count"] == 9
    assert result["cancelled"] is True
    assert airflow.cancelled is True


def test_run_backfill_rejects_a_same_sized_backfill_of_different_runs(airflow):
    """A matching count is not a matching plan."""
    airflow.dry_run_dates = ["2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z"]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-02")
    airflow.created_dates = ["2026-07-01T00:00:00Z", "2026-08-09T00:00:00Z"]

    result = _approve(DAG_ID, "2026-07-01", "2026-07-02", plan)

    assert result["created"] is False
    assert airflow.cancelled is True


def test_run_backfill_reports_runs_that_cancelling_could_not_stop(airflow):
    """Cancel fails *queued* runs; anything already running keeps going."""
    airflow.dry_run_dates = ["2026-07-01T00:00:00Z"]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-02")
    airflow.created_dates = ["2026-07-01T00:00:00Z"] * 3
    airflow.created_run_state = "running"

    result = _approve(DAG_ID, "2026-07-01", "2026-07-02", plan)

    assert result["cancelled"] is True
    assert len(result["surviving_runs"]) == 3
    assert "still going" in result["error"]


def test_run_backfill_says_so_loudly_when_the_cancel_also_fails(airflow):
    airflow.dry_run_dates = ["2026-07-01T00:00:00Z"]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-02")
    airflow.created_dates = ["2026-07-01T00:00:00Z"] * 9
    airflow.fail_cancel = httpx.ConnectError("boom")

    result = _approve(DAG_ID, "2026-07-01", "2026-07-02", plan)

    assert result["cancelled"] is False
    assert "CANCELLING IT FAILED" in result["error"]
    assert "backfill 7" in result["error"]


def test_run_backfill_plan_token_is_single_use(airflow):
    airflow.dry_run_dates = ["2026-07-01T00:00:00Z"]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-02")

    assert _approve(DAG_ID, "2026-07-01", "2026-07-02", plan)["created"] is True
    assert _approve(DAG_ID, "2026-07-01", "2026-07-02", plan)["created"] is False


def test_run_backfill_refuses_a_plan_the_confirmation_did_not_quote(airflow):
    """The runs go in the arguments so the confirm card shows them; they must be real."""
    airflow.dry_run_dates = ["2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z"]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-02")

    understated = server.run_backfill(
        DAG_ID, "2026-07-01", "2026-07-02", plan["plan_token"], [{"logical_date": "2026-07-01T00:00:00Z"}]
    )

    assert understated["created"] is False
    assert "must repeat the 2 runs" in understated["error"]
    assert ("POST", "/backfills") not in airflow.calls


def test_run_backfill_matches_the_same_instant_across_timezones(airflow):
    """The dry run may answer in the Dag's timezone; the created run reads back as UTC."""
    airflow.dry_run_dates = ["2026-07-01T02:00:00+02:00"]
    plan = server.plan_backfill(DAG_ID, "2026-07-01", "2026-07-02")
    airflow.created_dates = ["2026-07-01T00:00:00Z"]

    assert _approve(DAG_ID, "2026-07-01", "2026-07-02", plan)["created"] is True
    assert airflow.cancelled is False


def test_expired_tokens_are_not_redeemable(airflow, monkeypatch):
    token = server._issue_token("backfill", {"dag_id": DAG_ID})
    monkeypatch.setattr(server, "_TOKEN_TTL_S", -1.0)

    assert server._redeem_token("backfill", token) is None


def test_a_token_cannot_be_redeemed_as_another_kind(airflow):
    token = server._issue_token("unpause", {"dag_id": DAG_ID})

    assert server._redeem_token("backfill", token) is None


def test_get_blast_radius_maps_both_directions_through_assets(airflow):
    airflow.assets = [
        {
            "name": "sales_report",
            "producing_tasks": [{"dag_id": DAG_ID, "task_id": "report"}],
            # The self-reference must not put the Dag in its own blast radius.
            "scheduled_dags": [{"dag_id": "revenue_dashboard"}, {"dag_id": DAG_ID}],
            "consuming_tasks": [{"dag_id": "audit", "task_id": "check"}],
        },
        {
            "name": "raw_events",
            "producing_tasks": [{"dag_id": "ingest", "task_id": "collect"}],
            "scheduled_dags": [{"dag_id": DAG_ID}],
            "consuming_tasks": [],
        },
        {
            "name": "unrelated",
            "producing_tasks": [{"dag_id": "other", "task_id": "t"}],
            "scheduled_dags": [{"dag_id": "elsewhere"}],
            "consuming_tasks": [],
        },
    ]

    result = server.get_blast_radius(DAG_ID)

    assert result == {
        "dag_id": DAG_ID,
        "produces_assets": ["sales_report"],
        "downstream_dags": ["audit", "revenue_dashboard"],
        "consumes_assets": ["raw_events"],
        "upstream_dags": ["ingest"],
    }


def test_get_blast_radius_of_an_asset_free_dag_is_empty(airflow):
    airflow.assets = []

    result = server.get_blast_radius(DAG_ID)

    assert result["produces_assets"] == []
    assert result["downstream_dags"] == []


def test_rerun_dag_warns_instead_of_unpausing_on_a_first_call(airflow):
    airflow.is_paused = True

    result = server.rerun_dag(DAG_ID)

    assert result["triggered"] is False
    assert "resumes its scheduled runs" in result["error"]
    assert result["unpause_token"]
    assert ("PATCH", f"/dags/{DAG_ID}") not in airflow.calls
    assert ("POST", f"/dags/{DAG_ID}/dagRuns") not in airflow.calls


@pytest.mark.parametrize("token", ["", "forged"], ids=["missing", "forged"])
def test_rerun_dag_refuses_an_unpause_the_user_was_never_warned_about(airflow, token):
    airflow.is_paused = True

    result = server.rerun_dag(DAG_ID, unpause=True, unpause_token=token)

    assert result["triggered"] is False
    assert "needs the unpause_token" in result["error"]
    assert ("PATCH", f"/dags/{DAG_ID}") not in airflow.calls


def test_rerun_dag_refuses_an_unpause_token_issued_for_another_dag(airflow):
    airflow.is_paused = True
    other = server._issue_token("unpause", {"dag_id": "some_other_dag"})

    result = server.rerun_dag(DAG_ID, unpause=True, unpause_token=other)

    assert result["triggered"] is False
    assert ("PATCH", f"/dags/{DAG_ID}") not in airflow.calls


def test_rerun_dag_unpauses_once_the_warning_was_delivered(airflow):
    airflow.is_paused = True
    token = server.rerun_dag(DAG_ID)["unpause_token"]

    result = server.rerun_dag(DAG_ID, unpause=True, unpause_token=token)

    assert ("PATCH", f"/dags/{DAG_ID}") in airflow.calls
    assert result == {
        "triggered": True,
        "mutation_applied": True,
        "dag_id": DAG_ID,
        "dag_run_id": "manual__new",
        "state": "queued",
        "unpaused": True,
        "ui_updates": [{"kind": "dag_run", "dag_id": DAG_ID, "dag_run_id": "manual__new"}],
    }


def test_rerun_dag_leaves_an_active_dag_alone(airflow):
    result = server.rerun_dag(DAG_ID)

    assert ("PATCH", f"/dags/{DAG_ID}") not in airflow.calls
    assert result["triggered"] is True
    assert result["unpaused"] is False


def test_rerun_dag_sends_the_required_logical_date(airflow):
    # TriggerDAGRunPostBody.logical_date has no default: omitting it is a 422.
    server.rerun_dag(DAG_ID)
    assert {"logical_date": None, "conf": {}} in airflow.payloads


def test_force_reparse_tolerates_an_already_queued_request(airflow):
    airflow.reparse_status = 409
    result = _apply(('"column": "ammount"', '"column": "amount"'))
    assert result["applied"] is True
    assert "did not change" in result["reparse"]


def test_force_reparse_still_reports_other_http_errors(airflow):
    airflow.reparse_status = 500
    result = _apply(('"column": "ammount"', '"column": "amount"'))
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
