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
"""Unit tests for the incident_triage showcase Dag's pure functions and structure.

Run: ``uv run --project airflow-core pytest dev/airy_mcp/test_incident_triage.py -q``
"""

# Root pyproject.toml carries per-file S101 ignores for test files; this file
# cannot edit it, so the exemption lives here instead.
# ruff: noqa: S101

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest import mock

import incident_digest_dag
import incident_triage_dag as triage
import pytest
from incident_triage_dag import (
    INCIDENT_TEMPLATES,
    POISON_INCIDENT_ID,
    POISON_TIMESTAMP,
    SEVERITY_ORDER,
    ExecutiveSummary,
    IncidentAssessment,
    MalformedIncidentTimestampError,
    assess_offline,
    build_fixture_incidents,
    build_offline_summary,
    build_report,
    highest_severity,
    normalize_incidents,
    pick_severity_route,
    severity_meets,
    tally_severities,
)

MOMENT = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)


def classified(severities: list[str]) -> list[dict[str, str]]:
    return [
        {
            "id": f"INC-{100 + i}",
            "service": f"svc-{i}",
            "description": f"incident {i}",
            "reported_at": MOMENT.isoformat(),
            "severity": severity,
            "rationale": "test",
        }
        for i, severity in enumerate(severities)
    ]


class TestFixtureIncidents:
    @pytest.mark.parametrize(
        "moment",
        [
            datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc),
            datetime(2026, 8, 7, 3, 30, tzinfo=timezone.utc),
            datetime(2025, 12, 31, 23, 59, tzinfo=timezone.utc),
        ],
    )
    def test_deterministic_for_a_given_moment(self, moment):
        assert build_fixture_incidents(moment) == build_fixture_incidents(moment)

    def test_same_date_different_time_yields_same_seed_batch(self):
        morning = build_fixture_incidents(MOMENT.replace(hour=1))
        evening = build_fixture_incidents(MOMENT.replace(hour=23))
        assert [i["id"] for i in morning] == [i["id"] for i in evening]
        assert [i["description"] for i in morning] == [i["description"] for i in evening]

    def test_different_dates_yield_different_batches(self):
        a = build_fixture_incidents(MOMENT)
        b = build_fixture_incidents(MOMENT + timedelta(days=1))
        assert a != b

    def test_poison_record_is_always_last(self):
        incidents = build_fixture_incidents(MOMENT)
        assert incidents[-1]["id"] == POISON_INCIDENT_ID
        assert incidents[-1]["reported_at"] == POISON_TIMESTAMP

    def test_ids_unique_and_shape(self):
        incidents = build_fixture_incidents(MOMENT)
        ids = [incident["id"] for incident in incidents]
        assert len(ids) == len(set(ids))
        assert 7 <= len(incidents) <= 10
        assert all(set(incident) == {"id", "service", "description", "reported_at"} for incident in incidents)


class TestNormalizeIncidents:
    def test_poison_record_fails_by_default(self):
        with pytest.raises(MalformedIncidentTimestampError) as exc_info:
            normalize_incidents(
                build_fixture_incidents(MOMENT),
                skip_invalid=False,
                window_hours=48,
                window_end=MOMENT,
            )
        message = str(exc_info.value)
        assert POISON_INCIDENT_ID in message
        assert POISON_TIMESTAMP in message
        assert '"skip_invalid": true' in message
        assert "clear" in message
        # Prose recoveries, not "(1)/(2)" — numbers nest confusingly inside
        # diagnose_dag's own numbered summary.
        assert "(1)" not in message
        assert "(2)" not in message
        assert "either re-trigger" in message
        assert len(message) <= 340

    def test_skip_invalid_drops_poison_record(self):
        result = normalize_incidents(
            build_fixture_incidents(MOMENT),
            skip_invalid=True,
            window_hours=48,
            window_end=MOMENT,
        )
        assert [incident["id"] for incident in result["invalid"]] == [POISON_INCIDENT_ID]
        assert POISON_INCIDENT_ID not in {incident["id"] for incident in result["kept"]}

    @pytest.mark.parametrize(
        ("age_hours", "window_hours", "bucket"),
        [
            (2, 24, "kept"),
            (36, 24, "out_of_window"),
            (24, 24, "kept"),
            (25, 24, "out_of_window"),
        ],
    )
    def test_window_filter(self, age_hours, window_hours, bucket):
        incident = {
            "id": "INC-1",
            "service": "svc",
            "description": "d",
            "reported_at": (MOMENT - timedelta(hours=age_hours)).isoformat(),
        }
        result = normalize_incidents(
            [incident], skip_invalid=False, window_hours=window_hours, window_end=MOMENT
        )
        assert [i["id"] for i in result[bucket]] == ["INC-1"]

    def test_naive_timestamps_treated_as_utc(self):
        incident = {
            "id": "INC-1",
            "service": "svc",
            "description": "d",
            "reported_at": (MOMENT - timedelta(hours=1)).replace(tzinfo=None).isoformat(),
        }
        result = normalize_incidents([incident], skip_invalid=False, window_hours=24, window_end=MOMENT)
        assert len(result["kept"]) == 1
        assert datetime.fromisoformat(result["kept"][0]["reported_at"]).tzinfo is not None


class TestOfflineClassifier:
    @pytest.mark.parametrize(
        ("template_index", "expected_severity"),
        [
            (0, "critical"),  # data loss
            (1, "critical"),  # outage
            (2, "high"),  # timeout
            (3, "high"),  # failing
            (4, "high"),  # backlog growing
            (5, "medium"),  # degraded
            (6, "medium"),  # elevated latency
            (7, "low"),
            (8, "low"),
            (9, "medium"),  # retrying
        ],
    )
    def test_templates_map_to_expected_severity(self, template_index, expected_severity):
        assessment = assess_offline(INCIDENT_TEMPLATES[template_index])
        assert isinstance(assessment, IncidentAssessment)
        assert assessment.severity == expected_severity
        assert assessment.rationale

    def test_output_survives_schema_round_trip(self):
        dumped = assess_offline(INCIDENT_TEMPLATES[0]).model_dump()
        assert IncidentAssessment.model_validate(dumped).severity == "critical"

    def test_deterministic(self):
        incident = {"service": "svc", "description": "connection timeout again"}
        assert assess_offline(incident) == assess_offline(incident)


class TestEngineDecision:
    @pytest.fixture
    def choose_engine(self):
        return triage.triage_dag.get_task("choose_engine").python_callable

    @mock.patch.object(triage.BaseHook, "get_connection", autospec=True)
    def test_connection_present_routes_llm(self, mock_get_connection, choose_engine):
        assert choose_engine() == "llm"
        mock_get_connection.assert_called_once_with(triage.LLM_CONN_ID)

    @mock.patch.object(
        triage.BaseHook, "get_connection", autospec=True, side_effect=Exception("no such connection")
    )
    def test_api_key_without_connection_routes_offline(self, mock_get_connection, choose_engine, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        assert choose_engine() == "offline"

    @mock.patch.object(
        triage.BaseHook, "get_connection", autospec=True, side_effect=Exception("no such connection")
    )
    def test_no_credentials_routes_offline(self, mock_get_connection, choose_engine, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        assert choose_engine() == "offline"


class TestSeverityHelpers:
    @pytest.mark.parametrize(
        ("severity", "threshold", "expected"),
        [
            ("low", "high", False),
            ("high", "high", True),
            ("critical", "high", True),
            ("medium", "low", True),
            ("low", "critical", False),
        ],
    )
    def test_severity_meets(self, severity, threshold, expected):
        assert severity_meets(severity, threshold) is expected

    def test_tally_and_highest(self):
        records = classified(["low", "high", "high", "critical"])
        assert tally_severities(records) == {"low": 1, "medium": 0, "high": 2, "critical": 1}
        assert highest_severity(records) == "critical"
        assert highest_severity([]) is None

    @pytest.mark.parametrize(
        ("severities", "threshold", "expected"),
        [
            (["low", "medium"], "high", "digest.queue_for_digest"),
            (["low", "high"], "high", "page.assemble_page"),
            (["critical"], "critical", "page.assemble_page"),
            ([], "low", "digest.queue_for_digest"),
        ],
    )
    def test_pick_severity_route(self, severities, threshold, expected):
        assert pick_severity_route(classified(severities), threshold) == expected


class TestOfflineSummary:
    def test_schema_valid_and_deterministic(self):
        records = classified(["low", "high", "critical"])
        summary = build_offline_summary(records)
        assert isinstance(summary, ExecutiveSummary)
        assert summary == build_offline_summary(records)
        assert "highest severity: critical" in summary.headline
        assert "INC-102" in summary.overview
        assert "Page the on-call" in summary.recommended_action

    def test_empty_batch(self):
        summary = build_offline_summary([])
        assert ExecutiveSummary.model_validate(summary.model_dump()) == summary
        assert "No incidents" in summary.headline


class TestBuildReport:
    def test_report_contents(self):
        records = classified(["low", "high"])
        summary = build_offline_summary(records).model_dump()
        report = build_report(
            records=records,
            summary=summary,
            engine="offline",
            window_hours=24,
            threshold="high",
            moment_iso=MOMENT.isoformat(),
            invalid_count=1,
            out_of_window_count=2,
        )
        assert report.startswith("# Incident triage report")
        assert summary["headline"] in report
        assert "| high | 1 |" in report
        assert "| low | 1 |" in report
        assert "INC-100" in report
        assert "INC-101" in report
        assert "dropped: 1 invalid, 2 outside window" in report
        assert "Classification engine: offline" in report
        assert report == build_report(
            records=records,
            summary=summary,
            engine="offline",
            window_hours=24,
            threshold="high",
            moment_iso=MOMENT.isoformat(),
            invalid_count=1,
            out_of_window_count=2,
        )

    def test_empty_report(self):
        report = build_report(
            records=[],
            summary=build_offline_summary([]).model_dump(),
            engine="offline",
            window_hours=6,
            threshold="low",
            moment_iso=MOMENT.isoformat(),
            invalid_count=0,
            out_of_window_count=0,
        )
        assert "_No incidents in the window._" in report


class TestDagStructure:
    def test_triage_dag_shape(self):
        dag = triage.triage_dag
        assert dag.dag_id == "incident_triage"
        assert {
            "ingest",
            "normalize",
            "choose_engine",
            "route_classification",
            "classify_llm",
            "classify_offline",
            "collect_classifications",
            "route_summary",
            "summarize_llm",
            "summarize_offline",
            "collect_summary",
            "route_by_severity",
            "page.assemble_page",
            "page.notify_oncall",
            "digest.queue_for_digest",
            "publish_report",
        } == set(dag.task_ids)

    def test_params_defaults_and_enum(self):
        dag = triage.triage_dag
        assert dag.params["window_hours"] == 24
        assert dag.params["severity_threshold"] == "high"
        assert dag.params["skip_invalid"] is False
        threshold_param = dag.params.get_param("severity_threshold")
        assert threshold_param.schema["enum"] == list(SEVERITY_ORDER)

    def test_severity_branch_targets_exist(self):
        dag = triage.triage_dag
        route = dag.get_task("route_by_severity")
        assert route.downstream_task_ids == {"page.assemble_page", "digest.queue_for_digest"}
        assert dag.get_task("route_classification").downstream_task_ids == {
            "classify_llm",
            "classify_offline",
        }
        assert dag.get_task("route_summary").downstream_task_ids == {
            "summarize_llm",
            "summarize_offline",
        }

    def test_publish_report_emits_the_report_asset(self):
        publish = triage.triage_dag.get_task("publish_report")
        assert [asset.name for asset in publish.outlets] == ["incident_report"]

    def test_digest_dag_scheduled_on_the_report_asset(self):
        dag = incident_digest_dag.digest_dag
        assert dag.dag_id == "incident_digest"
        asset_names = {asset.name for asset in dag.timetable.asset_condition.objects}
        assert asset_names == {"incident_report"}
        assert set(dag.task_ids) == {"deliver_digest"}
