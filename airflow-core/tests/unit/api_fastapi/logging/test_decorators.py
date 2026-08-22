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

import asyncio
import json
from unittest.mock import MagicMock

import pytest
from fastapi import Request
from sqlalchemy.orm import Session

from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.logging.decorators import (
    _mask_connection_fields,
    _mask_variable_fields,
    _sanitize_for_stdlib_log,
    action_logging,
)


class TestSanitizeForStdlibLog:
    """User input passed to stdlib ``%s``-style logging must have CR/LF stripped.

    On a deployment configured with a non-JSON (plain-text) log formatter, a newline in the
    value would otherwise let an attacker forge log lines (CWE-117). The audit logging path
    in ``action_logging`` passes ``logical_date`` from the request through stdlib's ``logger``,
    so this sanitisation is unconditional regardless of the formatter actually in use.
    """

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
            ("bad\ndate", "bad date"),
            ("bad\r\ndate", "bad  date"),
            ("bad\rdate", "bad date"),
            ("a\nb\nc", "a b c"),
            ("", ""),
        ],
    )
    def test_strips_cr_and_lf(self, raw, expected):
        assert _sanitize_for_stdlib_log(raw) == expected


class TestMaskConnectionFields:
    """Connection ``extra`` values must never be recorded verbatim in the audit log.

    Secrets routinely live in connection ``extra`` under key names that are not in the
    masker's sensitive-key list (``sas_url``, ``endpoint``, ``jdbc_url``, …). Masking by
    key name alone therefore lets those values through. ``_mask_connection_fields`` fails
    closed instead: every ``extra`` value is masked and only the key names are kept.
    """

    def test_masks_extra_value_under_non_sensitive_key(self):
        result = _mask_connection_fields(
            {"extra": json.dumps({"sas_url": "SAS_LEAK_value", "account_name": "prodacct"})}
        )
        assert result["extra"] == {"sas_url": "***", "account_name": "***"}

    def test_masks_extra_value_under_sensitive_key(self):
        result = _mask_connection_fields({"extra": json.dumps({"password": "hunter2"})})
        assert result["extra"] == {"password": "***"}

    def test_preserves_extra_key_names(self):
        result = _mask_connection_fields({"extra": json.dumps({"host": "h", "login": "l", "endpoint": "e"})})
        assert set(result["extra"]) == {"host", "login", "endpoint"}
        assert all(v == "***" for v in result["extra"].values())

    @pytest.mark.enable_redact
    def test_top_level_password_is_masked_by_key_name(self):
        # Top-level connection fields keep the existing key-name redaction path
        # (unchanged by this fix); it only masks when the secrets masker is active.
        result = _mask_connection_fields({"connection_id": "c1", "password": "hunter2"})
        assert result["connection_id"] == "c1"
        assert result["password"] == "***"

    def test_non_dict_json_extra_returns_informational_string(self):
        result = _mask_connection_fields({"extra": json.dumps(["not", "a", "dict"])})
        assert result["extra"] == "Expected JSON object in `extra` field, got non-dict JSON"

    def test_non_json_extra_returns_informational_string(self):
        result = _mask_connection_fields({"extra": "this is not json"})
        assert result["extra"] == "Encountered non-JSON in `extra` field"

    def test_empty_extra_passes_through_key_name_redaction(self):
        # falsy ``extra`` skips the JSON branch and goes through key-name redaction
        result = _mask_connection_fields({"extra": ""})
        assert result["extra"] == ""


class TestMaskVariableFields:
    """The variable value is masked unconditionally in the audit log.

    A secret stored as a Variable under an innocuous key name (e.g.
    ``campaign_signing_material``) must not be persisted to the audit log, so the value is
    masked regardless of the key name; the key and description are kept.
    """

    def test_masks_value_under_non_sensitive_key(self):
        result = _mask_variable_fields(
            {"key": "campaign_signing_material", "value": "VARVAL_LEAK_token", "description": "d"}
        )
        assert result == {"key": "campaign_signing_material", "value": "***", "description": "d"}

    def test_masks_value_under_sensitive_key(self):
        result = _mask_variable_fields({"key": "api_secret", "value": "topsecret"})
        assert result == {"key": "api_secret", "value": "***"}

    def test_masks_val_alias(self):
        result = _mask_variable_fields({"key": "k", "val": "secretval"})
        assert result == {"key": "k", "val": "***"}

    def test_value_without_key_is_still_masked(self):
        result = _mask_variable_fields({"value": "secretval"})
        assert result == {"value": "***"}


class TestMaskBulkFields:
    """The bulk endpoints nest their entities, and the masking has to reach them."""

    def test_bulk_variable_values_are_masked(self):
        result = _mask_variable_fields(
            {
                "actions": [
                    {
                        "action": "create",
                        "entities": [
                            {"key": "campaign_signing_material", "value": "VARVAL_LEAK_token"},
                            {"key": "other", "val": "VARVAL_LEAK_alias"},
                        ],
                        "action_on_existence": "overwrite",
                    }
                ]
            }
        )
        assert result == {
            "actions": [
                {
                    "action": "create",
                    "entities": [
                        {"key": "campaign_signing_material", "value": "***"},
                        {"key": "other", "val": "***"},
                    ],
                    "action_on_existence": "overwrite",
                }
            ]
        }

    def test_bulk_connection_extra_is_masked(self):
        """``extra`` is the load-bearing case for connections.

        Key-name redaction already covered a nested ``password`` -- but only while
        ``hide_sensitive_var_conn_fields`` is enabled, which is a deployment setting and is off
        in this test environment. ``extra`` was never covered by it at all: the name is not a
        recognised sensitive field, and the value is a JSON *string*, which ``redact`` returns
        unchanged. Masking ``extra`` is structural here, so it does not depend on that setting.
        """
        result = _mask_connection_fields(
            {
                "actions": [
                    {
                        "action": "create",
                        "entities": [
                            {
                                "connection_id": "c1",
                                "conn_type": "http",
                                "password": "CONN_LEAK_pw",
                                "extra": json.dumps({"token": "CONN_LEAK_token", "region": "eu"}),
                            }
                        ],
                    }
                ]
            }
        )
        entity = result["actions"][0]["entities"][0]
        assert entity["extra"] == {"token": "***", "region": "***"}
        assert entity["connection_id"] == "c1"
        # every value is gone, only the key names of ``extra`` remain
        assert "CONN_LEAK_token" not in json.dumps(result)

    @pytest.mark.parametrize(
        ("body", "masker"),
        [
            (
                {"actions": [{"action": "create", "entities": [{"key": "k", "value": "LEAK_v"}]}]},
                _mask_variable_fields,
            ),
            (
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [{"connection_id": "c", "extra": json.dumps({"t": "LEAK_v"})}],
                        }
                    ]
                },
                _mask_connection_fields,
            ),
        ],
        ids=["variables", "connections"],
    )
    def test_no_secret_survives_in_the_serialized_entry(self, body, masker):
        """The audit entry is serialized whole, so assert on the serialization, not one field."""
        assert "LEAK_v" not in json.dumps(masker(body))

    def test_multiple_actions_and_entities_are_all_masked(self):
        result = _mask_variable_fields(
            {
                "actions": [
                    {
                        "action": "create",
                        "entities": [{"key": "a", "value": "s1"}, {"key": "b", "value": "s2"}],
                    },
                    {"action": "update", "entities": [{"key": "c", "value": "s3"}]},
                ]
            }
        )
        values = [e["value"] for a in result["actions"] for e in a["entities"]]
        assert values == ["***", "***", "***"]

    def test_delete_by_key_entities_are_left_alone(self):
        """``delete`` may list bare keys rather than entity objects; those carry no secret."""
        body = {"actions": [{"action": "delete", "entities": ["key_one", "key_two"]}]}
        assert _mask_variable_fields(body) == body

    @pytest.mark.parametrize(
        "body",
        [
            {"key": "k", "value": "secret"},
            {"actions": "not-a-list"},
            {"actions": [{"action": "create"}]},
            {"actions": [{"action": "create", "entities": "not-a-list"}]},
            {"actions": ["not-a-dict"]},
        ],
        ids=["flat-body", "actions-not-list", "no-entities", "entities-not-list", "action-not-dict"],
    )
    def test_non_bulk_and_malformed_shapes_do_not_raise(self, body):
        """The masker runs on request bodies before validation, so it must not add a failure mode."""
        _mask_variable_fields(body)
        _mask_connection_fields(body)

    @pytest.mark.parametrize(
        "extra",
        [
            pytest.param({"token": "CONN_LEAK_token"}, id="already-decoded-object"),
            pytest.param(["CONN_LEAK_token"], id="already-decoded-array"),
            pytest.param(123, id="number"),
            pytest.param(True, id="bool"),
        ],
    )
    def test_non_string_extra_does_not_raise_and_does_not_leak(self, extra):
        """``extra`` reaches this before validation, so it need not be a string.

        ``json.loads`` raises ``TypeError`` rather than ``JSONDecodeError`` for a non-string, which
        would otherwise escape the audit-log decorator.
        """
        body = {"actions": [{"action": "create", "entities": [{"connection_id": "c1", "extra": extra}]}]}

        result = _mask_connection_fields(body)

        assert "CONN_LEAK_token" not in json.dumps(result)
        assert result["actions"][0]["entities"][0]["connection_id"] == "c1"

    def test_flat_body_still_takes_the_single_entity_path(self):
        assert _mask_variable_fields({"key": "k", "value": "secret"}) == {"key": "k", "value": "***"}


class TestActionLoggingUserFields:
    """The audit Log records the raw identifier in ``owner`` and the human-friendly
    ``get_display_name()`` in ``owner_display_name``."""

    def test_owner_and_display_name_use_the_matching_user_methods(self):
        class FakeUser(BaseUser):
            def get_id(self) -> str:
                return "id"

            def get_name(self) -> str:
                return "jdoe"

            def get_display_name(self) -> str:
                return "Jane Doe"

        request = Request({"type": "http", "method": "GET", "headers": [], "query_string": b""})
        session = MagicMock(spec=Session)

        asyncio.run(action_logging(event="test_event")(request=request, session=session, user=FakeUser()))

        (logged,) = session.add.call_args.args
        assert logged.owner == "jdoe"
        assert logged.owner_display_name == "Jane Doe"


class _FakeUser(BaseUser):
    def get_id(self) -> str:
        return "id"

    def get_name(self) -> str:
        return "jdoe"

    def get_display_name(self) -> str:
        return "Jane Doe"


def _run_action_logging(event, *, path_params=None, query_string=b"", body=None, body_attribution_fields=()):
    scope = {
        "type": "http",
        "method": "POST",
        "headers": [(b"content-type", b"application/json")] if body is not None else [],
        "query_string": query_string,
        "path_params": path_params or {},
    }
    if body is None:
        request = Request(scope)
    else:
        body_bytes = json.dumps(body).encode()

        async def receive():
            return {"type": "http.request", "body": body_bytes, "more_body": False}

        request = Request(scope, receive=receive)

    session = MagicMock(spec=Session)
    asyncio.run(
        action_logging(event=event, body_attribution_fields=body_attribution_fields)(
            request=request, session=session, user=_FakeUser()
        )
    )
    (logged,) = session.add.call_args.args
    return logged


class TestActionLoggingAttribution:
    """The audit Log's ``dag_id`` / ``task_id`` / ``run_id`` columns gate per-Dag read access,
    so the free-form request body must not be able to set them. They come from the route's
    path/query params, plus only the body fields a route explicitly opts into."""

    def test_body_cannot_set_attribution_columns_without_optin(self):
        logged = _run_action_logging(
            "post_variable_like_event",
            body={"dag_id": "finance_etl", "task_id": "payout", "run_id": "manual__2026", "foo": "bar"},
        )
        assert logged.dag_id is None
        assert logged.task_id is None
        assert logged.run_id is None
        # the body is still recorded verbatim in ``extra`` for forensics, just not in the columns
        assert "finance_etl" in logged.extra

    def test_attribution_columns_come_from_path_params(self):
        logged = _run_action_logging(
            "test_event",
            path_params={"dag_id": "real_dag", "task_id": "real_task", "run_id": "real_run"},
        )
        assert logged.dag_id == "real_dag"
        assert logged.task_id == "real_task"
        assert logged.run_id == "real_run"

    def test_query_string_cannot_set_attribution_columns(self):
        # Undeclared query params are ignored by the route but still readable here; the URL is
        # attacker-supplied on any route, so the query string must not feed the columns either.
        logged = _run_action_logging(
            "test_event",
            query_string=b"dag_id=finance_etl&task_id=payout&run_id=manual__2026",
        )
        assert logged.dag_id is None
        assert logged.task_id is None
        assert logged.run_id is None

    def test_opted_in_body_field_populates_column(self):
        logged = _run_action_logging(
            "post_clear_task_instances",
            path_params={"dag_id": "real_dag"},
            body={"dag_run_id": "body_run", "dry_run": False},
            body_attribution_fields=("dag_run_id",),
        )
        assert logged.dag_id == "real_dag"
        assert logged.run_id == "body_run"

    def test_opted_in_body_field_cannot_override_the_same_path_param(self):
        logged = _run_action_logging(
            "some_event",
            path_params={"dag_run_id": "path_run"},
            body={"dag_run_id": "body_run"},
            body_attribution_fields=("dag_run_id",),
        )
        # even opted in, the body must not overwrite the authoritative path value
        assert logged.run_id == "path_run"

    def test_opt_in_does_not_let_body_override_a_path_param(self):
        logged = _run_action_logging(
            "create_backfill",
            path_params={"dag_id": "path_dag"},
            body={"dag_id": "body_dag", "dag_run_id": "body_run"},
            body_attribution_fields=("dag_run_id",),
        )
        # ``dag_id`` was not opted in, so the body value is ignored and the path value stands
        assert logged.dag_id == "path_dag"
        assert logged.run_id == "body_run"
