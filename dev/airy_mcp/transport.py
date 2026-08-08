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
"""
The HTTP conversation with Airflow, and the words for its failures.

Turn a method + path into a decoded response body, re-authenticating once on a
401, and turn an exception into a string that is safe to relay.  Nothing here
knows what a task instance is, what a page is, or what ``total_entries`` means -
it knows paths and status codes.  This module imports nothing from its siblings.

Wave 2 of the move-only extraction in ``docs/extraction-plan.md``.

``_api`` and ``API_URL`` are the suite's control points for "who is allowed to
talk to Airflow", so callers reach them as ``transport._api(...)`` rather than
importing them by name: one module object, one binding, one patch point.  They
are deliberately NOT re-exported from ``server.py`` - a re-export there would
be a display symbol that a ``monkeypatch.setattr(server, "_api", ...)`` could
rebind without touching the call path.
"""

from __future__ import annotations

import json
import os
from contextlib import suppress
from typing import Any
from urllib.parse import quote

import httpx

API_URL = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080").rstrip("/")
USERNAME = os.environ.get("AIRFLOW_USERNAME", "admin")
PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")

_token: str | None = None


def _login() -> str:
    resp = httpx.post(f"{API_URL}/auth/token", json={"username": USERNAME, "password": PASSWORD}, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def _api(method: str, path: str, **kwargs: Any) -> Any:
    """Call the Airflow REST API, logging in (or re-logging in) as needed."""
    global _token
    if _token is None:
        _token = _login()
    url = f"{API_URL}/api/v2{path}"
    resp = httpx.request(method, url, headers={"Authorization": f"Bearer {_token}"}, timeout=60, **kwargs)
    if resp.status_code == 401:
        _token = _login()
        resp = httpx.request(method, url, headers={"Authorization": f"Bearer {_token}"}, timeout=60, **kwargs)
    resp.raise_for_status()
    return resp.json() if resp.content else None


def _dag_url(dag_id: str, suffix: str = "") -> str:
    """Build a /dags/... path. Ids are model-supplied, so they are always escaped."""
    return f"/dags/{quote(dag_id, safe='')}{suffix}"


def _explain_unknown_dag(dag_id: str, e: httpx.HTTPStatusError) -> str | None:
    """A relayable message for a Dag the caller cannot see — or ``None`` to re-raise.

    A typo'd or unauthorized dag_id is a conversation, not a traceback: the raw
    ``httpx`` error names internal URLs and reads as a crash, and the model
    cannot relay it. 403 gets the same words as 404 on purpose — telling an
    unauthorized caller "it exists, you just can't see it" confirms the id.
    """
    if e.response.status_code in (403, 404):
        return f"Dag {dag_id!r} does not exist or you cannot see it"
    return None


def _explain_error(e: Exception) -> str:
    """An error as words a refusal can carry.

    ``str()`` of an ``httpx.HTTPStatusError`` names the internal URL it hit, so
    it must never reach a relayable string; the response body's ``detail`` is
    the API's own words for what went wrong, and the status code is the fallback.
    A transport error is the same hazard by another route — ``httpx`` builds its
    message from the URL it could not reach — so only the class name is relayed.
    """
    if isinstance(e, httpx.RequestError):
        return e.__class__.__name__
    if not isinstance(e, httpx.HTTPStatusError):
        return str(e) or e.__class__.__name__
    detail = None
    with suppress(ValueError, AttributeError):  # non-JSON body, or a shapeless one
        detail = e.response.json().get("detail")
    if isinstance(detail, str) and detail.strip():
        return detail.strip()
    if detail is not None:
        return json.dumps(detail)[:400]
    return f"HTTP {e.response.status_code}"


def _api_detail(response: httpx.Response) -> str:
    """The API's own ``detail``, or an empty string when it did not give one."""
    try:
        body = response.json()
    except ValueError:
        return ""
    detail = body.get("detail") if isinstance(body, dict) else None
    if detail in (None, "", [], {}):
        return ""
    return detail if isinstance(detail, str) else json.dumps(detail, default=str)
