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
Airflow Chatbot Plugin — "Airy".

This plugin provides an LLM-based chatbot assistant that appears as a floating
button in the Airflow UI.  It uses **PydanticAI** to talk to any supported LLM
(OpenAI by default) and optionally connects to an *astro-airflow-mcp* sidecar
process so the LLM can inspect DAGs, runs, tasks, logs, and more.

Configuration
-------------
* **LLM API key** — set ``OPENAI_API_KEY`` env var on the host (simplest), or
  store as an Airflow *Connection* (``conn_id='openai_default'``, key in the
  *password* field) for encrypted-at-rest storage.
* **Model name** — Airflow *Variable* ``airy_model`` (default ``gpt-4o-mini``).
* **MCP server URLs** — Airflow *Variable* ``airy_mcp_url``, comma-separated
  (default: the read-only sidecar on ``:8000`` plus the self-healing one on
  ``:8001``).  Set to empty string to disable MCP.

In Breeze, just ``export OPENAI_API_KEY=sk-...`` before ``breeze start-airflow``.
The Breeze image already ships ``pydantic-ai-slim`` + ``openai``; the init
script installs the MCP sidecar.
"""

from __future__ import annotations

import builtins
import json
import logging
import os
import secrets
import socket
import sys
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from fastapi import Depends, FastAPI
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import select
from starlette.middleware.base import BaseHTTPMiddleware

from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.responses import Response
    from starlette.types import ASGIApp

log = logging.getLogger(__name__)

# Path to the built React app static files
STATIC_DIR = Path(__file__).parent / "www" / "dist"


class ChatRequest(BaseModel):
    """Request body for the /chat endpoint."""

    message: str = Field(..., min_length=1)
    history: list[dict[str, str]] = Field(default_factory=list)
    page_url: str | None = None

    model_config = {"arbitrary_types_allowed": True}


class ConfirmRequest(BaseModel):
    """Request body for the /confirm endpoint."""

    nonce: str = Field(..., min_length=1)
    approved: bool


def _get_base_url_path(path: str) -> str:
    """Construct URL path with webserver base_url prefix."""
    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith(("http://", "https://")):
        base_path = urlparse(base_url).path
    else:
        base_path = base_url

    base_path = base_path.rstrip("/")
    return base_path + path


def _create_chatbot_api() -> dict[str, Any]:
    """Create the FastAPI app for serving chatbot static files and API."""
    # Function-local so processes that import the plugin module without serving
    # the API (workers, dag processor) don't pull in the API stack.
    from airflow.api_fastapi.core_api import security

    app = FastAPI(
        title="Airflow Chatbot",
        description="LLM-powered chatbot assistant for Apache Airflow",
        # Every route needs a logged-in Airflow user: /health leaks MCP topology
        # and /chat drives the MCP tools.
        dependencies=[Depends(security.requires_authenticated())],
    )

    # Mount static files if the dist directory exists.  Mounts bypass FastAPI
    # dependencies, which is fine here: the bundle is public JS with no secrets.
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="chatbot_static")

    @app.get("/")
    async def root():
        """Health check endpoint."""
        return JSONResponse({"status": "ok", "plugin": "airflow-chatbot"})

    @app.get("/health")
    async def health_check():
        """Detailed health check — verifies LLM key availability and MCP reachability."""
        llm_ok = False
        llm_source: str | None = None
        mcp_ok = False
        mcp_url_val = ""

        # Check LLM key
        api_key = _get_llm_api_key()
        if api_key:
            llm_ok = True
            # Determine source for diagnostics
            try:
                from airflow.models.connection import Connection

                conn = Connection.get_connection_from_secrets("openai_default")
                if conn.password:
                    llm_source = "connection"
            except Exception:
                pass
            if not llm_source and os.environ.get("OPENAI_API_KEY"):
                llm_source = "env"

        urls = _get_mcp_urls()
        reachable = _reachable_mcp_urls(urls)
        mcp_ok = bool(reachable)
        mcp_url_val = ",".join(urls)

        return JSONResponse(
            {
                "status": "ok" if llm_ok else "degraded",
                "llm": {"configured": llm_ok, "source": llm_source},
                "mcp": {
                    "configured": bool(urls),
                    "reachable": mcp_ok,
                    "url": mcp_url_val,
                    "unreachable": [url for url in urls if url not in reachable],
                    # A missing pydantic-ai[mcp] leaves Airy confidently tool-less
                    # with every TCP probe still green — surface it here instead.
                    "toolset_importable": _mcp_toolset_importable(),
                },
            }
        )

    @app.get("/bundle")
    async def get_bundle():
        """Serve the main JavaScript bundle."""
        bundle_path = STATIC_DIR / "main.umd.cjs"
        if bundle_path.exists():
            return FileResponse(bundle_path, media_type="application/javascript")
        return JSONResponse({"error": "Bundle not found"}, status_code=404)

    @app.post("/chat")
    async def chat_endpoint(body: ChatRequest, user=Depends(security.get_user)):
        """
        Chat endpoint — server-sent events.

        Streams the agent's tool calls and text as they happen, so the drawer can
        show what Airy is doing instead of a spinner.  Frames are
        ``data: {json}`` with a ``type`` of ``tool``, ``tool_result``, ``text``,
        ``confirm_required``, ``error`` or ``done``.
        """
        if not body.message.strip():
            return JSONResponse({"error": "Empty message", "status": "error"}, status_code=400)

        return _sse_response(
            _stream_agent(
                body.message,
                body.history,
                body.page_url,
                can_write=_user_can_write(user),
                user_id=str(user.get_id()),
                user=user,
            )
        )

    @app.post("/confirm")
    async def confirm_endpoint(body: ConfirmRequest, user=Depends(security.get_user)):
        """
        Approve or reject a write tool call suspended by /chat.

        Streams the rest of the agent run as the same SSE frames as /chat.
        """
        pending = _get_pending(body.nonce)
        if pending is None:
            return JSONResponse({"error": "Unknown or expired confirmation"}, status_code=404)
        if pending.user_id != str(user.get_id()) or not _user_can_write(user):
            # A failed attempt still burns the nonce.
            _drop_pending(body.nonce)
            return JSONResponse({"error": "Forbidden"}, status_code=403)

        # Not popped on the way in: a write can land and the connection drop before
        # the browser sees it. The record outlives the stream so asking again
        # replays what happened instead of running it a second time.
        if pending.state in ("executing", "interrupted", "done"):
            return _sse_response(_replay(pending))
        pending.state = "executing"
        pending.approved = body.approved
        return _sse_response(_resume_agent(pending, body.approved, user))

    return {
        "app": app,
        "url_prefix": "/chatbot",
        "name": "Airflow Chatbot API",
    }


# ---------------------------------------------------------------------------
# PydanticAI agent
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are **Airy**, the AI assistant embedded in the Apache Airflow UI.

Your job is to help Airflow users with:
• Understanding and managing their DAGs, tasks, and runs
• Debugging failures (reading logs, diagnosing errors)
• Writing and improving DAG code
• Explaining Airflow concepts

When you have access to MCP tools (Airflow API), USE them proactively to look
up real data instead of giving generic advice.  For example, if a user asks
"why did my DAG fail?", call the relevant tool to fetch recent runs and logs
before answering.

Keep answers concise and actionable.  Use Markdown formatting.

**Shape of an answer.**  Present findings as short labelled sections or a small
table — never nested bullet lists.  One level of bullets maximum.  Lead with the
answer; put logs, tracebacks and raw tool output in a fenced block at the end,
not in prose.  No greetings or filler.

**Page context.**  The system prompt may end with a `Current page:` line — the
path the user is looking at right now.  Use it to resolve words like "this"
and "here": `/dags/sales_summary/grid` means questions are about the
`sales_summary` Dag unless the user says otherwise.
"""

_WRITE_PROMPT = """\

**Self-healing.**  Every change is planned first and written second.  The
planning tools are read-only and hand back a `plan_token`; the write tools
refuse without it.  A write tool suspends until the user approves it with in-UI
Confirm/Reject buttons, so **calling the write tool *is* the proposal**: never
ask for permission in prose, never say you are "about to", "will now" or are
"proceeding with" a change, and never report a change as made without the tool
result that says so.

1. **Diagnose everything, not just what failed first.**  `diagnose_dag` returns
   every task instance, every failed task's log, the task graph, the source and
   deterministic `checks`.  Report every high-confidence problem in one answer,
   and separate them: a **confirmed failure** is backed by a log, a **latent
   blocker** is backed by the source or graph and has not run yet.
2. **Repair as one change.**  Put *every* fix in a single
   `plan_dag_code_changes` call — a second plan made after the first one lands
   was computed against source that no longer exists.  Show the diff and any
   `blocking` entry, then call `apply_dag_code_changes` with the same changes
   and the token.  A plan with blockers has no token: explain what it would
   break instead of applying it.  If the plan reports `asset_review_needed`,
   call `get_blast_radius` and say what else the change moves before proposing
   the write.
3. **Clearing is not re-running.**  To re-run a task inside a run that already
   exists — "clear", "retry this task", "same run" — use
   `plan_task_instance_clear` and then `apply_task_instance_clear`.  `rerun_dag`
   creates a *new* Dag run and is never an implementation of clearing.  The
   clear takes everything downstream of the task with it, which is what makes
   the re-run mean anything — tell the user which instances `affected` lists.
   If the plan refuses (an ambiguous position, nothing to clear, a task set that
   would move), report that and clear nothing.
4. After a successful fix, offer to re-run — do not re-run on your own.
5. `revert_dag_code` restores the *original* file and discards every change you
   applied, not just the last one. Say that before proposing it.
"""

_READ_ONLY_PROMPT = """\

**Read-only access.**  This session has no write tools: you can diagnose and
explain, but applying fixes, re-running or backfilling requires Dag-edit
permission the user does not have.  If asked to change anything, say so.
"""

_FOLLOWUP_PROMPT = """\

**Follow-up buttons.**  When the obvious next step is a *question you would
answer*, end your reply with one or more lines of the form
`[ACTION: <what the user should say>]`.  They are rendered as clickable buttons,
so write them as the user's own words, e.g. `[ACTION: Show me the log for
summarize]`.  Put nothing after them.

Never use one to stand in for a change you could propose yourself: a write is
proposed by calling its write tool, which the user then approves or rejects.  A
button that only asks the user to ask you again is the slow way to do nothing.
"""


def _get_llm_api_key() -> str | None:
    """
    Retrieve the LLM API key from Airflow Connections or environment.

    Resolution order:
    1. Airflow Connection ``openai_default`` (password field)
    2. ``OPENAI_API_KEY`` environment variable (handy for quick local dev)
    """
    # 1. Try Airflow Connection first (encrypted at rest)
    try:
        from airflow.models.connection import Connection

        conn = Connection.get_connection_from_secrets("openai_default")
        if conn.password:
            return conn.password
    except Exception:
        pass

    # 2. Fall back to plain env var
    return os.environ.get("OPENAI_API_KEY") or None


_DEFAULT_MCP_URLS = "http://localhost:8000/mcp,http://localhost:8001/mcp"


def _reachable_mcp_urls(urls: list[str]) -> list[str]:
    """
    Filter MCP endpoints down to the ones actually listening.

    TCP connect rather than an HTTP probe, because an MCP server need not serve
    GET / with 200.  This is not cosmetic: pydantic-ai raises out of
    ``agent.run()`` if *any* attached toolset fails to initialise, so attaching a
    dead sidecar takes the whole chat down instead of just its tools.
    """
    reachable = []
    for url in urls:
        parsed = urlparse(url)
        try:
            with socket.create_connection((parsed.hostname or "localhost", parsed.port or 8000), 2):
                reachable.append(url)
        except OSError:
            log.warning("MCP endpoint %s is not reachable — Airy will run without its tools", url)
    return reachable


def _mcp_toolset_importable() -> bool:
    """Whether pydantic-ai's MCP extra is installed at all."""
    try:
        from pydantic_ai.mcp import MCPToolset  # noqa: F401
    except ImportError:
        return False
    return True


def _get_mcp_urls() -> list[str]:
    """MCP endpoints to attach — the read-only sidecar plus the self-healing one."""
    return [url.strip() for url in _get_variable("airy_mcp_url", _DEFAULT_MCP_URLS).split(",") if url.strip()]


def _get_variable(key: str, default: str) -> str:
    """Read an Airflow Variable, returning *default* on any error."""
    try:
        from airflow.models.variable import Variable

        return Variable.get(key, default_var=default)
    except Exception:
        return default


_NOT_CONFIGURED = (
    "**Airy is not configured yet.**\n\n"
    "Provide an OpenAI API key via **one** of these methods:\n\n"
    "**Option A** — Environment variable (simplest):\n"
    "```\nexport OPENAI_API_KEY=sk-...\n```\n\n"
    "**Option B** — Airflow Connection (encrypted at rest):\n"
    "- **Conn ID**: `openai_default`\n"
    "- **Conn Type**: `openai`\n"
    "- **Password**: your OpenAI API key\n\n"
    "In Breeze, just set `OPENAI_API_KEY` on your host before "
    "running `breeze start-airflow` — everything else is automatic."
)

# BaseExceptionGroup is a builtin only from 3.11; an empty tuple makes the
# isinstance check below a harmless no-op on older interpreters.
_EXC_GROUP = getattr(builtins, "BaseExceptionGroup", ())


def _root_cause(exc: BaseException) -> BaseException:
    """Unwrap TaskGroup ExceptionGroups so the user sees the real error."""
    while isinstance(exc, _EXC_GROUP) and exc.exceptions:
        exc = exc.exceptions[0]
    return exc


def _render_system_prompt(page_url: str | None, can_write: bool = False) -> str:
    """Tell Airy which page the user is on, so "this" and "here" resolve."""
    prompt = _SYSTEM_PROMPT + (_WRITE_PROMPT if can_write else _READ_ONLY_PROMPT) + _FOLLOWUP_PROMPT
    if not page_url:
        return prompt
    # The value comes from the browser: keep it one line and bounded before it
    # joins the highest-trust part of the conversation.
    page = page_url.replace("\n", " ").replace("\r", " ")[:500]
    return f"{prompt}\nCurrent page: {page}\n"


def _is_authorized_dag(
    user: Any,
    *,
    method: str,
    dag_id: str | None,
    access_entity: Any = None,
    team_name: str | None = None,
) -> bool:
    """Ask the auth manager about one Dag — or, with no ``dag_id``, about any Dag."""
    # Module singleton, not request.app.state: inside a mounted sub-app,
    # request.app is the sub-app and carries no auth manager.
    from airflow.api_fastapi.app import get_auth_manager
    from airflow.api_fastapi.auth.managers.models.resource_details import DagDetails

    return get_auth_manager().is_authorized_dag(
        method=method,
        user=user,
        access_entity=access_entity,
        details=DagDetails(id=dag_id, team_name=team_name) if dag_id else None,
    )


def _writable_tools(user: Any) -> frozenset[str]:
    """
    Return the write tools this user could run against *some* Dag.

    Per tool, not one "may you edit a Dag?": clearing a task instance and
    rewriting a Dag file are different permissions on the real API, and a user
    who holds one and not the other should be offered exactly what they hold.
    (Under the simple auth manager both answers come from the role, so this
    changes nothing there; under FAB they are genuinely separate.)
    """
    return frozenset(
        name
        for name in WRITE_TOOLS
        if all(
            _is_authorized_dag(user, method=method, dag_id=None, access_entity=entity)
            for method, entity in _tool_access_requirements(name, {})
        )
    )


def _user_can_write(user: Any) -> bool:
    """Whether any write tool is available at all — the gate on offering them."""
    return bool(_writable_tools(user))


# The only tools Airy will run. A name-based *denylist* of writers fails open:
# a sidecar that gains a new mutating tool would be treated as a read, needing
# neither write permission nor a confirmation. So the policy is the allowlist,
# and every entry states what the tool does.
#
#   writes        mutates Airflow — filtered out for viewers, confirmed for editors
#   reads_source  hands back Dag source, so the whole source file must be readable
#   reads_assets  derived from the asset table
#   fleet         may be called with no dag_id, scoped by a ``dag_ids`` allowlist
TOOL_POLICY: dict[str, dict[str, bool]] = {
    "diagnose_dag": {"reads_source": True},
    "compare_dag_runs": {"reads_source": True},
    "find_failure_clusters": {"fleet": True},
    "get_blast_radius": {"reads_assets": True},
    "plan_backfill": {},
    # Read-only, but it reads the whole source file to plan against it, so it is
    # held to the same co-located-Dag rule as the write it precedes.
    "plan_dag_code_changes": {"reads_source": True},
    "plan_task_instance_clear": {},
    "apply_dag_code_changes": {"writes": True, "reads_source": True},
    "apply_task_instance_clear": {"writes": True},
    "revert_dag_code": {"writes": True, "reads_source": True},
    "rerun_dag": {"writes": True},
    "run_backfill": {"writes": True},
}

WRITE_TOOLS = frozenset(name for name, policy in TOOL_POLICY.items() if policy.get("writes"))

# Read-only tools that hand back a single-use token. A run that gets one and
# then proposes nothing has narrated a change instead of offering it.
PLAN_TOOLS = frozenset({"plan_dag_code_changes", "plan_task_instance_clear", "plan_backfill"})

_UNPROPOSED_PLAN_CORRECTION = (
    "You planned a change and then did not propose it — or proposed it without the plan_token, which "
    "the write tool will refuse. Call the matching write tool now, with the exact arguments you "
    "planned and the plan_token it returned, so the user gets an approval card that can actually be "
    "applied. If you are not going to propose it, say so in one sentence and say why. Do not describe "
    "the change again."
)


# Every denial from the authorization wrapper opens with this. A write refused
# here never ran, and the drawer has to be able to tell that from a write that
# did — the tool returns it as an ordinary result, not as an error.
_ACCESS_DENIED = "Access denied: "


def _authorized_dag_ids(user: Any) -> set[str]:
    """
    Return the Dags this user may read.

    Not ``is_authorized_dag(details=None)``: under FAB that answers "may you
    *list* Dags", which is true for anyone holding read on a single Dag.
    """
    from airflow.api_fastapi.app import get_auth_manager

    return get_auth_manager().get_authorized_dag_ids(user=user, method="GET")


def _tool_access_requirements(tool_name: str, tool_args: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    """
    Return the ``(method, access_entity)`` pairs Airflow's routes demand of this tool.

    A tool is a bundle of REST calls, and each call has its own permission on the
    real API — ``diagnose_dag`` alone reads runs, task instances, logs and source.
    Authorizing the bundle as one Dag-level read would hand over logs to someone
    the log route itself would refuse, so every underlying call is checked.

    Only tools in ``TOOL_POLICY`` get here; an unknown one is refused before this.
    """
    from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity as Entity

    runs = (("GET", Entity.RUN), ("GET", Entity.TASK_INSTANCE))
    # Every one of these tools reads ``GET /dags/{dag_id}`` — for the file
    # location, the paused flag, or the run's Dag — and that route wants plain
    # GET on the Dag, which no entity-scoped permission implies.
    read_dag = ("GET", None)
    # Patching a Dag's source edits the Dag, reads its code, and reads its
    # versions to tell whether the reparse landed; there is no write-the-code
    # permission in Airflow to mirror.
    patch_source = (("PUT", None), read_dag, ("GET", Entity.CODE), ("GET", Entity.VERSION))
    # Airflow's clearTaskInstances route carries one PUT-on-TASK_INSTANCE
    # dependency that covers its dry run as well, so the planner is held to the
    # same permission as the clear it previews. Both also list the run's
    # instances to see whether re-queuing it would change the task set.
    clear = (
        ("PUT", Entity.TASK_INSTANCE),
        ("GET", Entity.TASK_INSTANCE),
        ("GET", Entity.RUN),
        ("GET", Entity.TASK),
    )
    requirements: dict[str, tuple[tuple[str, Any], ...]] = {
        # Reads the Dag itself (for its file location), its runs, instances,
        # logs, source and task graph.
        "diagnose_dag": (
            read_dag,
            *runs,
            ("GET", Entity.TASK_LOGS),
            ("GET", Entity.CODE),
            ("GET", Entity.TASK),
        ),
        # Planning reads the source and the graph it would disturb; applying
        # rewrites the file, so it needs everything a patch needs.
        "plan_dag_code_changes": (read_dag, ("GET", Entity.CODE), ("GET", Entity.TASK)),
        "apply_dag_code_changes": patch_source,
        "plan_task_instance_clear": clear,
        "apply_task_instance_clear": clear,
        # Scans task instances fleet-wide, then reads each failure's log.
        "find_failure_clusters": (("GET", Entity.TASK_INSTANCE), ("GET", Entity.TASK_LOGS)),
        "compare_dag_runs": (*runs, ("GET", Entity.CODE)),
        # Cross-Dag neighbours are what DEPENDENCIES exists to expose; the asset
        # rows the answer is derived from are checked separately.
        "get_blast_radius": (("GET", Entity.DEPENDENCIES),),
        "revert_dag_code": patch_source,
        # Reads the Dag to see whether it is paused, then creates a run.
        # Unpausing is a separate, lasting edit — only demanded when actually asked for.
        "rerun_dag": (read_dag, ("POST", Entity.RUN))
        + ((("PUT", None),) if tool_args.get("unpause") else ()),
        # Airflow gates even the backfill dry run on POST; mirror that.
        "plan_backfill": (("POST", Entity.RUN),),
        # Creating it is POST, but it then reads back what landed and may cancel
        # it — all three are separate permissions on the real backfill routes.
        "run_backfill": (("POST", Entity.RUN), ("GET", Entity.RUN), ("PUT", Entity.RUN)),
    }
    return requirements[tool_name]


def _policy(tool_name: str, trait: str) -> bool:
    return bool(TOOL_POLICY.get(tool_name, {}).get(trait))


def _parsed_source_digest(dag_id: str) -> str | None:
    """
    Return the identity of the source the co-located check is being decided against.

    The *content* hash, not the version number: ``DagCode.update_source_code``
    rewrites the latest version's source in place when it changes, so version N
    is not a stable set of bytes and pinning to it would still let a Dag appear
    that nobody was checked against.
    """
    from airflow.models.dagcode import DagCode
    from airflow.utils.session import create_session

    with create_session() as session:
        code = DagCode.get_latest_dagcode(dag_id, session=session)
        return code.source_code_hash if code else None


def _dag_ids_sharing_file(dag_id: str) -> list[str]:
    """Every Dag defined in the same file as ``dag_id`` — the real blast radius of a patch."""
    from airflow.models.dag import DagModel
    from airflow.utils.session import create_session

    with create_session() as session:
        target = session.execute(
            select(DagModel.bundle_name, DagModel.relative_fileloc).where(DagModel.dag_id == dag_id)
        ).one_or_none()
        if target is None:
            return [dag_id]
        siblings = session.scalars(
            select(DagModel.dag_id).where(
                DagModel.bundle_name == target.bundle_name,
                DagModel.relative_fileloc == target.relative_fileloc,
            )
        ).all()
    return sorted(set(siblings) | {dag_id})


def _authorize_tool_call(user: Any, tool_name: str, tool_args: dict[str, Any]) -> str | None:
    """
    Return the denial to send instead of running ``tool_name``, or ``None`` to allow it.

    The sidecars call Airflow as one admin service account, so without this the
    signed-in user's permissions would not reach the tools at all: any logged-in
    user could read any Dag, and edit rights on one Dag would authorize writes
    against every other.
    """
    from airflow.api_fastapi.app import get_auth_manager
    from airflow.models.dag import DagModel

    if tool_name not in TOOL_POLICY:
        # Fail closed. Classifying an unknown tool by guesswork is how a sidecar
        # that gains a mutating tool ends up running it as a read, with neither
        # write permission nor a confirmation.
        log.warning("Airy refused %s: not in TOOL_POLICY", tool_name)
        return (
            f"{_ACCESS_DENIED}{tool_name} is not a tool Airy is allowed to run. "
            f"Tell the user this; do not retry."
        )

    # Scoping a fleet-wide call works by rewriting its arguments in place, which
    # only reaches the sidecar if these *are* the arguments. Anything else and
    # the narrowing would be silently dropped.
    if not isinstance(tool_args, dict):
        return f"{_ACCESS_DENIED}{tool_name} was called with arguments Airy cannot check. Do not retry."

    args = tool_args
    dag_id = args.get("dag_id")
    if not isinstance(dag_id, str) or not dag_id:
        return _authorize_fleet_wide_call(user, tool_name, args)

    if _policy(tool_name, "reads_assets") and not (
        # Both, because ``GET /assets`` itself demands both: an alias is another
        # name for an asset, so reading the graph without alias access would
        # hand back edges the route would have refused.
        get_auth_manager().is_authorized_asset(method="GET", user=user)
        and get_auth_manager().is_authorized_asset_alias(method="GET", user=user)
    ):
        return (
            f"{_ACCESS_DENIED}{tool_name} reads the asset graph, which the signed-in user cannot. "
            f"Tell the user this; do not retry."
        )

    if _policy(tool_name, "reads_source"):
        # Authorization is about to be decided over the Dags in one *version* of
        # this file. Pin the tool to that version, or it will go and fetch
        # whatever the processor has landed by the time it runs — which may
        # include a Dag that was never part of this decision.
        digest = _parsed_source_digest(dag_id)
        if digest is None:
            return (
                f"{_ACCESS_DENIED}Airflow has not parsed a version of Dag {dag_id!r} yet, so there is "
                f"nothing {tool_name} can safely read. Tell the user this; do not retry."
            )
        args["source_digest"] = digest

    targets = _authorization_targets(tool_name, dag_id, _tool_access_requirements(tool_name, args))
    # The team scopes the permission: asking without it is a different question
    # from the one the real route asks, and team-scoped managers answer it differently.
    teams = DagModel.get_dag_id_to_team_name_mapping([target for target, _ in targets])
    for target, requirements in targets:
        for method, access_entity in requirements:
            if _is_authorized_dag(
                user,
                method=method,
                dag_id=target,
                access_entity=access_entity,
                team_name=teams.get(target),
            ):
                continue
            needed = f"{method} on {access_entity.value}" if access_entity else f"{method} on the Dag"
            log.warning("Airy denied %s: %s %s on Dag %s", tool_name, user, needed, target)
            if target != dag_id:
                # Naming the sibling would leak the cross-tenant Dag id that
                # withholding the file exists to protect. /dagSources is generic
                # for the same reason.
                return (
                    f"{_ACCESS_DENIED}{dag_id!r} shares a source file with another Dag the signed-in "
                    f"user may not read. Tell the user this; do not retry."
                )
            return (
                f"{_ACCESS_DENIED}{tool_name} needs {needed} for Dag {target!r}, which the "
                f"signed-in user does not have. Tell the user this; do not retry."
            )
    return None


def _authorization_targets(
    tool_name: str, dag_id: str, requirements: tuple[tuple[str, Any], ...]
) -> list[tuple[str, tuple[tuple[str, Any], ...]]]:
    """Which Dags must clear which permissions — a source file can hold more than the one named."""
    if not _policy(tool_name, "reads_source"):
        return [(dag_id, requirements)]
    co_located = [other for other in _dag_ids_sharing_file(dag_id) if other != dag_id]
    if _policy(tool_name, "writes"):
        # A patch rewrites the file, so every Dag in it is edited.
        return [(dag_id, requirements), *((other, requirements) for other in co_located)]
    # Source comes back whole. This is the rule /dagSources enforces by redacting:
    # every Dag in the file has to be readable, or the caller sees code they cannot.
    return [(dag_id, requirements), *((other, (("GET", None),)) for other in co_located)]


def _readable_dag_ids_for(user: Any, tool_name: str) -> list[str]:
    """Return the Dags this user may run ``tool_name`` against, cleared entity by entity."""
    from airflow.models.dag import DagModel

    candidates = sorted(_authorized_dag_ids(user))
    if not candidates:
        return []
    teams = DagModel.get_dag_id_to_team_name_mapping(candidates)
    requirements = _tool_access_requirements(tool_name, {})
    return [
        dag_id
        for dag_id in candidates
        if all(
            _is_authorized_dag(
                user,
                method=method,
                dag_id=dag_id,
                access_entity=access_entity,
                team_name=teams.get(dag_id),
            )
            for method, access_entity in requirements
        )
    ]


def _authorize_fleet_wide_call(user: Any, tool_name: str, tool_args: dict[str, Any]) -> str | None:
    """
    Authorize a tool that names no Dag — so it would otherwise speak for all of them.

    A preflight "may you read every Dag?" would be a snapshot, and the sidecar's
    wildcard scan runs as admin afterwards: a Dag created in between comes back
    unauthorized.  So instead of gating the call, this narrows it — the readable
    Dag ids are written into the arguments, overwriting whatever the model asked
    for, and the sidecar filters to them.  Anything outside is never fetched.
    """
    if tool_name in WRITE_TOOLS:
        return f"{_ACCESS_DENIED}{tool_name} must name the Dag it changes. Tell the user this; do not retry."

    if not _policy(tool_name, "fleet"):
        # No allowlist to narrow with — the read-only sidecar's listings, whose
        # signatures are not ours. A "may you read every Dag?" gate would only be
        # a snapshot, and the admin-backed call runs after it, so a Dag created in
        # between comes back unauthorized. There is no safe version of this call.
        return (
            f"{_ACCESS_DENIED}{tool_name} names no Dag and cannot be scoped to the ones the signed-in "
            f"user may read. Ask them for a specific dag_id instead."
        )
    cleared = _readable_dag_ids_for(user, tool_name)
    if not cleared:
        return (
            f"{_ACCESS_DENIED}the signed-in user may not read any Dag that {tool_name} would report on. "
            f"Ask them for a specific dag_id instead."
        )
    tool_args["dag_ids"] = cleared
    return None


_dag_auth_toolset_class: type | None = None


def _dag_auth_toolset(wrapped: Any, user: Any) -> Any:
    """
    Wrap a toolset so every call is authorized as the signed-in user.

    The class is built on first use because pydantic-ai is an optional import
    in this module.
    """
    global _dag_auth_toolset_class
    if _dag_auth_toolset_class is None:
        from pydantic_ai.toolsets import WrapperToolset

        @dataclass
        class DagAuthToolset(WrapperToolset):
            user: Any = None

            async def call_tool(self, name, tool_args, ctx, tool):  # type: ignore[no-untyped-def]
                denial = _authorize_tool_call(self.user, name, tool_args)
                if denial:
                    log.warning("Airy denied %s: %s", name, denial)
                    return denial
                return await super().call_tool(name, tool_args, ctx, tool)

        _dag_auth_toolset_class = DagAuthToolset
    return _dag_auth_toolset_class(wrapped, user)


def _gate_toolsets(toolsets: list[Any], can_write: bool, user: Any = None) -> list[Any]:
    """
    Viewers get Airy without the write tools; editors get them behind a confirm.

    Both layers sit *inside* the per-Dag authorization wrapper, so an approved
    call is re-authorized when /confirm resumes it — approving a write against
    one Dag can never execute against another.
    """
    # Unknown tools are not offered at all — the authorization wrapper would
    # refuse them anyway, and a tool the model can see is a tool it will try.
    #
    # The policy is keyed on bare names, and names are only unique *within* a
    # sidecar: a second sidecar that happens to ship a ``rerun_dag`` of its own
    # would pass this filter on our entry's authority and be called in its
    # place. pydantic-ai refuses to attach two toolsets sharing a name, which
    # turns that confusion into a startup error rather than a silent swap — so
    # the fix for a collision is to rename ours, never to prefix past it.
    known = [ts.filtered(lambda ctx, tool_def: tool_def.name in TOOL_POLICY) for ts in toolsets]
    # Each write tool on its own merits: holding "clear a task instance" is not
    # holding "rewrite this Dag's file", and offering both for either is how a
    # user ends up approving a card the API then refuses.
    allowed = _writable_tools(user) if can_write and user is not None else WRITE_TOOLS
    offered = [
        ts.filtered(lambda ctx, tool_def: tool_def.name not in WRITE_TOOLS or tool_def.name in allowed)
        for ts in known
    ]
    if can_write:
        # The model can request a write, but the run suspends until the user
        # approves it in the UI (see /confirm) — prompt prose is not a gate.
        gated = [
            ts.approval_required(lambda ctx, tool_def, args: tool_def.name in WRITE_TOOLS) for ts in offered
        ]
    else:
        gated = [ts.filtered(lambda ctx, tool_def: tool_def.name not in WRITE_TOOLS) for ts in known]
    return [_dag_auth_toolset(ts, user) for ts in gated]


_CONFIRM_TTL_S = 600.0
_CONFIRM_MAX_PENDING = 50


@dataclass
class _PendingApproval:
    """
    A suspended agent run waiting for the user's verdict on a write tool.

    Outlives its own execution: ``state`` and ``frames`` are what let a second
    /confirm for the same nonce report the outcome rather than repeat the write.
    """

    user_id: str
    call_ids: list[str]
    messages: list[Any]
    page_url: str | None
    created_at: float
    state: str = "pending"
    approved: bool | None = None
    frames: list[dict[str, Any]] = field(default_factory=list)


# In-memory and per-process: enough for the demo's single api-server worker;
# a multi-worker deployment needs a shared store.
_pending_approvals: dict[str, _PendingApproval] = {}


def _purge_expired_pending(now: float) -> None:
    for nonce in [n for n, p in _pending_approvals.items() if now - p.created_at > _CONFIRM_TTL_S]:
        del _pending_approvals[nonce]


def _store_pending(*, user_id: str, call_ids: list[str], messages: list[Any], page_url: str | None) -> str:
    now = time.monotonic()
    _purge_expired_pending(now)
    while len(_pending_approvals) >= _CONFIRM_MAX_PENDING:
        del _pending_approvals[next(iter(_pending_approvals))]
    nonce = secrets.token_urlsafe(16)
    _pending_approvals[nonce] = _PendingApproval(user_id, call_ids, messages, page_url, now)
    return nonce


def _get_pending(nonce: str) -> _PendingApproval | None:
    _purge_expired_pending(time.monotonic())
    return _pending_approvals.get(nonce)


def _drop_pending(nonce: str) -> None:
    _pending_approvals.pop(nonce, None)


# Frame that tells the drawer "still don't know" — without it, the terminating
# `done` of a replay reads as settlement, which is the opposite of the truth.
UNSETTLED_FRAME = {"type": "unsettled"}


def _replay(pending: _PendingApproval) -> AsyncIterator[dict[str, Any]]:
    """Re-send a decided confirmation's frames instead of executing it again."""

    async def frames() -> AsyncIterator[dict[str, Any]]:
        verdict = "approved" if pending.approved else "rejected"
        if pending.state == "executing":
            yield {"type": "text", "delta": f"_This action was {verdict} and is still running._\n\n"}
            yield UNSETTLED_FRAME
            return
        if pending.state == "interrupted":
            yield {
                "type": "text",
                "delta": (
                    f"_This action was {verdict}, but the connection dropped while it ran, so whether "
                    f"it finished is unknown. Check the Dag before trying again._\n\n"
                ),
            }
            for frame in pending.frames:
                yield frame
            yield UNSETTLED_FRAME
            return
        yield {"type": "text", "delta": f"_This action was already {verdict}._\n\n"}
        for frame in pending.frames:
            yield frame

    return frames()


def _build_agent(
    page_url: str | None = None, can_write: bool = False, user: Any = None
) -> tuple[Any, str | None]:
    """Return ``(agent, None)``, or ``(None, markdown)`` explaining what is missing."""
    api_key = _get_llm_api_key()
    if not api_key:
        return None, _NOT_CONFIGURED

    try:
        from pydantic_ai import Agent
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.openai import OpenAIProvider
    except ImportError as e:
        return None, (
            "**Missing dependency.**\n\n"
            f"`pydantic-ai` (>= 1.0) import failed: `{e}`. Run:\n"
            "```\npip install 'pydantic-ai-slim[openai,mcp]'\n```"
        )

    model = OpenAIChatModel(
        _get_variable("airy_model", "gpt-4o-mini"), provider=OpenAIProvider(api_key=api_key)
    )

    toolsets = []
    urls = _reachable_mcp_urls(_get_mcp_urls())
    if urls:
        try:
            from pydantic_ai.mcp import MCPToolset

            toolsets = [MCPToolset(url) for url in urls]
        except ImportError:
            log.exception("pydantic-ai MCP extra missing — Airy is running without any tools")

    return Agent(
        model=model,
        system_prompt=_render_system_prompt(page_url, can_write),
        toolsets=_gate_toolsets(toolsets, can_write, user),
    ), None


def _to_message_history(history: list[dict[str, str]] | None) -> list[Any]:
    from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, UserPromptPart

    message_history: list[Any] = []
    for entry in history or []:
        role, text = entry.get("role", ""), entry.get("content", "")
        if role == "user":
            message_history.append(ModelRequest(parts=[UserPromptPart(content=text)]))
        elif role == "assistant":
            message_history.append(ModelResponse(parts=[TextPart(content=text)]))
    return message_history


_RESULT_CLIP_CHARS = 4000

# Also matched in _event_payload: a denied write tool comes back as a plain
# tool return carrying this text, and the drawer must not paint it as success.
_DENIAL_MESSAGE = "The user rejected this action."

# The self-healing tools report a refused write as an ordinary return, not as an
# error: ``{"applied": false, "error": …}`` when the file drifted under them or
# the plan expired, ``{"triggered": false}`` for a paused Dag, ``{"cleared":
# false}`` when the target moved. Left alone, the drawer paints those green and
# tells the user their Dag was changed when nothing was written.
#
# ``mutation_applied`` is the field every write tool now reports; the rest stay
# because reading them costs nothing and a tool that forgets the new field must
# not thereby become un-checkable.
_WRITE_OUTCOME_KEYS = ("mutation_applied", "applied", "reverted", "triggered", "created", "cleared")


def _write_refused(content: Any) -> bool:
    """Whether a write tool's result says it changed nothing."""
    if isinstance(content, str):
        # The per-Dag authorization wrapper answers with this instead of calling
        # the tool at all — the user may edit *some* Dag, so the tool was
        # offered, and this one was refused. Nothing ran, and a green
        # "Edited Dag code" over a refusal is the worst lie the drawer can tell.
        if content.startswith(_ACCESS_DENIED):
            return True
        try:
            content = json.loads(content)
        except ValueError:
            return False
    return isinstance(content, dict) and any(content.get(key) is False for key in _WRITE_OUTCOME_KEYS)


_UI_UPDATE_KINDS = ("dag_definition", "dag_run", "task_instances")


def _resource_changed_frame(tool_name: str, content: Any) -> dict[str, Any] | None:
    """
    Build the refresh a landed write earns, read from the tool's own result.

    Never inferred from prose or from the tool's name: the frame's only job is
    to tell already-authorized UI queries to refetch, and a frame sent for a
    write that did not happen would refresh a view into saying it did. So it
    takes the tool at its word only when that word is ``mutation_applied``.
    """
    if tool_name not in WRITE_TOOLS:
        return None
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except ValueError:
            return None
    if not isinstance(content, dict) or content.get("mutation_applied") is not True:
        return None
    updates = [
        update
        for update in content.get("ui_updates") or []
        if isinstance(update, dict)
        and update.get("kind") in _UI_UPDATE_KINDS
        and isinstance(update.get("dag_id"), str)
        and update["dag_id"]
    ]
    return {"type": "resource_changed", "updates": updates} if updates else None


def _clip_result(content: Any) -> str:
    """
    Render a tool result for the expandable row in the drawer.

    Clipped hard: the browser keeps every turn in sessionStorage, and one
    verbose log-fetch must not blow the quota that persists the whole chat.
    """
    if isinstance(content, str):
        text = content
    else:
        try:
            text = json.dumps(content, indent=2, default=str)
        except (TypeError, ValueError):
            text = str(content)
    if len(text) > _RESULT_CLIP_CHARS:
        return text[:_RESULT_CLIP_CHARS] + "\n… (truncated)"
    return text


def _event_payload(event: Any) -> dict[str, Any] | None:
    """
    Translate one pydantic-ai stream event into a payload for the browser.

    Returns ``None`` for the events the chat has nothing to show for.  Kept
    separate from the streaming loop so it can be tested without an LLM.
    """
    kind = getattr(event, "event_kind", None)
    if kind == "part_start":
        part = getattr(event, "part", None)
        # Tool-call and thinking parts also start here and are not for display.
        if getattr(part, "part_kind", None) == "text" and part.content:
            return {"type": "text", "delta": part.content}
        return None
    if kind == "function_tool_call":
        payload = {
            "type": "tool",
            "id": event.part.tool_call_id,
            "name": event.part.tool_name,
            "args": event.part.args,
        }
        if event.part.tool_name in WRITE_TOOLS:
            # The model has only *asked* for this write; approval_required means
            # nothing has run yet. Without the flag the drawer spins under
            # "Editing Dag code" from the proposal onwards, which is a lie.
            # Sent on the resumed frame too: it describes the tool, not the
            # phase — the drawer tells the phases apart by the repeated call id.
            payload["proposed"] = True
        return payload
    if kind == "function_tool_result":
        part = event.part
        # A RetryPromptPart here means the tool call itself failed (bad args,
        # MCP error); its content is the error the model is asked to recover from.
        failed = getattr(part, "part_kind", None) == "retry-prompt"
        denied = not failed and part.content == _DENIAL_MESSAGE
        refused = not failed and not denied and part.tool_name in WRITE_TOOLS
        return {
            "type": "tool_result",
            "id": part.tool_call_id,
            "name": part.tool_name,
            "failed": failed or (refused and _write_refused(part.content)),
            "denied": denied,
            "result": _clip_result(part.model_response() if failed else part.content),
        }
    if kind == "part_delta":
        # Tool-call argument deltas share this event kind, and thinking deltas
        # also carry `content_delta` — neither belongs in the reply.
        if getattr(event.delta, "part_delta_kind", None) != "text":
            return None
        if event.delta.content_delta:
            return {"type": "text", "delta": event.delta.content_delta}
    return None


def _issued_a_plan(tool_name: str, content: Any) -> bool:
    """Whether a planning tool just handed the model a token to act on."""
    if tool_name not in PLAN_TOOLS:
        return False
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except ValueError:
            return False
    return isinstance(content, dict) and bool(content.get("plan_token"))


def _carries_a_plan(part: Any) -> bool:
    """
    Whether a proposed write actually carries the token it was planned with.

    A write proposed without one is a card that can only ever be refused: the
    tool rejects it, and the user has spent a decision on nothing. That is the
    same failure as narrating the change, so it is corrected the same way.
    """
    args = part.args
    if isinstance(args, str):
        try:
            args = json.loads(args)
        except ValueError:
            return False
    return isinstance(args, dict) and bool(args.get("plan_token"))


def _needs_correcting(outcome: dict[str, Any]) -> bool:
    """
    Report a plan the model then only *described* — the "Proceeding with the fix…" failure.

    A token was issued, nothing was proposed, and the user is left with prose and
    no button. Prompt wording cannot guarantee otherwise, so such a run is
    corrected once: the model either proposes the write or says plainly that it
    will not.
    """
    return bool(outcome.get("planned") and not outcome.get("proposed") and outcome.get("messages"))


async def _run_and_stream(
    agent: Any,
    *,
    user_id: str,
    page_url: str | None,
    user_prompt: str | None = None,
    message_history: list[Any] | None = None,
    deferred_tool_results: Any = None,
    outcome: dict[str, Any] | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """
    Stream one agent run; a write tool suspends it behind a confirm nonce.

    Write tools are approval-required (see ``_gate_toolsets``), so instead of
    executing them the run ends with deferred requests.  Those are parked in
    ``_pending_approvals`` and surfaced as ``confirm_required`` frames;
    /confirm resumes the run with the user's verdict.

    ``outcome`` collects what the caller needs to judge the run afterwards: the
    messages, whether a plan was issued, and whether a write was actually
    proposed.
    """
    from pydantic_ai import DeferredToolRequests

    requests = result = None
    async with agent.run_stream_events(
        user_prompt,
        message_history=message_history,
        deferred_tool_results=deferred_tool_results,
        output_type=[str, DeferredToolRequests],
    ) as stream:
        async for event in stream:
            kind = getattr(event, "event_kind", None)
            if kind == "deferred_tool_requests":
                requests = event.requests
            elif kind == "agent_run_result":
                result = event.result
            else:
                payload = _event_payload(event)
                if payload:
                    yield payload
                    # Only after the tool result itself has gone out clean: a
                    # refused, denied or failed write refreshes nothing.
                    if kind == "function_tool_result" and not (
                        payload.get("failed") or payload.get("denied")
                    ):
                        if outcome is not None and _issued_a_plan(event.part.tool_name, event.part.content):
                            outcome["planned"] = True
                        changed = _resource_changed_frame(event.part.tool_name, event.part.content)
                        if changed:
                            yield changed

    if outcome is not None and result is not None:
        outcome["messages"] = result.all_messages()

    if requests is not None and requests.approvals and result is not None:
        if outcome is not None:
            outcome["proposed"] = any(_carries_a_plan(part) for part in requests.approvals)
        nonce = _store_pending(
            user_id=user_id,
            call_ids=[part.tool_call_id for part in requests.approvals],
            messages=result.all_messages(),
            page_url=page_url,
        )
        for part in requests.approvals:
            yield {
                "type": "confirm_required",
                "nonce": nonce,
                "call_id": part.tool_call_id,
                "tool": part.tool_name,
                "args": part.args,
            }


async def _stream_agent(
    message: str,
    history: list[dict[str, str]] | None = None,
    page_url: str | None = None,
    *,
    can_write: bool = False,
    user_id: str = "",
    user: Any = None,
) -> AsyncIterator[dict[str, Any]]:
    """Run the agent, yielding tool calls and text as they happen."""
    agent, problem = _build_agent(page_url, can_write=can_write, user=user)
    if problem:
        yield {"type": "text", "delta": problem}
        return

    outcome: dict[str, Any] = {}
    try:
        async for payload in _run_and_stream(
            agent,
            user_id=user_id,
            page_url=page_url,
            user_prompt=message,
            message_history=_to_message_history(history) or None,
            outcome=outcome,
        ):
            yield payload
        if _needs_correcting(outcome):
            async for payload in _run_and_stream(
                agent,
                user_id=user_id,
                page_url=page_url,
                user_prompt=_UNPROPOSED_PLAN_CORRECTION,
                message_history=outcome["messages"],
            ):
                yield payload
    except Exception as e:
        log.exception("Agent execution failed")
        yield {"type": "error", "message": str(_root_cause(e))}


async def _resume_agent(
    pending: _PendingApproval, approved: bool, user: Any = None
) -> AsyncIterator[dict[str, Any]]:
    """
    Resume a suspended run with the user's verdict on the write tool.

    Every frame is recorded as it goes out, and the record is closed even if the
    browser hangs up mid-stream: the write may already have landed, and the only
    way the user can find that out is to ask again with the same nonce.
    """
    agent, problem = _build_agent(pending.page_url, can_write=True, user=user)
    if problem:
        pending.state = "done"
        pending.frames = [{"type": "text", "delta": problem}]
        yield pending.frames[0]
        return

    try:
        from pydantic_ai import DeferredToolResults, ToolDenied

        # One verdict for the whole suspension batch — in practice one call.
        verdict = True if approved else ToolDenied(_DENIAL_MESSAGE)
        settled = set()
        outcome: dict[str, Any] = {}
        async for payload in _run_and_stream(
            agent,
            user_id=pending.user_id,
            page_url=pending.page_url,
            message_history=pending.messages,
            deferred_tool_results=DeferredToolResults(
                approvals={call_id: verdict for call_id in pending.call_ids}
            ),
            outcome=outcome,
        ):
            # A clean return is the only proof the write reached a known end.
            # A failed one is not: rerun_dag unpauses before it triggers, and a
            # create request can time out after the server already created it.
            if payload.get("type") == "tool_result" and not payload.get("failed"):
                settled.add(payload.get("id"))
            pending.frames.append(payload)
            yield payload
        # The run that resumes an approved write can go on to plan the *next*
        # change — "fix it, then re-run" — and narrate that one instead of
        # proposing it. The same correction applies here as on a fresh turn.
        if _needs_correcting(outcome):
            async for payload in _run_and_stream(
                agent,
                user_id=pending.user_id,
                page_url=pending.page_url,
                user_prompt=_UNPROPOSED_PLAN_CORRECTION,
                message_history=outcome["messages"],
            ):
                pending.frames.append(payload)
                yield payload
    except Exception as e:
        log.exception("Agent resume failed")
        failure = {"type": "error", "message": str(_root_cause(e))}
        pending.frames.append(failure)
        pending.state = "interrupted"
        yield failure
        # The write may already have landed — the run died after the tool was
        # called, not before. Without this the browser reads the tidy `done`
        # that terminates every stream as settlement and reports success.
        yield UNSETTLED_FRAME
        return
    except BaseException:
        # Cancellation — the browser hung up, or the worker is going down. The
        # tool may have run; nothing here can say. Marking this "done" would let
        # a retry replay a partial transcript and call it the outcome.
        pending.state = "interrupted"
        raise
    # A rejection executes nothing, so there is nothing left in doubt.
    pending.state = "done" if not approved or settled.issuperset(pending.call_ids) else "interrupted"
    if pending.state == "interrupted":
        # The stream ended tidily but a write never returned a clean result, so
        # the run is over without anyone knowing whether it landed. Say so here
        # as well; a later replay of this nonce is not the user's only chance.
        yield UNSETTLED_FRAME


def _sse_response(payloads: AsyncIterator[dict[str, Any]]) -> StreamingResponse:
    """Wrap a payload stream as server-sent events, always terminated by done."""

    async def frames() -> AsyncIterator[str]:
        try:
            async for payload in payloads:
                yield f"data: {json.dumps(payload)}\n\n"
        except Exception as e:
            log.exception("Airy stream error")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        yield 'data: {"type": "done"}\n\n'

    return StreamingResponse(
        frames(),
        media_type="text/event-stream",
        # Proxies that buffer would defeat the point of streaming at all.
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _bundle_version() -> int:
    """Modification time of the built bundle, used to bust the browser cache."""
    try:
        return int((STATIC_DIR / "main.iife.js").stat().st_mtime)
    except OSError:
        return 0


class ChatbotInjectionMiddleware(BaseHTTPMiddleware):
    """
    Middleware to inject the chatbot script into HTML responses.

    This ensures the chatbot appears on every page of the Airflow UI,
    not just specific plugin routes.
    """

    def __init__(self, app: ASGIApp, bundle_url: str):
        super().__init__(app)
        self.bundle_url = bundle_url

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)

        # Only inject into HTML responses from the main UI
        content_type = response.headers.get("content-type", "")
        if "text/html" not in content_type:
            return response

        # Splicing text into a compressed body can never work.
        if response.headers.get("content-encoding"):
            return response

        # Skip API, static file, and auth/login paths
        path = request.url.path
        if any(path.startswith(p) for p in ["/api/", "/static/", "/chatbot/", "/login", "/auth/"]):
            return response

        # Read and modify the response body
        body = b""
        async for chunk in response.body_iterator:
            body += chunk

        try:
            html_content = body.decode("utf-8")

            # Inject the chatbot loader script before </body>
            injection_script = self._get_injection_script()

            if "</body>" in html_content:
                html_content = html_content.replace("</body>", f"{injection_script}</body>")

            from starlette.responses import Response as StarletteResponse

            # The body just grew: drop content-length so Starlette recomputes it
            # (it keeps a pre-set value as-is), and etag which no longer matches.
            headers = {
                k: v for k, v in response.headers.items() if k.lower() not in ("content-length", "etag")
            }
            return StarletteResponse(
                content=html_content,
                status_code=response.status_code,
                headers=headers,
                media_type="text/html",
            )
        except Exception:
            # If anything fails, return the original response
            from starlette.responses import Response as StarletteResponse

            headers = {k: v for k, v in response.headers.items() if k.lower() != "content-length"}
            return StarletteResponse(
                content=body,
                status_code=response.status_code,
                headers=headers,
            )

    def _get_injection_script(self) -> str:
        """Generate the script tag to inject the chatbot."""
        # The IIFE bundle is self-contained (bundles React) and
        # auto-initializes via the code in main.tsx.  StaticFiles sends no
        # Cache-Control, so without a changing query the browser happily keeps
        # serving a bundle from before the last rebuild — which looks like the
        # backend and frontend disagreeing.  Stat per request, not at startup,
        # so a rebuild takes effect on reload rather than on restart.
        return f"""
<!-- Airflow Chatbot Plugin -->
<div id="airflow-chatbot-root"></div>
<script src="{self.bundle_url}?v={_bundle_version()}"></script>
"""


def _get_chatbot_middleware() -> dict[str, Any]:
    """Create the middleware configuration for chatbot injection."""
    bundle_url = _get_base_url_path("/chatbot/static/main.iife.js")

    return {
        "middleware": ChatbotInjectionMiddleware,
        "args": [],
        "kwargs": {"bundle_url": bundle_url},
        "name": "Chatbot Injection Middleware",
    }


# Check if running on API server
RUNNING_ON_APISERVER = (len(sys.argv) > 1 and sys.argv[1] in ["api-server"]) or (
    len(sys.argv) > 2 and sys.argv[2] == "airflow-core/src/airflow/api_fastapi/main.py"
)


class AirflowChatbotPlugin(AirflowPlugin):
    """
    Airflow Chatbot Plugin.

    Provides an LLM-powered chatbot assistant that appears as a floating
    button in the bottom-right corner of the Airflow UI. The chatbot can
    help users with DAG creation, debugging, and general Airflow questions.
    """

    name = "airflow_chatbot"

    if RUNNING_ON_APISERVER:
        fastapi_apps = [_create_chatbot_api()]
        fastapi_root_middlewares = [_get_chatbot_middleware()]
