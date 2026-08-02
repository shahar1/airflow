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
import socket
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from fastapi import Depends, FastAPI
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
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
        ``error`` or ``done``.
        """
        if not body.message.strip():
            return JSONResponse({"error": "Empty message", "status": "error"}, status_code=400)

        can_write = _user_can_write(user)

        async def frames() -> AsyncIterator[str]:
            try:
                async for payload in _stream_agent(
                    body.message, body.history, body.page_url, can_write=can_write
                ):
                    yield f"data: {json.dumps(payload)}\n\n"
            except Exception as e:
                log.exception("Airy chat endpoint error")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
            yield 'data: {"type": "done"}\n\n'

        return StreamingResponse(
            frames(),
            media_type="text/event-stream",
            # Proxies that buffer would defeat the point of streaming at all.
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

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

**Page context.**  The system prompt may end with a `Current page:` line — the
path the user is looking at right now.  Use it to resolve words like "this"
and "here": `/dags/sales_summary/grid` means questions are about the
`sales_summary` Dag unless the user says otherwise.
"""

_WRITE_PROMPT = """\

**Self-healing.**  You can diagnose a broken Dag (`diagnose_dag`), patch its
source (`fix_dag_code`) and trigger a fresh run (`rerun_dag`).  Work one step at
a time and never chain them without the user asking:

1. After diagnosing, name the failing task, quote the offending line, and say
   exactly what you would change — then stop.
2. Only call `fix_dag_code` when the user asks you to apply the fix.  Pass the
   smallest unique `old` snippet you can (it must occur exactly once in the file).
3. After a successful fix, offer to re-run — do not re-run on your own.
4. Call `revert_dag_code` **only** when the user explicitly asks to undo a fix.
   Never call it to recover from a failed step or because a run still fails.
"""

_READ_ONLY_PROMPT = """\

**Read-only access.**  This session has no write tools: you can diagnose and
explain, but applying fixes, re-running or backfilling requires Dag-edit
permission the user does not have.  If asked to change anything, say so.
"""

_FOLLOWUP_PROMPT = """\

**Follow-up buttons.**  When the obvious next step is a single action, end your
reply with one or more lines of the form `[ACTION: <what the user should say>]`.
They are rendered as clickable buttons, so write them as the user's own words,
e.g. `[ACTION: Apply the fix to sales_summary]` or `[ACTION: Re-run sales_summary]`.
Put nothing after them.
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


def _user_can_write(user: Any) -> bool:
    """Whether the auth manager grants the user Dag-edit rights (over any Dag)."""
    # Module singleton, not request.app.state: inside a mounted sub-app,
    # request.app is the sub-app and carries no auth manager.
    from airflow.api_fastapi.app import get_auth_manager

    return get_auth_manager().is_authorized_dag(method="PUT", user=user)


WRITE_TOOLS = frozenset({"fix_dag_code", "revert_dag_code", "rerun_dag", "run_backfill"})


def _gate_toolsets(toolsets: list[Any], can_write: bool) -> list[Any]:
    """Viewers get Airy without the write tools; Dag editors get everything."""
    if can_write:
        return toolsets
    return [ts.filtered(lambda ctx, tool_def: tool_def.name not in WRITE_TOOLS) for ts in toolsets]


def _build_agent(page_url: str | None = None, can_write: bool = False) -> tuple[Any, str | None]:
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
        toolsets=_gate_toolsets(toolsets, can_write),
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
        return {
            "type": "tool",
            "id": event.part.tool_call_id,
            "name": event.part.tool_name,
            "args": event.part.args,
        }
    if kind == "function_tool_result":
        return {"type": "tool_result", "id": event.part.tool_call_id, "name": event.part.tool_name}
    if kind == "part_delta":
        # Tool-call argument deltas share this event kind, and thinking deltas
        # also carry `content_delta` — neither belongs in the reply.
        if getattr(event.delta, "part_delta_kind", None) != "text":
            return None
        if event.delta.content_delta:
            return {"type": "text", "delta": event.delta.content_delta}
    return None


async def _stream_agent(
    message: str,
    history: list[dict[str, str]] | None = None,
    page_url: str | None = None,
    *,
    can_write: bool = False,
) -> AsyncIterator[dict[str, Any]]:
    """Run the agent, yielding tool calls and text as they happen."""
    agent, problem = _build_agent(page_url, can_write=can_write)
    if problem:
        yield {"type": "text", "delta": problem}
        return

    try:
        async with agent.run_stream_events(
            message, message_history=_to_message_history(history) or None
        ) as stream:
            async for event in stream:
                payload = _event_payload(event)
                if payload:
                    yield payload
    except Exception as e:
        log.exception("Agent execution failed")
        yield {"type": "error", "message": str(_root_cause(e))}


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
