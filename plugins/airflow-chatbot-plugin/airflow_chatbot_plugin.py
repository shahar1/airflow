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
* **MCP server URL** — Airflow *Variable* ``airy_mcp_url``
  (default ``http://localhost:8000/mcp``).  Set to empty string to disable MCP.

In Breeze, just ``export OPENAI_API_KEY=sk-...`` before ``breeze start-airflow``.
The init script auto-installs ``pydantic-ai[openai]``, creates the connection,
and starts the MCP sidecar.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
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

    model_config = {"arbitrary_types_allowed": True}


def _get_base_url_path(path: str) -> str:
    """Construct URL path with webserver base_url prefix."""
    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith(("http://", "https://")):
        from urllib.parse import urlparse

        base_path = urlparse(base_url).path
    else:
        base_path = base_url

    base_path = base_path.rstrip("/")
    return base_path + path


def _create_chatbot_api() -> dict[str, Any]:
    """Create the FastAPI app for serving chatbot static files and API."""
    app = FastAPI(
        title="Airflow Chatbot",
        description="LLM-powered chatbot assistant for Apache Airflow",
    )

    # Mount static files if the dist directory exists
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="chatbot_static")

    @app.get("/")
    async def root():
        """Health check endpoint."""
        return JSONResponse({"status": "ok", "plugin": "airflow-chatbot"})

    @app.get("/health")
    async def health_check():
        """Detailed health check — verifies LLM key availability and MCP reachability."""
        import os

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

        # Check MCP
        mcp_url_val = _get_variable("airy_mcp_url", "http://localhost:8000/mcp")
        if mcp_url_val:
            try:
                import socket

                # Parse host:port from the MCP URL and do a TCP connect check.
                # This is faster and more reliable than an HTTP probe because
                # the MCP server might not serve GET / with 200.
                from urllib.parse import urlparse

                parsed = urlparse(mcp_url_val)
                host = parsed.hostname or "localhost"
                port = parsed.port or 8000
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                sock.connect((host, port))
                sock.close()
                mcp_ok = True
            except Exception:
                mcp_ok = False

        return JSONResponse(
            {
                "status": "ok" if llm_ok else "degraded",
                "llm": {"configured": llm_ok, "source": llm_source},
                "mcp": {"configured": bool(mcp_url_val), "reachable": mcp_ok, "url": mcp_url_val},
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
    async def chat_endpoint(body: ChatRequest):
        """Chat API endpoint — forwards user messages to the PydanticAI agent."""
        try:
            message = body.message
            history = body.history

            if not message.strip():
                return JSONResponse(
                    {"error": "Empty message", "status": "error"},
                    status_code=400,
                )

            response_text = await _run_agent(message, history)

            return JSONResponse(
                {
                    "response": response_text,
                    "status": "success",
                }
            )
        except Exception as e:
            log.exception("Airy chat endpoint error")
            return JSONResponse(
                {
                    "error": str(e),
                    "status": "error",
                },
                status_code=500,
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
    import os

    return os.environ.get("OPENAI_API_KEY") or None


def _get_variable(key: str, default: str) -> str:
    """Read an Airflow Variable, returning *default* on any error."""
    try:
        from airflow.models.variable import Variable

        return Variable.get(key, default_var=default)
    except Exception:
        return default


async def _run_agent(message: str, history: list[dict[str, str]] | None = None) -> str:
    """Run the PydanticAI agent and return the assistant reply."""
    api_key = _get_llm_api_key()
    if not api_key:
        return (
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

    try:
        from pydantic_ai import Agent
        from pydantic_ai.models.openai import OpenAIModel
        from pydantic_ai.providers.openai import OpenAIProvider
    except ImportError:
        return (
            "**Missing dependency.**\n\n"
            "`pydantic-ai` is not installed. Run:\n"
            "```\npip install pydantic-ai\n```"
        )

    model_name = _get_variable("airy_model", "gpt-4o-mini")
    mcp_url = _get_variable("airy_mcp_url", "http://localhost:8000/mcp")

    provider = OpenAIProvider(api_key=api_key)
    model = OpenAIModel(model_name, provider=provider)
    agent = Agent(model=model, system_prompt=_SYSTEM_PROMPT)

    # ---------- optionally attach MCP tools ----------
    mcp_server = None
    if mcp_url:
        try:
            from pydantic_ai.mcp import MCPServerStreamableHTTP

            mcp_server = MCPServerStreamableHTTP(url=mcp_url)
            agent = Agent(
                model=model,
                system_prompt=_SYSTEM_PROMPT,
                mcp_servers=[mcp_server],
            )
        except Exception:
            log.warning("Could not configure MCP server at %s — running without tools", mcp_url)

    # ---------- build message history ----------
    from pydantic_ai.messages import (
        ModelRequest,
        ModelResponse,
        TextPart,
        UserPromptPart,
    )

    message_history: list[ModelRequest | ModelResponse] = []
    if history:
        for entry in history:
            role = entry.get("role", "")
            text = entry.get("content", "")
            if role == "user":
                message_history.append(ModelRequest(parts=[UserPromptPart(content=text)]))
            elif role == "assistant":
                message_history.append(ModelResponse(parts=[TextPart(content=text)]))

    # ---------- run ----------
    try:
        if mcp_server:
            async with agent.run_mcp_servers():
                result = await agent.run(
                    message,
                    message_history=message_history if message_history else None,
                )
        else:
            result = await agent.run(
                message,
                message_history=message_history if message_history else None,
            )
        return result.output
    except BaseException as e:
        # Unwrap ExceptionGroup / TaskGroup errors to surface the real cause
        real = e
        eg_type = getattr(__builtins__, "BaseExceptionGroup", None) or getattr(
            __builtins__, "ExceptionGroup", None
        )
        if eg_type is None:
            try:
                from exceptiongroup import BaseExceptionGroup as eg_type  # type: ignore[no-redef]
            except ImportError:
                eg_type = None
        if eg_type and isinstance(e, eg_type):
            leaves = list(e.exceptions)
            if leaves:
                real = leaves[0]
                if isinstance(real, eg_type) and real.exceptions:
                    real = real.exceptions[0]
        log.exception("Agent execution failed")
        return f"Sorry, I encountered an error: {real}"


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

            # Create new response with modified content
            from starlette.responses import Response as StarletteResponse

            return StarletteResponse(
                content=html_content,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type="text/html",
            )
        except Exception:
            # If anything fails, return the original response
            from starlette.responses import Response as StarletteResponse

            return StarletteResponse(
                content=body,
                status_code=response.status_code,
                headers=dict(response.headers),
            )

    def _get_injection_script(self) -> str:
        """Generate the script tag to inject the chatbot."""
        # The IIFE bundle is self-contained (bundles React) and
        # auto-initializes via the code in main.tsx.
        return f"""
<!-- Airflow Chatbot Plugin -->
<div id="airflow-chatbot-root"></div>
<script src="{self.bundle_url}"></script>
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
