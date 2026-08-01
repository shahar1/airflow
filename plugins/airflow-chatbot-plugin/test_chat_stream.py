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

import builtins
import json
from types import SimpleNamespace

import airflow_chatbot_plugin as plugin
import pytest
from pydantic_ai.messages import (
    FunctionToolCallEvent,
    FunctionToolResultEvent,
    ModelRequest,
    ModelResponse,
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    TextPartDelta,
    ThinkingPart,
    ThinkingPartDelta,
    ToolCallPart,
    ToolReturnPart,
)
from starlette.testclient import TestClient

# BaseExceptionGroup is a builtin only from 3.11; airflow-core still supports 3.10.
_EXC_GROUP = getattr(builtins, "BaseExceptionGroup", None)
needs_exception_groups = pytest.mark.skipif(_EXC_GROUP is None, reason="needs Python 3.11+")


def tool_call_event(name="diagnose_dag", args=None, call_id="c1"):
    return FunctionToolCallEvent(
        part=ToolCallPart(tool_name=name, args=args or {"dag_id": "sales_summary"}, tool_call_id=call_id)
    )


def tool_result_event(name="diagnose_dag", call_id="c1"):
    return FunctionToolResultEvent(part=ToolReturnPart(tool_name=name, content="ok", tool_call_id=call_id))


def text_delta_event(delta="hello"):
    return PartDeltaEvent(index=0, delta=TextPartDelta(content_delta=delta))


def test_event_payload_reports_a_tool_call():
    assert plugin._event_payload(tool_call_event()) == {
        "type": "tool",
        "id": "c1",
        "name": "diagnose_dag",
        "args": {"dag_id": "sales_summary"},
    }


def test_event_payload_reports_a_tool_result():
    assert plugin._event_payload(tool_result_event()) == {
        "type": "tool_result",
        "id": "c1",
        "name": "diagnose_dag",
    }


def test_event_payload_reports_text_deltas():
    assert plugin._event_payload(text_delta_event("Task ")) == {"type": "text", "delta": "Task "}


def test_event_payload_emits_the_opening_chunk_of_a_text_part():
    # pydantic-ai delivers a text part's first chunk as PartStartEvent and only
    # later chunks as deltas; dropping it eats the first word of every reply.
    event = PartStartEvent(index=0, part=TextPart(content="Task "))
    assert plugin._event_payload(event) == {"type": "text", "delta": "Task "}


@pytest.mark.parametrize(
    "event",
    [
        PartStartEvent(index=0, part=TextPart(content="")),
        PartStartEvent(index=0, part=ToolCallPart(tool_name="diagnose_dag", args={})),
        PartStartEvent(index=0, part=ThinkingPart(content="let me think")),
        # Tool-call argument deltas arrive as part_delta but carry no text.
        PartDeltaEvent(index=0, delta=TextPartDelta(content_delta="")),
        PartDeltaEvent(index=0, delta=ThinkingPartDelta(content_delta="reasoning aloud")),
        SimpleNamespace(),
    ],
    ids=["empty_text", "tool_call_part", "thinking_part", "empty_delta", "thinking_delta", "no_kind"],
)
def test_event_payload_ignores_events_with_nothing_to_show(event):
    assert plugin._event_payload(event) is None


def test_to_message_history_replays_both_sides_of_the_conversation():
    history = plugin._to_message_history(
        [
            {"role": "user", "content": "why did it fail?"},
            {"role": "assistant", "content": "a typo in op_kwargs"},
            {"role": "system", "content": "ignored"},
        ]
    )

    assert [type(m) for m in history] == [ModelRequest, ModelResponse]
    assert history[0].parts[0].content == "why did it fail?"
    assert history[1].parts[0].content == "a typo in op_kwargs"


@pytest.mark.parametrize("history", [None, []], ids=["none", "empty"])
def test_to_message_history_of_a_fresh_conversation(history):
    assert plugin._to_message_history(history) == []


@needs_exception_groups
def test_root_cause_unwraps_nested_task_group_errors():
    inner = ValueError("the real problem")
    nested = _EXC_GROUP("inner", [inner])
    assert plugin._root_cause(_EXC_GROUP("outer", [nested])) is inner


def test_root_cause_passes_a_plain_exception_through():
    err = RuntimeError("boom")
    assert plugin._root_cause(err) is err


@pytest.mark.asyncio
async def test_stream_agent_explains_a_missing_api_key(monkeypatch):
    monkeypatch.setattr(plugin, "_get_llm_api_key", lambda: None)

    payloads = [p async for p in plugin._stream_agent("hi")]

    assert len(payloads) == 1
    assert payloads[0]["type"] == "text"
    assert "not configured yet" in payloads[0]["delta"]


@needs_exception_groups
@pytest.mark.asyncio
async def test_stream_agent_surfaces_the_root_cause_of_a_failure(monkeypatch):
    class ExplodingAgent:
        def run_stream_events(self, *args, **kwargs):
            raise _EXC_GROUP("tg", [ConnectionError("mcp sidecar is gone")])

    monkeypatch.setattr(plugin, "_build_agent", lambda: (ExplodingAgent(), None))

    payloads = [p async for p in plugin._stream_agent("hi")]

    assert payloads == [{"type": "error", "message": "mcp sidecar is gone"}]


@pytest.fixture
def client():
    return TestClient(plugin._create_chatbot_api()["app"])


def test_chat_endpoint_streams_sse_frames_and_always_terminates(client, monkeypatch):
    async def fake_stream(message, history=None):
        yield {"type": "tool", "id": "c1", "name": "diagnose_dag", "args": {}}
        yield {"type": "tool_result", "id": "c1", "name": "diagnose_dag"}
        yield {"type": "text", "delta": "Task summarize failed."}

    monkeypatch.setattr(plugin, "_stream_agent", fake_stream)

    with client.stream("POST", "/chat", json={"message": "what happened?"}) as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        frames = [
            json.loads(line.removeprefix("data:").strip())
            for line in response.iter_lines()
            if line.startswith("data:")
        ]

    assert [f["type"] for f in frames] == ["tool", "tool_result", "text", "done"]


def test_chat_endpoint_disables_proxy_buffering(client, monkeypatch):
    # Without these a buffering proxy holds every frame until the end, which
    # silently undoes the whole point of streaming.
    async def fake_stream(message, history=None):
        yield {"type": "text", "delta": "hi"}

    monkeypatch.setattr(plugin, "_stream_agent", fake_stream)

    with client.stream("POST", "/chat", json={"message": "hi"}) as response:
        response.read()
        assert response.headers["cache-control"] == "no-cache"
        assert response.headers["x-accel-buffering"] == "no"


def test_chat_endpoint_reports_a_mid_stream_failure_then_terminates(client, monkeypatch):
    async def exploding_stream(message, history=None):
        yield {"type": "text", "delta": "starting"}
        raise RuntimeError("stream died")

    monkeypatch.setattr(plugin, "_stream_agent", exploding_stream)

    with client.stream("POST", "/chat", json={"message": "hi"}) as response:
        frames = [
            json.loads(line.removeprefix("data:").strip())
            for line in response.iter_lines()
            if line.startswith("data:")
        ]

    assert [f["type"] for f in frames] == ["text", "error", "done"]
    assert frames[1]["message"] == "stream died"


def test_chat_endpoint_rejects_an_empty_message(client):
    assert client.post("/chat", json={"message": "   "}).status_code == 400
