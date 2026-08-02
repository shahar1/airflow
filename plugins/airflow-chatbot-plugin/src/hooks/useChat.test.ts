/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { describe, expect, it } from "vitest";

import { Message } from "../components/types";
import { applyEvent, dispatchResourceChanged, finalizeTools, parseFrames } from "./useChat";

const blank = (): Message => ({
  content: "",
  id: "assistant-1",
  role: "assistant",
  timestamp: new Date(0),
});

describe("parseFrames", () => {
  it("parses whole frames and keeps the partial one for the next chunk", () => {
    const { events, rest } = parseFrames('data: {"type":"text","delta":"hi"}\n\ndata: {"type":"do');

    expect(events).toEqual([{ delta: "hi", type: "text" }]);
    expect(rest).toBe('data: {"type":"do');
  });

  it("reassembles a frame split across chunks", () => {
    const first = parseFrames('data: {"type":"text","del');
    expect(first.events).toEqual([]);

    const second = parseFrames(`${first.rest}ta":"hello"}\n\n`);
    expect(second.events).toEqual([{ delta: "hello", type: "text" }]);
  });

  it("handles CRLF-normalised frames", () => {
    const { events, rest } = parseFrames('data: {"type":"text","delta":"hi"}\r\n\r\n');

    expect(events).toEqual([{ delta: "hi", type: "text" }]);
    expect(rest).toBe("");
  });

  it("skips frames that are not JSON rather than killing the stream", () => {
    const { events } = parseFrames('data: not json\n\ndata: {"type":"text","delta":"ok"}\n\n');

    expect(events).toEqual([{ delta: "ok", type: "text" }]);
  });

  it("ignores comment and event lines that carry no data", () => {
    const { events } = parseFrames(": keep-alive\n\nevent: ping\n\n");

    expect(events).toEqual([]);
  });
});

describe("applyEvent", () => {
  it("appends text deltas in order", () => {
    const message = [
      { delta: "Task ", type: "text" },
      { delta: "summarize ", type: "text" },
      { delta: "failed.", type: "text" },
    ].reduce((acc, event) => applyEvent(acc, event), blank());

    expect(message.content).toBe("Task summarize failed.");
  });

  it("records a running tool call, then times it when the result arrives", () => {
    const called = applyEvent(
      blank(),
      { args: { dag_id: "sales_summary" }, id: "c1", name: "diagnose_dag", type: "tool" },
      1_000,
    );

    expect(called.tools).toEqual([
      {
        args: { dag_id: "sales_summary" },
        id: "c1",
        name: "diagnose_dag",
        startedAt: 1_000,
      },
    ]);
    expect(called.tools?.[0]?.durationMs).toBeUndefined();

    const done = applyEvent(called, { id: "c1", name: "diagnose_dag", type: "tool_result" }, 2_400);

    expect(done.tools?.[0]?.durationMs).toBe(1_400);
  });

  it("stores the result payload and failure flag when the result arrives", () => {
    const called = applyEvent(blank(), { id: "c1", name: "diagnose_dag", type: "tool" }, 0);

    const done = applyEvent(called, { failed: true, id: "c1", result: "boom", type: "tool_result" }, 100);

    expect(done.tools?.[0]?.failed).toBe(true);
    expect(done.tools?.[0]?.result).toBe("boom");
  });

  it("marks a suspended call as awaiting instead of done", () => {
    let message = applyEvent(blank(), { id: "c1", name: "apply_dag_code_changes", type: "tool" }, 0);
    message = applyEvent(
      message,
      { call_id: "c1", nonce: "n1", tool: "apply_dag_code_changes", type: "confirm_required" },
      500,
    );

    expect(message.tools?.[0]?.awaitingConfirm).toBe(true);
    expect(message.tools?.[0]?.durationMs).toBe(500);
  });

  it("resolves an awaiting call when its result finally arrives", () => {
    let message = applyEvent(blank(), { id: "c1", name: "apply_dag_code_changes", type: "tool" }, 0);
    message = applyEvent(
      message,
      { call_id: "c1", nonce: "n1", tool: "apply_dag_code_changes", type: "confirm_required" },
      500,
    );
    message = applyEvent(message, { denied: true, id: "c1", result: "no", type: "tool_result" }, 900);

    expect(message.tools?.[0]?.awaitingConfirm).toBeUndefined();
    expect(message.tools?.[0]?.denied).toBe(true);
  });

  it("reuses the existing row when a resumed call streams under the same id", () => {
    let message = applyEvent(blank(), { id: "c1", name: "apply_dag_code_changes", type: "tool" }, 0);
    message = applyEvent(
      message,
      { call_id: "c1", nonce: "n1", tool: "apply_dag_code_changes", type: "confirm_required" },
      500,
    );
    message = applyEvent(message, { id: "c1", name: "apply_dag_code_changes", type: "tool" }, 1_000);

    expect(message.tools).toHaveLength(1);
    expect(message.tools?.[0]?.durationMs).toBeUndefined();
    expect(message.tools?.[0]?.awaitingConfirm).toBeUndefined();
  });

  it("carries the proposed flag onto a write call that has not run", () => {
    const message = applyEvent(
      blank(),
      { args: {}, id: "c1", name: "apply_dag_code_changes", proposed: true, type: "tool" },
      0,
    );

    expect(message.tools?.[0]?.proposed).toBe(true);
  });

  it("leaves a read call unproposed", () => {
    const message = applyEvent(blank(), { id: "c1", name: "diagnose_dag", type: "tool" }, 0);

    expect(message.tools?.[0]?.proposed).toBeUndefined();
  });

  it("clears the proposal when the approved call resumes under the same id", () => {
    // The resumed frame still says `proposed` — that describes the tool, while
    // the repeated id proves this call is the approved one going back to work.
    let message = applyEvent(
      blank(),
      { id: "c1", name: "apply_dag_code_changes", proposed: true, type: "tool" },
      0,
    );
    message = applyEvent(
      message,
      { call_id: "c1", nonce: "n1", tool: "apply_dag_code_changes", type: "confirm_required" },
      500,
    );
    message = applyEvent(
      message,
      { id: "c1", name: "apply_dag_code_changes", proposed: true, type: "tool" },
      1_000,
    );

    expect(message.tools?.[0]?.proposed).toBeUndefined();
    expect(message.tools?.[0]?.awaitingConfirm).toBeUndefined();
    expect(message.tools?.[0]?.durationMs).toBeUndefined();
    expect(message.tools?.[0]?.startedAt).toBe(1_000);
  });

  it("never lets a rejected call claim it is executing on its way to rejected", () => {
    // pydantic-ai replays a FunctionToolCallEvent for denied deferred calls
    // too, so the resumed frame is not proof that anything ran.
    let message = applyEvent(
      blank(),
      { id: "c1", name: "apply_dag_code_changes", proposed: true, type: "tool" },
      0,
    );
    message = applyEvent(
      message,
      { call_id: "c1", nonce: "n1", tool: "apply_dag_code_changes", type: "confirm_required" },
      500,
    );
    message = {
      ...message,
      confirms: message.confirms?.map((c) => ({ ...c, resolution: "rejected" as const })),
    };

    const resumed = applyEvent(
      message,
      { id: "c1", name: "apply_dag_code_changes", proposed: true, type: "tool" },
      900,
    );

    expect(resumed.tools?.[0]?.durationMs).toBe(500);
    expect(resumed.tools?.[0]?.startedAt).toBe(0);

    const settled = applyEvent(resumed, { denied: true, id: "c1", result: "no", type: "tool_result" }, 1_000);

    expect(settled.tools?.[0]?.denied).toBe(true);
    expect(settled.tools?.[0]?.result).toBe("no");
    expect(settled.tools?.[0]?.awaitingConfirm).toBeUndefined();
  });

  it("times each call separately when several are in flight", () => {
    let message = applyEvent(blank(), { id: "a", name: "one", type: "tool" }, 0);
    message = applyEvent(message, { id: "b", name: "two", type: "tool" }, 500);
    message = applyEvent(message, { id: "b", type: "tool_result" }, 900);

    expect(message.tools?.[0]?.durationMs).toBeUndefined();
    expect(message.tools?.[1]?.durationMs).toBe(400);
  });

  it("does not re-time a call when a second result arrives for it", () => {
    let message = applyEvent(blank(), { id: "a", name: "one", type: "tool" }, 0);
    message = applyEvent(message, { id: "a", type: "tool_result" }, 100);
    message = applyEvent(message, { id: "a", type: "tool_result" }, 9_000);

    expect(message.tools?.[0]?.durationMs).toBe(100);
  });

  it("marks the message as an error", () => {
    const message = applyEvent(blank(), {
      message: "boom",
      type: "error",
    });

    expect(message.isError).toBe(true);
    expect(message.content).toBe("**Error:** boom");
  });

  it("leaves the message untouched for unknown event types", () => {
    const before = blank();
    expect(applyEvent(before, { type: "done" })).toEqual(before);
  });
});

describe("finalizeTools", () => {
  const withTools = (tools: Message["tools"], confirms?: Message["confirms"]): Message => ({
    ...blank(),
    ...(confirms ? { confirms } : {}),
    tools,
  });

  it("does not green-check a call that never reported back", () => {
    const message = finalizeTools(withTools([{ id: "c1", name: "diagnose_dag", startedAt: 0 }]), 900);

    expect(message.tools?.[0]?.cancelled).toBe(true);
    expect(message.tools?.[0]?.durationMs).toBe(900);
  });

  it("does not turn an unapproved proposal into an edit", () => {
    // The run can die before `confirm_required` ever arrives; the approval gate
    // guarantees nothing was written.
    const message = finalizeTools(
      withTools([{ id: "c1", name: "apply_dag_code_changes", proposed: true, startedAt: 0 }]),
      500,
    );

    expect(message.tools?.[0]?.cancelled).toBe(true);
    expect(message.tools?.[0]?.proposed).toBeUndefined();
  });

  it("says the outcome is unknown for an approved write that never reported back", () => {
    // The file may well have been rewritten — "cancelled" would be as wrong as
    // a green check.
    const message = finalizeTools(
      withTools(
        [{ id: "c1", name: "apply_dag_code_changes", startedAt: 0 }],
        [{ args: {}, callId: "c1", nonce: "n1", resolution: "approved", tool: "apply_dag_code_changes" }],
      ),
      700,
    );

    expect(message.tools?.[0]?.unsettled).toBe(true);
    expect(message.tools?.[0]?.cancelled).toBeUndefined();
  });

  it("leaves a call that already reported back alone", () => {
    const message = finalizeTools(
      withTools([{ durationMs: 1_400, id: "c1", name: "diagnose_dag", result: "ok", startedAt: 0 }]),
      9_000,
    );

    expect(message.tools?.[0]?.durationMs).toBe(1_400);
    expect(message.tools?.[0]?.cancelled).toBeUndefined();
  });
});

describe("dispatchResourceChanged", () => {
  const RESOURCE_CHANGED_EVENT = "airflow:resource-changed:v1";

  const captured = (frame: Record<string, unknown>): Array<unknown> => {
    const seen: Array<unknown> = [];
    const listener = (event: Event) => seen.push((event as CustomEvent<unknown>).detail);

    globalThis.addEventListener(RESOURCE_CHANGED_EVENT, listener);
    dispatchResourceChanged(frame);
    globalThis.removeEventListener(RESOURCE_CHANGED_EVENT, listener);

    return seen;
  };

  it("tells the host UI which Dag to refetch", () => {
    const updates = [{ dag_id: "sales_summary", kind: "dag_definition", version_number: 2 }];

    expect(captured({ type: "resource_changed", updates })).toEqual([{ updates }]);
  });

  it.each([
    ["no updates", {}],
    ["an empty list", { updates: [] }],
    ["a kind this build cannot act on", { updates: [{ dag_id: "d", kind: "everything" }] }],
    ["an update naming no Dag", { updates: [{ kind: "dag_definition" }] }],
  ])("dispatches nothing for %s", (_label, frame) => {
    expect(captured(frame)).toEqual([]);
  });
});
