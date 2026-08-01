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
import { applyEvent, parseFrames } from "./useChat";

const blank = (): Message => ({
  content: "",
  id: "assistant-1",
  role: "assistant",
  timestamp: new Date(0),
});

describe("parseFrames", () => {
  it("parses whole frames and keeps the partial one for the next chunk", () => {
    const { events, rest } = parseFrames(
      'data: {"type":"text","delta":"hi"}\n\ndata: {"type":"do',
    );

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
    const { events, rest } = parseFrames(
      'data: {"type":"text","delta":"hi"}\r\n\r\n',
    );

    expect(events).toEqual([{ delta: "hi", type: "text" }]);
    expect(rest).toBe("");
  });

  it("skips frames that are not JSON rather than killing the stream", () => {
    const { events } = parseFrames(
      'data: not json\n\ndata: {"type":"text","delta":"ok"}\n\n',
    );

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

    const done = applyEvent(
      called,
      { id: "c1", name: "diagnose_dag", type: "tool_result" },
      2_400,
    );

    expect(done.tools?.[0]?.durationMs).toBe(1_400);
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
