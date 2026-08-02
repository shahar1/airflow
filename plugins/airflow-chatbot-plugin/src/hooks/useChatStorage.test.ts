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

import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { loadStoredMessages, useChat } from "./useChat";

const KEY = "airy-chat-history";

const stored = (extra: object = {}) => [
  { content: "why?", id: "u1", role: "user", timestamp: "2026-08-01T10:00:00.000Z" },
  {
    content: "a typo",
    id: "a1",
    role: "assistant",
    timestamp: "2026-08-01T10:00:05.000Z",
    ...extra,
  },
];

afterEach(() => {
  sessionStorage.clear();
  vi.unstubAllGlobals();
});

describe("loadStoredMessages", () => {
  it("restores messages with real Date timestamps", () => {
    sessionStorage.setItem(KEY, JSON.stringify(stored()));

    const messages = loadStoredMessages();

    expect(messages).toHaveLength(2);
    expect(messages[0]?.content).toBe("why?");
    expect(messages[1]?.timestamp).toBeInstanceOf(Date);
  });

  it("cancels a chip that was mid-flight at reload rather than calling it done", () => {
    // A write proposed but never approved never ran; restoring it as a finished
    // call would claim the Dag was edited.
    sessionStorage.setItem(
      KEY,
      JSON.stringify(
        stored({ tools: [{ id: "c1", name: "fix_dag_code", proposed: true, startedAt: 5 }] }),
      ),
    );

    const [, assistant] = loadStoredMessages();

    expect(assistant?.tools?.[0]?.cancelled).toBe(true);
    expect(assistant?.tools?.[0]?.durationMs).toBeGreaterThanOrEqual(0);
    expect(assistant?.tools?.[0]?.proposed).toBeUndefined();
  });

  it("leaves a call that had already finished alone", () => {
    sessionStorage.setItem(
      KEY,
      JSON.stringify(
        stored({ tools: [{ durationMs: 1_400, id: "c1", name: "diagnose_dag", startedAt: 5 }] }),
      ),
    );

    const [, assistant] = loadStoredMessages();

    expect(assistant?.tools?.[0]?.cancelled).toBeUndefined();
    expect(assistant?.tools?.[0]?.durationMs).toBe(1_400);
  });

  it("drops the blank streaming bubble a reload interrupted", () => {
    sessionStorage.setItem(
      KEY,
      JSON.stringify([...stored(), { content: "", id: "a2", role: "assistant", timestamp: "2026-08-01T10:01:00.000Z" }]),
    );

    expect(loadStoredMessages()).toHaveLength(2);
  });

  it.each([
    ["corrupt JSON", "{not json"],
    ["a non-array value", JSON.stringify({ nope: true })],
  ])("tolerates %s", (_name, raw) => {
    sessionStorage.setItem(KEY, raw);

    expect(loadStoredMessages()).toEqual([]);
  });

  it("returns nothing when storage is empty", () => {
    expect(loadStoredMessages()).toEqual([]);
  });
});

describe("useChat persistence", () => {
  it("starts from the stored conversation", () => {
    sessionStorage.setItem(KEY, JSON.stringify(stored()));

    const { result } = renderHook(() => useChat());

    expect(result.current.messages).toHaveLength(2);
    expect(result.current.messages[1]?.content).toBe("a typo");
  });

  it("persists the conversation as it streams", async () => {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        const encoder = new TextEncoder();
        controller.enqueue(encoder.encode('data: {"type":"text","delta":"hi"}\n\n'));
        controller.enqueue(encoder.encode('data: {"type":"done"}\n\n'));
        controller.close();
      },
    });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ body, ok: true, status: 200 } as Response));

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("hello");
    });

    const persisted = JSON.parse(sessionStorage.getItem(KEY) ?? "[]");
    expect(persisted).toHaveLength(2);
    expect(persisted[1].content).toBe("hi");
  });

  it("clearMessages also clears the stored copy", () => {
    sessionStorage.setItem(KEY, JSON.stringify(stored()));

    const { result } = renderHook(() => useChat());
    act(() => {
      result.current.clearMessages();
    });

    expect(result.current.messages).toEqual([]);
    expect(sessionStorage.getItem(KEY)).toBeNull();
  });
});
