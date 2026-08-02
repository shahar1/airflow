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

import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { useChat } from "./useChat";

/** A Response whose body yields `chunks` as a stream, like the real endpoint. */
const streamingResponse = (chunks: string[]): Response =>
  ({
    body: new ReadableStream<Uint8Array>({
      start(controller) {
        const encoder = new TextEncoder();
        for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
        controller.close();
      },
    }),
    ok: true,
    status: 200,
  }) as Response;

const frame = (payload: Record<string, unknown>) =>
  `data: ${JSON.stringify(payload)}\n\n`;

const mockFetch = (response: Response | Promise<never>) => {
  const fetchMock = vi.fn().mockReturnValue(Promise.resolve(response));
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
};

afterEach(() => {
  sessionStorage.clear();
  vi.unstubAllGlobals();
});

describe("useChat streaming", () => {
  it("streams tool calls and text into a single assistant message", async () => {
    mockFetch(
      streamingResponse([
        frame({ args: { dag_id: "sales_summary" }, id: "c1", name: "diagnose_dag", type: "tool" }),
        frame({ id: "c1", name: "diagnose_dag", type: "tool_result" }),
        frame({ delta: "Task ", type: "text" }),
        frame({ delta: "summarize failed.", type: "text" }),
        frame({ type: "done" }),
      ]),
    );

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("what happened?");
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    const [user, assistant] = result.current.messages;
    expect(user?.content).toBe("what happened?");
    expect(assistant?.content).toBe("Task summarize failed.");
    expect(assistant?.tools?.[0]?.name).toBe("diagnose_dag");
    expect(assistant?.tools?.[0]?.durationMs).toBeGreaterThanOrEqual(0);
    expect(assistant?.isError).toBeUndefined();
  });

  it("reassembles frames split across chunk boundaries", async () => {
    const whole = frame({ delta: "hello", type: "text" }) + frame({ type: "done" });
    mockFetch(
      streamingResponse([whole.slice(0, 7), whole.slice(7, 20), whole.slice(20)]),
    );

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("hi");
    });

    expect(result.current.messages[1]?.content).toBe("hello");
  });

  it("sends prior turns as history", async () => {
    const fetchMock = mockFetch(
      streamingResponse([frame({ delta: "one", type: "text" }), frame({ type: "done" })]),
    );

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("first");
    });

    fetchMock.mockReturnValue(
      Promise.resolve(
        streamingResponse([frame({ delta: "two", type: "text" }), frame({ type: "done" })]),
      ),
    );
    await act(async () => {
      await result.current.sendMessage("second");
    });

    const body = JSON.parse(String(fetchMock.mock.lastCall?.[1]?.body));
    expect(body).toEqual({
      history: [
        { content: "first", role: "user" },
        { content: "one", role: "assistant" },
      ],
      message: "second",
      page_url: globalThis.location.pathname,
    });
  });

  it("sends the page path, never the full URL", async () => {
    const fetchMock = mockFetch(
      streamingResponse([frame({ delta: "hi", type: "text" }), frame({ type: "done" })]),
    );

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("what is wrong here?");
    });

    const body = JSON.parse(String(fetchMock.mock.lastCall?.[1]?.body));
    // The query string and fragment are attacker-influenceable and this value
    // reaches the system prompt, so only the path may be sent.
    expect(body.page_url).toBe(globalThis.location.pathname);
    expect(body.page_url).not.toContain("?");
  });

  it("never replays a failed turn back to the model as its own words", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("Failed to fetch"));
    vi.stubGlobal("fetch", fetchMock);

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("first");
    });
    expect(result.current.messages[1]?.isError).toBe(true);

    fetchMock.mockReturnValue(
      Promise.resolve(
        streamingResponse([frame({ delta: "ok", type: "text" }), frame({ type: "done" })]),
      ),
    );
    await act(async () => {
      await result.current.sendMessage("second");
    });

    const body = JSON.parse(String(fetchMock.mock.lastCall?.[1]?.body));
    expect(body.history).toEqual([{ content: "first", role: "user" }]);
  });

  it("keeps the streamed text and stops the spinner when the run errors", async () => {
    mockFetch(
      streamingResponse([
        frame({ args: {}, id: "c1", name: "fix_dag_code", type: "tool" }),
        frame({ delta: "I found the bug: ", type: "text" }),
        frame({ message: "mcp sidecar is gone", type: "error" }),
        frame({ type: "done" }),
      ]),
    );

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("fix it");
    });

    const assistant = result.current.messages[1];
    expect(assistant?.content).toContain("I found the bug: ");
    expect(assistant?.content).toContain("**Error:** mcp sidecar is gone");
    expect(assistant?.isError).toBe(true);
    // A tool still in flight when the run died must not spin forever.
    expect(assistant?.tools?.[0]?.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("says so when the stream stops without finishing", async () => {
    mockFetch(streamingResponse([frame({ delta: "half an ans", type: "text" })]));

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("hi");
    });

    expect(result.current.messages[1]?.content).toContain("half an ans");
    expect(result.current.messages[1]?.content).toContain(
      "connection ended before Airy finished",
    );
  });

  it("surfaces a transport failure in the assistant bubble", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("Failed to fetch")));

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("hi");
    });

    expect(result.current.messages[1]?.isError).toBe(true);
    expect(result.current.messages[1]?.content).toContain("Failed to fetch");
    expect(result.current.isLoading).toBe(false);
  });

  it("surfaces a non-200 response", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        json: () => Promise.resolve({ error: "Empty message" }),
        ok: false,
        status: 400,
      } as unknown as Response),
    );

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("   x");
    });

    expect(result.current.messages[1]?.content).toBe("**Error:** Empty message");
    expect(result.current.isLoading).toBe(false);
  });

  it.each([
    [401, "session has expired"],
    [403, "permission to use Airy"],
  ])("explains a %i in the user's terms", async (status, text) => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        json: () => Promise.resolve({ detail: "Forbidden" }),
        ok: false,
        status,
      } as unknown as Response),
    );

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("hi");
    });

    expect(result.current.messages[1]?.content).toContain(text);
    expect(result.current.messages[1]?.isError).toBe(true);
  });

  it("surfaces a FastAPI detail body", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        json: () => Promise.resolve({ detail: "boom" }),
        ok: false,
        status: 422,
      } as unknown as Response),
    );

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("hi");
    });

    expect(result.current.messages[1]?.content).toBe("**Error:** boom");
  });

  it("says something rather than nothing when the reply is empty", async () => {
    mockFetch(streamingResponse([frame({ type: "done" })]));

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("hi");
    });

    // A blank bubble is hidden, so an empty reply must not leave a blank drawer.
    expect(result.current.messages[1]?.content).toContain("empty response");
    expect(result.current.messages[1]?.isError).toBe(true);
  });

  it("does not call a tool-only reply empty", async () => {
    mockFetch(
      streamingResponse([
        frame({ args: {}, id: "c1", name: "rerun_dag", type: "tool" }),
        frame({ id: "c1", name: "rerun_dag", type: "tool_result" }),
        frame({ type: "done" }),
      ]),
    );

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("re-run it");
    });

    expect(result.current.messages[1]?.content).toBe("");
    expect(result.current.messages[1]?.isError).toBeUndefined();
  });

  it("does not replay the connection-ended notice as Airy's own words", async () => {
    const fetchMock = mockFetch(streamingResponse([frame({ delta: "half", type: "text" })]));

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("first");
    });

    fetchMock.mockReturnValue(
      Promise.resolve(
        streamingResponse([frame({ delta: "ok", type: "text" }), frame({ type: "done" })]),
      ),
    );
    await act(async () => {
      await result.current.sendMessage("second");
    });

    const body = JSON.parse(String(fetchMock.mock.lastCall?.[1]?.body));
    expect(body.history).toEqual([{ content: "first", role: "user" }]);
  });

  it("stops a chip that was in flight when the stream was cut off", async () => {
    mockFetch(
      streamingResponse([frame({ args: {}, id: "c1", name: "fix_dag_code", type: "tool" })]),
    );

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("fix it");
    });

    expect(result.current.messages[1]?.tools?.[0]?.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("blocks a second send while one is streaming", async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    vi.stubGlobal(
      "fetch",
      vi.fn().mockReturnValue(
        gate.then(() =>
          streamingResponse([frame({ delta: "hi", type: "text" }), frame({ type: "done" })]),
        ),
      ),
    );

    const { result } = renderHook(() => useChat());
    let pending: Promise<void> | undefined;
    act(() => {
      pending = result.current.sendMessage("first");
    });

    // The drawer disables input off this flag; without it a double-click sends twice.
    expect(result.current.isLoading).toBe(true);

    await act(async () => {
      release?.();
      await pending;
    });
    expect(result.current.isLoading).toBe(false);
  });

  it("keeps only the most recent turns", async () => {
    const fetchMock = mockFetch(
      streamingResponse([frame({ delta: "x", type: "text" }), frame({ type: "done" })]),
    );

    const { result } = renderHook(() => useChat());
    for (let turn = 0; turn < 12; turn++) {
      fetchMock.mockReturnValue(
        Promise.resolve(
          streamingResponse([frame({ delta: "x", type: "text" }), frame({ type: "done" })]),
        ),
      );
      await act(async () => {
        await result.current.sendMessage(`turn ${turn}`);
      });
    }

    const body = JSON.parse(String(fetchMock.mock.lastCall?.[1]?.body));
    expect(body.history).toHaveLength(20);
    expect(body.history[0]).toEqual({ content: "turn 1", role: "user" });
  });

  it("recovers a final frame whose terminator never arrived", async () => {
    // The decoder and the leftover buffer both have to be flushed, or the last
    // thing Airy said is silently dropped.
    mockFetch(
      streamingResponse([
        'data: {"type":"text","delta":"the fix landed"}\n\ndata: {"type":"done"}',
      ]),
    );

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("hi");
    });

    expect(result.current.messages[1]?.content).toBe("the fix landed");
    expect(result.current.messages[1]?.isError).toBeUndefined();
  });

  const confirmFrames = [
    frame({ args: { dag_id: "sales_summary" }, id: "c9", name: "fix_dag_code", type: "tool" }),
    frame({ delta: "I can fix that.", type: "text" }),
    frame({ args: { dag_id: "sales_summary" }, call_id: "c9", nonce: "n1", tool: "fix_dag_code", type: "confirm_required" }),
    frame({ type: "done" }),
  ];

  it("parks a confirm_required frame on the message and stops the chip", async () => {
    mockFetch(streamingResponse(confirmFrames));

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("fix it");
    });

    const assistant = result.current.messages[1];
    expect(assistant?.confirms).toEqual([
      { args: { dag_id: "sales_summary" }, callId: "c9", nonce: "n1", tool: "fix_dag_code" },
    ]);
    // The suspended call must not spin while the user decides.
    expect(assistant?.tools?.[0]?.durationMs).toBeGreaterThanOrEqual(0);
    expect(assistant?.isError).toBeUndefined();
  });

  it.each([true, false])(
    "resolveConfirm(%s) posts the verdict and streams the continuation into the same bubble",
    async (approved) => {
      const fetchMock = mockFetch(streamingResponse(confirmFrames));

      const { result } = renderHook(() => useChat());
      await act(async () => {
        await result.current.sendMessage("fix it");
      });

      fetchMock.mockReturnValue(
        Promise.resolve(
          streamingResponse([frame({ delta: " Applied.", type: "text" }), frame({ type: "done" })]),
        ),
      );
      await act(async () => {
        await result.current.resolveConfirm("n1", approved);
      });

      const [url, init] = fetchMock.mock.lastCall ?? [];
      expect(String(url)).toContain("/chatbot/confirm");
      expect(JSON.parse(String(init?.body))).toEqual({ approved, nonce: "n1" });
      const assistant = result.current.messages[1];
      expect(assistant?.content).toBe("I can fix that. Applied.");
      expect(assistant?.confirms?.[0]?.resolution).toBe(approved ? "approved" : "rejected");
      expect(result.current.messages).toHaveLength(2);
    },
  );

  it("explains an expired confirmation", async () => {
    const fetchMock = mockFetch(streamingResponse(confirmFrames));

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("fix it");
    });

    fetchMock.mockResolvedValue({
      json: () => Promise.resolve({ error: "Unknown or expired confirmation" }),
      ok: false,
      status: 404,
    } as unknown as Response);
    await act(async () => {
      await result.current.resolveConfirm("n1", true);
    });

    expect(result.current.messages[1]?.content).toContain("no longer valid");
    expect(result.current.messages[1]?.isError).toBe(true);
  });

  it("ignores a confirm that was already resolved", async () => {
    const fetchMock = mockFetch(streamingResponse(confirmFrames));

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("fix it");
    });

    fetchMock.mockReturnValue(
      Promise.resolve(streamingResponse([frame({ type: "done" })])),
    );
    await act(async () => {
      await result.current.resolveConfirm("n1", true);
    });
    const callsAfterFirst = fetchMock.mock.calls.length;
    await act(async () => {
      await result.current.resolveConfirm("n1", true);
    });

    expect(fetchMock.mock.calls).toHaveLength(callsAfterFirst);
  });

  it("clears the conversation", async () => {
    mockFetch(streamingResponse([frame({ delta: "hi", type: "text" }), frame({ type: "done" })]));

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("hello");
    });
    act(() => {
      result.current.clearMessages();
    });

    expect(result.current.messages).toEqual([]);
  });
});
