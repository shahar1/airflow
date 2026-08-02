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
      expect(assistant?.confirms?.[0]?.outcomeUnknown).toBe(false);
      expect(result.current.messages).toHaveLength(2);
    },
  );

  it("keeps a confirm unsettled when the server says it still does not know", async () => {
    // Every stream ends with `done`, replays included — so `done` alone cannot
    // mean the action is settled.
    const fetchMock = mockFetch(streamingResponse(confirmFrames));

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("fix it");
    });

    fetchMock.mockReturnValue(
      Promise.resolve(
        streamingResponse([
          frame({ delta: "still running", type: "text" }),
          frame({ type: "unsettled" }),
          frame({ type: "done" }),
        ]),
      ),
    );
    await act(async () => {
      await result.current.resolveConfirm("n1", true);
    });

    expect(result.current.messages[1]?.confirms?.[0]?.outcomeUnknown).toBe(true);
  });

  it("leaves a confirm answerable when the reply never finishes", async () => {
    // The write may well have landed; the only way to find out is to ask again.
    const fetchMock = mockFetch(streamingResponse(confirmFrames));

    const { result } = renderHook(() => useChat());
    await act(async () => {
      await result.current.sendMessage("fix it");
    });

    // No `done` frame: the connection dropped mid-stream.
    fetchMock.mockReturnValue(
      Promise.resolve(streamingResponse([frame({ delta: " Applying", type: "text" })])),
    );
    await act(async () => {
      await result.current.resolveConfirm("n1", true);
    });

    expect(result.current.messages[1]?.confirms?.[0]).toMatchObject({
      outcomeUnknown: true,
      resolution: "approved",
    });

    // Asking again with the same nonce is allowed, and settles it.
    fetchMock.mockReturnValue(
      Promise.resolve(
        streamingResponse([frame({ delta: " Applied.", type: "text" }), frame({ type: "done" })]),
      ),
    );
    await act(async () => {
      await result.current.resolveConfirm("n1", true);
    });

    expect(result.current.messages[1]?.confirms?.[0]?.outcomeUnknown).toBe(false);
    expect(JSON.parse(String(fetchMock.mock.lastCall?.[1]?.body))).toEqual({
      approved: true,
      nonce: "n1",
    });
  });

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

  /** A body whose reader rejects like an aborted fetch does. */
  const abortingResponse = (): Response =>
    ({
      body: {
        getReader: () => ({
          read: () =>
            Promise.reject(Object.assign(new Error("The operation was aborted."), { name: "AbortError" })),
        }),
      },
      ok: true,
      status: 200,
    }) as unknown as Response;

  describe("stopping a response", () => {
    it("passes an abort signal and exposes a stop control while streaming", async () => {
      let release: (() => void) | undefined;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const fetchMock = vi.fn().mockReturnValue(
        gate.then(() => streamingResponse([frame({ delta: "hi", type: "text" }), frame({ type: "done" })])),
      );
      vi.stubGlobal("fetch", fetchMock);

      const { result } = renderHook(() => useChat());
      let pending: Promise<void> | undefined;
      act(() => {
        pending = result.current.sendMessage("first");
      });

      expect(result.current.canStop).toBe(true);
      expect(result.current.isApplyingChange).toBe(false);
      expect(fetchMock.mock.lastCall?.[1]?.signal).toBeInstanceOf(AbortSignal);

      await act(async () => {
        release?.();
        await pending;
      });
      expect(result.current.canStop).toBe(false);
    });

    it("reads a stop as a stop, not as an error", async () => {
      mockFetch(abortingResponse());

      const { result } = renderHook(() => useChat());
      await act(async () => {
        await result.current.sendMessage("hi");
      });

      const assistant = result.current.messages[1];
      expect(assistant?.content).toContain("_Stopped._");
      expect(assistant?.stopped).toBe(true);
      expect(assistant?.isError).toBeUndefined();
      expect(assistant?.content).not.toContain("connection ended before Airy finished");
      expect(result.current.isLoading).toBe(false);
      expect(result.current.canStop).toBe(false);
    });

    it("keeps the stopped turn on screen but out of the model's history", async () => {
      const fetchMock = mockFetch(abortingResponse());

      const { result } = renderHook(() => useChat());
      await act(async () => {
        await result.current.sendMessage("the stopped question");
      });
      expect(result.current.messages).toHaveLength(2);

      fetchMock.mockReturnValue(
        Promise.resolve(streamingResponse([frame({ delta: "ok", type: "text" }), frame({ type: "done" })])),
      );
      await act(async () => {
        await result.current.sendMessage("next");
      });

      expect(JSON.parse(String(fetchMock.mock.lastCall?.[1]?.body)).history).toEqual([]);
    });

    it("cancels the tools that were still in flight instead of ticking them green", async () => {
      const encoder = new TextEncoder();
      // A tool call arrives, then the reader is aborted mid-answer.
      mockFetch({
        body: {
          getReader: () => {
            let sent = false;
            return {
              read: () => {
                if (sent) {
                  return Promise.reject(
                    Object.assign(new Error("aborted"), { name: "AbortError" }),
                  );
                }
                sent = true;
                return Promise.resolve({
                  done: false,
                  value: encoder.encode(
                    frame({ args: {}, id: "c1", name: "diagnose_dag", type: "tool" }) +
                      frame({ args: {}, id: "c2", name: "fix_dag_code", proposed: true, type: "tool" }),
                  ),
                });
              },
            };
          },
        },
        ok: true,
        status: 200,
      } as unknown as Response);

      const { result } = renderHook(() => useChat());
      await act(async () => {
        await result.current.sendMessage("fix it");
      });

      const tools = result.current.messages[1]?.tools ?? [];
      expect(tools).toHaveLength(2);
      expect(tools.every((tool) => tool.cancelled === true)).toBe(true);
      // A write stopped before it was ever confirmed must not read as applied.
      expect(tools[1]?.proposed).toBeUndefined();
      expect(tools.every((tool) => tool.failed !== true)).toBe(true);
    });

    it("offers no stop while an approved write may already be running", async () => {
      let release: (() => void) | undefined;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const fetchMock = mockFetch(streamingResponse(confirmFrames));

      const { result } = renderHook(() => useChat());
      await act(async () => {
        await result.current.sendMessage("fix it");
      });

      fetchMock.mockReturnValue(
        gate.then(() =>
          streamingResponse([frame({ delta: " Applied.", type: "text" }), frame({ type: "done" })]),
        ),
      );
      let pending: Promise<void> | undefined;
      act(() => {
        pending = result.current.resolveConfirm("n1", true);
      });

      expect(result.current.isApplyingChange).toBe(true);
      expect(result.current.canStop).toBe(false);

      await act(async () => {
        release?.();
        await pending;
      });
      expect(result.current.isApplyingChange).toBe(false);
    });

    it("reads a stopped rejection continuation as stopped, not as an error", async () => {
      const fetchMock = mockFetch(streamingResponse(confirmFrames));

      const { result } = renderHook(() => useChat());
      await act(async () => {
        await result.current.sendMessage("fix it");
      });

      fetchMock.mockReturnValue(Promise.resolve(abortingResponse()));
      await act(async () => {
        await result.current.resolveConfirm("n1", false);
      });

      const assistant = result.current.messages[1];
      expect(assistant?.stopped).toBe(true);
      expect(assistant?.isError).toBeUndefined();
      expect(assistant?.content).toContain("_Stopped._");
      // The marker is ours, so this bubble never goes back as Airy's words.
      expect(assistant?.excludeFromHistory).toBe(true);
    });

    it("still offers stop for the prose that follows a rejection", async () => {
      let release: (() => void) | undefined;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const fetchMock = mockFetch(streamingResponse(confirmFrames));

      const { result } = renderHook(() => useChat());
      await act(async () => {
        await result.current.sendMessage("fix it");
      });

      fetchMock.mockReturnValue(
        gate.then(() =>
          streamingResponse([frame({ delta: " Left alone.", type: "text" }), frame({ type: "done" })]),
        ),
      );
      let pending: Promise<void> | undefined;
      act(() => {
        pending = result.current.resolveConfirm("n1", false);
      });

      expect(result.current.canStop).toBe(true);

      await act(async () => {
        release?.();
        await pending;
      });
    });
  });

  describe("retrying a failed turn", () => {
    it("re-asks exactly that question, with the failed pair out of history", async () => {
      const fetchMock = mockFetch(
        streamingResponse([frame({ delta: "one", type: "text" }), frame({ type: "done" })]),
      );

      const { result } = renderHook(() => useChat());
      await act(async () => {
        await result.current.sendMessage("first");
      });

      fetchMock.mockRejectedValueOnce(new Error("Failed to fetch"));
      await act(async () => {
        await result.current.sendMessage("the failed question");
      });
      const failedId = result.current.messages[3]?.id ?? "";
      expect(result.current.messages[3]?.isError).toBe(true);

      fetchMock.mockReturnValue(
        Promise.resolve(streamingResponse([frame({ delta: "ok", type: "text" }), frame({ type: "done" })])),
      );
      await act(async () => {
        await result.current.retryMessage(failedId);
      });

      const body = JSON.parse(String(fetchMock.mock.lastCall?.[1]?.body));
      expect(body.message).toBe("the failed question");
      // Neither the question that failed nor the synthetic error goes back.
      expect(body.history).toEqual([
        { content: "first", role: "user" },
        { content: "one", role: "assistant" },
      ]);
      expect(result.current.messages).toHaveLength(6);
      expect(result.current.messages[5]?.content).toBe("ok");
    });

    it("re-asks the question that failed, not the newest one on screen", async () => {
      // A confirmation resumes into an older bubble, so "the last user message"
      // is the wrong anchor.
      const fetchMock = mockFetch(
        streamingResponse([frame({ delta: "one", type: "text" }), frame({ type: "done" })]),
      );

      const { result } = renderHook(() => useChat());
      fetchMock.mockRejectedValueOnce(new Error("Failed to fetch"));
      await act(async () => {
        await result.current.sendMessage("the old broken question");
      });
      const failedId = result.current.messages[1]?.id ?? "";

      fetchMock.mockReturnValue(
        Promise.resolve(streamingResponse([frame({ delta: "two", type: "text" }), frame({ type: "done" })])),
      );
      await act(async () => {
        await result.current.sendMessage("a newer, unrelated question");
      });
      await act(async () => {
        await result.current.retryMessage(failedId);
      });

      expect(JSON.parse(String(fetchMock.mock.lastCall?.[1]?.body)).message).toBe(
        "the old broken question",
      );
    });

    it("does nothing for a message that is not there", async () => {
      const fetchMock = mockFetch(streamingResponse([frame({ type: "done" })]));

      const { result } = renderHook(() => useChat());
      await act(async () => {
        await result.current.retryMessage("nope");
      });

      expect(fetchMock).not.toHaveBeenCalled();
    });
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
