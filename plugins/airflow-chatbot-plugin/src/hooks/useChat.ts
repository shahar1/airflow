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
import { useCallback, useEffect, useRef, useState } from "react";

import { ConfirmRequest, Message, ToolCall } from "../components/types";

/** Generate a unique ID for messages. */
const generateId = (): string => `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

const CHATBOT_BASE = () => `${globalThis.location.origin}/chatbot`;

const HISTORY_TURNS = 20;
const STORAGE_KEY = "airy-chat-history";

/** Restore the conversation a page reload would otherwise wipe mid-demo. */
export const loadStoredMessages = (): Message[] => {
  try {
    const raw = globalThis.sessionStorage?.getItem(STORAGE_KEY);
    if (raw === null || raw === undefined) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    const reloadedAt = Date.now();
    return (parsed as Message[])
      .filter((m) => m.content !== "" || (m.tools?.length ?? 0) > 0)
      .map((m) =>
        // A call in flight when the page reloaded lost its stream: it must
        // neither spin forever nor be dressed up as a finished one.
        finalizeTools({ ...m, timestamp: new Date(m.timestamp) }, reloadedAt),
      );
  } catch {
    return [];
  }
};

const persistMessages = (messages: Message[]) => {
  try {
    globalThis.sessionStorage?.setItem(STORAGE_KEY, JSON.stringify(messages));
  } catch {
    // Storage full or blocked: the chat still works, it just won't survive reload.
  }
};

/** The backend takes prior turns as history; the new turn is sent separately. */
export const toHistory = (msgs: Message[]): Array<{ content: string; role: string }> =>
  msgs
    .filter((m) => m.content !== "" && m.isError !== true && m.excludeFromHistory !== true)
    .slice(-HISTORY_TURNS)
    .map((m) => ({ content: m.content, role: m.role }));

/**
 * Split a server-sent-events buffer into parsed frames plus whatever partial
 * frame is left over for the next chunk.
 */
export const parseFrames = (buffer: string): { events: Array<Record<string, unknown>>; rest: string } => {
  const chunks = buffer.split(/\r?\n\r?\n/u);
  const rest = chunks.pop() ?? "";
  const events: Array<Record<string, unknown>> = [];

  for (const chunk of chunks) {
    const line = chunk.split(/\r?\n/u).find((l) => l.startsWith("data:"));
    if (!line) continue;
    try {
      events.push(JSON.parse(line.slice(5).trim()));
    } catch {
      // A frame we can't parse is not worth killing the stream over.
    }
  }
  return { events, rest };
};

/**
 * Stop the clock on any call still in flight when the run ended.
 *
 * A call that never returned a result did not demonstrably finish, so none of
 * them may end up as a green check.  Which untruth to avoid depends on the
 * call: an approved write may well have landed on the host and its outcome is
 * simply unknown, while anything else — a read cut short, a proposal the run
 * never came back to — never ran at all.
 */
export const finalizeTools = (message: Message, now: number): Message => {
  const approved = new Set(
    (message.confirms ?? []).filter((c) => c.resolution === "approved").map((c) => c.callId),
  );
  return {
    ...message,
    tools: message.tools?.map((tool) =>
      tool.durationMs === undefined
        ? {
            ...tool,
            ...(approved.has(tool.id) ? { unsettled: true } : { cancelled: true }),
            durationMs: now - tool.startedAt,
            proposed: undefined,
          }
        : tool,
    ),
  };
};

/** Fold one streamed event into the assistant message being built. */
export const applyEvent = (
  message: Message,
  event: Record<string, unknown>,
  now: number = Date.now(),
): Message => {
  switch (event.type) {
    case "tool": {
      // A *rejected* call is streamed again under its id as well — the runner
      // replays every deferred call, approved or not. Nothing runs for this
      // one, so it must not flip to the running state on its way to rejected.
      if ((message.confirms ?? []).some((c) => c.callId === event.id && c.resolution === "rejected")) {
        return message;
      }
      // An approved write tool streams again under its original call id when
      // the run resumes; that is the same call going back to work, not a new
      // row (and duplicate ids would collide as React keys).
      if ((message.tools ?? []).some((tool) => tool.id === event.id)) {
        return {
          ...message,
          tools: (message.tools ?? []).map((tool) =>
            tool.id === event.id
              ? {
                  ...tool,
                  awaitingConfirm: undefined,
                  cancelled: undefined,
                  denied: undefined,
                  durationMs: undefined,
                  failed: undefined,
                  // The frame still says `proposed` — it describes the tool
                  // class. The repeated id is what proves this is the approved
                  // call going back to work, so the proposal state ends here.
                  proposed: undefined,
                  result: undefined,
                  startedAt: now,
                }
              : tool,
          ),
        };
      }
      return {
        ...message,
        tools: [
          ...(message.tools ?? []),
          {
            args: event.args,
            id: String(event.id ?? ""),
            name: String(event.name ?? "tool"),
            proposed: event.proposed === true ? true : undefined,
            startedAt: now,
          } satisfies ToolCall,
        ],
      };
    }
    case "tool_result":
      return {
        ...message,
        tools: (message.tools ?? []).map((tool) =>
          tool.id === event.id && (tool.durationMs === undefined || tool.awaitingConfirm === true)
            ? {
                ...tool,
                awaitingConfirm: undefined,
                denied: event.denied === true,
                durationMs: tool.durationMs ?? now - tool.startedAt,
                failed: event.failed === true,
                result: typeof event.result === "string" ? event.result : undefined,
              }
            : tool,
        ),
      };
    case "confirm_required":
      return {
        ...message,
        confirms: [
          ...(message.confirms ?? []),
          {
            args: event.args,
            callId: String(event.call_id ?? ""),
            nonce: String(event.nonce ?? ""),
            tool: String(event.tool ?? "tool"),
          } satisfies ConfirmRequest,
        ],
        // The suspended call must neither spin nor read as done while the
        // user decides — freeze the clock and flag it as awaiting.
        tools: (message.tools ?? []).map((tool) =>
          tool.id === event.call_id && tool.durationMs === undefined
            ? { ...tool, awaitingConfirm: true, durationMs: now - tool.startedAt }
            : tool,
        ),
      };
    case "text":
      return { ...message, content: message.content + String(event.delta ?? "") };
    case "error":
      return {
        ...finalizeTools(message, now),
        content: `${message.content}\n\n**Error:** ${String(event.message ?? "unknown error")}`.trimStart(),
        isError: true,
      };
    default:
      return message;
  }
};

/** Index of the user turn an errored assistant message was answering. */
export const findAskedIndex = (messages: Message[], errorMessageId: string): number | undefined => {
  const failedIndex = messages.findIndex((m) => m.id === errorMessageId);
  if (failedIndex < 0) return undefined;
  for (let index = failedIndex - 1; index >= 0; index--) {
    if (messages[index]?.role === "user") return index;
  }
  return undefined;
};

/** Map an HTTP failure to a message the user can act on. */
const errorForResponse = async (response: Response): Promise<Error> => {
  if (response.status === 401) {
    return new Error("Your Airflow session has expired — sign in again to keep chatting.");
  }
  if (response.status === 403) {
    return new Error("You don't have permission to use Airy.");
  }
  if (response.status === 404) {
    return new Error("This confirmation is no longer valid — ask Airy again.");
  }
  const errBody = await response.json().catch(() => null);
  // FastAPI errors arrive as {detail}, our own as {error}.
  return new Error(errBody?.error ?? errBody?.detail ?? `Server error (${response.status})`);
};

// ── Health status ──────────────────────────────────────────────────────

export interface HealthStatus {
  ok: boolean;
  llm: boolean;
  mcp: boolean;
  /** Some MCP endpoints are down, or the MCP extra is missing: Airy answers, but tool-less. */
  degraded: boolean;
  loading: boolean;
}

/**
 * Periodically polls `/chatbot/health` and exposes connectivity state.
 * Polls every 30 s while the component is mounted, plus on-demand via
 * `recheckHealth()`.
 */
export const useHealth = () => {
  const [health, setHealth] = useState<HealthStatus>({
    degraded: false,
    llm: false,
    loading: true,
    mcp: false,
    ok: false,
  });

  const fetchHealth = useCallback(async () => {
    try {
      const res = await fetch(`${CHATBOT_BASE()}/health`, {
        credentials: "include",
      });
      if (!res.ok) throw new Error(`status ${res.status}`);
      const data = await res.json();
      setHealth({
        degraded: (data.mcp?.unreachable?.length ?? 0) > 0 || data.mcp?.toolset_importable === false,
        llm: data.llm?.configured ?? false,
        loading: false,
        mcp: data.mcp?.reachable ?? false,
        ok: data.llm?.configured ?? false,
      });
    } catch {
      setHealth({ degraded: false, llm: false, loading: false, mcp: false, ok: false });
    }
  }, []);

  useEffect(() => {
    fetchHealth();
    const id = setInterval(fetchHealth, 30_000);
    return () => clearInterval(id);
  }, [fetchHealth]);

  return { health, recheckHealth: fetchHealth };
};

// ── Chat hook ──────────────────────────────────────────────────────────

/**
 * What the drawer is doing, which is not the same question as "is it busy".
 *
 * `applying` is a /confirm the user approved: the lasting mutation may already
 * be executing on the host, and hanging up the HTTP reader would not undo it.
 * Only `streaming` may be stopped.
 */
export type ChatPhase = "applying" | "idle" | "streaming";

const isAbort = (err: unknown): boolean => (err as { name?: string } | null)?.name === "AbortError";

/**
 * Hook for managing chat state and interactions.
 *
 * `POST /chatbot/chat` answers with server-sent events, so the assistant
 * bubble is created empty and filled in as tool calls and text arrive.  On
 * error the reply is marked `isError: true` so the UI can render it distinctly.
 */
export const useChat = () => {
  const [messages, setMessages] = useState<Message[]>(loadStoredMessages);
  const [phase, setPhase] = useState<ChatPhase>("idle");
  // The message currently receiving the SSE stream — not always the last one:
  // a confirmation can be resolved after later turns were added.
  const [streamingId, setStreamingId] = useState<string | null>(null);
  const messagesRef = useRef<Message[]>(messages);
  const abortRef = useRef<AbortController | null>(null);
  const isLoading = phase !== "idle";

  const commit = useCallback((next: Message[]) => {
    messagesRef.current = next;
    setMessages(next);
    persistMessages(next);
  }, []);

  /** Fold a /chat or /confirm SSE response into the given assistant message. */
  const streamInto = useCallback(
    async (assistantId: string, response: Response) => {
      const update = (fn: (message: Message) => Message) =>
        commit(messagesRef.current.map((m) => (m.id === assistantId ? fn(m) : m)));

      const body = response.body;
      if (!body) return;

      const reader = body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let complete = false;
      let unsettled = false;

      const consume = (chunk: string) => {
        buffer += chunk;
        const { events, rest } = parseFrames(buffer);
        buffer = rest;
        for (const event of events) {
          complete ||= event.type === "done";
          // The server saying "I still don't know" outranks the stream ending
          // tidily — every stream ends with `done`, including that one.
          unsettled ||= event.type === "unsettled";
          update((message) => applyEvent(message, event));
        }
      };

      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        consume(decoder.decode(value, { stream: true }));
      }
      // A frame left in the buffer only completes once the decoder is flushed.
      consume(`${decoder.decode()}\n\n`);

      if (!complete) {
        update((message) => ({
          ...message,
          content: `${message.content}\n\n_The connection ended before Airy finished._`,
          // Otherwise `toHistory` replays this notice as Airy's own words.
          isError: true,
        }));
      }
      return complete && !unsettled;
    },
    [commit],
  );

  /** Post one user turn and stream the answer into a fresh assistant bubble. */
  const runTurn = useCallback(
    async (content: string, history: Array<{ content: string; role: string }>) => {
      const userId = generateId();
      const assistantId = generateId();

      commit([
        ...messagesRef.current,
        { content, id: userId, role: "user", timestamp: new Date() },
        {
          content: "",
          id: assistantId,
          role: "assistant",
          timestamp: new Date(),
        },
      ]);
      setPhase("streaming");
      setStreamingId(assistantId);
      const controller = new AbortController();
      abortRef.current = controller;

      const update = (fn: (message: Message) => Message) =>
        commit(messagesRef.current.map((m) => (m.id === assistantId ? fn(m) : m)));

      try {
        const response = await fetch(`${CHATBOT_BASE()}/chat`, {
          body: JSON.stringify({
            history,
            message: content,
            page_url: globalThis.location.pathname,
          }),
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          method: "POST",
          signal: controller.signal,
        });

        if (!response.ok || !response.body) {
          throw await errorForResponse(response);
        }

        await streamInto(assistantId, response);
      } catch (err) {
        if (isAbort(err)) {
          // The user's own stop is not a failure: keep the partial answer on
          // screen, but keep the half-turn out of what the model is told next.
          // The tools left in flight are settled by `finalizeTools` below.
          update((message) => ({
            ...message,
            content: `${message.content}\n\n_Stopped._`.trimStart(),
            excludeFromHistory: true,
            stopped: true,
          }));
          commit(
            messagesRef.current.map((m) => (m.id === userId ? { ...m, excludeFromHistory: true } : m)),
          );
        } else {
          update((message) => ({
            ...message,
            content:
              err instanceof Error
                ? `**Error:** ${err.message}`
                : "**Error:** Failed to get a response from Airy.",
            isError: true,
          }));
        }
      } finally {
        abortRef.current = null;
        setPhase("idle");
        setStreamingId(null);
        // A bubble with nothing in it is hidden, so without this the drawer
        // would show the question and no answer at all.
        update((message) =>
          message.content === "" && !message.tools?.length
            ? { ...message, content: "_Airy returned an empty response._", isError: true }
            : message,
        );
        // A run that dies between a tool call and its result would otherwise
        // leave that chip spinning for the rest of the session.
        const ended = Date.now();
        update((message) => finalizeTools(message, ended));
      }
    },
    [commit, streamInto],
  );

  const sendMessage = useCallback(
    async (content: string) => runTurn(content, toHistory(messagesRef.current)),
    [runTurn],
  );

  /**
   * Re-ask the question behind one failed answer.
   *
   * Located by the errored message's own id, never by "the last user message":
   * a confirmation stream resumes into an older bubble, so the newest question
   * may belong to an entirely different turn.
   */
  const retryMessage = useCallback(
    async (errorMessageId: string) => {
      const messages = messagesRef.current;
      const askedIndex = findAskedIndex(messages, errorMessageId);
      if (askedIndex === undefined) return;
      const asked = messages[askedIndex];
      if (!asked) return;
      // History stops before the failed pair: the question is being asked again
      // as this turn, and the synthetic error was never Airy's answer.
      return runTurn(asked.content, toHistory(messages.slice(0, askedIndex)));
    },
    [runTurn],
  );

  /** Hang up on a stoppable stream; an approved mutation is never stoppable. */
  const stopResponse = useCallback(() => {
    abortRef.current?.abort();
  }, []);

  /** Answer a confirm_required frame; the reply streams into the same bubble. */
  const resolveConfirm = useCallback(
    async (nonce: string, approved: boolean) => {
      // A confirm whose outcome is unknown is still answerable: the server kept
      // the record, so posting the same nonce replays what happened.
      const owner = messagesRef.current.find((m) =>
        m.confirms?.some(
          (c) => c.nonce === nonce && (c.resolution === undefined || c.outcomeUnknown === true),
        ),
      );
      if (!owner) return;
      const assistantId = owner.id;

      const update = (fn: (message: Message) => Message) =>
        commit(messagesRef.current.map((m) => (m.id === assistantId ? fn(m) : m)));

      const settle = (outcomeUnknown: boolean) =>
        update((message) => ({
          ...message,
          confirms: message.confirms?.map((c) =>
            c.nonce === nonce
              ? { ...c, outcomeUnknown, resolution: approved ? "approved" : "rejected" }
              : c,
          ),
        }));

      // Submitted, not settled: the write may land even if the reply never
      // arrives, so the outcome stays unknown until the stream says otherwise.
      settle(true);
      // An approved write may already be running on the host; hanging up the
      // reader would not undo it, so that phase offers no Stop.
      setPhase(approved ? "applying" : "streaming");
      setStreamingId(assistantId);
      const controller = new AbortController();
      abortRef.current = approved ? null : controller;

      try {
        const response = await fetch(`${CHATBOT_BASE()}/confirm`, {
          body: JSON.stringify({ approved, nonce }),
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          method: "POST",
          signal: controller.signal,
        });

        if (!response.ok || !response.body) {
          throw await errorForResponse(response);
        }

        settle(!(await streamInto(assistantId, response)));
      } catch (err) {
        if (isAbort(err)) {
          update((message) => ({
            ...message,
            content: `${message.content}\n\n_Stopped._`.trimStart(),
            // The marker is ours, not Airy's; it must never come back as history.
            excludeFromHistory: true,
            stopped: true,
          }));
        } else {
          update((message) =>
            applyEvent(message, {
              message: err instanceof Error ? err.message : "Failed to get a response from Airy.",
              type: "error",
            }),
          );
        }
      } finally {
        abortRef.current = null;
        setPhase("idle");
        setStreamingId(null);
        const ended = Date.now();
        update((message) => finalizeTools(message, ended));
      }
    },
    [commit, streamInto],
  );

  const clearMessages = useCallback(() => {
    setMessages([]);
    messagesRef.current = [];
    try {
      globalThis.sessionStorage?.removeItem(STORAGE_KEY);
    } catch {
      // Nothing to clean up if storage is unavailable.
    }
  }, []);

  return {
    canStop: phase === "streaming",
    clearMessages,
    isApplyingChange: phase === "applying",
    isLoading,
    messages,
    resolveConfirm,
    retryMessage,
    sendMessage,
    stopResponse,
    streamingId,
  };
};
