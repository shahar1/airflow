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

import { Message, ToolCall } from "../components/types";

/** Generate a unique ID for messages. */
const generateId = (): string =>
  `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

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
    return (parsed as Message[])
      .filter((m) => m.content !== "" || (m.tools?.length ?? 0) > 0)
      .map((m) => ({
        ...m,
        timestamp: new Date(m.timestamp),
        // A chip still in flight when the page reloaded must not spin forever.
        tools: m.tools?.map((tool) =>
          tool.durationMs === undefined ? { ...tool, durationMs: 0 } : tool,
        ),
      }));
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
export const toHistory = (
  msgs: Message[],
): Array<{ content: string; role: string }> =>
  msgs
    .filter((m) => m.content !== "" && m.isError !== true)
    .slice(-HISTORY_TURNS)
    .map((m) => ({ content: m.content, role: m.role }));

/**
 * Split a server-sent-events buffer into parsed frames plus whatever partial
 * frame is left over for the next chunk.
 */
export const parseFrames = (
  buffer: string,
): { events: Array<Record<string, unknown>>; rest: string } => {
  const chunks = buffer.split(/\r?\n\r?\n/u);
  const rest = chunks.pop() ?? "";
  const events: Array<Record<string, unknown>> = [];

  for (const chunk of chunks) {
    const line = chunk
      .split(/\r?\n/u)
      .find((l) => l.startsWith("data:"));
    if (!line) continue;
    try {
      events.push(JSON.parse(line.slice(5).trim()));
    } catch {
      // A frame we can't parse is not worth killing the stream over.
    }
  }
  return { events, rest };
};

/** Stop the clock on any tool call still marked as running. */
export const finalizeTools = (message: Message, now: number): Message => ({
  ...message,
  tools: message.tools?.map((tool) =>
    tool.durationMs === undefined
      ? { ...tool, durationMs: now - tool.startedAt }
      : tool,
  ),
});

/** Fold one streamed event into the assistant message being built. */
export const applyEvent = (
  message: Message,
  event: Record<string, unknown>,
  now: number = Date.now(),
): Message => {
  switch (event.type) {
    case "tool":
      return {
        ...message,
        tools: [
          ...(message.tools ?? []),
          {
            args: event.args,
            id: String(event.id ?? ""),
            name: String(event.name ?? "tool"),
            startedAt: now,
          } satisfies ToolCall,
        ],
      };
    case "tool_result":
      return {
        ...message,
        tools: (message.tools ?? []).map((tool) =>
          tool.id === event.id && tool.durationMs === undefined
            ? { ...tool, durationMs: now - tool.startedAt }
            : tool,
        ),
      };
    case "text":
      return { ...message, content: message.content + String(event.delta ?? "") };
    case "error":
      return {
        ...finalizeTools(message, now),
        content:
          `${message.content}\n\n**Error:** ${String(event.message ?? "unknown error")}`.trimStart(),
        isError: true,
      };
    default:
      return message;
  }
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
    ok: false,
    llm: false,
    mcp: false,
    degraded: false,
    loading: true,
  });

  const fetchHealth = useCallback(async () => {
    try {
      const res = await fetch(`${CHATBOT_BASE()}/health`, {
        credentials: "include",
      });
      if (!res.ok) throw new Error(`status ${res.status}`);
      const data = await res.json();
      setHealth({
        ok: data.llm?.configured ?? false,
        llm: data.llm?.configured ?? false,
        mcp: data.mcp?.reachable ?? false,
        degraded:
          (data.mcp?.unreachable?.length ?? 0) > 0 ||
          data.mcp?.toolset_importable === false,
        loading: false,
      });
    } catch {
      setHealth({ ok: false, llm: false, mcp: false, degraded: false, loading: false });
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
 * Hook for managing chat state and interactions.
 *
 * `POST /chatbot/chat` answers with server-sent events, so the assistant
 * bubble is created empty and filled in as tool calls and text arrive.  On
 * error the reply is marked `isError: true` so the UI can render it distinctly.
 */
export const useChat = () => {
  const [messages, setMessages] = useState<Message[]>(loadStoredMessages);
  const [isLoading, setIsLoading] = useState(false);
  const messagesRef = useRef<Message[]>(messages);

  const commit = useCallback((next: Message[]) => {
    messagesRef.current = next;
    setMessages(next);
    persistMessages(next);
  }, []);

  const sendMessage = useCallback(
    async (content: string) => {
      const history = toHistory(messagesRef.current);
      const assistantId = generateId();

      commit([
        ...messagesRef.current,
        { content, id: generateId(), role: "user", timestamp: new Date() },
        {
          content: "",
          id: assistantId,
          role: "assistant",
          timestamp: new Date(),
        },
      ]);
      setIsLoading(true);

      const update = (fn: (message: Message) => Message) =>
        commit(
          messagesRef.current.map((m) => (m.id === assistantId ? fn(m) : m)),
        );

      try {
        const response = await fetch(`${CHATBOT_BASE()}/chat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "include",
          body: JSON.stringify({
            history,
            message: content,
            page_url: globalThis.location.pathname,
          }),
        });

        if (!response.ok || !response.body) {
          if (response.status === 401) {
            throw new Error(
              "Your Airflow session has expired — sign in again to keep chatting.",
            );
          }
          if (response.status === 403) {
            throw new Error("You don't have permission to use Airy.");
          }
          const errBody = await response.json().catch(() => null);
          // FastAPI errors arrive as {detail}, our own as {error}.
          throw new Error(
            errBody?.error ?? errBody?.detail ?? `Server error (${response.status})`,
          );
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        let complete = false;

        const consume = (chunk: string) => {
          buffer += chunk;
          const { events, rest } = parseFrames(buffer);
          buffer = rest;
          for (const event of events) {
            complete ||= event.type === "done";
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
      } catch (err) {
        update((message) => ({
          ...message,
          content:
            err instanceof Error
              ? `**Error:** ${err.message}`
              : "**Error:** Failed to get a response from Airy.",
          isError: true,
        }));
      } finally {
        setIsLoading(false);
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
    [commit],
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
    clearMessages,
    isLoading,
    messages,
    sendMessage,
  };
};
