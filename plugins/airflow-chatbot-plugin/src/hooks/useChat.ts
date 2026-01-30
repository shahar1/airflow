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

import { Message } from "../components/types";

/** Generate a unique ID for messages. */
const generateId = (): string =>
  `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

const CHATBOT_BASE = () => `${globalThis.location.origin}/chatbot`;

/**
 * Convert our Message[] array into the lightweight history format
 * that the backend expects.  We *exclude* the last message because the
 * backend receives it separately in the `message` field.
 */
const toHistory = (
  msgs: Message[],
): Array<{ role: string; content: string }> =>
  msgs.slice(0, -1).map((m) => ({ role: m.role, content: m.content }));

// ── Health status ──────────────────────────────────────────────────────

export interface HealthStatus {
  ok: boolean;
  llm: boolean;
  mcp: boolean;
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
        loading: false,
      });
    } catch {
      setHealth({ ok: false, llm: false, mcp: false, loading: false });
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
 * Sends messages to the Airy backend (`POST /chatbot/chat`) which
 * forwards them to the PydanticAI agent.  On error the reply is
 * inserted as an assistant message with `isError: true` so the UI
 * can render it distinctly.
 */
export const useChat = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const messagesRef = useRef<Message[]>([]);

  const sendMessage = useCallback(async (content: string) => {
    const userMessage: Message = {
      content,
      id: generateId(),
      role: "user",
      timestamp: new Date(),
    };

    setMessages((prev) => {
      const next = [...prev, userMessage];
      messagesRef.current = next;
      return next;
    });
    setIsLoading(true);

    try {
      const response = await fetch(`${CHATBOT_BASE()}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({
          message: content,
          history: toHistory(messagesRef.current),
        }),
      });

      if (!response.ok) {
        const errBody = await response.json().catch(() => null);
        throw new Error(
          errBody?.error ?? `Server error (${response.status})`,
        );
      }

      const data: { response: string; status: string } =
        await response.json();

      const assistantMessage: Message = {
        content: data.response,
        id: generateId(),
        role: "assistant",
        timestamp: new Date(),
      };

      setMessages((prev) => {
        const next = [...prev, assistantMessage];
        messagesRef.current = next;
        return next;
      });
    } catch (err) {
      // Surface the error as a visible assistant message
      const errorMessage: Message = {
        content:
          err instanceof Error
            ? `**Error:** ${err.message}`
            : "**Error:** Failed to get a response from Airy.",
        id: generateId(),
        role: "assistant",
        timestamp: new Date(),
        isError: true,
      };
      setMessages((prev) => {
        const next = [...prev, errorMessage];
        messagesRef.current = next;
        return next;
      });
    } finally {
      setIsLoading(false);
    }
  }, []);

  const clearMessages = useCallback(() => {
    setMessages([]);
    messagesRef.current = [];
  }, []);

  return {
    clearMessages,
    isLoading,
    messages,
    sendMessage,
  };
};
