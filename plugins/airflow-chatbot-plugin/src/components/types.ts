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

/**
 * Type definitions for the chatbot plugin.
 */

/** One MCP/agent tool call, shown in the bubble as it happens. */
export interface ToolCall {
  id: string;
  name: string;
  args?: unknown;
  /** Client clock when the call appeared, used to time it. */
  startedAt: number;
  /** Set when the result arrives; until then the call is still running. */
  durationMs?: number;
  /** Clipped tool output (or error text when failed), for the expandable row. */
  result?: string;
  /** True when the call errored instead of returning. */
  failed?: boolean;
  /** True while the server holds the call waiting for the user's verdict. */
  awaitingConfirm?: boolean;
  /** True when the user rejected the call instead of approving it. */
  denied?: boolean;
}

/** A write tool call the server suspended, waiting for the user's verdict. */
export interface ConfirmRequest {
  nonce: string;
  callId: string;
  tool: string;
  args?: unknown;
  /** Set once the user has clicked; the buttons then freeze. */
  resolution?: "approved" | "rejected";
  /**
   * Set when the reply never finished, so the write may or may not have landed.
   * The nonce is still good: asking again replays the outcome rather than
   * repeating the action.
   */
  outcomeUnknown?: boolean;
}

export interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
  /** Tool calls the assistant made while producing this message. */
  tools?: ToolCall[];
  /** Write tool calls awaiting (or given) an explicit user verdict. */
  confirms?: ConfirmRequest[];
  /** When true the message represents an error response. */
  isError?: boolean;
}

export interface ChatState {
  messages: Message[];
  isLoading: boolean;
  error: string | null;
}
