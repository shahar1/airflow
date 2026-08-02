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

import { ChakraProvider } from "@chakra-ui/react";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ColorModeProvider } from "src/context/colorMode";

import { ChatDrawer } from "./ChatDrawer";
import { localSystem } from "../theme";
import { Message } from "./types";
import { HealthStatus } from "../hooks/useChat";

const health: HealthStatus = {
  degraded: false,
  llm: true,
  loading: false,
  mcp: true,
  ok: true,
};

const oneMessage: Message[] = [
  { content: "hi", id: "u1", role: "user", timestamp: new Date(0) },
];

const drawer = (over: Partial<Parameters<typeof ChatDrawer>[0]> = {}) =>
  render(
    <ChakraProvider value={localSystem}>
      <ColorModeProvider>
        <ChatDrawer
          health={health}
          isOpen
          messages={oneMessage}
          onClear={vi.fn()}
          onClose={vi.fn()}
          onSendMessage={vi.fn()}
          {...over}
        />
      </ColorModeProvider>
    </ChakraProvider>,
  );

describe("ChatDrawer reset button", () => {
  it("clears the conversation on click", () => {
    const onClear = vi.fn();
    drawer({ onClear });

    fireEvent.click(screen.getByLabelText("Clear conversation"));

    expect(onClear).toHaveBeenCalledOnce();
  });

  it("is disabled when there is nothing to clear", () => {
    drawer({ messages: [] });

    expect(screen.getByLabelText("Clear conversation")).toHaveProperty("disabled", true);
  });

  it("is disabled while a reply is streaming", () => {
    drawer({ isLoading: true });

    expect(screen.getByLabelText("Clear conversation")).toHaveProperty("disabled", true);
  });
});

describe("ChatDrawer send control", () => {
  it("sends by default", () => {
    drawer();

    expect(screen.getByLabelText("Send message")).not.toBeNull();
    expect(screen.queryByLabelText("Stop response")).toBeNull();
  });

  it("becomes an enabled Stop while a stoppable stream is in flight", () => {
    const onStop = vi.fn();
    drawer({ canStop: true, isLoading: true, onStop });

    const stop = screen.getByLabelText("Stop response");
    expect(stop).toHaveProperty("disabled", false);
    fireEvent.click(stop);
    expect(onStop).toHaveBeenCalledOnce();

    // The textarea stays disabled: the turn is still Airy's.
    expect(screen.getByPlaceholderText(/Ask anything/u)).toHaveProperty("disabled", true);
  });

  it("offers no stop while an approved change may be executing", () => {
    const onStop = vi.fn();
    drawer({ isApplyingChange: true, isLoading: true, onStop });

    expect(screen.queryByLabelText("Stop response")).toBeNull();
    const applying = screen.getByLabelText("Applying approved change…");
    expect(applying).toHaveProperty("disabled", true);

    fireEvent.click(applying);
    expect(onStop).not.toHaveBeenCalled();
  });
});
