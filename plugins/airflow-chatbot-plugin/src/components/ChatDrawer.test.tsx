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
import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

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
  readOnly: false,
  unauthenticated: false,
  writeToolsAvailable: true,
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

const rerenderDrawer = (
  rerender: (ui: React.ReactElement) => void,
  over: Partial<Parameters<typeof ChatDrawer>[0]> = {},
) =>
  rerender(
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

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("ChatDrawer mobile backdrop", () => {
  it("closes the drawer on a backdrop tap, but not for taps inside the panel", () => {
    // Below the md breakpoint the backdrop is the only large close target —
    // phones have no hardware Escape.
    const onClose = vi.fn();
    drawer({ onClose });

    fireEvent.click(screen.getByRole("dialog"));
    expect(onClose).not.toHaveBeenCalled();

    const backdrop = document.querySelector('[data-backdrop="true"]');
    expect(backdrop).not.toBeNull();
    fireEvent.click(backdrop as Element);

    expect(onClose).toHaveBeenCalledOnce();
  });
});

describe("ChatDrawer reset button", () => {
  it("asks for confirmation before clearing: one click arms, the second clears", () => {
    const onClear = vi.fn();
    drawer({ onClear });

    fireEvent.click(screen.getByLabelText("Clear conversation"));

    // The first click never clears — it only arms the confirmation.
    expect(onClear).not.toHaveBeenCalled();
    fireEvent.click(screen.getByLabelText("Confirm clear conversation"));

    expect(onClear).toHaveBeenCalledOnce();
    // Decided: the button goes back to its resting state.
    expect(screen.queryByLabelText("Confirm clear conversation")).toBeNull();
  });

  it("disarms itself when the confirmation is left hanging", () => {
    vi.useFakeTimers();
    const onClear = vi.fn();
    drawer({ onClear });

    fireEvent.click(screen.getByLabelText("Clear conversation"));
    act(() => {
      vi.advanceTimersByTime(5_100);
    });

    expect(screen.queryByLabelText("Confirm clear conversation")).toBeNull();
    expect(screen.getByLabelText("Clear conversation")).not.toBeNull();
    expect(onClear).not.toHaveBeenCalled();
  });

  it("is disabled when there is nothing to clear", () => {
    drawer({ messages: [] });

    expect(screen.getByLabelText("Clear conversation")).toHaveProperty("disabled", true);
  });

  it("is disabled while a reply is streaming", () => {
    drawer({ isLoading: true });

    expect(screen.getByLabelText("Clear conversation")).toHaveProperty("disabled", true);
  });

  it("sits grouped with the close button on the right of the header", () => {
    drawer();

    const clear = screen.getByLabelText("Clear conversation");
    const close = screen.getByLabelText("Close chat");
    // Same immediate parent: one right-aligned group, not scattered by
    // space-between across the header.
    expect(clear.parentElement).toBe(close.parentElement);
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

    // The textarea stays editable: the next question can be composed while
    // Airy answers. Only sending is gated.
    expect(screen.getByPlaceholderText(/Ask anything/u)).toHaveProperty("disabled", false);
  });

  it("lets a draft be typed, but not sent, while streaming", () => {
    const onSendMessage = vi.fn();
    drawer({ isLoading: true, onSendMessage });

    const textarea = screen.getByPlaceholderText(/Ask anything/u);
    fireEvent.change(textarea, { target: { value: "next question" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    expect(onSendMessage).not.toHaveBeenCalled();
    expect(textarea).toHaveProperty("value", "next question");
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

  it("grows the textarea with the draft and resets it when the draft is cleared", () => {
    drawer();

    const textarea = screen.getByPlaceholderText(/Ask anything/u) as HTMLTextAreaElement;
    fireEvent.change(textarea, { target: { value: "line1\nline2\nline3" } });
    // Grown to fit the draft (measured from scrollHeight, so not the resting "auto").
    expect(textarea.style.height).not.toBe("auto");

    fireEvent.change(textarea, { target: { value: "" } });
    // A clear — from any source, including the value prop — resets the height.
    expect(textarea.style.height).toBe("auto");
  });

  it("does not send on the Enter that confirms an IME composition", () => {
    const onSendMessage = vi.fn();
    drawer({ onSendMessage });

    const textarea = screen.getByPlaceholderText(/Ask anything/u);
    fireEvent.change(textarea, { target: { value: "日本語" } });
    fireEvent.keyDown(textarea, { isComposing: true, key: "Enter" });
    expect(onSendMessage).not.toHaveBeenCalled();

    fireEvent.keyDown(textarea, { key: "Enter" });
    expect(onSendMessage).toHaveBeenCalledWith("日本語");
  });
});

describe("ChatDrawer dialog semantics and focus", () => {
  it("names itself as a dialog", () => {
    drawer();

    const dialog = screen.getByRole("dialog");
    expect(dialog.getAttribute("aria-label")).toBe("Airy assistant");
  });

  it("moves focus into the input when it opens", () => {
    drawer();

    expect(document.activeElement).toBe(screen.getByPlaceholderText(/Ask anything/u));
  });

  it("gives focus back to the input when the turn ends", () => {
    const { rerender } = drawer({ isLoading: true });
    const textarea = screen.getByPlaceholderText(/Ask anything/u);
    (textarea as HTMLTextAreaElement).blur();
    expect(document.activeElement).not.toBe(textarea);

    rerenderDrawer(rerender, { isLoading: false });

    expect(document.activeElement).toBe(textarea);
  });

  it("closes on Escape, but not on the Escape that cancels an IME composition", () => {
    const onClose = vi.fn();
    drawer({ onClose });

    fireEvent.keyDown(document, { isComposing: true, key: "Escape" });
    expect(onClose).not.toHaveBeenCalled();

    fireEvent.keyDown(document, { key: "Escape" });
    expect(onClose).toHaveBeenCalledOnce();
  });

  it("wraps Tab inside the drawer while it covers the page", () => {
    vi.stubGlobal("matchMedia", vi.fn().mockReturnValue({ matches: true }));
    drawer();

    const dialog = screen.getByRole("dialog");
    expect(dialog.getAttribute("aria-modal")).toBe("true");
    const focusables = dialog.querySelectorAll<HTMLElement>("button, textarea, [tabindex]");
    const last = focusables[focusables.length - 1]!;
    last.focus();

    fireEvent.keyDown(document, { key: "Tab" });

    expect(document.activeElement).not.toBe(last);
    expect(dialog.contains(document.activeElement)).toBe(true);
  });
});

describe("ChatDrawer resize handle", () => {
  it("is a keyboard-operable separator", () => {
    drawer();

    const handle = screen.getByRole("separator");
    expect(handle.getAttribute("aria-orientation")).toBe("vertical");
    expect(handle.getAttribute("aria-valuenow")).toBe("420");
    expect(handle.getAttribute("tabindex")).toBe("0");

    fireEvent.keyDown(handle, { key: "ArrowLeft" });
    expect(handle.getAttribute("aria-valuenow")).toBe("444");

    fireEvent.keyDown(handle, { key: "ArrowRight" });
    expect(handle.getAttribute("aria-valuenow")).toBe("420");
  });
});

describe("ChatDrawer health handling", () => {
  const disconnected: HealthStatus = { ...health, llm: false, mcp: false, ok: false };

  it("routes a suggestion into the input instead of firing a doomed request", () => {
    const onSendMessage = vi.fn();
    drawer({ health: disconnected, messages: [], onSendMessage });

    fireEvent.click(screen.getByText("How do I create a Dag?"));

    expect(onSendMessage).not.toHaveBeenCalled();
    expect(screen.getByPlaceholderText(/Ask anything/u)).toHaveProperty(
      "value",
      "How do I create a Dag?",
    );
  });

  it("sends a suggestion directly while connected", () => {
    const onSendMessage = vi.fn();
    drawer({ messages: [], onSendMessage });

    fireEvent.click(screen.getByText("How do I create a Dag?"));

    expect(onSendMessage).toHaveBeenCalledWith("How do I create a Dag?");
  });

  it.each([
    [
      "the language model is missing",
      disconnected,
      /language model is not configured/u,
    ],
    ["the tools are unreachable", { ...health, mcp: false, ok: true }, /can't reach its Airflow tools/u],
    [
      "the session expired",
      { ...health, llm: false, ok: false, unauthenticated: true },
      /session has expired — sign in again/u,
    ],
  ])("explains why it is disconnected when %s", (_label, sick, expected) => {
    drawer({ health: sick });

    expect(screen.getByText(expected)).not.toBeNull();
  });

  it("offers a retry that rechecks the connection", () => {
    const onRecheckHealth = vi.fn();
    drawer({ health: disconnected, onRecheckHealth });

    fireEvent.click(screen.getByText("Retry connection"));

    expect(onRecheckHealth).toHaveBeenCalledOnce();
  });

  it("shows no callout while healthy or still loading", () => {
    drawer();
    expect(screen.queryByText("Retry connection")).toBeNull();

    drawer({ health: { ...disconnected, loading: true } });
    expect(screen.queryByText("Retry connection")).toBeNull();
  });
});

describe("ChatDrawer badges", () => {
  it("labels itself Experimental", () => {
    drawer();

    expect(screen.getByText("Experimental")).not.toBeNull();
  });

  it.each([
    ["an admin disabled writes", { ...health, readOnly: true }],
    ["the write tools are unavailable", { ...health, writeToolsAvailable: false }],
  ])("shows a Read-only badge when %s", (_label, sick) => {
    drawer({ health: sick });

    expect(screen.getByText("Read-only")).not.toBeNull();
  });

  it("shows no Read-only badge when writes are available", () => {
    drawer();

    expect(screen.queryByText("Read-only")).toBeNull();
  });
});
