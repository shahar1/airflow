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
import { ReactElement } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ColorModeProvider } from "src/context/colorMode";

import { localSystem } from "../theme";
import { MessageList, splitActions } from "./MessageList";
import { Message } from "./types";

const show = (ui: ReactElement) =>
  render(
    <ChakraProvider value={localSystem}>
      <ColorModeProvider>{ui}</ColorModeProvider>
    </ChakraProvider>,
  );

const user = (content: string): Message => ({
  content,
  id: "u1",
  role: "user",
  timestamp: new Date(0),
});

const assistant = (partial: Partial<Message> = {}): Message => ({
  content: "",
  id: "a1",
  role: "assistant",
  timestamp: new Date(0),
  ...partial,
});

describe("splitActions", () => {
  it("hides a half-streamed action line instead of showing raw markers", () => {
    const partials = ["Fix it.\n\n[", "Fix it.\n\n[ACT", "Fix it.\n\n[ACTION: Apply the fi"];

    for (const partial of partials) {
      const { actions, text } = splitActions(partial, true);
      expect(actions).toEqual([]);
      expect(text).toBe("Fix it.");
    }
  });

  it("never truncates a finished message that ends in an open bracket", () => {
    // Airy is told to quote the offending line, and Python lines end in "[" a lot.
    const quoted = "The failing line is `values = [1, 2` in your Dag";

    expect(splitActions(quoted).text).toBe(quoted);
    expect(splitActions("Use op_kwargs[dag_id").text).toBe("Use op_kwargs[dag_id");
  });

  it("produces the chip once the line is complete", () => {
    const { actions, text } = splitActions("Fix it.\n\n[ACTION: Apply the fix to sales_summary]");

    expect(actions).toEqual(["Apply the fix to sales_summary"]);
    expect(text).toBe("Fix it.");
  });

  it("leaves ordinary brackets in the text alone", () => {
    expect(splitActions("see [the grid](http://x) for details").text).toBe(
      "see [the grid](http://x) for details",
    );
  });
});

describe("MessageList", () => {
  it("shows the thinking indicator instead of an empty bubble while waiting", () => {
    show(<MessageList messages={[user("why?"), assistant()]} isLoading />);

    expect(screen.getByText("Thinking...")).not.toBeNull();
  });

  it.each([
    ["whitespace", " \n"],
    ["a half-streamed action marker", "[ACTION: Re-ru"],
  ])("keeps the thinking indicator while only %s has streamed", (_label, content) => {
    show(<MessageList messages={[user("why?"), assistant({ content })]} isLoading />);

    expect(screen.getByText("Thinking...")).not.toBeNull();
  });

  it("renders nothing for a finished whitespace-only assistant message", () => {
    const { container } = show(<MessageList messages={[assistant({ content: " \n" })]} />);

    expect(container.textContent).toBe("");
  });

  it("drops the indicator once the first tool call arrives", () => {
    show(
      <MessageList
        messages={[
          user("why?"),
          assistant({
            tools: [{ id: "c1", name: "diagnose_dag", startedAt: 0 }],
          }),
        ]}
        isLoading
      />,
    );

    expect(screen.queryByText("Thinking...")).toBeNull();
    expect(screen.getByText("diagnose_dag")).not.toBeNull();
  });

  it("shows how long a finished tool call took", () => {
    show(
      <MessageList
        messages={[
          assistant({
            content: "done",
            tools: [{ durationMs: 1400, id: "c1", name: "diagnose_dag", startedAt: 0 }],
          }),
        ]}
      />,
    );

    expect(screen.getByText("· 1.4s")).not.toBeNull();
  });

  it("renders the tool arguments compactly", () => {
    show(
      <MessageList
        messages={[
          assistant({
            content: "done",
            tools: [
              {
                args: { dag_id: "sales_summary" },
                durationMs: 1,
                id: "c1",
                name: "diagnose_dag",
                startedAt: 0,
              },
            ],
          }),
        ]}
      />,
    );

    expect(screen.getByText("diagnose_dag (dag_id=sales_summary)")).not.toBeNull();
  });

  it("labels a finished tool call in plain words", () => {
    show(
      <MessageList
        messages={[
          assistant({
            content: "done",
            tools: [{ durationMs: 200, id: "c1", name: "diagnose_dag", startedAt: 0 }],
          }),
        ]}
      />,
    );

    expect(screen.getByText("Diagnosed Dag")).not.toBeNull();
  });

  it("keeps tool output collapsed until the row is expanded", () => {
    show(
      <MessageList
        messages={[
          assistant({
            content: "done",
            tools: [{ durationMs: 200, id: "c1", name: "diagnose_dag", result: "42 rows", startedAt: 0 }],
          }),
        ]}
      />,
    );

    expect(screen.queryByText(/42 rows/u)).toBeNull();

    fireEvent.click(screen.getByRole("button", { expanded: false }));

    expect(screen.getByText(/output: 42 rows/u)).not.toBeNull();
  });

  it("marks a failed tool call and reveals the error on expand", () => {
    show(
      <MessageList
        messages={[
          assistant({
            content: "done",
            tools: [
              { durationMs: 200, failed: true, id: "c1", name: "diagnose_dag", result: "boom", startedAt: 0 },
            ],
          }),
        ]}
      />,
    );

    expect(screen.getByText("Diagnose Dag failed")).not.toBeNull();

    fireEvent.click(screen.getByRole("button", { expanded: false }));

    expect(screen.getByText(/error: boom/u)).not.toBeNull();
  });

  it("collapses three finished tools behind a Used-N-tools summary", () => {
    const tools = ["diagnose_dag", "get_task_log", "compare_dag_runs"].map((name, index) => ({
      durationMs: 100,
      id: `c${index}`,
      name,
      startedAt: 0,
    }));
    show(<MessageList messages={[assistant({ content: "done", tools })]} />);

    expect(screen.queryByText("Diagnosed Dag")).toBeNull();

    fireEvent.click(screen.getByText("Used 3 tools"));

    expect(screen.getByText("Diagnosed Dag")).not.toBeNull();
  });

  it("keeps the tool rows visible while one of several is still running", () => {
    const tools = [
      { durationMs: 100, id: "c0", name: "diagnose_dag", startedAt: 0 },
      { durationMs: 100, id: "c1", name: "get_task_log", startedAt: 0 },
      { id: "c2", name: "compare_dag_runs", startedAt: 0 },
    ];
    show(<MessageList messages={[assistant({ tools })]} isLoading />);

    expect(screen.getByText("Using 3 tools…")).not.toBeNull();
    expect(screen.getByText("Diagnosed Dag")).not.toBeNull();
    expect(screen.getByText("Comparing Dag runs")).not.toBeNull();
  });

  it("shows a suspended write tool as awaiting approval, not done", () => {
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [{ args: {}, callId: "c1", nonce: "n1", tool: "fix_dag_code" }],
            content: "",
            tools: [{ awaitingConfirm: true, durationMs: 500, id: "c1", name: "fix_dag_code", startedAt: 0 }],
          }),
        ]}
      />,
    );

    expect(screen.getByText("Editing Dag code")).not.toBeNull();
    expect(screen.getByText("· awaiting approval")).not.toBeNull();
    expect(screen.queryByText("Edited Dag code")).toBeNull();
  });

  it.each([
    { expected: "Re-run and resume this Dag's schedule", unpause: true },
    { expected: "Trigger a new Dag run", unpause: false },
  ])("titles a $unpause unpause rerun card by its lasting effect", ({ expected, unpause }) => {
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [{ args: { dag_id: "a", unpause }, callId: "c1", nonce: "n1", tool: "rerun_dag" }],
            content: "",
          }),
        ]}
      />,
    );

    expect(screen.getByText(expected)).not.toBeNull();
  });

  it("offers a way to find out what happened when the outcome is unknown", () => {
    const onConfirmClick = vi.fn();
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              {
                args: { dag_id: "a" },
                callId: "c1",
                nonce: "n1",
                outcomeUnknown: true,
                resolution: "approved",
                tool: "rerun_dag",
              },
            ],
            content: "",
          }),
        ]}
        onConfirmClick={onConfirmClick}
      />,
    );

    expect(screen.getByText("Approved — outcome unknown")).not.toBeNull();
    fireEvent.click(screen.getByText("Check outcome"));

    // Re-asks with the same verdict, so the server replays instead of re-running.
    expect(onConfirmClick).toHaveBeenCalledWith("n1", true);
  });

  it("shows a rejected write tool as rejected, not done", () => {
    show(
      <MessageList
        messages={[
          assistant({
            content: "ok",
            tools: [
              { denied: true, durationMs: 500, id: "c1", name: "fix_dag_code", result: "no", startedAt: 0 },
            ],
          }),
        ]}
      />,
    );

    expect(screen.getByText("Fix Dag code rejected")).not.toBeNull();
    expect(screen.queryByText("Edited Dag code")).toBeNull();
  });

  it("renders one card deciding the whole batch when several writes share a nonce", () => {
    const onConfirmClick = vi.fn();
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              { args: { dag_id: "a" }, callId: "c1", nonce: "n1", tool: "fix_dag_code" },
              { args: { dag_id: "a" }, callId: "c2", nonce: "n1", tool: "rerun_dag" },
            ],
            content: "Two steps.",
          }),
        ]}
        onConfirmClick={onConfirmClick}
      />,
    );

    expect(screen.getByText("Approve 2 actions")).not.toBeNull();
    // Every lasting effect is spelled out before the one click authorizes them all.
    expect(screen.getByText("Proposed Dag source change")).not.toBeNull();
    expect(screen.getByText("Trigger a new Dag run")).not.toBeNull();
    expect(screen.getAllByText("Writes the Dag file · reparses immediately")).toHaveLength(1);
    expect(screen.getByText("Creates a Dag run")).not.toBeNull();
    expect(screen.getByText(/Creates one manual run of/u)).not.toBeNull();

    fireEvent.click(screen.getByText("Approve all"));

    expect(onConfirmClick).toHaveBeenCalledTimes(1);
    expect(onConfirmClick).toHaveBeenCalledWith("n1", true);
  });

  it("cannot collapse the tool group while a tool is still running", () => {
    const tools = [
      { durationMs: 100, id: "c0", name: "diagnose_dag", startedAt: 0 },
      { durationMs: 100, id: "c1", name: "get_task_log", startedAt: 0 },
      { id: "c2", name: "compare_dag_runs", startedAt: 0 },
    ];
    show(<MessageList messages={[assistant({ tools })]} isLoading />);

    fireEvent.click(screen.getByText("Using 3 tools…"));

    expect(screen.getByText("Diagnosed Dag")).not.toBeNull();
  });

  it("presents a write tool as an action card with explicit actions", () => {
    const onConfirmClick = vi.fn();
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              {
                args: { dag_id: "sales_summary", new: "b", old: "a" },
                callId: "c1",
                nonce: "n1",
                tool: "fix_dag_code",
              },
            ],
            content: "I can fix that.",
          }),
        ]}
        onConfirmClick={onConfirmClick}
      />,
    );

    expect(screen.getByText("Proposed Dag source change")).not.toBeNull();
    expect(screen.getByText("Writes the Dag file · reparses immediately")).not.toBeNull();
    expect(screen.getByText(/there is no review step after this/u)).not.toBeNull();

    // The diff is the thing being approved, so it is already on screen; the raw
    // arguments stay one click away.
    expect(screen.getByText("-a")).not.toBeNull();
    expect(screen.getByText("+b")).not.toBeNull();
    expect(screen.queryByText(/"old": "a"/u)).toBeNull();
    fireEvent.click(screen.getByText("Technical details"));
    expect(screen.getByText(/"old": "a"/u)).not.toBeNull();

    fireEvent.click(screen.getByText("Apply to Dag source file"));
    expect(onConfirmClick).toHaveBeenCalledWith("n1", true);
  });

  it("offers a copy control on fenced code blocks", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    show(<MessageList messages={[assistant({ content: "```python\nx = 1\n```" })]} />);

    fireEvent.click(screen.getByText("Copy"));

    await screen.findByText("Copied");
    expect(writeText).toHaveBeenCalledWith("x = 1\n");
  });

  it("hides a half-streamed action marker in the message being written", () => {
    // Pins the wiring, not just splitActions: the streaming bubble must be the
    // one that gets the flag, or a raw "[ACTION:" reaches the projector.
    show(
      <MessageList
        messages={[user("fix it"), assistant({ content: "Change line 42.\n\n[ACTION: Apply the fi" })]}
        isLoading
      />,
    );

    expect(screen.queryByText(/\[ACTION/u)).toBeNull();
    expect(screen.getByText("Change line 42.")).not.toBeNull();
  });

  it("keeps an unclosed bracket in a message that is not the streaming one", () => {
    show(
      <MessageList messages={[assistant({ content: "the list is [1, 2" }), user("and then?")]} isLoading />,
    );

    expect(screen.getByText(/the list is \[1, 2/u)).not.toBeNull();
  });

  it("does not offer an action chip until the stream has finished", () => {
    const onSuggestionClick = vi.fn();
    const messages = [user("fix it"), assistant({ content: "Done.\n\n[ACTION: Re-run sales_summary]" })];

    const { rerender } = show(
      <MessageList messages={messages} isLoading onSuggestionClick={onSuggestionClick} />,
    );
    expect(screen.queryByText("Re-run sales_summary")).toBeNull();

    rerender(
      <ChakraProvider value={localSystem}>
        <ColorModeProvider>
          <MessageList messages={messages} onSuggestionClick={onSuggestionClick} />
        </ColorModeProvider>
      </ChakraProvider>,
    );
    expect(screen.getByText("Re-run sales_summary")).not.toBeNull();
  });

  it("shows no timestamp, so nothing shifts on hover", () => {
    const { container } = show(
      <MessageList messages={[user("why?"), assistant({ content: "done" })]} />,
    );

    expect(container.textContent).not.toMatch(/\d{1,2}:\d{2}/u);
  });

  it("colours added and removed lines in a diff block", () => {
    const { container } = show(
      <MessageList
        messages={[
          assistant({
            content: "The fix:\n\n```diff\n+new line\n-old line\ncontext\n```",
          }),
        ]}
      />,
    );

    const added = container.querySelector('[data-diff="add"]');
    const removed = container.querySelector('[data-diff="del"]');
    expect(added?.textContent).toBe("+new line");
    expect(removed?.textContent).toBe("-old line");
    expect(added?.getAttribute("style")).toContain("color");
    // Context lines stay uncoloured.
    expect(container.querySelectorAll("[data-diff]")).toHaveLength(2);
  });

  it("leaves non-diff code blocks alone", () => {
    const { container } = show(
      <MessageList messages={[assistant({ content: "```python\nx = 1 - 2\n```" })]} />,
    );

    expect(container.querySelector("[data-diff]")).toBeNull();
  });

  it("shows a proposed write as awaiting a decision, never as work in progress", () => {
    const { container } = show(
      <MessageList
        messages={[
          assistant({
            tools: [{ id: "c1", name: "fix_dag_code", proposed: true, startedAt: 0 }],
          }),
        ]}
        isLoading
      />,
    );

    expect(screen.getByText("Proposed code change")).not.toBeNull();
    expect(screen.getByText("· approval required")).not.toBeNull();
    expect(screen.queryByText("Editing Dag code")).toBeNull();
    expect(screen.queryByText("Edited Dag code")).toBeNull();
    expect(screen.queryByText(/\ds$/u)).toBeNull();
    expect(container.querySelector(".chakra-spinner")).toBeNull();
  });

  it("does not repeat the approval suffix once the call is suspended", () => {
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [{ args: { dag_id: "a" }, callId: "c1", nonce: "n1", tool: "fix_dag_code" }],
            tools: [
              {
                awaitingConfirm: true,
                durationMs: 500,
                id: "c1",
                name: "fix_dag_code",
                proposed: true,
                startedAt: 0,
              },
            ],
          }),
        ]}
      />,
    );

    expect(screen.getByText("Editing Dag code")).not.toBeNull();
    expect(screen.getByText("· awaiting approval")).not.toBeNull();
    expect(screen.queryByText("· approval required")).toBeNull();
    expect(screen.queryByText("Proposed code change")).toBeNull();
  });

  it("keeps the group icon still while a proposed write waits, but the rows visible", () => {
    const tools = [
      { durationMs: 100, id: "c0", name: "diagnose_dag", startedAt: 0 },
      { durationMs: 100, id: "c1", name: "get_blast_radius", startedAt: 0 },
      { id: "c2", name: "fix_dag_code", proposed: true, startedAt: 0 },
    ];
    const { container } = show(<MessageList messages={[assistant({ tools })]} isLoading />);

    expect(screen.queryByText("Using 3 tools…")).toBeNull();
    expect(screen.getByText("3 tools · approval required")).not.toBeNull();
    expect(container.querySelector(".chakra-spinner")).toBeNull();
    // The waiting row cannot be collapsed out of sight.
    expect(screen.getByText("Proposed code change")).not.toBeNull();
  });

  it("shows a cancelled call as cancelled rather than as a success", () => {
    show(
      <MessageList
        messages={[
          assistant({
            content: "_Stopped._",
            tools: [{ cancelled: true, durationMs: 300, id: "c1", name: "diagnose_dag", startedAt: 0 }],
          }),
        ]}
      />,
    );

    expect(screen.getByText("Diagnose Dag cancelled")).not.toBeNull();
    expect(screen.queryByText("Diagnosed Dag")).toBeNull();
  });

  it("never derives a file name from the Dag id", () => {
    const { container } = show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              {
                args: { dag_id: "sales_summary", new: "x = 2", old: "x = 1" },
                callId: "c1",
                nonce: "n1",
                tool: "fix_dag_code",
              },
            ],
          }),
        ]}
      />,
    );

    expect(container.textContent).not.toContain("sales_summary.py");
    // The old truncated-argument line is gone from the card entirely.
    expect(container.textContent).not.toContain("old=");
  });

  it("says so instead of inventing a diff when the arguments are not a change", () => {
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              { args: "not json at all", callId: "c1", nonce: "n1", tool: "fix_dag_code" },
            ],
          }),
        ]}
      />,
    );

    expect(screen.getByText(/Diff unavailable/u)).not.toBeNull();
    // The technical details are already open, since nothing else describes the call.
    expect(screen.getByText(/not json at all/u)).not.toBeNull();
  });

  it.each([
    ["revert_dag_code", {}, "Restore original Dag source", "Restore original source"],
    ["run_backfill", { from_date: "2026-01-01", to_date: "2026-01-05" }, "Run the reviewed backfill", "Run backfill"],
  ])("states what %s does before it is approved", (tool, extra, title, approve) => {
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [{ args: { dag_id: "sales_summary", ...extra }, callId: "c1", nonce: "n1", tool }],
          }),
        ]}
      />,
    );

    expect(screen.getByText(title)).not.toBeNull();
    expect(screen.getByText(approve)).not.toBeNull();
  });

  it("warns rather than guessing when the write tool is unknown to this build", () => {
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [{ args: { dag_id: "a" }, callId: "c1", nonce: "n1", tool: "delete_everything" }],
          }),
        ]}
      />,
    );

    expect(screen.getByText("Makes a lasting change")).not.toBeNull();
    expect(screen.getByText(/cannot describe its effect/u)).not.toBeNull();
    expect(screen.getByText("Approve")).not.toBeNull();
  });

  describe("following the stream", () => {
    let scrollIntoView: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      scrollIntoView = vi.fn();
      Element.prototype.scrollIntoView = scrollIntoView;
    });

    /** Pretend the container is taller than its viewport and scrolled up. */
    const scrollUp = (container: HTMLElement) => {
      const scroller = container.firstElementChild?.firstElementChild as HTMLElement;
      Object.defineProperty(scroller, "clientHeight", { configurable: true, value: 200 });
      Object.defineProperty(scroller, "scrollHeight", { configurable: true, value: 2_000 });
      scroller.scrollTop = 0;
      fireEvent.scroll(scroller);
      return scroller;
    };

    it("stays put when the user has scrolled up, and offers a way back", () => {
      const { container, rerender } = show(
        <MessageList messages={[user("why?"), assistant({ content: "one" })]} isLoading />,
      );
      const scroller = scrollUp(container);
      scrollIntoView.mockClear();

      rerender(
        <ChakraProvider value={localSystem}>
          <ColorModeProvider>
            <MessageList
              messages={[user("why?"), assistant({ content: "one and then some more" })]}
              isLoading
            />
          </ColorModeProvider>
        </ChakraProvider>,
      );

      expect(scrollIntoView).not.toHaveBeenCalled();
      const pill = screen.getByText("Jump to latest");

      fireEvent.click(pill);
      expect(scrollIntoView).toHaveBeenCalled();
      expect(screen.queryByText("Jump to latest")).toBeNull();
      expect(scroller).not.toBeNull();
    });

    it("keeps following while the user is already at the bottom", () => {
      const { rerender } = show(
        <MessageList messages={[user("why?"), assistant({ content: "one" })]} isLoading />,
      );
      scrollIntoView.mockClear();

      rerender(
        <ChakraProvider value={localSystem}>
          <ColorModeProvider>
            <MessageList messages={[user("why?"), assistant({ content: "one two" })]} isLoading />
          </ColorModeProvider>
        </ChakraProvider>,
      );

      expect(scrollIntoView).toHaveBeenCalled();
      expect(screen.queryByText("Jump to latest")).toBeNull();
    });

    it("clears the pill when the user scrolls back down by hand", () => {
      const { container, rerender } = show(
        <MessageList messages={[user("why?"), assistant({ content: "one" })]} isLoading />,
      );
      const scroller = scrollUp(container);
      rerender(
        <ChakraProvider value={localSystem}>
          <ColorModeProvider>
            <MessageList messages={[user("why?"), assistant({ content: "one two" })]} isLoading />
          </ColorModeProvider>
        </ChakraProvider>,
      );
      expect(screen.getByText("Jump to latest")).not.toBeNull();

      scroller.scrollTop = 1_800;
      fireEvent.scroll(scroller);

      expect(screen.queryByText("Jump to latest")).toBeNull();
    });

    it("does not scroll when a tool row is expanded", () => {
      show(
        <MessageList
          messages={[
            assistant({
              content: "done",
              tools: [{ durationMs: 200, id: "c1", name: "diagnose_dag", result: "42 rows", startedAt: 0 }],
            }),
          ]}
        />,
      );
      scrollIntoView.mockClear();

      fireEvent.click(screen.getByRole("button", { expanded: false }));

      expect(screen.getByText(/42 rows/u)).not.toBeNull();
      expect(scrollIntoView).not.toHaveBeenCalled();
    });
  });

  describe("assistive announcements", () => {
    it("keeps one live region that reports the latest tool state", () => {
      const { container, rerender } = show(
        <MessageList
          messages={[assistant({ tools: [{ id: "c1", name: "diagnose_dag", startedAt: 0 }] })]}
          isLoading
        />,
      );

      const region = container.querySelector('[role="status"]');
      expect(region?.getAttribute("aria-live")).toBe("polite");
      expect(region?.getAttribute("aria-atomic")).toBe("true");
      expect(region?.textContent).toBe("Airy: Diagnosing Dag");

      rerender(
        <ChakraProvider value={localSystem}>
          <ColorModeProvider>
            <MessageList
              messages={[
                assistant({
                  content: "done",
                  tools: [{ durationMs: 200, id: "c1", name: "diagnose_dag", startedAt: 0 }],
                }),
              ]}
            />
          </ColorModeProvider>
        </ChakraProvider>,
      );

      // The same node, updated — not a newly mounted one.
      expect(container.querySelector('[role="status"]')).toBe(region);
      expect(region?.textContent).toBe("Airy: Diagnosed Dag");
      expect(container.querySelectorAll('[role="status"]')).toHaveLength(1);
    });

    it("raises an error response as an assertive alert", () => {
      const { container } = show(
        <MessageList messages={[assistant({ content: "**Error:** boom", isError: true })]} />,
      );

      const alert = container.querySelector('[role="alert"]');
      expect(alert?.getAttribute("aria-live")).toBe("assertive");
      expect(alert?.textContent).toContain("boom");
    });

    it("gives the code-block controls a 24px target", () => {
      show(<MessageList messages={[assistant({ content: "```python\nx = 1\n```" })]} />);

      const copy = screen.getByText("Copy");
      expect(getComputedStyle(copy).minHeight).toBe("24px");
      expect(getComputedStyle(copy).minWidth).toBe("24px");
    });
  });

  it("sends the action's own label when its chip is clicked", () => {
    const onSuggestionClick = vi.fn();
    show(
      <MessageList
        messages={[assistant({ content: "Done.\n\n[ACTION: Re-run sales_summary]" })]}
        onSuggestionClick={onSuggestionClick}
      />,
    );

    fireEvent.click(screen.getByText("Re-run sales_summary"));

    expect(onSuggestionClick).toHaveBeenCalledWith("Re-run sales_summary");
  });
});
