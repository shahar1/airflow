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
import { describe, expect, it, vi } from "vitest";

import { ColorModeProvider } from "src/context/colorMode";

import { MessageList, splitActions } from "./MessageList";
import { localSystem } from "../theme";
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
    const partials = [
      "Fix it.\n\n[",
      "Fix it.\n\n[ACT",
      "Fix it.\n\n[ACTION: Apply the fi",
    ];

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
    const { actions, text } = splitActions(
      "Fix it.\n\n[ACTION: Apply the fix to sales_summary]",
    );

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
    // The empty placeholder must not render as a bare bubble with a timestamp;
    // only the user's message should carry one. (Matched loosely: the exact
    // format depends on the machine's locale and timezone.)
    expect(screen.queryAllByText(/^\d{1,2}:\d{2}/u)).toHaveLength(1);
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
            tools: [
              { durationMs: 1400, id: "c1", name: "diagnose_dag", startedAt: 0 },
            ],
          }),
        ]}
      />,
    );

    expect(screen.getByText("1.4s")).not.toBeNull();
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

    expect(screen.getByText("(dag_id=sales_summary)")).not.toBeNull();
  });

  it("hides a half-streamed action marker in the message being written", () => {
    // Pins the wiring, not just splitActions: the streaming bubble must be the
    // one that gets the flag, or a raw "[ACTION:" reaches the projector.
    show(
      <MessageList
        messages={[
          user("fix it"),
          assistant({ content: "Change line 42.\n\n[ACTION: Apply the fi" }),
        ]}
        isLoading
      />,
    );

    expect(screen.queryByText(/\[ACTION/u)).toBeNull();
    expect(screen.getByText("Change line 42.")).not.toBeNull();
  });

  it("keeps an unclosed bracket in a message that is not the streaming one", () => {
    show(
      <MessageList
        messages={[
          assistant({ content: "the list is [1, 2" }),
          user("and then?"),
        ]}
        isLoading
      />,
    );

    expect(screen.getByText(/the list is \[1, 2/u)).not.toBeNull();
  });

  it("does not offer an action chip until the stream has finished", () => {
    const onSuggestionClick = vi.fn();
    const messages = [
      user("fix it"),
      assistant({ content: "Done.\n\n[ACTION: Re-run sales_summary]" }),
    ];

    const { rerender } = show(
      <MessageList
        messages={messages}
        isLoading
        onSuggestionClick={onSuggestionClick}
      />,
    );
    expect(
      screen.queryByText("Re-run sales_summary"),
    ).toBeNull();

    rerender(
      <ChakraProvider value={localSystem}>
        <ColorModeProvider>
          <MessageList
            messages={messages}
            onSuggestionClick={onSuggestionClick}
          />
        </ColorModeProvider>
      </ChakraProvider>,
    );
    expect(screen.getByText("Re-run sales_summary")).not.toBeNull();
  });

  it("keeps timestamps out of the way until the bubble is hovered", () => {
    const { container } = show(
      <MessageList messages={[assistant({ content: "done" })]} />,
    );

    const timestamp = container.querySelector(".airy-timestamp");
    expect(timestamp).not.toBeNull();
    expect(timestamp?.textContent).toMatch(/\d{1,2}:\d{2}/u);
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
      <MessageList
        messages={[assistant({ content: "```python\nx = 1 - 2\n```" })]}
      />,
    );

    expect(container.querySelector("[data-diff]")).toBeNull();
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
