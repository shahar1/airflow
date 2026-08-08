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
import { ReactElement } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ColorModeProvider } from "src/context/colorMode";

import { localSystem } from "../theme";
import {
  buildConfirmState,
  buildGroupState,
  buildPrompts,
  buildReceiptLabel,
  buildSelectionCss,
  buildToolStatus,
  buildWriteEffect,
  canRetry,
  collectKnownDagIds,
  ConfirmState,
  MessageList,
  resolveMarkdownHref,
  splitActions,
} from "./MessageList";
import { frameProvenDagIds } from "../hooks/useChat";
import { ConfirmRequest, Message, ToolCall } from "./types";

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

  it.each(["action", "Action", "ACTION"])("matches a [%s: …] marker regardless of case", (marker) => {
    const { actions, text } = splitActions(`Here.\n\n[${marker}: Show me the log for summarize]`);

    expect(actions).toEqual(["Show me the log for summarize"]);
    expect(text).toBe("Here.");
  });
});

describe("buildConfirmState", () => {
  const confirm = (partial: Partial<ConfirmRequest> = {}): ConfirmRequest => ({
    callId: "c1",
    nonce: "n1",
    tool: "apply_dag_code_changes",
    ...partial,
  });
  const tool = (partial: Partial<ToolCall> = {}): ToolCall => ({
    durationMs: 500,
    id: "c1",
    name: "apply_dag_code_changes",
    startedAt: 0,
    ...partial,
  });

  it.each([
    ["undecided", confirm(), tool({ awaitingConfirm: true }), false, "pending"],
    ["undecided mid-stream", confirm(), tool({ awaitingConfirm: true }), true, "pending"],
    // `resolution` is set the moment /confirm is posted, together with an
    // optimistic `outcomeUnknown`; neither may preempt the running write.
    ["submitted", confirm({ outcomeUnknown: true, resolution: "approved" }), tool(), true, "applying"],
    ["completed", confirm({ resolution: "approved" }), tool(), false, "applied"],
    ["unverified", confirm({ outcomeUnknown: true, resolution: "approved" }), tool(), false, "unknown"],
    ["unsettled", confirm({ resolution: "approved" }), tool({ unsettled: true }), false, "unknown"],
    ["untraceable", confirm({ resolution: "approved" }), undefined, false, "unknown"],
    ["errored", confirm({ resolution: "approved" }), tool({ failed: true }), false, "failed"],
    // A reported error is an answer; the ambiguous flag does not outrank it.
    [
      "errored unverifiably",
      confirm({ outcomeUnknown: true, resolution: "approved" }),
      tool({ failed: true }),
      false,
      "failed",
    ],
    ["refused", confirm({ resolution: "rejected" }), tool({ denied: true }), false, "rejected"],
    [
      "refused unanswered",
      confirm({ outcomeUnknown: true, resolution: "rejected" }),
      undefined,
      false,
      "unknown",
    ],
    [
      "refused mid-stream",
      confirm({ outcomeUnknown: true, resolution: "rejected" }),
      undefined,
      true,
      "rejected",
    ],
  ])("reads a %s write as %s", (_label, request, call, streaming, expected) => {
    expect(buildConfirmState(request, call, streaming)).toBe(expected);
  });

  // Red says the write did not happen. A clear whose request went out and whose
  // response went wrong says precisely that it does not know, so it gets the
  // amber "may have landed" rendering the drawer already has, not the red one.
  // The server decides that and sends it on the frame; nothing here re-derives
  // it from a result string that storage clips.
  const unknownOutcome = '{"cleared": false, "mutation_outcome": "unknown", "http_status": null}';

  it("reads a write whose outcome is unknown as unsettled, not failed", () => {
    expect(buildToolStatus(tool({ result: unknownOutcome, unsettled: true }))).toBe("unsettled");
    expect(
      buildConfirmState(
        confirm({ resolution: "approved", tool: "apply_task_instance_clear" }),
        tool({ name: "apply_task_instance_clear", result: unknownOutcome, unsettled: true }),
        false,
      ),
    ).toBe("unknown");
  });

  it("never paints a clipped unknown-outcome result amber on its own", () => {
    // The old fallback read `mutation_outcome` out of the result text. A result
    // the server did NOT flag is a result whose row has no claim to amber.
    expect(buildToolStatus(tool({ failed: true, result: unknownOutcome }))).toBe("failed");
  });

  it("still reads a reported refusal as failed", () => {
    expect(
      buildToolStatus(tool({ failed: true, result: '{"cleared": false, "error": "no reviewed plan"}' })),
    ).toBe("failed");
  });

  it("never claims a write landed on the decision alone", () => {
    // The tool is still awaiting: approving is not applying.
    expect(
      buildConfirmState(confirm({ resolution: "approved" }), tool({ awaitingConfirm: true }), false),
    ).toBe("unknown");
  });
});

describe("buildGroupState", () => {
  it.each([
    [["applied", "applied"], "applied"],
    [["applied", "applying"], "applying"],
    [["applied", "unknown"], "unknown"],
    [["applied", "unknown", "failed"], "failed"],
    [["applying", "unknown"], "unknown"],
  ])("settles %s as %s", (states, expected) => {
    expect(buildGroupState(states as ConfirmState[])).toBe(expected);
  });

  it("counts what actually landed when a batch ends mixed", () => {
    const states: ConfirmState[] = ["applied", "failed", "unknown"];

    expect(
      buildReceiptLabel("failed", states, { callId: "c1", nonce: "n1", tool: "apply_dag_code_changes" }),
    ).toBe("1 of 3 applied · 2 need attention");
  });
});

describe("buildWriteEffect apply_task_instance_clear", () => {
  const clear = (args: Record<string, unknown>): ConfirmRequest => ({
    args,
    callId: "c1",
    nonce: "n1",
    tool: "apply_task_instance_clear",
  });
  const summarize = (args: Record<string, unknown>): string => buildWriteEffect(clear(args)).summary(args);
  const base = {
    dag_id: "dunmore_chargeback_filing",
    dag_run_id: "manual__1",
    task_ids: ["seed"],
  };

  // Airflow resolves an absent `run_on_latest_version` from the Dag's own
  // `rerun_with_latest_version`, then from `[core] rerun_with_latest_version`,
  // then falls back to false — so the omitted case is the version the run used
  // on any deployment that sets neither, which is the opposite of "latest".
  // The sidecar omits the key by default, putting every default clear here.
  it.each([
    ["absent", base],
    ["false", { ...base, run_on_latest_version: false }],
  ])("never claims the latest parsed code when run_on_latest_version is %s", (_label, args) => {
    expect(summarize(args)).not.toContain("latest parsed code");
  });

  it("names Airflow's own default rather than picking a side, when the key is absent", () => {
    const summary = summarize(base);

    expect(summary).toContain("whichever Dag version Airflow's own default selects");
    expect(summary).toContain("the version the run used");
    expect(summary).toContain("`[core] rerun_with_latest_version`");
  });

  it("says the run's own version when the caller chose false", () => {
    expect(summarize({ ...base, run_on_latest_version: false })).toContain(
      "it runs again on the version that run used",
    );
  });

  it("says the latest parsed code only when the caller chose true", () => {
    expect(summarize({ ...base, run_on_latest_version: true })).toContain(
      "it runs again on the latest parsed code",
    );
  });

  it("names every instance the clear touches instead of 'everything downstream of it'", () => {
    const summary = summarize({
      ...base,
      include_downstream: true,
      reviewed_instances: ["seed", "fan[0]", "fan[1]", "transmit", "notice"],
    });

    expect(summary).toContain("5 task instances");
    for (const instance of ["`seed`", "`fan[0]`", "`fan[1]`", "`transmit`", "`notice`"]) {
      expect(summary).toContain(instance);
    }
    expect(summary).not.toContain("everything downstream of it");
  });

  it("keeps the count exact when a wide fan-out is too long to name in full", () => {
    const summary = summarize({
      ...base,
      reviewed_instances: Array.from({ length: 26 }, (_, index) => `fan[${index}]`),
    });

    expect(summary).toContain("26 task instances");
    expect(summary).toContain("`fan[19]`");
    expect(summary).toContain("and 6 more");
    expect(summary).not.toContain("`fan[20]`");
  });

  it("falls back to the seed only when the reviewed set never arrived", () => {
    expect(summarize({ ...base, include_downstream: true })).toContain("everything downstream of it");
  });

  it("states the state rule the server will use when the card carries no flag", () => {
    // Absent is not "no behaviour": the tool's own default is only_failed=true.
    expect(summarize(base)).toContain("Only failed and upstream_failed");
  });

  it("says when the clear reaches instances that already succeeded", () => {
    expect(summarize({ ...base, only_failed: false })).toContain("including ones that already succeeded");
    expect(summarize({ ...base, only_failed: true })).toContain("Only failed and upstream_failed");
  });

  it("renders the reviewed instances on the approval card", () => {
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              {
                args: {
                  ...base,
                  include_downstream: true,
                  only_failed: false,
                  reviewed_instances: ["seed", "fan[0]", "transmit"],
                },
                callId: "c1",
                nonce: "n1",
                tool: "apply_task_instance_clear",
              },
            ],
            content: "",
          }),
        ]}
      />,
    );

    expect(screen.getByText(/fan\[0\]/u)).toBeTruthy();
    expect(screen.queryByText(/everything downstream of it/u)).toBeNull();
  });
});

describe("buildWriteEffect rerun_dag run options", () => {
  const rerun = (args: unknown): ConfirmRequest => ({
    args,
    callId: "c1",
    nonce: "n1",
    tool: "rerun_dag",
  });

  it("shows every conf entry the approved run would carry, one code line each", () => {
    const summary = buildWriteEffect(rerun({ dag_id: "incident_triage" })).summary({
      conf: { retries: 2, severity_threshold: "urgent" },
      dag_id: "incident_triage",
    });

    expect(summary).toContain('`severity_threshold: "urgent"`');
    expect(summary).toContain("`retries: 2`");
    // One entry per line, so the card reads as a compact key: value list.
    expect(summary.split("\n").slice(-2)).toEqual(["`retries: 2`  ", '`severity_threshold: "urgent"`']);
  });

  it("shows the run note being approved", () => {
    const summary = buildWriteEffect(rerun({ dag_id: "incident_triage" })).summary({
      dag_id: "incident_triage",
      note: "Checking urgent incidents",
    });

    expect(summary).toContain('`note: "Checking urgent incidents"`');
  });

  it("keeps the unpause card's conf as visible as the plain one's", () => {
    const summary = buildWriteEffect(rerun({ dag_id: "incident_triage", unpause: true })).summary({
      conf: { severity_threshold: "urgent" },
      dag_id: "incident_triage",
      unpause: true,
    });

    expect(summary).toContain("Unpauses");
    expect(summary).toContain('`severity_threshold: "urgent"`');
  });

  it.each([
    ["absent", { dag_id: "incident_triage" }],
    ["empty", { conf: {}, dag_id: "incident_triage" }],
  ])("keeps today's wording when conf is %s", (_label, args) => {
    expect(buildWriteEffect(rerun(args)).summary(args)).toBe(
      "Creates one manual run of `incident_triage` using the latest parsed code.",
    );
  });

  it("renders the conf pairs on the approval card", () => {
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              rerun({ conf: { severity_threshold: "urgent" }, dag_id: "incident_triage" }),
            ],
            content: "",
          }),
        ]}
      />,
    );

    expect(screen.getByText('severity_threshold: "urgent"')).not.toBeNull();
  });
});

describe("buildSelectionCss", () => {
  // White on the #017cee brand blue is 4.10:1; these pairs are 6.31:1 and 12.32:1.
  it.each([
    [true, "#005FB8", "#FFFFFF"],
    [false, "#B7DBFF", "#111827"],
  ])("gives dark=%s selections their own readable pair", (isDark, background, color) => {
    expect(buildSelectionCss(isDark)["&::selection, & *::selection"]).toEqual({
      background,
      color,
      textShadow: "none",
    });
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
            confirms: [{ args: {}, callId: "c1", nonce: "n1", tool: "apply_dag_code_changes" }],
            content: "",
            tools: [
              {
                awaitingConfirm: true,
                durationMs: 500,
                id: "c1",
                name: "apply_dag_code_changes",
                startedAt: 0,
              },
            ],
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
              {
                denied: true,
                durationMs: 500,
                id: "c1",
                name: "apply_dag_code_changes",
                result: "no",
                startedAt: 0,
              },
            ],
          }),
        ]}
      />,
    );

    expect(screen.getByText("Apply Dag code changes rejected")).not.toBeNull();
    expect(screen.queryByText("Edited Dag code")).toBeNull();
  });

  it("renders one card deciding the whole batch when several writes share a nonce", () => {
    const onConfirmClick = vi.fn();
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              { args: { dag_id: "a" }, callId: "c1", nonce: "n1", tool: "apply_dag_code_changes" },
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
                args: { changes: [{ new: "b", old: "a" }], dag_id: "sales_summary" },
                callId: "c1",
                nonce: "n1",
                tool: "apply_dag_code_changes",
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

  it("shows every hunk of an atomic multi-edit repair under one decision", () => {
    const onConfirmClick = vi.fn();
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              {
                args: {
                  changes: [
                    { new: '"column": "amount"', old: '"column": "gross"' },
                    { new: "task_ids='summarize'", old: "task_ids='summarise'" },
                  ],
                  dag_id: "sales_summary",
                },
                callId: "c1",
                nonce: "n1",
                tool: "apply_dag_code_changes",
              },
            ],
          }),
        ]}
        onConfirmClick={onConfirmClick}
      />,
    );

    expect(screen.getByText(/Applies 2 changes/u)).not.toBeNull();
    expect(screen.getByText('-"column": "gross"')).not.toBeNull();
    expect(screen.getByText('+"column": "amount"')).not.toBeNull();
    expect(screen.getByText("-task_ids='summarise'")).not.toBeNull();
    expect(screen.getByText("+task_ids='summarize'")).not.toBeNull();

    // One decision covers both edits — they land together or not at all.
    fireEvent.click(screen.getByText("Apply to Dag source file"));
    expect(onConfirmClick).toHaveBeenCalledTimes(1);
  });

  it("shows nothing rather than a partial diff when one hunk is unreadable", () => {
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              {
                args: { changes: [{ new: "b", old: "a" }, { old: "c" }], dag_id: "sales_summary" },
                callId: "c1",
                nonce: "n1",
                tool: "apply_dag_code_changes",
              },
            ],
          }),
        ]}
      />,
    );

    expect(screen.getByText(/Diff unavailable/u)).not.toBeNull();
    expect(screen.queryByText("-a")).toBeNull();
  });

  it("says exactly what a task clear does, and what it does not", () => {
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              {
                args: {
                  dag_id: "sales_summary",
                  dag_run_id: "manual__2026-08-02",
                  run_on_latest_version: true,
                  task_ids: ["report"],
                },
                callId: "c1",
                nonce: "n1",
                tool: "apply_task_instance_clear",
              },
            ],
          }),
        ]}
      />,
    );

    expect(screen.getByText("Clear a task instance in an existing run")).not.toBeNull();
    expect(screen.getByText("Re-runs an existing task · creates no Dag run")).not.toBeNull();
    const summary = screen.getByText(/Clears/u).textContent ?? "";

    expect(summary).toContain("report");
    expect(summary).toContain("manual__2026-08-02");
    expect(summary).toContain("latest parsed code");
    expect(summary).toContain("no new Dag run is created");
    // The clear takes the downstream with it — the card has to say which way.
    expect(summary).toContain("everything downstream of it");
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
    const { container } = show(<MessageList messages={[user("why?"), assistant({ content: "done" })]} />);

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
            tools: [{ id: "c1", name: "apply_dag_code_changes", proposed: true, startedAt: 0 }],
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
            confirms: [{ args: { dag_id: "a" }, callId: "c1", nonce: "n1", tool: "apply_dag_code_changes" }],
            tools: [
              {
                awaitingConfirm: true,
                durationMs: 500,
                id: "c1",
                name: "apply_dag_code_changes",
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
      { id: "c2", name: "apply_dag_code_changes", proposed: true, startedAt: 0 },
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

  it("does not claim an edit when an approved write never reported back", () => {
    show(
      <MessageList
        messages={[
          assistant({
            content: "**Error:** the sidecar went away",
            isError: true,
            tools: [
              { durationMs: 400, id: "c1", name: "apply_dag_code_changes", startedAt: 0, unsettled: true },
            ],
          }),
        ]}
      />,
    );

    expect(screen.getByText("Apply Dag code changes — outcome unknown")).not.toBeNull();
    expect(screen.queryByText("Edited Dag code")).toBeNull();
    // A duration would imply the call ran to completion.
    expect(screen.queryByText(/^· \d/u)).toBeNull();
  });

  it("never derives a file name from the Dag id", () => {
    const { container } = show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              {
                args: { changes: [{ new: "x = 2", old: "x = 1" }], dag_id: "sales_summary" },
                callId: "c1",
                nonce: "n1",
                tool: "apply_dag_code_changes",
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
              { args: "not json at all", callId: "c1", nonce: "n1", tool: "apply_dag_code_changes" },
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
    [
      "run_backfill",
      { from_date: "2026-01-01", to_date: "2026-01-05" },
      "Run the reviewed backfill",
      "Run backfill",
    ],
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
    let scrollTo: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      scrollIntoView = vi.fn();
      Element.prototype.scrollIntoView = scrollIntoView;
      scrollTo = vi.fn();
      Element.prototype.scrollTo = scrollTo;
    });

    it("scrolls the log itself, never a sentinel that drags scrollable ancestors", () => {
      // scrollIntoView walks every scrollable ancestor. The drawer wraps this
      // list in an overflow-clipped box that the status regions overflow by a
      // few pixels, so a sentinel scroll displaces that wrapper too — and with
      // no scrollbar to bring it back, the whole transcript sits above the
      // viewport and the drawer looks empty for the rest of the session.
      const { container, rerender } = show(
        <MessageList messages={[user("why?"), assistant({ content: "one" })]} isLoading />,
      );
      const scroller = container.querySelector('[role="log"]') as HTMLElement;
      Object.defineProperty(scroller, "scrollHeight", { configurable: true, value: 2_000 });
      const containerScrollTo = vi.fn();
      scroller.scrollTo = containerScrollTo;
      scrollIntoView.mockClear();

      rerender(
        <ChakraProvider value={localSystem}>
          <ColorModeProvider>
            <MessageList messages={[user("why?"), assistant({ content: "one two" })]} isLoading />
          </ColorModeProvider>
        </ChakraProvider>,
      );

      expect(scrollIntoView).not.toHaveBeenCalled();
      expect(containerScrollTo).toHaveBeenCalledWith({ behavior: "auto", top: 2_000 });
    });

    /** Pretend the container is taller than its viewport and scrolled up.
     * A real user's scroll always starts with a wheel/touch/key gesture —
     * that gesture is what tells the programmatic-scroll guard to stand down,
     * so the simulation must send it too. */
    const scrollUp = (container: HTMLElement) => {
      const scroller = container.firstElementChild?.firstElementChild as HTMLElement;
      Object.defineProperty(scroller, "clientHeight", { configurable: true, value: 200 });
      Object.defineProperty(scroller, "scrollHeight", { configurable: true, value: 2_000 });
      scroller.scrollTop = 0;
      fireEvent.wheel(scroller);
      fireEvent.scroll(scroller);
      return scroller;
    };

    it("stays put when the user has scrolled up, and offers a way back", () => {
      const { container, rerender } = show(
        <MessageList messages={[user("why?"), assistant({ content: "one" })]} isLoading />,
      );
      const scroller = scrollUp(container);
      scrollTo.mockClear();

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

      expect(scrollTo).not.toHaveBeenCalled();
      const pill = screen.getByText("Jump to latest");

      fireEvent.click(pill);
      expect(scrollTo).toHaveBeenCalled();
      expect(screen.queryByText("Jump to latest")).toBeNull();
      expect(scroller).not.toBeNull();
    });

    it("keeps following while the user is already at the bottom", () => {
      const { rerender } = show(
        <MessageList messages={[user("why?"), assistant({ content: "one" })]} isLoading />,
      );
      scrollTo.mockClear();

      rerender(
        <ChakraProvider value={localSystem}>
          <ColorModeProvider>
            <MessageList messages={[user("why?"), assistant({ content: "one two" })]} isLoading />
          </ColorModeProvider>
        </ChakraProvider>,
      );

      expect(scrollTo).toHaveBeenCalled();
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
      scrollTo.mockClear();

      fireEvent.click(screen.getByRole("button", { expanded: false }));

      expect(screen.getByText(/42 rows/u)).not.toBeNull();
      expect(scrollTo).not.toHaveBeenCalled();
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
      // Exactly two: this tool region plus the completion announcement region.
      expect(container.querySelectorAll('[role="status"]')).toHaveLength(2);
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

  describe("empty-state prompts", () => {
    it.each([
      ["/dags/sales_summary/grid", "sales_summary"],
      ["/prod/airflow/dags/sales_summary", "sales_summary"],
      ["/dags/sales%20summary/runs", "sales summary"],
    ])("offers %s the Dag's own prompts", (pathname, dagId) => {
      expect(buildPrompts(pathname)).toEqual([
        `Why did ${dagId} fail?`,
        `Check ${dagId} for warnings`,
        `Summarize recent runs of ${dagId}`,
      ]);
    });

    it.each([["/dags"], ["/dags/"], ["/home"], ["/dags/%E0%A4%A"], ["/dags/%20"]])(
      "falls back to the generic prompts for %s",
      (pathname) => {
        expect(buildPrompts(pathname)).toEqual([
          "How do I create a Dag?",
          "Explain task dependencies",
          "Debug a failed task",
        ]);
      },
    );

    it("renders the contextual prompts as ordinary chips", () => {
      const onSuggestionClick = vi.fn();
      globalThis.history.pushState({}, "", "/dags/sales_summary/grid");
      show(<MessageList messages={[]} onSuggestionClick={onSuggestionClick} />);

      fireEvent.click(screen.getByText("Why did sales_summary fail?"));

      expect(onSuggestionClick).toHaveBeenCalledWith("Why did sales_summary fail?");
      act(() => {
        globalThis.history.pushState({}, "", "/");
      });
    });

    it("resamples the prompts when the host SPA navigates", () => {
      // The chatbot is its own React root: the host router never re-renders it,
      // so route changes must be observed, not assumed.
      globalThis.history.pushState({}, "", "/home");
      show(<MessageList messages={[]} />);
      expect(screen.getByText("How do I create a Dag?")).not.toBeNull();

      act(() => {
        globalThis.history.pushState({}, "", "/dags/nav_dag/grid");
      });

      expect(screen.getByText("Why did nav_dag fail?")).not.toBeNull();
      expect(screen.queryByText("How do I create a Dag?")).toBeNull();
      act(() => {
        globalThis.history.pushState({}, "", "/");
      });
    });
  });

  describe("retrying a failed turn", () => {
    const failed = (partial: Partial<Message> = {}): Message[] => [
      user("why?"),
      assistant({ content: "**Error:** boom", id: "a1", isError: true, ...partial }),
    ];

    it("offers Retry beside a failed read-only answer", () => {
      const onRetry = vi.fn();
      show(<MessageList messages={failed()} onRetry={onRetry} />);

      fireEvent.click(screen.getByText("Retry"));

      // Identified by the failed message, not by "the last question asked".
      expect(onRetry).toHaveBeenCalledWith("a1");
    });

    it.each([
      [
        "an approved write",
        {
          confirms: [
            {
              args: {},
              callId: "c1",
              nonce: "n1",
              resolution: "approved" as const,
              tool: "apply_dag_code_changes",
            },
          ],
        },
      ],
      [
        "a write that got past the proposal",
        { tools: [{ durationMs: 10, id: "c1", name: "apply_dag_code_changes", startedAt: 0 }] },
      ],
    ])("suppresses Retry after %s", (_label, partial) => {
      show(<MessageList messages={failed(partial)} onRetry={vi.fn()} />);

      expect(screen.queryByText("Retry")).toBeNull();
    });

    it("still offers Retry for a write that never left the proposal", () => {
      show(
        <MessageList
          messages={failed({
            tools: [{ id: "c1", name: "apply_dag_code_changes", proposed: true, startedAt: 0 }],
          })}
          onRetry={vi.fn()}
        />,
      );

      expect(screen.getByText("Retry")).not.toBeNull();
    });

    it.each([
      ["a stream is active", { isLoading: true }],
      [
        "there is no question to re-ask",
        { messages: [assistant({ content: "**Error:** boom", isError: true })] },
      ],
    ])("suppresses Retry while %s", (_label, over) => {
      show(<MessageList messages={failed()} onRetry={vi.fn()} {...over} />);

      expect(screen.queryByText("Retry")).toBeNull();
    });

    it("offers nothing to retry on a successful answer", () => {
      expect(canRetry([user("why?"), assistant({ content: "fine" })], 1)).toBe(false);
    });
  });

  describe("decided approvals", () => {
    const decided = (
      confirm: Partial<ConfirmRequest>,
      tools: ToolCall[] = [],
      over: Partial<Message> = {},
    ) => [
      user("fix it"),
      assistant({
        confirms: [
          {
            args: { changes: [{ new: "x = 2", old: "x = 1" }], dag_id: "a" },
            callId: "c1",
            nonce: "n1",
            tool: "apply_dag_code_changes",
            ...confirm,
          },
        ],
        tools,
        ...over,
      }),
    ];

    const done = (over: Partial<ToolCall> = {}): ToolCall => ({
      durationMs: 500,
      id: "c1",
      name: "apply_dag_code_changes",
      startedAt: 0,
      ...over,
    });

    it("replaces the evidence card with a one-line receipt once the write lands", () => {
      show(<MessageList messages={decided({ resolution: "approved" }, [done()])} />);

      expect(screen.getByText("Edited Dag code · approved by you")).not.toBeNull();
      // The summary, the diff and the technical controls are all gone.
      expect(screen.queryByText("Proposed Dag source change")).toBeNull();
      expect(screen.queryByText("-x = 1")).toBeNull();
      expect(screen.queryByText("Technical details")).toBeNull();
      expect(screen.queryByText("Apply to Dag source file")).toBeNull();
    });

    it("gives back the exact reviewed diff on request, but never the decision", () => {
      show(<MessageList messages={decided({ resolution: "approved" }, [done()])} />);

      fireEvent.click(screen.getByText("View change"));

      expect(screen.getByText("-x = 1")).not.toBeNull();
      expect(screen.getByText("+x = 2")).not.toBeNull();
      expect(screen.queryByText("Apply to Dag source file")).toBeNull();
      expect(screen.queryByText("Reject")).toBeNull();
    });

    it.each([
      ["rejected", { resolution: "rejected" as const }, [done({ denied: true })], "Rejected by you"],
      ["failed", { resolution: "approved" as const }, [done({ failed: true })], "Approved change failed"],
      [
        "unsettled",
        { resolution: "approved" as const },
        [done({ unsettled: true })],
        "Approved — outcome unknown",
      ],
    ])("records a %s decision without claiming success", (_label, confirm, tools, expected) => {
      show(<MessageList messages={decided(confirm, tools)} />);

      expect(screen.getByText(expected)).not.toBeNull();
      expect(screen.queryByText("Edited Dag code · approved by you")).toBeNull();
      expect(screen.getByText("View proposal")).not.toBeNull();
    });

    it("reports progress rather than doubt while the approved write is still running", () => {
      show(
        <MessageList
          messages={decided({ outcomeUnknown: true, resolution: "approved" }, [
            { awaitingConfirm: true, durationMs: 10, id: "c1", name: "apply_dag_code_changes", startedAt: 0 },
          ])}
          streamingId="a1"
          isLoading
        />,
      );

      expect(screen.getByText("Approved · Editing Dag code…")).not.toBeNull();
      expect(screen.queryByText("Approved — outcome unknown")).toBeNull();
      expect(screen.queryByText("Check outcome")).toBeNull();
    });

    it("keeps a way to find out what happened when a rejection went unanswered", () => {
      const onConfirmClick = vi.fn();
      show(
        <MessageList
          messages={decided({ outcomeUnknown: true, resolution: "rejected" })}
          onConfirmClick={onConfirmClick}
        />,
      );

      expect(screen.getByText("Rejected — outcome unknown")).not.toBeNull();
      fireEvent.click(screen.getByText("Check outcome"));

      expect(onConfirmClick).toHaveBeenCalledWith("n1", false);
    });

    it("says how much of a batch landed when the actions disagree", () => {
      show(
        <MessageList
          messages={[
            assistant({
              confirms: [
                {
                  args: {},
                  callId: "c1",
                  nonce: "n1",
                  resolution: "approved",
                  tool: "apply_dag_code_changes",
                },
                { args: {}, callId: "c2", nonce: "n1", resolution: "approved", tool: "rerun_dag" },
              ],
              tools: [done(), { durationMs: 5, failed: true, id: "c2", name: "rerun_dag", startedAt: 0 }],
            }),
          ]}
        />,
      );

      expect(screen.getByText("1 of 2 applied · 1 needs attention")).not.toBeNull();
    });

    it("hands focus to the receipt that replaces the button the user pressed", () => {
      const messages = decided({}, [
        { awaitingConfirm: true, durationMs: 10, id: "c1", name: "apply_dag_code_changes", startedAt: 0 },
      ]);
      const { rerender } = show(<MessageList messages={messages} onConfirmClick={vi.fn()} />);

      fireEvent.click(screen.getByText("Apply to Dag source file"));
      rerender(
        <ChakraProvider value={localSystem}>
          <ColorModeProvider>
            <MessageList messages={decided({ resolution: "approved" }, [done()])} />
          </ColorModeProvider>
        </ChakraProvider>,
      );

      expect(document.activeElement?.textContent).toContain("Edited Dag code · approved by you");
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

describe("conversation log semantics", () => {
  it("exposes the transcript as a labelled log", () => {
    const { container } = show(<MessageList messages={[user("why?")]} />);

    const log = container.querySelector('[role="log"]');
    expect(log?.getAttribute("aria-label")).toBe("Conversation with Airy");
  });

  it("attributes each turn to its speaker for screen readers", () => {
    show(<MessageList messages={[user("why?"), assistant({ content: "a typo" })]} />);

    expect(screen.getByText("You said:")).not.toBeNull();
    expect(screen.getByText("Airy said:")).not.toBeNull();
  });

  it("announces politely when the answer finishes", () => {
    const messages = [user("why?"), assistant({ content: "a typo" })];
    const { rerender } = show(<MessageList messages={messages} isLoading />);
    expect(screen.queryByText("Airy finished responding")).toBeNull();

    rerender(
      <ChakraProvider value={localSystem}>
        <ColorModeProvider>
          <MessageList messages={messages} />
        </ColorModeProvider>
      </ChakraProvider>,
    );

    expect(screen.getByText("Airy finished responding")).not.toBeNull();
  });
});

describe("user messages", () => {
  it("renders the user's words as typed, never as markdown", () => {
    const { container } = show(<MessageList messages={[user("**not bold** and `not code`")]} />);

    expect(container.querySelector("strong")).toBeNull();
    expect(container.querySelector("code")).toBeNull();
    expect(screen.getByText(/\*\*not bold\*\* and `not code`/u)).not.toBeNull();
  });
});

describe("safe links", () => {
  it("opens an external link in a new tab with a hardened rel", () => {
    const { container } = show(
      <MessageList messages={[assistant({ content: "see [the docs](https://example.com/x)" })]} />,
    );

    const anchor = container.querySelector("a");
    expect(anchor?.getAttribute("href")).toBe("https://example.com/x");
    expect(anchor?.getAttribute("target")).toBe("_blank");
    expect(anchor?.getAttribute("rel")).toBe("noopener noreferrer");
  });

  it("keeps a same-origin path link in this tab", () => {
    const { container } = show(
      <MessageList messages={[assistant({ content: "open [the grid](/dags/sales_summary/grid)" })]} />,
    );

    const anchor = container.querySelector("a");
    expect(anchor?.getAttribute("href")).toBe("/dags/sales_summary/grid");
    expect(anchor?.getAttribute("target")).toBeNull();
  });

  const origin = "http://airflow.example";

  it.each([
    ["a path under a prefixed deployment", "/dags/x", "/prod/dags/x", false],
    ["an already-prefixed path", "/prod/dags/x", "/prod/dags/x", false],
    ["a cross-origin URL", "https://evil.example/x", "https://evil.example/x", true],
    ["a same-origin absolute URL", `${origin}/dags/x`, `${origin}/dags/x`, false],
  ])("resolves %s", (_label, href, expected, external) => {
    expect(resolveMarkdownHref(href, "/prod", origin)).toEqual({ external, href: expected });
  });

  it("leaves fragments and empty hrefs alone", () => {
    expect(resolveMarkdownHref("#section", "/prod", origin)).toEqual({
      external: false,
      href: "#section",
    });
    expect(resolveMarkdownHref(undefined, "/prod", origin)).toBeUndefined();
  });
});

describe("entity links", () => {
  it("collects Dag ids only from tool args, confirm cards, and server frames", () => {
    frameProvenDagIds.add("from_frame");
    const messages = [
      assistant({
        confirms: [{ args: { dag_id: "from_confirm" }, callId: "c2", nonce: "n1", tool: "rerun_dag" }],
        content: "text mentions `from_prose` only",
        tools: [
          { args: { dag_id: "from_tool" }, durationMs: 1, id: "c1", name: "diagnose_dag", startedAt: 0 },
        ],
      }),
    ];

    const ids = collectKnownDagIds(messages);

    expect(ids.has("from_tool")).toBe(true);
    expect(ids.has("from_confirm")).toBe(true);
    expect(ids.has("from_frame")).toBe(true);
    expect(ids.has("from_prose")).toBe(false);
    frameProvenDagIds.delete("from_frame");
  });

  it("links a backticked Dag id the transcript has proven to exist", () => {
    const { container } = show(
      <MessageList
        messages={[
          assistant({
            content: "The Dag `linkify_me` failed twice.",
            tools: [
              { args: { dag_id: "linkify_me" }, durationMs: 1, id: "c1", name: "diagnose_dag", startedAt: 0 },
            ],
          }),
        ]}
      />,
    );

    const link = container.querySelector('a[data-dag-link="linkify_me"]');
    expect(link?.getAttribute("href")).toBe("/dags/linkify_me");
    expect(link?.querySelector("code")?.textContent).toBe("linkify_me");
  });

  it("never linkifies an id the transcript cannot prove", () => {
    const { container } = show(
      <MessageList messages={[assistant({ content: "Maybe check `some_guess` too." })]} />,
    );

    expect(container.querySelector("a[data-dag-link]")).toBeNull();
    expect(screen.getByText("some_guess")).not.toBeNull();
  });
});

describe("per-message copy", () => {
  it("copies the whole answer and reports honestly", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    show(<MessageList messages={[assistant({ content: "the diagnosis" })]} />);

    fireEvent.click(screen.getByLabelText("Copy message"));

    await screen.findByText("Copied");
    expect(writeText).toHaveBeenCalledWith("the diagnosis");
  });

  it("says Copy failed when the clipboard is blocked", async () => {
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText: vi.fn().mockRejectedValue(new Error("denied")) },
    });
    show(<MessageList messages={[assistant({ content: "the diagnosis" })]} />);

    fireEvent.click(screen.getByLabelText("Copy message"));

    await screen.findByText("Copy failed");
  });

  it("offers no copy on user bubbles or while the answer still streams", () => {
    show(
      <MessageList
        messages={[user("why?"), assistant({ content: "still writing" })]}
        isLoading
        streamingId="a1"
      />,
    );

    expect(screen.queryByLabelText("Copy message")).toBeNull();
  });
});

describe("long code blocks", () => {
  it("collapses a tall block and expands it on request", () => {
    Object.defineProperty(HTMLElement.prototype, "scrollHeight", {
      configurable: true,
      get: () => 1_000,
    });
    try {
      const { container } = show(
        <MessageList messages={[assistant({ content: "```text\nlots of log lines\n```" })]} />,
      );

      const pre = container.querySelector("pre");
      expect(pre?.getAttribute("style")).toContain("max-height: 320px");
      const toggle = screen.getByText("Show more");

      fireEvent.click(toggle);

      expect(screen.getByText("Show less")).not.toBeNull();
      expect(container.querySelector("pre")?.getAttribute("style") ?? "").not.toContain("max-height");
    } finally {
      delete (HTMLElement.prototype as unknown as Record<string, unknown>).scrollHeight;
    }
  });

  it("offers no collapse control on a short block", () => {
    show(<MessageList messages={[assistant({ content: "```python\nx = 1\n```" })]} />);

    expect(screen.queryByText("Show more")).toBeNull();
  });
});

describe("typed error frames", () => {
  it("suppresses Retry when the server says retrying cannot help", () => {
    show(
      <MessageList
        messages={[
          user("why?"),
          assistant({ content: "**Error:** bad key", id: "a1", isError: true, retryable: false }),
        ]}
        onRetry={vi.fn()}
      />,
    );

    expect(screen.queryByText("Retry")).toBeNull();
  });

  it("keeps Retry when the server says the error is transient", () => {
    show(
      <MessageList
        messages={[
          user("why?"),
          assistant({ content: "**Error:** rate limited", id: "a1", isError: true, retryable: true }),
        ]}
        onRetry={vi.fn()}
      />,
    );

    expect(screen.getByText("Retry")).not.toBeNull();
  });
});

describe("grouped approvals", () => {
  it("says that one decision covers the whole batch", () => {
    show(
      <MessageList
        messages={[
          assistant({
            confirms: [
              { args: { dag_id: "a" }, callId: "c1", nonce: "n1", tool: "apply_dag_code_changes" },
              { args: { dag_id: "a" }, callId: "c2", nonce: "n1", tool: "rerun_dag" },
            ],
          }),
        ]}
      />,
    );

    expect(screen.getByText(/one decision covers all 2 actions/u)).not.toBeNull();
  });
});
