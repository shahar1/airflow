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
import { Box, chakra, Flex, Spinner, Text, VisuallyHidden, VStack } from "@chakra-ui/react";
import { FC, memo, ReactNode, useEffect, useRef, useState } from "react";
import Markdown, { Components } from "react-markdown";
import remarkGfm from "remark-gfm";

import { useColorMode } from "src/context/colorMode";

import { SparkleIcon } from "./icons/SparkleIcon";
import { ConfirmRequest, Message, ToolCall } from "./types";

/**
 * `[ACTION: Re-run sales_summary]` lines are stripped from the rendered text and
 * turned into chips that send their own label as the next user message.
 */
const ACTION_RE = /^[\s>*-]*\**\[ACTION:\s*(.+?)\]\**\s*$/gmu;

export const splitActions = (content: string, streaming = false): { actions: string[]; text: string } => {
  const actions = [...content.matchAll(ACTION_RE)].map((m) => m[1]?.trim() ?? "").filter(Boolean);
  const stripped = content.replace(ACTION_RE, "");
  // Mid-stream an action line arrives a few characters at a time, so hide the
  // trailing "[ACTION: Re-run sa" until its "]" lands.  Only while streaming:
  // a finished message may legitimately end in "values = [1, 2".
  const text = (streaming ? stripped.replace(/\[[^\]\n]*$/u, "") : stripped).trimEnd();
  return { actions, text };
};

interface MessageListProps {
  readonly messages: Message[];
  readonly isLoading?: boolean;
  /** Message receiving the SSE stream; a resumed confirmation may not be the last one. */
  readonly streamingId?: string | null;
  readonly onSuggestionClick?: (text: string) => void;
  readonly onConfirmClick?: (nonce: string, approved: boolean) => void;
  readonly onRetry?: (errorMessageId: string) => void;
}

/** Distance from the bottom still counted as "following the stream". */
const AT_BOTTOM_SLACK_PX = 80;

// Not anchored at the start: Airflow is often served below a URL prefix, so
// `/prod/dags/sales_summary/grid` has to match as readily as `/dags/…`.
const DAG_PATH_RE = /\/dags\/([^/?#]+)/u;

const GENERIC_PROMPTS = ["How do I create a Dag?", "Explain task dependencies", "Debug a failed task"];

/** The Dag the user is looking at, or nothing if the path does not name one. */
export const dagIdFromPath = (pathname: string): string | undefined => {
  const raw = DAG_PATH_RE.exec(pathname)?.[1];
  if (raw === undefined) return undefined;
  try {
    // A percent sign that is not an escape throws rather than yielding a lie.
    const decoded = decodeURIComponent(raw).trim();
    return decoded === "" ? undefined : decoded;
  } catch {
    return undefined;
  }
};

export const buildPrompts = (pathname: string): string[] => {
  const dagId = dagIdFromPath(pathname);
  return dagId === undefined
    ? GENERIC_PROMPTS
    : [`Why did ${dagId} fail?`, `Check ${dagId} for warnings`, `Summarise recent runs of ${dagId}`];
};

/**
 * Message list component displaying chat history.
 *
 * Follows the stream only while the user is already at the bottom; reading
 * earlier output mid-answer is otherwise impossible.
 */
export const MessageList: FC<MessageListProps> = ({
  isLoading = false,
  messages,
  onConfirmClick,
  onRetry,
  onSuggestionClick,
  streamingId,
}) => {
  const { colorMode } = useColorMode();
  const bottomRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  // A ref, not state: a render caused by a new token would otherwise measure
  // the freshly-grown scrollHeight and mistake it for the user's position.
  const atBottomRef = useRef(true);
  const [hasNewer, setHasNewer] = useState(false);

  const isDark = colorMode === "dark";

  const scrollToBottom = (behavior: ScrollBehavior) => {
    atBottomRef.current = true;
    setHasNewer(false);
    bottomRef.current?.scrollIntoView({ behavior });
  };

  const handleScroll = () => {
    const el = containerRef.current;
    if (!el) return;
    const atBottom = el.scrollTop + el.clientHeight >= el.scrollHeight - AT_BOTTOM_SLACK_PX;
    atBottomRef.current = atBottom;
    if (atBottom) setHasNewer(false);
  };

  useEffect(() => {
    if (atBottomRef.current) {
      bottomRef.current?.scrollIntoView({ behavior: isLoading ? "auto" : "smooth" });
      setHasNewer(false);
    } else {
      setHasNewer(true);
    }
  }, [messages, isLoading]);

  if (messages.length === 0 && !isLoading) {
    return (
      <Flex height="100%" align="center" justify="center" px={6} py={8}>
        <VStack gap={4} textAlign="center" color={isDark ? "gray.400" : "gray.600"}>
          <Box bg={isDark ? "gray.800" : "gray.200"} p={4} borderRadius="full">
            <SparkleIcon />
          </Box>
          <VStack gap={1}>
            <Text fontWeight="medium" fontSize="md" color={isDark ? "gray.200" : "gray.700"}>
              How can I help you today?
            </Text>
            <Text fontSize="sm" color={isDark ? "gray.500" : "gray.500"}>
              Ask me anything about Airflow
            </Text>
          </VStack>
          <VStack gap={2} mt={4} width="100%">
            {buildPrompts(globalThis.location.pathname).map((prompt) => (
              <SuggestionChip key={prompt} onClick={onSuggestionClick}>
                {prompt}
              </SuggestionChip>
            ))}
          </VStack>
        </VStack>
      </Flex>
    );
  }

  return (
    <Box position="relative" height="100%">
      <Box
        ref={containerRef}
        onScroll={handleScroll}
        height="100%"
        overflowY="auto"
        px={4}
        py={4}
        css={{
          "&::-webkit-scrollbar": {
            width: "6px",
          },
          "&::-webkit-scrollbar-thumb": {
            background: isDark ? "rgba(255,255,255,0.2)" : "rgba(0,0,0,0.2)",
            borderRadius: "3px",
          },
          "&::-webkit-scrollbar-track": {
            background: "transparent",
          },
        }}
      >
        <VStack gap={5} align="stretch">
          {messages.map((message, index) => {
            const streaming =
              isLoading && (streamingId == null ? index === messages.length - 1 : message.id === streamingId);
            return isBlank(message, streaming) ? undefined : (
              <MessageBubble
                key={message.id}
                message={message}
                isStreaming={streaming}
                // Chips do nothing while a stream is in flight, so don't offer them.
                onActionClick={isLoading ? undefined : onSuggestionClick}
                onConfirmClick={isLoading ? undefined : onConfirmClick}
                onRetry={isLoading || !canRetry(messages, index) ? undefined : onRetry}
              />
            );
          })}
          {isLoading && isBlank(messages[messages.length - 1], true) && <LoadingIndicator />}
          <div ref={bottomRef} />
        </VStack>
      </Box>
      {/* Mounted before its text ever changes: a region that appears together
          with its message is announced far less reliably. */}
      <VisuallyHidden role="status" aria-live="polite" aria-atomic="true">
        {buildToolAnnouncement(messages)}
      </VisuallyHidden>
      {hasNewer && (
        <Box
          as="button"
          onClick={() => scrollToBottom("smooth")}
          position="absolute"
          bottom={3}
          left="50%"
          transform="translateX(-50%)"
          px={3}
          py={1.5}
          borderRadius="full"
          borderWidth="1px"
          borderColor={isDark ? "gray.600" : "gray.300"}
          bg={isDark ? "gray.800" : "white"}
          color={isDark ? "gray.200" : "gray.700"}
          fontSize="sm"
          boxShadow="md"
          cursor="pointer"
          _focusVisible={{ outline: "2px solid", outlineColor: "brand.500", outlineOffset: "2px" }}
        >
          Jump to latest
        </Box>
      )}
    </Box>
  );
};

/**
 * The newest tool's own label — "Diagnosing Dag" or "Diagnosed Dag" is already
 * the start/finish state.  Prefixed so the announcement identifies itself as
 * Airy's status rather than repeating the visible row verbatim.
 */
export const buildToolAnnouncement = (messages: Message[]): string => {
  const tools = messages.flatMap((message) => message.tools ?? []);
  const latest = tools[tools.length - 1];
  return latest ? `Airy: ${buildToolLabel(latest)}` : "";
};

interface MessageBubbleProps {
  readonly message: Message;
  readonly isStreaming?: boolean;
  readonly onActionClick?: (text: string) => void;
  readonly onConfirmClick?: (nonce: string, approved: boolean) => void;
  readonly onRetry?: (errorMessageId: string) => void;
}

/**
 * Whether re-asking this failed turn is safe and possible.
 *
 * Not after a mutation: an approved write, or a write that got past the
 * proposal, may have changed the Dag already.  Recovery from that starts as a
 * fresh request and passes through a new approval, not a silent replay.
 */
export const canRetry = (messages: Message[], index: number): boolean => {
  const message = messages[index];
  if (message?.isError !== true || message.role !== "assistant") return false;
  if (message.confirms?.some((confirm) => confirm.resolution === "approved")) return false;
  if (message.tools?.some((tool) => WRITE_EFFECTS[tool.name] && !UNSTARTED.has(buildToolStatus(tool)))) {
    return false;
  }
  return messages.slice(0, index).some((earlier) => earlier.role === "user");
};

const DIFF_LANG_RE = /\blanguage-diff\b/u;

const diffLineKind = (line: string): "add" | "del" | undefined =>
  line.startsWith("+") ? "add" : line.startsWith("-") ? "del" : undefined;

const diffPalette = (isDark: boolean) => ({
  add: isDark ? "var(--chakra-colors-green-300)" : "var(--chakra-colors-green-600)",
  del: isDark ? "var(--chakra-colors-red-300)" : "var(--chakra-colors-red-600)",
});

/** One `-`/`+` prefixed line, coloured by kind; the unit both diff views share. */
const DiffLines: FC<{ readonly isDark: boolean; readonly lines: string[] }> = ({ isDark, lines }) => {
  const palette = diffPalette(isDark);
  return (
    <>
      {lines.map((line, index) => {
        const kind = diffLineKind(line);
        return (
          <span
            data-diff={kind}
            key={`${index}-${line}`}
            style={{ color: kind ? palette[kind] : undefined, display: "block" }}
          >
            {line || " "}
          </span>
        );
      })}
    </>
  );
};

/**
 * The fix diff is the money shot of the self-healing flow; flat grey text
 * undersells it. Colour +/- lines inside ```diff fences, leave every other
 * code block to the default renderer.
 */
const buildMarkdownComponents = (isDark: boolean): Components => ({
  code: ({ children, className, ...props }) => {
    if (!DIFF_LANG_RE.test(className ?? "")) {
      return (
        <code className={className} {...props}>
          {children}
        </code>
      );
    }
    return (
      <code className={className}>
        <DiffLines isDark={isDark} lines={String(children).replace(/\n$/u, "").split("\n")} />
      </code>
    );
  },
  pre: ({ children, ...props }) => <CodeBlock {...props}>{children}</CodeBlock>,
});

/**
 * An assistant turn with nothing visible is not worth showing. Judged on what
 * would actually render — whitespace-only or half-streamed action-marker
 * content counts as blank even though the raw string is non-empty.
 */
const isBlank = (message?: Message, streaming = false): boolean => {
  if (
    message === undefined ||
    message.role !== "assistant" ||
    message.tools?.length ||
    message.confirms?.length
  ) {
    return false;
  }
  const { actions, text } = splitActions(message.content, streaming);
  return text === "" && actions.length === 0;
};

const MessageBubble: FC<MessageBubbleProps> = memo(
  ({ isStreaming = false, message, onActionClick, onConfirmClick, onRetry }) => {
    const { colorMode } = useColorMode();
    const isDark = colorMode === "dark";
    const { actions, text } = splitActions(message.content, isStreaming);
    const tools = message.tools ?? [];
    const confirms = message.confirms ?? [];

    if (message.role === "user") {
      return (
        <Flex justify="flex-end">
          <Box
            bg={isDark ? "gray.700" : "gray.200"}
            color={isDark ? "gray.100" : "gray.900"}
            px={4}
            py={2.5}
            borderRadius="xl"
            borderBottomRightRadius="sm"
            maxWidth="80%"
            wordBreak="break-word"
            fontSize="md"
            lineHeight="tall"
            css={markdownCss(isDark)}
          >
            <Markdown components={buildMarkdownComponents(isDark)} remarkPlugins={[remarkGfm]}>
              {text}
            </Markdown>
          </Box>
        </Flex>
      );
    }

    // Assistant: prose sits directly on the panel; only tool calls and errors keep a card.
    return (
      <Flex align="flex-start">
        <Flex
          align="center"
          justify="center"
          bg={isDark ? "gray.700" : "gray.200"}
          color="brand.500"
          boxSize={8}
          borderRadius="full"
          mr={3}
          flexShrink={0}
        >
          <SparkleIcon />
        </Flex>
        <VStack align="flex-start" gap={2} flex="1" minWidth={0} maxWidth="720px">
          {tools.length > 0 && <ToolActivity tools={tools} />}
          {text ? (
            <Box
              role={message.isError ? "alert" : undefined}
              aria-live={message.isError ? "assertive" : undefined}
              width="100%"
              bg={message.isError ? (isDark ? "red.900" : "red.50") : undefined}
              color={message.isError ? (isDark ? "red.200" : "red.700") : isDark ? "gray.100" : "gray.800"}
              borderWidth={message.isError ? "1px" : undefined}
              borderColor={message.isError ? (isDark ? "red.700" : "red.200") : undefined}
              borderRadius={message.isError ? "lg" : undefined}
              px={message.isError ? 4 : 0}
              py={message.isError ? 2.5 : 0}
              wordBreak="break-word"
              fontSize="md"
              lineHeight="tall"
              css={markdownCss(isDark)}
            >
              <Markdown components={buildMarkdownComponents(isDark)} remarkPlugins={[remarkGfm]}>
                {text}
              </Markdown>
            </Box>
          ) : undefined}
          {groupConfirmsByNonce(confirms).map((group) => (
            <ConfirmPanel
              key={group[0]?.nonce}
              confirms={group}
              isStreaming={isStreaming}
              onDecide={onConfirmClick}
              tools={tools}
            />
          ))}
          {onRetry !== undefined && (
            <SuggestionChip onClick={() => onRetry(message.id)}>Retry</SuggestionChip>
          )}
          {onActionClick !== undefined &&
            actions.map((action, index) => (
              <SuggestionChip key={`${index}-${action}`} onClick={onActionClick}>
                {action}
              </SuggestionChip>
            ))}
        </VStack>
      </Flex>
    );
  },
);

/** One server suspension = one nonce = one decision; render it as one card. */
const groupConfirmsByNonce = (confirms: ConfirmRequest[]): ConfirmRequest[][] => {
  const groups = new Map<string, ConfirmRequest[]>();
  for (const confirm of confirms) {
    const group = groups.get(confirm.nonce) ?? [];
    group.push(confirm);
    groups.set(confirm.nonce, group);
  }
  return [...groups.values()];
};

interface ConfirmPanelProps {
  /** Every request sharing one nonce — the server decides them as one batch. */
  readonly confirms: ConfirmRequest[];
  /** The turn's calls; the matching one is what says whether the write landed. */
  readonly tools: ToolCall[];
  readonly isStreaming: boolean;
  readonly onDecide?: (nonce: string, approved: boolean) => void;
}

interface WriteEffect {
  /** Label of the primary button that authorizes it. */
  approve: string;
  /** The lasting change, in four or five words. */
  badge: string;
  /** Row label while the model has only asked for the call. */
  proposed: string;
  /** One sentence saying what happens on approval. */
  summary: (args: Record<string, unknown>) => string;
  title: string;
}

/**
 * What each write tool actually does, in the user's terms.
 *
 * `fix_dag_code` and `revert_dag_code` rewrite the Dag's source file on the
 * Airflow host and force a reparse — no pull request, no staging, no review
 * step after the Approve button.  The card has to say so.
 *
 * The confirmation carries a `dag_id`, never a path: a Dag id is not a file
 * name and one file can define several Dags, so nothing here may derive one.
 */
const WRITE_EFFECTS: Record<string, WriteEffect> = {
  fix_dag_code: {
    approve: "Apply to Dag source file",
    badge: "Writes the Dag file · reparses immediately",
    proposed: "Proposed code change",
    summary: (args) =>
      `Rewrites the source file containing ${describeDag(args)} on the Airflow host and reparses it straight away — there is no review step after this.`,
    title: "Proposed Dag source change",
  },
  rerun_dag: {
    approve: "Re-run Dag",
    badge: "Creates a Dag run",
    proposed: "Proposed Dag run",
    summary: (args) => `Creates one manual run of ${describeDag(args)} using the latest parsed code.`,
    title: "Trigger a new Dag run",
  },
  revert_dag_code: {
    approve: "Restore original source",
    badge: "Writes the Dag file · reparses immediately",
    proposed: "Proposed source restore",
    summary: (args) =>
      `Restores the one-time Airy backup for the source file containing ${describeDag(args)}, discards every Airy fix since that backup, and reparses immediately.`,
    title: "Restore original Dag source",
  },
  run_backfill: {
    approve: "Run backfill",
    badge: "Creates Dag runs",
    proposed: "Proposed backfill",
    // No count: the number of runs is not in the confirmation arguments.
    summary: (args) =>
      `Creates the reviewed backfill runs for ${describeDag(args)} from ${describeArg(args.from_date)} through ${describeArg(args.to_date)}.`,
    title: "Run the reviewed backfill",
  },
};

/** `rerun_dag --unpause` resumes the schedule for good; that is a different card. */
const UNPAUSE_EFFECT: WriteEffect = {
  approve: "Unpause and re-run",
  badge: "Unpauses Dag · resumes scheduled runs",
  proposed: "Proposed Dag run",
  summary: (args) =>
    `Unpauses ${describeDag(args)}, resumes its future scheduled runs, and creates one manual run now.`,
  title: "Re-run and resume this Dag's schedule",
};

/** A write tool this build has never heard of: warn, do not guess the effect. */
const buildUnknownEffect = (tool: string): WriteEffect => ({
  approve: "Approve",
  badge: "Makes a lasting change",
  proposed: "Proposed change",
  summary: () =>
    `Runs ${tool} against your Airflow. This build cannot describe its effect, so read the technical details before approving — the change will last.`,
  title: humanizeToolName(tool),
});

const describeDag = (args: Record<string, unknown>): string =>
  typeof args.dag_id === "string" && args.dag_id ? `\`${args.dag_id}\`` : "the named Dag";

const describeArg = (value: unknown): string =>
  typeof value === "string" && value ? `\`${value}\`` : "the requested date";

export const parseArgs = (args: unknown): Record<string, unknown> => {
  const parsed = typeof args === "string" ? tryParse(args) : args;
  return typeof parsed === "object" && parsed !== null ? (parsed as Record<string, unknown>) : {};
};

/**
 * What approving this one call would do.  Arguments that change the *lasting*
 * effect have to reach the card: `rerun_dag` with `unpause` resumes the Dag's
 * schedule, which "Trigger a new Dag run" does not say.
 */
export const buildWriteEffect = (confirm: ConfirmRequest): WriteEffect => {
  if (confirm.tool === "rerun_dag" && parseArgs(confirm.args).unpause === true) {
    return UNPAUSE_EFFECT;
  }
  return WRITE_EFFECTS[confirm.tool] ?? buildUnknownEffect(confirm.tool);
};

/**
 * Write tools the server refuses to run without an explicit go-ahead.
 *
 * While the decision is open this is an action card, not prose: what lastingly
 * changes, and — for a source patch — the diff itself, because that is the
 * thing being approved.  One nonce covers the whole batch, so a multi-tool
 * suspension is one card carrying every action's own effect: approving must
 * never silently authorize an unseen call.  Once decided the evidence has done
 * its job and a `ConfirmReceipt` takes its place.
 */
const ConfirmPanel: FC<ConfirmPanelProps> = ({ confirms, isStreaming, onDecide, tools }) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";
  // Clicking a decision unmounts the button; the receipt that replaces it takes
  // the focus, but only for the panel the click came from.
  const decidedHere = useRef(false);
  const [first] = confirms;
  if (first === undefined) return undefined;
  const batch = confirms.length > 1;
  const effect = buildWriteEffect(first);
  const states = confirms.map((confirm) => buildConfirmState(confirm, findTool(tools, confirm), isStreaming));

  if (first.resolution !== undefined) {
    return (
      <ConfirmReceipt
        confirms={confirms}
        focusOnMount={decidedHere.current}
        isDark={isDark}
        onDecide={onDecide}
        states={states}
      />
    );
  }

  const decide = (approved: boolean) => {
    decidedHere.current = true;
    onDecide?.(first.nonce, approved);
  };

  return (
    <Box
      width="100%"
      bg={isDark ? "gray.800" : "white"}
      borderWidth="1px"
      borderColor={isDark ? "whiteAlpha.200" : "blackAlpha.200"}
      borderLeftWidth="3px"
      borderLeftColor={isDark ? "orange.300" : "orange.500"}
      borderRadius="lg"
      px={3}
      py={2.5}
    >
      {/* Caution, not error: one amber cue plus the words, never colour alone. */}
      <Flex align="center" gap={1.5} mb={1.5} color={isDark ? "orange.300" : "orange.700"}>
        <ClockIcon />
        <Text fontSize="xs" fontWeight="medium">
          Approval required
        </Text>
      </Flex>
      {batch ? (
        <ConfirmHeading
          effect="Modifies your Airflow"
          isDark={isDark}
          title={`Approve ${confirms.length} actions`}
        />
      ) : undefined}
      {confirms.map((confirm) => (
        <ConfirmDetail key={confirm.callId} confirm={confirm} isDark={isDark} nested={batch} />
      ))}
      <Flex gap={2} mt={2} wrap="wrap">
        <ConfirmButton onClick={() => decide(true)} disabled={!onDecide} large primary>
          {batch ? "Approve all" : effect.approve}
        </ConfirmButton>
        <ConfirmButton onClick={() => decide(false)} disabled={!onDecide} large>
          {batch ? "Reject all" : "Reject"}
        </ConfirmButton>
      </Flex>
    </Box>
  );
};

/**
 * The call this confirmation suspended.  A `confirm_required` frame always
 * follows its own `tool` frame, so the row exists; a missing one is treated as
 * "cannot verify" rather than assumed successful.
 */
const findTool = (tools: ToolCall[], confirm: ConfirmRequest): ToolCall | undefined =>
  tools.find((tool) => tool.id === confirm.callId);

/** What became of one decided confirmation — the user's verdict is only half of it. */
export type ConfirmState = "applied" | "applying" | "failed" | "pending" | "rejected" | "unknown";

/**
 * The user's decision does not say whether the write landed; the matching tool
 * does.  `resolution` is set optimistically the moment `/confirm` is posted,
 * together with `outcomeUnknown: true` — so while the reply still streams the
 * card reports progress rather than flashing an unknown-outcome warning.
 */
export const buildConfirmState = (
  confirm: ConfirmRequest,
  tool: ToolCall | undefined,
  isStreaming: boolean,
): ConfirmState => {
  if (confirm.resolution === undefined) return "pending";
  if (confirm.resolution === "rejected") {
    // A rejection whose reply never arrived may not have reached the server at
    // all, which leaves the run suspended — the nonce is still the way to ask.
    return !isStreaming && confirm.outcomeUnknown === true ? "unknown" : "rejected";
  }
  if (isStreaming) return "applying";
  const status = tool === undefined ? undefined : buildToolStatus(tool);
  // A call that reported an error answered the question; anything else that is
  // not demonstrably done leaves it open.
  if (status === "failed") return "failed";
  if (confirm.outcomeUnknown === true || status !== "done") return "unknown";
  return "applied";
};

/** Worst-first: a group is only as settled as its least settled action. */
const STATE_RANK: Record<ConfirmState, number> = {
  applied: 5,
  applying: 3,
  failed: 1,
  pending: 0,
  rejected: 4,
  unknown: 2,
};

export const buildGroupState = (states: ConfirmState[]): ConfirmState =>
  states.reduce<ConfirmState>(
    (worst, state) => (STATE_RANK[state] < STATE_RANK[worst] ? state : worst),
    "applied",
  );

/** Which shared status icon stands for each decided state. */
const RECEIPT_ICONS: Record<ConfirmState, ToolStatus> = {
  applied: "done",
  applying: "running",
  failed: "failed",
  pending: "awaiting",
  rejected: "denied",
  unknown: "unsettled",
};

/**
 * One line naming the decision and what came of it.  Successful states borrow
 * the tool row's own verbs so the two can never contradict each other; a
 * rejection or a failure names the verdict instead, because repeating the row's
 * wording would print the same sentence twice, one under the other.
 */
export const buildReceiptLabel = (
  state: ConfirmState,
  states: ConfirmState[],
  confirm: ConfirmRequest,
): string => {
  const total = states.length;
  if (total > 1) {
    if (state === "applying") return `Approved · applying ${total} actions…`;
    if (state === "applied") return `${total} actions applied · approved by you`;
    if (state === "rejected") return `${total} actions rejected`;
    const applied = states.filter((each) => each === "applied").length;
    const open = total - applied;
    return `${applied} of ${total} applied · ${open} need${open === 1 ? "s" : ""} attention`;
  }
  const known = TOOL_LABELS[confirm.tool];
  const name = humanizeToolName(confirm.tool);
  switch (state) {
    case "applying":
      return `Approved · ${known?.running ?? name}…`;
    case "applied":
      return `${known?.done ?? name} · approved by you`;
    // The tool row above already reports the failure and the rejection in its
    // own words; the receipt's job is to record whose decision it was.
    case "failed":
      return "Approved change failed";
    case "rejected":
      return "Rejected by you";
    default:
      return `${confirm.resolution === "approved" ? "Approved" : "Rejected"} — outcome unknown`;
  }
};

interface ConfirmReceiptProps {
  readonly confirms: ConfirmRequest[];
  readonly focusOnMount: boolean;
  readonly isDark: boolean;
  readonly onDecide?: (nonce: string, approved: boolean) => void;
  readonly states: ConfirmState[];
}

/**
 * A decided approval, compressed to one row.
 *
 * The evidence mattered while the decision was open; afterwards the transcript
 * needs a truthful record, not a stack of orange boxes.  The reviewed proposal
 * stays one click away, but the decision itself cannot be taken again.
 */
const ConfirmReceipt: FC<ConfirmReceiptProps> = ({ confirms, focusOnMount, isDark, onDecide, states }) => {
  const [open, setOpen] = useState(false);
  // Typed as the element Chakra renders; `focus` comes from HTMLElement either way.
  const disclosureRef = useRef<HTMLDivElement>(null);
  const [first] = confirms;
  const state = buildGroupState(states);

  useEffect(() => {
    if (focusOnMount) disclosureRef.current?.focus();
    // Mount only: a later `applying` → `applied` update must not grab focus
    // back from wherever the user has moved on to.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  if (first === undefined) return undefined;
  const muted = isDark ? "gray.400" : "gray.600";

  return (
    <Box width="100%">
      <Flex align="center" gap={2} wrap="wrap">
        <Flex
          as="button"
          ref={disclosureRef}
          onClick={() => setOpen((was) => !was)}
          aria-expanded={open}
          align="center"
          gap={2}
          flex="1"
          minWidth="180px"
          textAlign="left"
          px={1.5}
          py={1}
          borderRadius="md"
          cursor="pointer"
          _hover={{ bg: isDark ? "whiteAlpha.100" : "blackAlpha.100" }}
          _focusVisible={{ outline: "2px solid", outlineColor: "brand.500", outlineOffset: "-2px" }}
        >
          <ToolStatusIcon status={RECEIPT_ICONS[state]} />
          <Text fontSize="sm" color={isDark ? "gray.200" : "gray.800"} flex="1" minWidth={0}>
            {buildReceiptLabel(state, states, first)}
          </Text>
          <Text as="span" fontSize="xs" color={muted} flexShrink={0}>
            {state === "applied" ? "View change" : "View proposal"}
          </Text>
          <Box color={muted}>
            <Chevron open={open} />
          </Box>
        </Flex>
        {state === "unknown" && (
          // The nonce still answers what happened: asking again replays the
          // recorded outcome rather than repeating the write.
          <ConfirmButton
            onClick={() => onDecide?.(first.nonce, first.resolution === "approved")}
            disabled={!onDecide}
          >
            Check outcome
          </ConfirmButton>
        )}
      </Flex>
      {open && (
        <Box px={1.5} pb={1}>
          {confirms.map((confirm) => (
            <ConfirmDetail
              key={confirm.callId}
              confirm={confirm}
              isDark={isDark}
              nested={confirms.length > 1}
            />
          ))}
        </Box>
      )}
    </Box>
  );
};

interface ConfirmHeadingProps {
  /** The lasting change this action makes, in four or five words. */
  readonly effect: string;
  readonly isDark: boolean;
  readonly title: string;
}

const ConfirmHeading: FC<ConfirmHeadingProps> = ({ effect, isDark, title }) => (
  <Box>
    <Text fontSize="sm" fontWeight="medium" color={isDark ? "gray.100" : "gray.900"}>
      {title}
    </Text>
    <Text fontSize="xs" color={isDark ? "gray.400" : "gray.600"}>
      {effect}
    </Text>
  </Box>
);

/**
 * `old` and `new` *are* the change, so show them as a diff rather than as
 * truncated arguments.  Built as elements, never as a Markdown fence: a
 * snippet containing backticks would break out of one.
 */
export const buildDiffLines = (args: Record<string, unknown>): string[] | undefined => {
  const { new: added, old: removed } = args;
  if (typeof removed !== "string" || typeof added !== "string") return undefined;
  return [...removed.split("\n").map((line) => `-${line}`), ...added.split("\n").map((line) => `+${line}`)];
};

interface ConfirmDetailProps {
  readonly confirm: ConfirmRequest;
  readonly isDark: boolean;
  /** One of several actions under a shared "Approve N actions" heading. */
  readonly nested: boolean;
}

/** One action's own effect: title, badge, sentence, and the diff being approved. */
const ConfirmDetail: FC<ConfirmDetailProps> = ({ confirm, isDark, nested }) => {
  const effect = buildWriteEffect(confirm);
  const args = parseArgs(confirm.args);
  const diff = confirm.tool === "fix_dag_code" ? buildDiffLines(args) : undefined;
  const diffUnavailable = confirm.tool === "fix_dag_code" && diff === undefined;
  const [diffOpen, setDiffOpen] = useState(true);
  // Nothing else says what this call would do when the diff cannot be built.
  const [detailsOpen, setDetailsOpen] = useState(diffUnavailable);

  return (
    <Box
      mt={nested ? 3 : 0}
      pl={nested ? 2 : 0}
      borderLeftWidth={nested ? "2px" : undefined}
      borderColor={isDark ? "whiteAlpha.300" : "blackAlpha.200"}
    >
      <ConfirmHeading effect={effect.badge} isDark={isDark} title={effect.title} />
      <Box fontSize="sm" color={isDark ? "gray.300" : "gray.700"} mt={1} css={markdownCss(isDark)}>
        <Markdown>{effect.summary(args)}</Markdown>
      </Box>
      {diffUnavailable && (
        <Text fontSize="xs" color={isDark ? "orange.300" : "orange.700"} mt={2}>
          Diff unavailable — read the technical details below before approving.
        </Text>
      )}
      {diff !== undefined && diffOpen && (
        <Box
          as="pre"
          fontSize="xs"
          fontFamily="mono"
          whiteSpace="pre-wrap"
          wordBreak="break-word"
          bg={isDark ? "blackAlpha.400" : "blackAlpha.50"}
          color={isDark ? "gray.300" : "gray.700"}
          borderRadius="md"
          px={2.5}
          py={2}
          mt={2}
          maxHeight="260px"
          overflowY="auto"
        >
          <DiffLines isDark={isDark} lines={diff} />
        </Box>
      )}
      {detailsOpen && (
        <Box
          as="pre"
          fontSize="xs"
          fontFamily="mono"
          whiteSpace="pre-wrap"
          wordBreak="break-word"
          bg={isDark ? "blackAlpha.400" : "blackAlpha.50"}
          color={isDark ? "gray.300" : "gray.700"}
          borderRadius="md"
          px={2.5}
          py={2}
          mt={2}
          maxHeight="240px"
          overflowY="auto"
        >
          {`${confirm.tool}: ${formatArgsFull(confirm.args) || "(no arguments)"}`}
        </Box>
      )}
      <Flex gap={2} mt={2} wrap="wrap">
        {diff !== undefined && (
          <ConfirmButton onClick={() => setDiffOpen((open) => !open)} aria-expanded={diffOpen}>
            {diffOpen ? "Hide diff" : "Show diff"}
          </ConfirmButton>
        )}
        <ConfirmButton onClick={() => setDetailsOpen((open) => !open)} aria-expanded={detailsOpen}>
          Technical details
        </ConfirmButton>
      </Flex>
    </Box>
  );
};

interface ConfirmButtonProps {
  readonly "aria-expanded"?: boolean;
  readonly children: string;
  readonly disabled?: boolean;
  /** A decision button: the one target worth the full 44px. */
  readonly large?: boolean;
  readonly onClick: () => void;
  readonly primary?: boolean;
}

const ConfirmButton: FC<ConfirmButtonProps> = ({
  "aria-expanded": ariaExpanded,
  children,
  disabled = false,
  large = false,
  onClick,
  primary = false,
}) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";

  return (
    <chakra.button
      aria-expanded={ariaExpanded}
      onClick={onClick}
      disabled={disabled}
      px={4}
      py={1.5}
      minHeight={large ? "44px" : undefined}
      bg={primary ? "brand.500" : isDark ? "gray.800" : "white"}
      color={primary ? "white" : isDark ? "gray.300" : "gray.700"}
      borderWidth="1px"
      borderColor={primary ? "brand.500" : isDark ? "gray.600" : "gray.300"}
      borderRadius="full"
      fontSize="sm"
      cursor={disabled ? "not-allowed" : "pointer"}
      opacity={disabled ? 0.6 : 1}
      _focusVisible={{ outline: "2px solid", outlineColor: "brand.500", outlineOffset: "2px" }}
    >
      {children}
    </chakra.button>
  );
};

interface CodeBlockProps {
  readonly children?: ReactNode;
}

/**
 * A fenced code block with hover-revealed Copy and Wrap/Scroll controls.
 * Wrap is offered only when a line actually overflows, so short snippets keep
 * their exact formatting and no controls clutter.
 */
const CodeBlock: FC<CodeBlockProps> = ({ children, ...props }) => {
  // react-markdown hands its AST node along; that must not reach the DOM.
  const preProps = { ...props };
  delete (preProps as Record<string, unknown>).node;
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";
  const preRef = useRef<HTMLPreElement>(null);
  const [wrapped, setWrapped] = useState(false);
  const [copyLabel, setCopyLabel] = useState("Copy");
  const [overflows, setOverflows] = useState(false);

  // Re-measure while unwrapped (content still streaming in, panel resized);
  // once wrapped there is nothing to measure and the toggle must stay.
  useEffect(() => {
    const el = preRef.current;
    if (!el || wrapped) return undefined;
    const measure = () => setOverflows(el.scrollWidth > el.clientWidth + 1);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, [children, wrapped]);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(preRef.current?.textContent ?? "");
      setCopyLabel("Copied");
    } catch {
      // Clipboard blocked (permissions, http): say so instead of pretending.
      setCopyLabel("Copy failed");
    }
    setTimeout(() => setCopyLabel("Copy"), 1500);
  };

  return (
    <Box
      position="relative"
      css={{
        "& .airy-code-controls": { opacity: 0, transition: "opacity 0.15s" },
        "&:hover .airy-code-controls, & .airy-code-controls:focus-within": { opacity: 1 },
      }}
    >
      <Flex className="airy-code-controls" position="absolute" top={1} right={1} gap={1}>
        {(overflows || wrapped) && (
          <CodeControl isDark={isDark} onClick={() => setWrapped((w) => !w)} pressed={wrapped}>
            {wrapped ? "Scroll" : "Wrap"}
          </CodeControl>
        )}
        <CodeControl isDark={isDark} onClick={copy}>
          {copyLabel}
        </CodeControl>
      </Flex>
      <pre
        ref={preRef}
        style={wrapped ? { whiteSpace: "pre-wrap", wordBreak: "break-word" } : undefined}
        {...preProps}
      >
        {children}
      </pre>
    </Box>
  );
};

interface CodeControlProps {
  readonly children: string;
  readonly isDark: boolean;
  readonly onClick: () => void;
  readonly pressed?: boolean;
}

const CodeControl: FC<CodeControlProps> = ({ children, isDark, onClick, pressed }) => (
  <Box
    as="button"
    onClick={onClick}
    aria-pressed={pressed}
    // Every other target already clears the 24px minimum; this one did not.
    minHeight="24px"
    minWidth="24px"
    px={1.5}
    py={0.5}
    fontSize="xs"
    borderRadius="sm"
    bg={isDark ? "whiteAlpha.200" : "blackAlpha.100"}
    color={isDark ? "gray.200" : "gray.700"}
    cursor="pointer"
    _hover={{ bg: isDark ? "whiteAlpha.300" : "blackAlpha.200" }}
    _focusVisible={{ opacity: 1, outline: "2px solid", outlineColor: "brand.500" }}
  >
    {children}
  </Box>
);

/** Human labels for the tools Airy ships with; anything else gets humanized. */
const TOOL_LABELS: Record<string, { running: string; done: string }> = {
  compare_dag_runs: { done: "Compared Dag runs", running: "Comparing Dag runs" },
  diagnose_dag: { done: "Diagnosed Dag", running: "Diagnosing Dag" },
  find_failure_clusters: { done: "Scanned for failure clusters", running: "Scanning for failure clusters" },
  fix_dag_code: { done: "Edited Dag code", running: "Editing Dag code" },
  get_blast_radius: { done: "Checked downstream impact", running: "Checking downstream impact" },
  plan_backfill: { done: "Planned backfill", running: "Planning backfill" },
  rerun_dag: { done: "Re-ran Dag", running: "Re-running Dag" },
  revert_dag_code: { done: "Reverted Dag code", running: "Reverting Dag code" },
  run_backfill: { done: "Ran backfill", running: "Running backfill" },
};

const humanizeToolName = (name: string): string => {
  const words = name.replaceAll("_", " ").replace(/\bdags?\b/gu, (m) => (m === "dags" ? "Dags" : "Dag"));
  return words.charAt(0).toUpperCase() + words.slice(1);
};

export const buildToolLabel = (tool: ToolCall): string => {
  const status = buildToolStatus(tool);
  if (status === "proposed") {
    return WRITE_EFFECTS[tool.name]?.proposed ?? "Proposed change";
  }
  if (status === "cancelled") {
    return `${humanizeToolName(tool.name)} cancelled`;
  }
  if (status === "unsettled") {
    return `${humanizeToolName(tool.name)} — outcome unknown`;
  }
  if (status === "failed") {
    return `${humanizeToolName(tool.name)} failed`;
  }
  if (status === "denied") {
    return `${humanizeToolName(tool.name)} rejected`;
  }
  const known = TOOL_LABELS[tool.name];
  if (known) {
    // Awaiting approval means the work has not happened yet — keep it in the
    // running form so the row never claims "Edited" before the user says yes.
    return status === "done" ? known.done : known.running;
  }
  return humanizeToolName(tool.name);
};

const CheckIcon: FC = () => (
  <svg fill="none" height="14" stroke="currentColor" strokeWidth={2.5} viewBox="0 0 24 24" width="14">
    <path d="M5 13l4 4L19 7" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

const WarnIcon: FC = () => (
  <svg fill="none" height="14" stroke="currentColor" strokeWidth={2} viewBox="0 0 24 24" width="14">
    <path
      d="M12 9v4m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

const Chevron: FC<{ readonly open: boolean }> = ({ open }) => (
  <Box as="span" flexShrink={0} transform={open ? "rotate(180deg)" : undefined} transition="transform 0.15s">
    <svg fill="none" height="12" stroke="currentColor" strokeWidth={2} viewBox="0 0 24 24" width="12">
      <path d="M6 9l6 6 6-6" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  </Box>
);

const ClockIcon: FC = () => (
  <svg fill="none" height="14" stroke="currentColor" strokeWidth={2} viewBox="0 0 24 24" width="14">
    <circle cx="12" cy="12" r="9" />
    <path d="M12 7v5l3 2" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

const CrossIcon: FC = () => (
  <svg fill="none" height="14" stroke="currentColor" strokeWidth={2.5} viewBox="0 0 24 24" width="14">
    <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

type ToolStatus =
  | "awaiting"
  | "cancelled"
  | "denied"
  | "done"
  | "failed"
  | "proposed"
  | "running"
  | "unsettled";

/** Statuses in which the tool has certainly not run. */
const UNSTARTED: ReadonlySet<ToolStatus> = new Set<ToolStatus>(["awaiting", "cancelled", "proposed"]);

/** Statuses still waiting on the user, so the row has to stay on screen. */
const PENDING_APPROVAL: ReadonlySet<ToolStatus> = new Set<ToolStatus>(["awaiting", "proposed"]);

export const buildToolStatus = (tool: ToolCall): ToolStatus => {
  // Suspended outranks proposed: the server is now holding this exact call.
  if (tool.awaitingConfirm === true) return "awaiting";
  if (tool.cancelled === true) return "cancelled";
  if (tool.unsettled === true) return "unsettled";
  if (tool.durationMs === undefined) return tool.proposed === true ? "proposed" : "running";
  if (tool.failed === true) return "failed";
  if (tool.denied === true) return "denied";
  return "done";
};

const ToolStatusIcon: FC<{ readonly status: ToolStatus }> = ({ status }) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";

  if (status === "running") {
    return <Spinner size="xs" color="brand.500" flexShrink={0} />;
  }
  const palette: Record<Exclude<ToolStatus, "running">, { color: string; icon: ReactNode }> = {
    awaiting: { color: isDark ? "orange.300" : "orange.600", icon: <ClockIcon /> },
    cancelled: { color: isDark ? "gray.400" : "gray.600", icon: <CrossIcon /> },
    denied: { color: isDark ? "gray.400" : "gray.600", icon: <CrossIcon /> },
    done: { color: isDark ? "green.300" : "green.600", icon: <CheckIcon /> },
    // Red is reserved for a reported failure; amber stays with the unverifiable.
    failed: { color: isDark ? "red.300" : "red.600", icon: <WarnIcon /> },
    proposed: { color: isDark ? "orange.300" : "orange.600", icon: <ClockIcon /> },
    unsettled: { color: isDark ? "orange.300" : "orange.600", icon: <WarnIcon /> },
  };
  return (
    <Box color={palette[status].color} flexShrink={0}>
      {palette[status].icon}
    </Box>
  );
};

/** Full pretty-printed arguments for the expanded view; empty when there are none. */
const formatArgsFull = (args: unknown): string => {
  const parsed = typeof args === "string" ? tryParse(args) : args;
  if (parsed === undefined || parsed === null) {
    // Malformed argument JSON is exactly what a failed call needs to show.
    return typeof args === "string" ? args : "";
  }
  try {
    return JSON.stringify(parsed, undefined, 2);
  } catch {
    return String(parsed);
  }
};

interface ToolRowProps {
  readonly tool: ToolCall;
}

/**
 * One tool call as a compact status row: icon, human label, muted technicals.
 * Finished rows expand to the full input and (clipped) output or error.
 */
const ToolRow: FC<ToolRowProps> = ({ tool }) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";
  const [open, setOpen] = useState(false);
  const status = buildToolStatus(tool);
  const failed = status === "failed";
  // Only a call that reported back has a duration worth quoting, or anything
  // to expand.
  const reported = status === "denied" || status === "done" || failed;
  const expandable = reported && (tool.result !== undefined || failed);
  const muted = isDark ? "gray.400" : "gray.600";

  return (
    <Box width="100%">
      <Flex
        as={expandable ? "button" : undefined}
        onClick={expandable ? () => setOpen((o) => !o) : undefined}
        aria-expanded={expandable ? open : undefined}
        align="flex-start"
        gap={2}
        width="100%"
        textAlign="left"
        px={1.5}
        py={1}
        borderRadius="md"
        cursor={expandable ? "pointer" : undefined}
        _hover={expandable ? { bg: isDark ? "whiteAlpha.100" : "blackAlpha.100" } : undefined}
        _focusVisible={{ outline: "2px solid", outlineColor: "brand.500", outlineOffset: "-2px" }}
      >
        <Box mt="3px">
          <ToolStatusIcon status={status} />
        </Box>
        <Box flex="1" minWidth={0}>
          <Flex align="baseline" gap={1.5}>
            <Text fontSize="sm" color={isDark ? "gray.200" : "gray.800"}>
              {buildToolLabel(tool)}
            </Text>
            {status === "awaiting" || status === "proposed" ? (
              <Text as="span" fontSize="xs" color={muted} flexShrink={0}>
                {status === "awaiting" ? "· awaiting approval" : "· approval required"}
              </Text>
            ) : (
              reported && (
                <Text as="span" fontSize="xs" color={muted} flexShrink={0}>
                  · {((tool.durationMs ?? 0) / 1000).toFixed(1)}s
                </Text>
              )
            )}
          </Flex>
          <Text fontSize="xs" fontFamily="mono" color={muted} truncate>
            {tool.name} {formatArgs(tool.args)}
          </Text>
        </Box>
        {expandable && (
          <Box color={muted} mt="4px">
            <Chevron open={open} />
          </Box>
        )}
      </Flex>
      {open && (
        <Box
          as="pre"
          fontSize="xs"
          fontFamily="mono"
          whiteSpace="pre-wrap"
          wordBreak="break-word"
          bg={isDark ? "blackAlpha.400" : "blackAlpha.50"}
          color={failed ? (isDark ? "orange.200" : "orange.700") : isDark ? "gray.300" : "gray.700"}
          borderRadius="md"
          px={2.5}
          py={2}
          mx={1.5}
          mb={1}
          maxHeight="240px"
          overflowY="auto"
        >
          {buildToolDetail(tool)}
        </Box>
      )}
    </Box>
  );
};

export const buildToolDetail = (tool: ToolCall): string => {
  const input = formatArgsFull(tool.args);
  const output = `${tool.failed === true ? "error" : "output"}: ${tool.result ?? "(no output)"}`;
  return input ? `input: ${input}\n\n${output}` : output;
};

interface ToolActivityProps {
  readonly tools: ToolCall[];
}

/**
 * The turn's tool calls in one subtle container.  Three or more collapse to a
 * "Used N tools" summary once they have all finished; the rows stay visible
 * while any of them is still running.
 */
const ToolActivity: FC<ToolActivityProps> = ({ tools }) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";
  const [userOpen, setUserOpen] = useState<boolean>();
  const statuses = tools.map(buildToolStatus);
  const anyRunning = statuses.includes("running");
  // A write the user has not approved is not work in progress: it keeps its row
  // on screen and blocks collapsing, but must not make the group spin.
  const anyPending = anyRunning || statuses.some((status) => PENDING_APPROVAL.has(status));
  const anyFailed = tools.some((tool) => tool.failed === true);
  const grouped = tools.length >= 3;
  // Live rows must stay visible: while anything runs the group cannot collapse.
  const open = anyPending || (userOpen ?? !grouped);

  return (
    <Box
      width="100%"
      bg={isDark ? "whiteAlpha.50" : "blackAlpha.50"}
      borderWidth="1px"
      borderColor={isDark ? "whiteAlpha.200" : "blackAlpha.200"}
      borderRadius="lg"
      px={1.5}
      py={1}
    >
      {grouped && (
        <Flex
          as={anyPending ? undefined : "button"}
          onClick={anyPending ? undefined : () => setUserOpen(!open)}
          aria-expanded={anyPending ? undefined : open}
          align="center"
          gap={2}
          width="100%"
          textAlign="left"
          px={1.5}
          py={1}
          borderRadius="md"
          cursor={anyPending ? undefined : "pointer"}
          _hover={anyPending ? undefined : { bg: isDark ? "whiteAlpha.100" : "blackAlpha.100" }}
          _focusVisible={{ outline: "2px solid", outlineColor: "brand.500", outlineOffset: "-2px" }}
        >
          <ToolStatusIcon
            status={anyRunning ? "running" : anyPending ? "awaiting" : anyFailed ? "failed" : "done"}
          />
          <Text fontSize="sm" color={isDark ? "gray.200" : "gray.800"} flex="1" minWidth={0}>
            {anyRunning
              ? `Using ${tools.length} tools…`
              : anyPending
                ? `${tools.length} tools · approval required`
                : `Used ${tools.length} tools`}
          </Text>
          {!anyPending && (
            <Box color={isDark ? "gray.400" : "gray.600"}>
              <Chevron open={open} />
            </Box>
          )}
        </Flex>
      )}
      {open && (
        <VStack align="stretch" gap={0}>
          {tools.map((tool) => (
            <ToolRow key={tool.id} tool={tool} />
          ))}
        </VStack>
      )}
    </Box>
  );
};

/** Compact one-line rendering of a tool's arguments. */
const formatArgs = (args: unknown): string => {
  const parsed = typeof args === "string" ? tryParse(args) : args;
  if (parsed === undefined || parsed === null || typeof parsed !== "object") {
    // Malformed argument JSON still deserves a glimpse in the compact line.
    return typeof args === "string" && args !== "" ? `(${truncate(args)})` : "";
  }
  const pairs = Object.entries(parsed as Record<string, unknown>).map(
    ([key, value]) => `${key}=${truncate(String(value))}`,
  );
  return `(${pairs.join(", ")})`;
};

const tryParse = (raw: string): unknown => {
  try {
    return JSON.parse(raw);
  } catch {
    return undefined;
  }
};

const truncate = (value: string): string => (value.length > 40 ? `${value.slice(0, 39)}…` : value);

/** Just enough spacing so rendered Markdown doesn't collapse inside the bubble. */
const markdownCss = (isDark: boolean) => ({
  "& :last-child": { marginBottom: 0 },
  "& a": { textDecoration: "underline" },
  "& code": {
    background: isDark ? "rgba(255,255,255,0.12)" : "rgba(0,0,0,0.08)",
    borderRadius: "4px",
    fontSize: "0.9em",
    padding: "0.1em 0.3em",
  },
  "& li": { listStyle: "revert" },
  "& li > ul, & li > ol": { paddingLeft: "0.9em" },
  "& p, & ul, & ol, & pre, & table": { marginBottom: "0.5em" },
  "& pre": {
    background: isDark ? "rgba(0,0,0,0.35)" : "rgba(0,0,0,0.06)",
    borderRadius: "6px",
    overflowX: "auto",
    padding: "0.6em",
  },
  "& pre code": { background: "transparent", padding: 0 },
  "& table": { display: "block", maxWidth: "100%", overflowX: "auto", width: "fit-content" },
  "& th, & td": {
    borderBottom: isDark ? "1px solid rgba(255,255,255,0.15)" : "1px solid rgba(0,0,0,0.1)",
    padding: "0.2em 0.5em",
    textAlign: "left",
  },
  "& ul, & ol": { paddingLeft: "1.25em" },
});

const LoadingIndicator: FC = () => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";

  return (
    <Flex align="center" gap={3}>
      <Flex
        align="center"
        justify="center"
        bg={isDark ? "gray.700" : "gray.200"}
        color="brand.500"
        boxSize={8}
        borderRadius="full"
        flexShrink={0}
      >
        <SparkleIcon />
      </Flex>
      <Spinner size="sm" color="brand.500" />
      <Text fontSize="sm" color="gray.500">
        Thinking...
      </Text>
    </Flex>
  );
};

interface SuggestionChipProps {
  readonly children: string;
  readonly onClick?: (text: string) => void;
}

const SuggestionChip: FC<SuggestionChipProps> = ({ children, onClick }) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";

  const handleClick = () => {
    onClick?.(children);
  };

  return (
    <Box
      as="button"
      onClick={handleClick}
      px={4}
      py={2}
      bg={isDark ? "gray.800" : "white"}
      borderWidth="1px"
      borderColor={isDark ? "gray.700" : "gray.300"}
      borderRadius="full"
      fontSize="sm"
      color={isDark ? "gray.300" : "gray.700"}
      cursor="pointer"
      transition="all 0.2s"
      _hover={{
        bg: isDark ? "gray.700" : "gray.100",
        borderColor: isDark ? "gray.600" : "gray.400",
        color: isDark ? "gray.100" : "gray.900",
      }}
      _focusVisible={{ outline: "2px solid", outlineColor: "brand.500", outlineOffset: "2px" }}
    >
      {children}
    </Box>
  );
};
