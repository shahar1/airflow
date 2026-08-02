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
import { Box, chakra, Flex, Spinner, Text, VStack } from "@chakra-ui/react";
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
}

/**
 * Message list component displaying chat history.
 * Automatically scrolls to bottom on new messages.
 */
export const MessageList: FC<MessageListProps> = ({
  isLoading = false,
  messages,
  onConfirmClick,
  onSuggestionClick,
  streamingId,
}) => {
  const { colorMode } = useColorMode();
  const bottomRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const isDark = colorMode === "dark";

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    bottomRef.current?.scrollIntoView({
      behavior: isLoading ? "auto" : "smooth",
    });
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
            <SuggestionChip onClick={onSuggestionClick}>How do I create a Dag?</SuggestionChip>
            <SuggestionChip onClick={onSuggestionClick}>Explain task dependencies</SuggestionChip>
            <SuggestionChip onClick={onSuggestionClick}>Debug a failed task</SuggestionChip>
          </VStack>
        </VStack>
      </Flex>
    );
  }

  return (
    <Box
      ref={containerRef}
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
            />
          );
        })}
        {isLoading && isBlank(messages[messages.length - 1], true) && <LoadingIndicator />}
        <div ref={bottomRef} />
      </VStack>
    </Box>
  );
};

interface MessageBubbleProps {
  readonly message: Message;
  readonly isStreaming?: boolean;
  readonly onActionClick?: (text: string) => void;
  readonly onConfirmClick?: (nonce: string, approved: boolean) => void;
}

const DIFF_LANG_RE = /\blanguage-diff\b/u;

const diffLineKind = (line: string): "add" | "del" | undefined =>
  line.startsWith("+") ? "add" : line.startsWith("-") ? "del" : undefined;

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
    const palette = {
      add: isDark ? "var(--chakra-colors-green-300)" : "var(--chakra-colors-green-600)",
      del: isDark ? "var(--chakra-colors-red-300)" : "var(--chakra-colors-red-600)",
    };
    return (
      <code className={className}>
        {String(children)
          .replace(/\n$/u, "")
          .split("\n")
          .map((line, index) => {
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

// Timestamps are projector noise; keep them one hover away.
const timestampCss = {
  "& .airy-timestamp": { opacity: 0, transition: "opacity 0.15s" },
  "&:hover .airy-timestamp": { opacity: 1 },
};

const MessageBubble: FC<MessageBubbleProps> = memo(
  ({ isStreaming = false, message, onActionClick, onConfirmClick }) => {
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
            css={{ ...markdownCss(isDark), ...timestampCss }}
          >
            <Markdown components={buildMarkdownComponents(isDark)} remarkPlugins={[remarkGfm]}>
              {text}
            </Markdown>
            <Text
              className="airy-timestamp"
              fontSize="xs"
              color={isDark ? "gray.300" : "gray.600"}
              mt={1}
              textAlign="right"
            >
              {formatTime(message.timestamp)}
            </Text>
          </Box>
        </Flex>
      );
    }

    // Assistant: prose sits directly on the panel; only tool calls and errors keep a card.
    return (
      <Flex align="flex-start" css={timestampCss}>
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
            <ConfirmPanel key={group[0]?.nonce} confirms={group} onDecide={onConfirmClick} />
          ))}
          <Text className="airy-timestamp" fontSize="xs" color={isDark ? "gray.400" : "gray.600"}>
            {formatTime(message.timestamp)}
          </Text>
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
  readonly onDecide?: (nonce: string, approved: boolean) => void;
}

/** Human titles and approve-button labels for the write tools behind a confirm. */
const CONFIRM_LABELS: Record<string, { title: string; approve: string }> = {
  fix_dag_code: { approve: "Apply fix", title: "Apply a code fix" },
  rerun_dag: { approve: "Re-run Dag", title: "Trigger a new Dag run" },
  revert_dag_code: { approve: "Restore original", title: "Discard every Airy fix to this Dag" },
  run_backfill: { approve: "Run backfill", title: "Run a backfill" },
};

/**
 * The card's title, which is the only line most people read.  Arguments that
 * change what the action *lastingly* does have to reach it: `rerun_dag` with
 * `unpause` resumes the Dag's schedule for good, which "Trigger a new Dag run"
 * does not say.
 */
export const buildConfirmTitle = (confirm: ConfirmRequest): string => {
  const args = typeof confirm.args === "string" ? tryParse(confirm.args) : confirm.args;
  if (confirm.tool === "rerun_dag" && (args as { unpause?: unknown } | undefined)?.unpause === true) {
    return "Re-run and resume this Dag's schedule";
  }
  return CONFIRM_LABELS[confirm.tool]?.title ?? humanizeToolName(confirm.tool);
};

/**
 * Write tools the server refuses to run without an explicit go-ahead.  Reads
 * as an action card, not prose: what will change, a badge saying it mutates
 * state, and a "Review change" expander with the exact arguments.  One nonce
 * covers the whole batch, so a multi-tool suspension is one card with every
 * tool listed — approving must never silently authorize an unseen call.
 */
const ConfirmPanel: FC<ConfirmPanelProps> = ({ confirms, onDecide }) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";
  const [reviewOpen, setReviewOpen] = useState(false);
  const [first] = confirms;
  if (first === undefined) return undefined;
  const single = confirms.length === 1 ? (CONFIRM_LABELS[first.tool] ?? undefined) : undefined;
  const title =
    confirms.length === 1 ? buildConfirmTitle(first) : `Approve ${confirms.length} actions`;
  const approveLabel = confirms.length === 1 ? (single?.approve ?? "Approve") : "Approve all";

  return (
    <Box
      width="100%"
      bg={isDark ? "gray.800" : "white"}
      borderWidth="1px"
      borderColor={isDark ? "orange.600" : "orange.300"}
      borderRadius="lg"
      px={3}
      py={2.5}
    >
      <Flex align="center" gap={2} wrap="wrap">
        <Text fontSize="sm" fontWeight="medium" color={isDark ? "gray.100" : "gray.900"}>
          {title}
        </Text>
        <Text
          fontSize="xs"
          px={1.5}
          py={0.5}
          borderRadius="sm"
          bg={isDark ? "orange.900" : "orange.100"}
          color={isDark ? "orange.200" : "orange.800"}
        >
          Modifies your Airflow
        </Text>
      </Flex>
      {confirms.map((confirm) => (
        <Text
          key={confirm.callId}
          fontSize="xs"
          fontFamily="mono"
          color={isDark ? "gray.400" : "gray.600"}
          mt={1}
          truncate
        >
          {confirm.tool} {formatArgs(confirm.args)}
        </Text>
      ))}
      {reviewOpen && (
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
          {confirms
            .map((confirm) => `${confirm.tool}: ${formatArgsFull(confirm.args) || "(no arguments)"}`)
            .join("\n\n")}
        </Box>
      )}
      {first.resolution === undefined ? (
        <Flex gap={2} mt={2} wrap="wrap">
          <ConfirmButton onClick={() => setReviewOpen((o) => !o)} aria-expanded={reviewOpen}>
            {reviewOpen ? "Hide change" : "Review change"}
          </ConfirmButton>
          <ConfirmButton onClick={() => onDecide?.(first.nonce, true)} disabled={!onDecide} primary>
            {approveLabel}
          </ConfirmButton>
          <ConfirmButton onClick={() => onDecide?.(first.nonce, false)} disabled={!onDecide}>
            {confirms.length === 1 ? "Reject" : "Reject all"}
          </ConfirmButton>
        </Flex>
      ) : first.outcomeUnknown === true ? (
        // The reply never finished, so the action may or may not have run. The
        // nonce still answers that question — asking again replays the outcome
        // rather than repeating the write.
        <Flex gap={2} mt={2} align="center" wrap="wrap">
          <Text fontSize="xs" color={isDark ? "orange.300" : "orange.700"}>
            {first.resolution === "approved" ? "Approved" : "Rejected"} — outcome unknown
          </Text>
          <ConfirmButton
            onClick={() => onDecide?.(first.nonce, first.resolution === "approved")}
            disabled={!onDecide}
          >
            Check outcome
          </ConfirmButton>
        </Flex>
      ) : (
        <Text fontSize="xs" color="gray.500" mt={2}>
          {first.resolution === "approved" ? "Approved ✓" : "Rejected ✕"}
        </Text>
      )}
    </Box>
  );
};

interface ConfirmButtonProps {
  readonly "aria-expanded"?: boolean;
  readonly children: string;
  readonly disabled?: boolean;
  readonly onClick: () => void;
  readonly primary?: boolean;
}

const ConfirmButton: FC<ConfirmButtonProps> = ({
  "aria-expanded": ariaExpanded,
  children,
  disabled = false,
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
  if (tool.failed === true) {
    return `${humanizeToolName(tool.name)} failed`;
  }
  if (tool.denied === true) {
    return `${humanizeToolName(tool.name)} rejected`;
  }
  const known = TOOL_LABELS[tool.name];
  if (known) {
    // Awaiting approval means the work has not happened yet — keep it in the
    // running form so the row never claims "Edited" before the user says yes.
    return tool.durationMs === undefined || tool.awaitingConfirm === true ? known.running : known.done;
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

type ToolStatus = "awaiting" | "denied" | "done" | "failed" | "running";

export const buildToolStatus = (tool: ToolCall): ToolStatus => {
  if (tool.durationMs === undefined) return "running";
  if (tool.awaitingConfirm === true) return "awaiting";
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
    denied: { color: isDark ? "gray.400" : "gray.600", icon: <CrossIcon /> },
    done: { color: isDark ? "green.300" : "green.600", icon: <CheckIcon /> },
    failed: { color: isDark ? "orange.300" : "orange.600", icon: <WarnIcon /> },
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
  const running = status === "running";
  const failed = status === "failed";
  const expandable = !running && status !== "awaiting" && (tool.result !== undefined || failed);
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
            {status === "awaiting" ? (
              <Text as="span" fontSize="xs" color={muted} flexShrink={0}>
                · awaiting approval
              </Text>
            ) : (
              !running && (
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
  const anyRunning = tools.some((tool) => tool.durationMs === undefined);
  const anyFailed = tools.some((tool) => tool.failed === true);
  const grouped = tools.length >= 3;
  // Live rows must stay visible: while anything runs the group cannot collapse.
  const open = anyRunning || (userOpen ?? !grouped);

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
          as={anyRunning ? undefined : "button"}
          onClick={anyRunning ? undefined : () => setUserOpen(!open)}
          aria-expanded={anyRunning ? undefined : open}
          align="center"
          gap={2}
          width="100%"
          textAlign="left"
          px={1.5}
          py={1}
          borderRadius="md"
          cursor={anyRunning ? undefined : "pointer"}
          _hover={anyRunning ? undefined : { bg: isDark ? "whiteAlpha.100" : "blackAlpha.100" }}
          _focusVisible={{ outline: "2px solid", outlineColor: "brand.500", outlineOffset: "-2px" }}
        >
          <ToolStatusIcon status={anyRunning ? "running" : anyFailed ? "failed" : "done"} />
          <Text fontSize="sm" color={isDark ? "gray.200" : "gray.800"} flex="1" minWidth={0}>
            {anyRunning ? `Using ${tools.length} tools…` : `Used ${tools.length} tools`}
          </Text>
          {!anyRunning && (
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

/**
 * Format timestamp to a human-readable time string.
 */
const formatTime = (date: Date): string => {
  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });
};
