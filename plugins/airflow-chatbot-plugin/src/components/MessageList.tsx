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
import { Box, Flex, Spinner, Text, VStack } from "@chakra-ui/react";
import { FC, memo, useEffect, useRef } from "react";
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
        {messages.map((message, index) =>
          isBlank(message) ? undefined : (
            <MessageBubble
              key={message.id}
              message={message}
              isStreaming={isLoading && index === messages.length - 1}
              // Chips do nothing while a stream is in flight, so don't offer them.
              onActionClick={isLoading ? undefined : onSuggestionClick}
              onConfirmClick={isLoading ? undefined : onConfirmClick}
            />
          ),
        )}
        {isLoading && isBlank(messages[messages.length - 1]) && <LoadingIndicator />}
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
});

/** An assistant bubble that has streamed nothing yet is not worth showing. */
const isBlank = (message?: Message): boolean =>
  message !== undefined && message.role === "assistant" && message.content === "" && !message.tools?.length;

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
            color={isDark ? "gray.400" : "gray.500"}
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
      <VStack align="flex-start" gap={2} flex="1" minWidth={0} maxWidth="720px" css={timestampCss}>
        {tools.length > 0 && (
          <VStack
            align="stretch"
            gap={1.5}
            width="100%"
            bg={isDark ? "gray.800" : "white"}
            borderWidth="1px"
            borderColor={isDark ? "gray.700" : "gray.200"}
            borderRadius="lg"
            px={3}
            py={2}
          >
            {tools.map((tool) => (
              <ToolChip key={tool.id} tool={tool} />
            ))}
          </VStack>
        )}
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
        {confirms.map((confirm) => (
          <ConfirmPanel
            key={`${confirm.nonce}-${confirm.callId}`}
            confirm={confirm}
            onDecide={onConfirmClick}
          />
        ))}
        <Text className="airy-timestamp" fontSize="xs" color="gray.500">
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
});

interface ConfirmPanelProps {
  readonly confirm: ConfirmRequest;
  readonly onDecide?: (nonce: string, approved: boolean) => void;
}

/** A write tool the server refuses to run without an explicit go-ahead. */
const ConfirmPanel: FC<ConfirmPanelProps> = ({ confirm, onDecide }) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";

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
      <Text fontSize="xs" color={isDark ? "gray.400" : "gray.600"}>
        Airy wants to run:
      </Text>
      <Text fontSize="xs" fontFamily="mono" color={isDark ? "gray.100" : "gray.900"} mt={1}>
        {confirm.tool}
        {formatArgs(confirm.args)}
      </Text>
      {confirm.resolution === undefined ? (
        <Flex gap={2} mt={2}>
          <ConfirmButton onClick={() => onDecide?.(confirm.nonce, true)} disabled={!onDecide} primary>
            Confirm
          </ConfirmButton>
          <ConfirmButton onClick={() => onDecide?.(confirm.nonce, false)} disabled={!onDecide}>
            Reject
          </ConfirmButton>
        </Flex>
      ) : (
        <Text fontSize="xs" color="gray.500" mt={2}>
          {confirm.resolution === "approved" ? "Approved ✓" : "Rejected ✕"}
        </Text>
      )}
    </Box>
  );
};

interface ConfirmButtonProps {
  readonly children: string;
  readonly disabled?: boolean;
  readonly onClick: () => void;
  readonly primary?: boolean;
}

const ConfirmButton: FC<ConfirmButtonProps> = ({ children, disabled = false, onClick, primary = false }) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";

  return (
    <Box
      as="button"
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
    </Box>
  );
};

interface ToolChipProps {
  readonly tool: ToolCall;
}

/** One MCP tool call, shown while it runs and then with how long it took. */
const ToolChip: FC<ToolChipProps> = ({ tool }) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";
  const running = tool.durationMs === undefined;

  return (
    <Flex align="center" gap={2} fontSize="xs" fontFamily="mono" color={isDark ? "gray.400" : "gray.600"}>
      {running ? (
        <Spinner size="xs" color="brand.500" flexShrink={0} />
      ) : (
        <Box boxSize="7px" borderRadius="full" bg="brand.500" flexShrink={0} />
      )}
      <Text as="span" color={isDark ? "gray.200" : "gray.800"}>
        {tool.name}
      </Text>
      <Text as="span" truncate>
        {formatArgs(tool.args)}
      </Text>
      {running ? undefined : (
        <Text as="span" flexShrink={0}>
          {((tool.durationMs ?? 0) / 1000).toFixed(1)}s
        </Text>
      )}
    </Flex>
  );
};

/** Compact one-line rendering of a tool's arguments. */
const formatArgs = (args: unknown): string => {
  const parsed = typeof args === "string" ? tryParse(args) : args;
  if (parsed === undefined || parsed === null || typeof parsed !== "object") {
    return "";
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
