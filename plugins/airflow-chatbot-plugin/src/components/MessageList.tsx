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
import { FC, useEffect, useRef } from "react";

import { useColorMode } from "src/context/colorMode";

import { SparkleIcon } from "./icons/SparkleIcon";
import { Message } from "./types";

interface MessageListProps {
  readonly messages: Message[];
  readonly isLoading?: boolean;
  readonly onSuggestionClick?: (text: string) => void;
}

/**
 * Message list component displaying chat history.
 * Automatically scrolls to bottom on new messages.
 */
export const MessageList: FC<MessageListProps> = ({
  isLoading = false,
  messages,
  onSuggestionClick,
}) => {
  const { colorMode } = useColorMode();
  const bottomRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const isDark = colorMode === "dark";

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isLoading]);

  if (messages.length === 0 && !isLoading) {
    return (
      <Flex
        height="100%"
        align="center"
        justify="center"
        px={6}
        py={8}
      >
        <VStack gap={4} textAlign="center" color={isDark ? "gray.400" : "gray.600"}>
          <Box
            bg={isDark ? "gray.800" : "gray.200"}
            p={4}
            borderRadius="full"
          >
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
      <VStack gap={4} align="stretch">
        {messages.map((message) => (
          <MessageBubble key={message.id} message={message} />
        ))}
        {isLoading && <LoadingIndicator />}
        <div ref={bottomRef} />
      </VStack>
    </Box>
  );
};

interface MessageBubbleProps {
  readonly message: Message;
}

const MessageBubble: FC<MessageBubbleProps> = ({ message }) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";
  const isUser = message.role === "user";

  const userBg = "brand.500";
  const userColor = "white";
  const assistantBg = message.isError
    ? isDark ? "red.900" : "red.50"
    : isDark ? "gray.800" : "gray.100";
  const assistantColor = message.isError
    ? isDark ? "red.200" : "red.700"
    : isDark ? "gray.100" : "gray.800";

  return (
    <Flex justify={isUser ? "flex-end" : "flex-start"}>
      {!isUser && (
        <Flex
          align="center"
          justify="center"
          bg={isDark ? "gray.700" : "gray.200"}
          color="brand.500"
          boxSize={8}
          borderRadius="full"
          mr={2}
          flexShrink={0}
        >
          <SparkleIcon />
        </Flex>
      )}
      <Box
        bg={isUser ? userBg : assistantBg}
        color={isUser ? userColor : assistantColor}
        px={4}
        py={2.5}
        borderRadius="xl"
        borderBottomRightRadius={isUser ? "sm" : "xl"}
        borderBottomLeftRadius={isUser ? "xl" : "sm"}
        maxWidth="85%"
        wordBreak="break-word"
      >
        <Text fontSize="sm" lineHeight="tall" whiteSpace="pre-wrap">
          {message.content}
        </Text>
        <Text
          fontSize="xs"
          color={isUser ? "whiteAlpha.700" : "gray.500"}
          mt={1}
          textAlign={isUser ? "right" : "left"}
        >
          {formatTime(message.timestamp)}
        </Text>
      </Box>
    </Flex>
  );
};

const LoadingIndicator: FC = () => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";

  return (
    <Flex align="center" gap={2}>
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
      <Box
        bg={isDark ? "gray.800" : "gray.100"}
        px={4}
        py={3}
        borderRadius="xl"
        borderBottomLeftRadius="sm"
      >
        <Flex align="center" gap={2}>
          <Spinner size="sm" color="brand.500" />
          <Text fontSize="sm" color="gray.500">
            Thinking...
          </Text>
        </Flex>
      </Box>
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
        color: isDark ? "gray.100" : "gray.900",
        borderColor: isDark ? "gray.600" : "gray.400",
      }}
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
