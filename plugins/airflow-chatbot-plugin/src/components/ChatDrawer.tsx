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

import {
  Box,
  Flex,
  Heading,
  IconButton,
  Portal,
  Text,
  VStack,
} from "@chakra-ui/react";
import { FC, useCallback, useEffect, useRef, useState } from "react";

import { useColorMode } from "src/context/colorMode";

import { ChatInput } from "./ChatInput";
import { MessageList } from "./MessageList";
import { Message } from "./types";
import { HealthStatus } from "../hooks/useChat";

const DEFAULT_WIDTH = 420;
const MIN_WIDTH = 320;
const MAX_WIDTH_RATIO = 0.6; // max 60% of viewport

interface ChatDrawerProps {
  readonly isOpen: boolean;
  readonly onClose: () => void;
  readonly messages: Message[];
  readonly onSendMessage: (content: string) => void;
  readonly isLoading?: boolean;
  readonly health: HealthStatus;
}

/**
 * Chat drawer component that slides in from the right side and pushes
 * the main content. The left edge is draggable to resize the panel
 * (only toward the content, never past the right viewport edge).
 */
export const ChatDrawer: FC<ChatDrawerProps> = ({
  isLoading = false,
  isOpen,
  messages,
  onClose,
  onSendMessage,
  health,
}) => {
  const { colorMode } = useColorMode();
  const drawerRef = useRef<HTMLDivElement>(null);
  const [inputValue, setInputValue] = useState("");
  const [width, setWidth] = useState(DEFAULT_WIDTH);
  const isDragging = useRef(false);

  // Clamp width to valid range
  const clampWidth = useCallback((w: number) => {
    const maxW = window.innerWidth * MAX_WIDTH_RATIO;
    return Math.max(MIN_WIDTH, Math.min(w, maxW));
  }, []);

  // --- Push main content when drawer is open ---
  useEffect(() => {
    if (isOpen) {
      document.body.style.marginRight = `${width}px`;
      document.body.style.transition = isDragging.current
        ? "none"
        : "margin-right 0.3s ease-in-out";
    } else {
      document.body.style.marginRight = "";
      document.body.style.transition = "margin-right 0.3s ease-in-out";
    }
    return () => {
      document.body.style.marginRight = "";
      document.body.style.transition = "";
    };
  }, [isOpen, width]);

  // --- Drag-to-resize logic ---
  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      isDragging.current = true;
      const startX = e.clientX;
      const startWidth = width;

      const onMouseMove = (ev: MouseEvent) => {
        // Dragging left => larger width
        const delta = startX - ev.clientX;
        const newWidth = clampWidth(startWidth + delta);
        setWidth(newWidth);
        // Remove transition while dragging for responsiveness
        document.body.style.transition = "none";
      };

      const onMouseUp = () => {
        isDragging.current = false;
        document.body.style.transition = "margin-right 0.3s ease-in-out";
        window.removeEventListener("mousemove", onMouseMove);
        window.removeEventListener("mouseup", onMouseUp);
      };

      window.addEventListener("mousemove", onMouseMove);
      window.addEventListener("mouseup", onMouseUp);
    },
    [width, clampWidth],
  );

  // Handle escape key to close
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape" && isOpen) {
        onClose();
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [isOpen, onClose]);

  // Handle click outside to close on mobile
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (
        drawerRef.current &&
        !drawerRef.current.contains(e.target as Node) &&
        isOpen
      ) {
        const target = e.target as HTMLElement;
        if (target.getAttribute("data-backdrop") === "true") {
          onClose();
        }
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const isDark = colorMode === "dark";
  const bgColor = isDark ? "gray.900" : "gray.50";
  const borderColor = isDark ? "gray.700" : "gray.300";
  const headerBg = isDark ? "gray.800" : "white";
  const shadowColor = isDark ? "rgba(0,0,0,0.5)" : "rgba(0,0,0,0.15)";
  const handleColor = isDark ? "gray.500" : "gray.400";

  return (
    <Portal>
      {/* Backdrop - only visible on mobile */}
      <Box
        data-backdrop="true"
        display={{ base: "block", md: "none" }}
        position="fixed"
        top={0}
        left={0}
        right={0}
        bottom={0}
        bg="blackAlpha.600"
        zIndex="overlay"
        opacity={isOpen ? 1 : 0}
        transition="opacity 0.2s"
      />

      {/* Drawer Panel */}
      <Flex
        ref={drawerRef}
        position="fixed"
        top={0}
        right={0}
        bottom={0}
        width={{ base: "100%", md: `${width}px` }}
        maxWidth="100vw"
        bg={bgColor}
        borderLeftWidth={{ base: 0, md: "1px" }}
        borderColor={borderColor}
        boxShadow={`-4px 0 20px ${shadowColor}`}
        zIndex="modal"
        direction="column"
        transform={isOpen ? "translateX(0)" : "translateX(100%)"}
        transition={isDragging.current ? "none" : "transform 0.3s ease-in-out"}
      >
        {/* Drag handle on the left edge */}
        <Box
          position="absolute"
          top={0}
          left={0}
          bottom={0}
          width="6px"
          cursor="col-resize"
          onMouseDown={handleMouseDown}
          zIndex={1}
          display={{ base: "none", md: "block" }}
          _hover={{ bg: handleColor }}
          transition="background 0.15s"
          borderLeftRadius="sm"
        />

        {/* Header */}
        <Flex
          align="center"
          justify="space-between"
          px={4}
          py={3}
          bg={headerBg}
          borderBottomWidth="1px"
          borderColor={borderColor}
          flexShrink={0}
        >
          <Flex align="center" gap={2}>
            <Box
              bg="brand.500"
              color="white"
              p={1.5}
              borderRadius="md"
            >
              <svg
                fill="currentColor"
                height="16"
                viewBox="0 0 24 24"
                width="16"
                xmlns="http://www.w3.org/2000/svg"
              >
                <path d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
              </svg>
            </Box>
            <VStack align="start" gap={0}>
              <Heading size="sm">Airy</Heading>
              <Flex
                align="center"
                gap={1.5}
                cursor={health.loading ? "default" : "help"}
                title={
                  health.loading
                    ? ""
                    : health.llm && health.mcp
                      ? "LLM: configured \u2022 MCP: reachable"
                      : health.llm
                        ? "LLM: configured \u2022 MCP: not reachable"
                        : "LLM: not configured \u2022 MCP: " + (health.mcp ? "reachable" : "not reachable")
                }
              >
                {health.loading ? (
                  <Text fontSize="xs" color="gray.500">Connecting…</Text>
                ) : health.llm && health.mcp ? (
                  <>
                    <Box boxSize="7px" borderRadius="full" bg="green.400" flexShrink={0} />
                    <Text fontSize="xs" color="gray.500">Connected</Text>
                  </>
                ) : (
                  <>
                    <Box boxSize="7px" borderRadius="full" bg="red.400" flexShrink={0} />
                    <Text fontSize="xs" color="red.400">Disconnected</Text>
                  </>
                )}
              </Flex>
            </VStack>
          </Flex>
          <IconButton
            aria-label="Close chat"
            onClick={onClose}
            variant="ghost"
            size="sm"
          >
            <svg
              fill="none"
              height="20"
              stroke="currentColor"
              strokeWidth={2}
              viewBox="0 0 24 24"
              width="20"
              xmlns="http://www.w3.org/2000/svg"
            >
              <path
                d="M6 18L18 6M6 6l12 12"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </IconButton>
        </Flex>

        {/* Messages Area */}
        <Box flex={1} overflow="hidden">
          <MessageList
            messages={messages}
            isLoading={isLoading}
            onSuggestionClick={setInputValue}
          />
        </Box>

        {/* Input Area */}
        <Box
          borderTopWidth="1px"
          borderColor={borderColor}
          p={4}
          bg={headerBg}
          flexShrink={0}
        >
          <ChatInput
            onSend={(msg) => {
              onSendMessage(msg);
              setInputValue("");
            }}
            disabled={isLoading}
            buttonDisabled={!health.llm || !health.mcp}
            value={inputValue}
            onValueChange={setInputValue}
          />
        </Box>
      </Flex>
    </Portal>
  );
};
