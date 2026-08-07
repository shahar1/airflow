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
import { Box, Flex, Heading, IconButton, Portal, Text, VStack } from "@chakra-ui/react";
import { FC, useCallback, useEffect, useRef, useState } from "react";

import { useColorMode } from "src/context/colorMode";

import { HealthStatus } from "../hooks/useChat";
import { ChatInput } from "./ChatInput";
import { MessageList } from "./MessageList";
import { Message } from "./types";

const DEFAULT_WIDTH = 420;
const MIN_WIDTH = 320;
const MAX_WIDTH_RATIO = 0.6; // max 60% of viewport
const KEYBOARD_RESIZE_STEP = 24;
/** An armed clear that is never confirmed disarms itself. */
const CLEAR_CONFIRM_TIMEOUT_MS = 5_000;

/** Below Chakra's `md` breakpoint the drawer covers the page and acts modal. */
const isMobileViewport = (): boolean => globalThis.matchMedia?.("(max-width: 767px)")?.matches ?? false;

const FOCUSABLE_SELECTOR =
  'a[href], button:not([disabled]), textarea:not([disabled]), input:not([disabled]), [tabindex]:not([tabindex="-1"])';

interface ChatDrawerProps {
  readonly isOpen: boolean;
  readonly onClose: () => void;
  readonly onClear: () => void;
  readonly messages: Message[];
  readonly onSendMessage: (content: string) => void;
  readonly onConfirmClick?: (nonce: string, approved: boolean) => void;
  readonly isLoading?: boolean;
  readonly streamingId?: string | null;
  readonly health: HealthStatus;
  readonly onRecheckHealth?: () => void;
  readonly canStop?: boolean;
  readonly onStop?: () => void;
  readonly onRetry?: (errorMessageId: string) => void;
  readonly isApplyingChange?: boolean;
}

/**
 * Chat drawer component that slides in from the right side and pushes
 * the main content. The left edge is draggable to resize the panel
 * (only toward the content, never past the right viewport edge).
 */
export const ChatDrawer: FC<ChatDrawerProps> = ({
  canStop = false,
  health,
  isApplyingChange = false,
  isLoading = false,
  isOpen,
  messages,
  onClear,
  onClose,
  onConfirmClick,
  onRecheckHealth,
  onRetry,
  onSendMessage,
  onStop,
  streamingId,
}) => {
  const { colorMode } = useColorMode();
  const drawerRef = useRef<HTMLDivElement>(null);
  const [inputValue, setInputValue] = useState("");
  const [clearArmed, setClearArmed] = useState(false);

  const healthy = health.llm && health.mcp;

  // A stable identity here is what lets MessageBubble's memo actually hold.
  const handleSuggestionClick = useCallback(
    (text: string) => {
      if (isLoading) return;
      // Disconnected: a chip send is doomed, so hand the text to the input
      // instead — visible, editable, and sendable once the connection is back.
      if (!health.llm || !health.mcp) {
        setInputValue(text);
        return;
      }
      onSendMessage(text);
      setInputValue("");
    },
    [health.llm, health.mcp, isLoading, onSendMessage],
  );
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
      document.body.style.transition = isDragging.current ? "none" : "margin-right 0.3s ease-in-out";
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

  const handleResizeKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
      e.preventDefault();
      // The handle sits on the left edge, so left = wider, right = narrower.
      const delta = e.key === "ArrowLeft" ? KEYBOARD_RESIZE_STEP : -KEYBOARD_RESIZE_STEP;
      setWidth((current) => clampWidth(current + delta));
    },
    [clampWidth],
  );

  // Handle escape key to close
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Escape mid-IME cancels the composition, not the drawer.
      if (e.key === "Escape" && isOpen && !e.isComposing) {
        onClose();
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [isOpen, onClose]);

  // On mobile the backdrop makes the drawer modal, so Tab must wrap inside it.
  useEffect(() => {
    const trapFocus = (e: KeyboardEvent) => {
      if (e.key !== "Tab" || !isOpen || !isMobileViewport()) return;
      const root = drawerRef.current;
      if (!root) return;
      // Only visible targets: the resize handle matches the selector but is
      // display:none on mobile, and wrapping onto it strands focus on a
      // no-op element forever.
      const focusables = [...root.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR)].filter(
        (el) => el.offsetParent !== null,
      );
      const first = focusables[0];
      const last = focusables[focusables.length - 1];
      if (!first || !last) return;
      const active = document.activeElement;
      if (e.shiftKey) {
        if (active === first || !root.contains(active)) {
          e.preventDefault();
          last.focus();
        }
      } else if (active === last || !root.contains(active)) {
        e.preventDefault();
        first.focus();
      }
    };

    document.addEventListener("keydown", trapFocus);
    return () => document.removeEventListener("keydown", trapFocus);
  }, [isOpen]);

  // A drawer that closes with the clear half-armed must not stay armed.
  useEffect(() => {
    if (!clearArmed) return undefined;
    const id = setTimeout(() => setClearArmed(false), CLEAR_CONFIRM_TIMEOUT_MS);
    return () => clearTimeout(id);
  }, [clearArmed]);

  const handleClearClick = useCallback(() => {
    if (clearArmed) {
      onClear();
      setClearArmed(false);
    } else {
      setClearArmed(true);
    }
  }, [clearArmed, onClear]);

  if (!isOpen) return null;

  const isDark = colorMode === "dark";
  const bgColor = isDark ? "gray.900" : "gray.50";
  const borderColor = isDark ? "gray.700" : "gray.300";
  const headerBg = isDark ? "gray.800" : "white";
  const shadowColor = isDark ? "rgba(0,0,0,0.5)" : "rgba(0,0,0,0.15)";
  const handleColor = isDark ? "gray.500" : "gray.400";
  const maxWidthPx = Math.round(window.innerWidth * MAX_WIDTH_RATIO);
  const readOnly = !health.loading && (health.readOnly || !health.writeToolsAvailable);

  return (
    <Portal>
      {/* Backdrop - only visible on mobile. Tapping it closes the drawer:
          phones have no hardware Escape, so without this the close button
          would be the only way out. A sibling of the panel, so clicks inside
          the panel never reach it. */}
      <Box
        data-backdrop="true"
        onClick={onClose}
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
        role="dialog"
        aria-label="Airy assistant"
        aria-modal={isMobileViewport() ? true : undefined}
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
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize chat panel"
          aria-valuemin={MIN_WIDTH}
          aria-valuemax={maxWidthPx}
          aria-valuenow={Math.round(width)}
          tabIndex={0}
          position="absolute"
          top={0}
          left={0}
          bottom={0}
          width="6px"
          cursor="col-resize"
          onMouseDown={handleMouseDown}
          onKeyDown={handleResizeKeyDown}
          zIndex={1}
          display={{ base: "none", md: "block" }}
          _hover={{ bg: handleColor }}
          _focusVisible={{ bg: handleColor, outline: "2px solid", outlineColor: "brand.500" }}
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
            <Box bg="brand.500" color="white" p={1.5} borderRadius="md">
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
              <Flex align="center" gap={1.5}>
                <Heading size="sm">Airy</Heading>
                <Text
                  as="span"
                  fontSize="10px"
                  fontWeight="medium"
                  textTransform="uppercase"
                  letterSpacing="wide"
                  px={1.5}
                  borderRadius="full"
                  borderWidth="1px"
                  borderColor={isDark ? "purple.300" : "purple.500"}
                  color={isDark ? "purple.300" : "purple.600"}
                  title="Airy is an experimental assistant — review what it proposes before approving"
                >
                  Experimental
                </Text>
                {readOnly && (
                  <Text
                    as="span"
                    fontSize="10px"
                    fontWeight="medium"
                    textTransform="uppercase"
                    letterSpacing="wide"
                    px={1.5}
                    borderRadius="full"
                    borderWidth="1px"
                    borderColor={isDark ? "orange.300" : "orange.500"}
                    color={isDark ? "orange.300" : "orange.600"}
                    title={
                      health.readOnly
                        ? "An administrator has disabled Airy's write actions"
                        : "Airy's write tools are unreachable right now — reads still work"
                    }
                  >
                    Read-only
                  </Text>
                )}
              </Flex>
              <Flex
                align="center"
                gap={1.5}
                cursor={health.loading ? "default" : "help"}
                title={
                  health.loading
                    ? ""
                    : health.llm && health.mcp
                      ? "LLM: configured • MCP: " +
                        (health.degraded ? "partially reachable" : "reachable")
                      : health.llm
                        ? "LLM: configured • MCP: not reachable"
                        : "LLM: not configured • MCP: " + (health.mcp ? "reachable" : "not reachable")
                }
              >
                {health.loading ? (
                  <Text fontSize="xs" color={isDark ? "gray.400" : "gray.600"}>
                    Connecting…
                  </Text>
                ) : health.llm && health.mcp ? (
                  <>
                    <Box
                      boxSize="7px"
                      borderRadius="full"
                      bg={health.degraded ? "orange.400" : "green.400"}
                      flexShrink={0}
                    />
                    <Text fontSize="xs" color={isDark ? "gray.400" : "gray.600"}>
                      {health.degraded ? "Some tools unavailable" : "Connected"}
                    </Text>
                  </>
                ) : (
                  <>
                    <Box boxSize="7px" borderRadius="full" bg="red.400" flexShrink={0} />
                    <Text fontSize="xs" color={isDark ? "red.300" : "red.600"}>
                      Disconnected
                    </Text>
                  </>
                )}
              </Flex>
            </VStack>
          </Flex>
          <Flex align="center" gap={1}>
            <IconButton
              aria-label={clearArmed ? "Confirm clear conversation" : "Clear conversation"}
              title={clearArmed ? "Click again to clear the conversation" : "Clear conversation"}
              onClick={handleClearClick}
              disabled={messages.length === 0 || isLoading}
              variant="ghost"
              size="sm"
              color={clearArmed ? (isDark ? "red.300" : "red.600") : undefined}
            >
              {clearArmed ? (
                <Text as="span" fontSize="xs" fontWeight="medium" px={1}>
                  Clear?
                </Text>
              ) : (
                <svg
                  fill="none"
                  height="18"
                  stroke="currentColor"
                  strokeWidth={2}
                  viewBox="0 0 24 24"
                  width="18"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  <path
                    d="M3 6h18M8 6V4a1 1 0 011-1h6a1 1 0 011 1v2m3 0v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6h14zM10 11v6M14 11v6"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              )}
            </IconButton>
            <IconButton aria-label="Close chat" onClick={onClose} variant="ghost" size="sm">
              <svg
                fill="none"
                height="20"
                stroke="currentColor"
                strokeWidth={2}
                viewBox="0 0 24 24"
                width="20"
                xmlns="http://www.w3.org/2000/svg"
              >
                <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </IconButton>
          </Flex>
        </Flex>

        {/* Messages Area. `clip`, not `hidden`: a hidden box is still
            programmatically scrollable, so any scrollIntoView or focus that
            reaches an overflowing child could shift this wrapper — with no
            scrollbar to bring it back, the transcript would sit displaced
            out of view for the rest of the session. Explicit min-height:
            unlike a `hidden` scroll container, a clipped box keeps the flex
            item's content-based minimum, which would grow this area past the
            drawer and push the input off screen. */}
        <Box flex={1} minHeight={0} overflow="clip">
          <MessageList
            messages={messages}
            isLoading={isLoading}
            streamingId={streamingId}
            onSuggestionClick={handleSuggestionClick}
            onConfirmClick={onConfirmClick}
            onRetry={onRetry}
          />
        </Box>

        {/* Disconnected: name the cause and offer a way back, not just a red dot. */}
        {!health.loading && !healthy && (
          <Flex
            role="status"
            direction="column"
            align="flex-start"
            gap={2}
            mx={4}
            mb={2}
            px={3}
            py={2.5}
            borderWidth="1px"
            borderRadius="lg"
            borderColor={isDark ? "red.700" : "red.200"}
            bg={isDark ? "red.900" : "red.50"}
            color={isDark ? "red.200" : "red.700"}
            flexShrink={0}
          >
            <Text fontSize="sm">
              {health.unauthenticated
                ? "Your Airflow session has expired — sign in again to keep chatting."
                : health.llm
                  ? "Airy can't reach its Airflow tools right now, so it can't answer."
                  : "Airy's language model is not configured — an administrator needs to set an API key."}
            </Text>
            <Box
              as="button"
              onClick={onRecheckHealth}
              px={3}
              py={1}
              borderRadius="full"
              borderWidth="1px"
              borderColor={isDark ? "red.300" : "red.600"}
              fontSize="sm"
              cursor="pointer"
              _hover={{ bg: isDark ? "whiteAlpha.100" : "blackAlpha.50" }}
              _focusVisible={{ outline: "2px solid", outlineColor: "brand.500", outlineOffset: "2px" }}
            >
              Retry connection
            </Box>
          </Flex>
        )}

        {/* Input Area */}
        <Box borderTopWidth="1px" borderColor={borderColor} p={4} bg={headerBg} flexShrink={0}>
          <ChatInput
            onSend={(msg) => {
              onSendMessage(msg);
              setInputValue("");
            }}
            busy={isLoading}
            buttonDisabled={!health.llm || !health.mcp}
            canStop={canStop}
            isApplyingChange={isApplyingChange}
            onStop={onStop}
            value={inputValue}
            onValueChange={setInputValue}
          />
        </Box>
      </Flex>
    </Portal>
  );
};
