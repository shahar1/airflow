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

import { Flex, IconButton, Spinner, Textarea } from "@chakra-ui/react";
import { FC, KeyboardEvent, useEffect, useRef, useState } from "react";

import { useColorMode } from "src/context/colorMode";

import { SendIcon } from "./icons/SendIcon";

interface ChatInputProps {
  readonly onSend: (message: string) => void;
  /**
   * A turn is in flight. Sending is gated, but the textarea stays editable so
   * the next question can be composed while Airy is still answering.
   */
  readonly busy?: boolean;
  /** When true, only sending is blocked — the textarea stays editable. */
  readonly buttonDisabled?: boolean;
  readonly placeholder?: string;
  readonly value?: string;
  readonly onValueChange?: (value: string) => void;
  /** A stoppable stream is in flight: Send becomes Stop. */
  readonly canStop?: boolean;
  readonly onStop?: () => void;
  /** An approved write may already be running; nothing here can call it back. */
  readonly isApplyingChange?: boolean;
}

const StopIcon: FC = () => (
  <svg fill="currentColor" height="14" viewBox="0 0 24 24" width="14">
    <rect height="16" rx="2" width="16" x="4" y="4" />
  </svg>
);

const MAX_TEXTAREA_HEIGHT_PX = 120;

/**
 * Chat input component with auto-resizing textarea.
 * Supports Enter to send (Shift+Enter for new line).
 * Can be controlled via value/onValueChange props.
 */
export const ChatInput: FC<ChatInputProps> = ({
  busy = false,
  buttonDisabled = false,
  canStop = false,
  isApplyingChange = false,
  onSend,
  onStop,
  onValueChange,
  placeholder = "Ask anything about Airflow...",
  value: controlledValue,
}) => {
  const [internalValue, setInternalValue] = useState("");
  const value = controlledValue ?? internalValue;
  const setValue = onValueChange ?? setInternalValue;
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const wasBusyRef = useRef(busy);
  const { colorMode } = useColorMode();

  const isDark = colorMode === "dark";
  const inputBg = isDark ? "gray.800" : "white";
  const inputBorder = isDark ? "gray.600" : "gray.300";

  // Autofocus: the input mounts exactly when the drawer opens.
  useEffect(() => {
    textareaRef.current?.focus();
  }, []);

  // Give the keyboard back the moment the turn ends; without this every
  // exchange costs a re-click even though the user never left the input.
  useEffect(() => {
    if (wasBusyRef.current && !busy) textareaRef.current?.focus();
    wasBusyRef.current = busy;
  }, [busy]);

  // Grow with the draft, shrink when it is cleared — including a clear that
  // arrives via the value prop rather than through handleSend.
  useEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    textarea.style.height = "auto";
    if (value !== "") {
      textarea.style.height = `${Math.min(textarea.scrollHeight, MAX_TEXTAREA_HEIGHT_PX)}px`;
    }
  }, [value]);

  const handleSend = () => {
    const trimmed = value.trim();
    if (trimmed && !busy && !buttonDisabled) {
      onSend(trimmed);
      setValue("");
    }
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    // Send on Enter without Shift (only when sending is allowed)
    if (e.key === "Enter" && !e.shiftKey) {
      // Enter inside IME composition confirms the conversion, not the message.
      if (e.nativeEvent.isComposing) return;
      e.preventDefault();
      handleSend();
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setValue(e.target.value);
  };

  const canSend = value.trim().length > 0 && !busy && !buttonDisabled;

  // Button colors — exact match of the Airflow "Sign in" button (brand.600 / brand.700)
  const buttonBg = "oklch(0.469 0.084 257.657)";
  const buttonHoverBg = "oklch(0.399 0.084 257.850)";
  const buttonDisabledBg = isDark ? "gray.600" : "gray.300";

  return (
    <Flex gap={3} alignItems="flex-end" minHeight="44px">
      <Textarea
        ref={textareaRef}
        value={value}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        bg={inputBg}
        borderColor={inputBorder}
        borderRadius="xl"
        resize="none"
        overflow="hidden"
        flex={1}
        height="44px"
        minHeight="44px"
        maxHeight={`${MAX_TEXTAREA_HEIGHT_PX}px`}
        py={3}
        px={4}
        fontSize="sm"
        _focus={{
          borderColor: "oklch(0.469 0.084 257.657)",
          boxShadow: "0 0 0 1px oklch(0.469 0.084 257.657)",
        }}
        _placeholder={{
          color: isDark ? "gray.500" : "gray.400",
        }}
        rows={1}
      />
      <IconButton
        aria-label={
          canStop ? "Stop response" : isApplyingChange ? "Applying approved change…" : "Send message"
        }
        title={isApplyingChange ? "Applying approved change…" : undefined}
        onClick={canStop ? onStop : handleSend}
        disabled={canStop ? false : isApplyingChange || !canSend}
        borderRadius="full"
        width="44px"
        height="44px"
        minWidth="44px"
        bg={canStop || canSend ? buttonBg : buttonDisabledBg}
        color="white"
        _hover={{
          bg: canStop || canSend ? buttonHoverBg : buttonDisabledBg,
        }}
        _disabled={{
          bg: buttonDisabledBg,
          cursor: "not-allowed",
          opacity: 0.6,
        }}
        transition="all 0.2s"
      >
        {canStop ? <StopIcon /> : isApplyingChange ? <Spinner size="sm" /> : <SendIcon />}
      </IconButton>
    </Flex>
  );
};
