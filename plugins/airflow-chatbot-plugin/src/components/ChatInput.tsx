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

import { Box, Flex, IconButton, Textarea } from "@chakra-ui/react";
import { FC, KeyboardEvent, useRef, useState } from "react";

import { useColorMode } from "src/context/colorMode";

import { SendIcon } from "./icons/SendIcon";

interface ChatInputProps {
  readonly onSend: (message: string) => void;
  readonly disabled?: boolean;
  /** When true, only the send button is blocked — the textarea stays editable. */
  readonly buttonDisabled?: boolean;
  readonly placeholder?: string;
  readonly value?: string;
  readonly onValueChange?: (value: string) => void;
}

/**
 * Chat input component with auto-resizing textarea.
 * Supports Enter to send (Shift+Enter for new line).
 * Can be controlled via value/onValueChange props.
 */
export const ChatInput: FC<ChatInputProps> = ({
  disabled = false,
  buttonDisabled = false,
  onSend,
  placeholder = "Ask anything about Airflow...",
  value: controlledValue,
  onValueChange,
}) => {
  const [internalValue, setInternalValue] = useState("");
  const value = controlledValue ?? internalValue;
  const setValue = onValueChange ?? setInternalValue;
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const { colorMode } = useColorMode();

  const isDark = colorMode === "dark";
  const inputBg = isDark ? "gray.800" : "white";
  const inputBorder = isDark ? "gray.600" : "gray.300";

  const handleSend = () => {
    const trimmed = value.trim();
    if (trimmed && !disabled && !buttonDisabled) {
      onSend(trimmed);
      setValue("");
      // Reset textarea height
      if (textareaRef.current) {
        textareaRef.current.style.height = "auto";
      }
    }
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    // Send on Enter without Shift (only when sending is allowed)
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      if (!disabled && !buttonDisabled) {
        handleSend();
      }
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setValue(e.target.value);
    // Auto-resize textarea
    const textarea = e.target;
    textarea.style.height = "auto";
    textarea.style.height = `${Math.min(textarea.scrollHeight, 120)}px`;
  };

  const canSend = value.trim().length > 0 && !disabled && !buttonDisabled;

  // Button colors — exact match of the Airflow "Sign in" button (brand.600 / brand.700)
  const buttonBg = "oklch(0.469 0.084 257.657)";
  const buttonHoverBg = "oklch(0.399 0.084 257.850)";
  const buttonDisabledBg = isDark ? "gray.600" : "gray.300";

  return (
    <Flex gap={3} alignItems="center" height="44px">
      <Textarea
        ref={textareaRef}
        value={value}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        disabled={disabled}
        bg={inputBg}
        borderColor={inputBorder}
        borderRadius="xl"
        resize="none"
        overflow="hidden"
        flex={1}
        height="44px"
        minHeight="44px"
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
        aria-label="Send message"
        onClick={handleSend}
        disabled={!canSend}
        borderRadius="full"
        width="44px"
        height="44px"
        minWidth="44px"
        bg={canSend ? buttonBg : buttonDisabledBg}
        color="white"
        _hover={{
          bg: canSend ? buttonHoverBg : buttonDisabledBg,
        }}
        _disabled={{
          bg: buttonDisabledBg,
          cursor: "not-allowed",
          opacity: 0.6,
        }}
        transition="all 0.2s"
      >
        <SendIcon />
      </IconButton>
    </Flex>
  );
};
