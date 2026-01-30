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

import { Box, IconButton } from "@chakra-ui/react";
import { FC } from "react";

import { useColorMode } from "src/context/colorMode";

import { ChatIcon } from "./icons/ChatIcon";

interface ChatButtonProps {
  readonly onClick: () => void;
  readonly isOpen: boolean;
}

/**
 * Floating chat button component that appears in the bottom-right corner.
 * Uses Airflow's brand colors and follows the design system.
 * Hidden when the chat drawer is open.
 */
export const ChatButton: FC<ChatButtonProps> = ({ isOpen, onClick }) => {
  const { colorMode } = useColorMode();
  const isDark = colorMode === "dark";

  // Hide the button when drawer is open
  if (isOpen) {
    return null;
  }

  return (
    <Box
      position="fixed"
      bottom={{ base: 4, md: 6 }}
      right={{ base: 4, md: 6 }}
      zIndex="popover"
    >
      <IconButton
        aria-label="Open Airy"
        onClick={onClick}
        borderRadius="full"
        size="lg"
        boxShadow="lg"
        bg="oklch(0.609 0.126 221.723)"
        color="white"
        _hover={{
          bg: "oklch(0.715 0.143 215.221)",
          boxShadow: "xl",
          transform: "scale(1.05)",
        }}
        transition="all 0.2s"
      >
        <ChatIcon />
      </IconButton>
    </Box>
  );
};
