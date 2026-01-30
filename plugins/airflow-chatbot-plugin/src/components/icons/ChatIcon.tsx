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

import { Icon } from "@chakra-ui/react";
import { FC } from "react";

interface ChatIconProps {
  readonly isOpen?: boolean;
}

/**
 * Animated chat icon that transforms between chat bubble and X when toggled.
 */
export const ChatIcon: FC<ChatIconProps> = ({ isOpen = false }) => {
  if (isOpen) {
    // X icon when chat is open
    return (
      <Icon boxSize={6}>
        <svg
          fill="none"
          stroke="currentColor"
          strokeWidth={2}
          viewBox="0 0 24 24"
          xmlns="http://www.w3.org/2000/svg"
        >
          <path
            d="M6 18L18 6M6 6l12 12"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </Icon>
    );
  }

  // Chat bubble icon when closed
  return (
    <Icon boxSize={6}>
      <svg
        fill="none"
        stroke="currentColor"
        strokeWidth={2}
        viewBox="0 0 24 24"
        xmlns="http://www.w3.org/2000/svg"
      >
        <path
          d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Icon>
  );
};
