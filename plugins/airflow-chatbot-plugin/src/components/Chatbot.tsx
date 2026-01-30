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

import { FC, useState } from "react";

import { ChatButton } from "./ChatButton";
import { ChatDrawer } from "./ChatDrawer";
import { useChat, useHealth } from "../hooks/useChat";

/**
 * Main Chatbot component that orchestrates the floating button and drawer.
 * This is the primary export for the chatbot plugin.
 */
export const Chatbot: FC = () => {
  const [isOpen, setIsOpen] = useState(false);
  const { isLoading, messages, sendMessage } = useChat();
  const { health } = useHealth();

  const handleToggle = () => {
    setIsOpen((prev) => !prev);
  };

  const handleClose = () => {
    setIsOpen(false);
  };

  return (
    <>
      <ChatDrawer
        isOpen={isOpen}
        onClose={handleClose}
        messages={messages}
        onSendMessage={sendMessage}
        isLoading={isLoading}
        health={health}
      />
      <ChatButton onClick={handleToggle} isOpen={isOpen} />
    </>
  );
};
