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
import { FC, useEffect, useRef, useState } from "react";

import { useChat, useHealth } from "../hooks/useChat";
import { ChatButton } from "./ChatButton";
import { ChatDrawer } from "./ChatDrawer";

/**
 * Main Chatbot component that orchestrates the floating button and drawer.
 * This is the primary export for the chatbot plugin.
 */
export const Chatbot: FC = () => {
  const [isOpen, setIsOpen] = useState(false);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const wasOpenRef = useRef(false);
  const {
    canStop,
    clearMessages,
    isApplyingChange,
    isLoading,
    messages,
    resolveConfirm,
    retryMessage,
    sendMessage,
    stopResponse,
    streamingId,
  } = useChat();
  const { health, recheckHealth } = useHealth();

  // The trigger unmounts while the drawer is open, so focus cannot be captured
  // and given back — instead it lands on the freshly remounted button on close.
  useEffect(() => {
    if (wasOpenRef.current && !isOpen) buttonRef.current?.focus();
    wasOpenRef.current = isOpen;
  }, [isOpen]);

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
        onClear={clearMessages}
        messages={messages}
        onSendMessage={sendMessage}
        onConfirmClick={resolveConfirm}
        onRetry={retryMessage}
        isLoading={isLoading}
        canStop={canStop}
        isApplyingChange={isApplyingChange}
        onStop={stopResponse}
        streamingId={streamingId}
        health={health}
        onRecheckHealth={recheckHealth}
      />
      <ChatButton onClick={handleToggle} isOpen={isOpen} ref={buttonRef} />
    </>
  );
};
