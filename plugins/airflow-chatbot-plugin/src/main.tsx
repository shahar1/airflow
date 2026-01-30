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

import { ChakraProvider } from "@chakra-ui/react";
import { FC } from "react";
import { createRoot } from "react-dom/client";

import { Chatbot } from "src/components";
import { ColorModeProvider } from "src/context/colorMode";

import { localSystem } from "./theme";

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface PluginComponentProps {
  // Props passed from Airflow UI context (extensible for future use)
}

/**
 * Airflow Chatbot Plugin Component
 *
 * This plugin renders a floating chatbot interface that provides
 * AI-powered assistance for Airflow users. It appears as a button
 * in the bottom-right corner and opens a drawer for chat interaction.
 */
const PluginComponent: FC<PluginComponentProps> = () => {
  // Use the globalChakraUISystem provided by the Airflow Core UI,
  // so the plugin has a consistent theming with the host Airflow UI,
  // fallback to localSystem for local development.
  const system = globalThis.ChakraUISystem ?? localSystem;

  return (
    <ChakraProvider value={system}>
      <ColorModeProvider>
        <Chatbot />
      </ColorModeProvider>
    </ChakraProvider>
  );
};

// Self-initialize when loaded as a standalone script
// Creates its own container and renders the chatbot
function initChatbot() {
  // Check if already initialized to prevent duplicates
  if (document.getElementById("airflow-chatbot-root")?.hasChildNodes()) {
    return;
  }

  // Create container if it doesn't exist
  let container = document.getElementById("airflow-chatbot-root");
  if (!container) {
    container = document.createElement("div");
    container.id = "airflow-chatbot-root";
    document.body.appendChild(container);
  }

  // Render the chatbot
  const root = createRoot(container);
  root.render(<PluginComponent />);
}

// Initialize when DOM is ready
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initChatbot);
} else {
  initChatbot();
}

// Also export for potential programmatic use
export default PluginComponent;
