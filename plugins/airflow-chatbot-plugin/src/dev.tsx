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

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import PluginComponent from "./main";

// Development entry point for testing the chatbot component
// Run with: pnpm dev
createRoot(document.querySelector("#root") as HTMLDivElement).render(
  <StrictMode>
    <div style={{ background: "#f5f5f5", minHeight: "100vh" }}>
      <div style={{ margin: "0 auto", maxWidth: "800px", padding: "20px" }}>
        <h1 style={{ marginBottom: "16px" }}>Airflow Chatbot Plugin - Dev Mode</h1>
        <p style={{ color: "#666" }}>
          Click the chat button in the bottom-right corner to open the chatbot.
        </p>
      </div>
      <PluginComponent />
    </div>
  </StrictMode>,
);
