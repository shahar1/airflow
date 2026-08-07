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

/**
 * Where the chatbot backend lives, URL-prefix deployments included.
 *
 * Airflow can be served under a path prefix (`[api] base_url`), and the
 * middleware that injects this bundle prefixes the script `src` accordingly —
 * so the script tag itself is the one reliable record of the deployed base.
 * Read at module init because `document.currentScript` is only set while the
 * injected (classic) script is executing.
 */
export const deriveChatbotBase = (script: unknown, origin: string): string => {
  const element = script as { dataset?: { chatbotBase?: string }; src?: string } | null;
  const fromAttribute = element?.dataset?.chatbotBase;
  if (fromAttribute !== undefined && fromAttribute !== "") {
    try {
      const url = new URL(fromAttribute, origin);
      return `${url.origin}${url.pathname.replace(/\/$/u, "")}`;
    } catch {
      // An unusable attribute falls through to the src-derived base.
    }
  }
  const src = element?.src;
  if (src !== undefined && src !== "") {
    try {
      const url = new URL(src, origin);
      const marker = url.pathname.lastIndexOf("/chatbot/static/");
      if (marker !== -1) {
        return `${url.origin}${url.pathname.slice(0, marker)}/chatbot`;
      }
    } catch {
      // A src we cannot parse tells us nothing about the base.
    }
  }
  return `${origin}/chatbot`;
};

export const CHATBOT_BASE = deriveChatbotBase(document.currentScript, globalThis.location.origin);

/** The host UI's path prefix ("" for a root deployment, "/prod" under one). */
export const hostBasePrefix = (chatbotBase: string = CHATBOT_BASE): string => {
  try {
    return new URL(chatbotBase).pathname.replace(/\/chatbot\/?$/u, "");
  } catch {
    return "";
  }
};
