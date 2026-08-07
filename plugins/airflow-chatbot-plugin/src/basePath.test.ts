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
import { describe, expect, it } from "vitest";

import { deriveChatbotBase, hostBasePrefix } from "./basePath";

const ORIGIN = "http://airflow.example";

describe("deriveChatbotBase", () => {
  it("recovers the URL prefix from the injected script src", () => {
    // The middleware prefixes the bundle URL with `[api] base_url`'s path, so
    // the src is the one truthful record of a prefixed deployment.
    const script = { src: `${ORIGIN}/prod/chatbot/static/main.iife.js?v=123` };

    expect(deriveChatbotBase(script, ORIGIN)).toBe(`${ORIGIN}/prod/chatbot`);
  });

  it("yields the plain mount for an unprefixed deployment", () => {
    const script = { src: `${ORIGIN}/chatbot/static/main.iife.js?v=123` };

    expect(deriveChatbotBase(script, ORIGIN)).toBe(`${ORIGIN}/chatbot`);
  });

  it("prefers an explicit data attribute over the src", () => {
    const script = {
      dataset: { chatbotBase: "/prod/chatbot" },
      src: `${ORIGIN}/elsewhere/main.iife.js`,
    };

    expect(deriveChatbotBase(script, ORIGIN)).toBe(`${ORIGIN}/prod/chatbot`);
  });

  it.each([
    ["no script element (module scripts, tests)", null],
    ["a src that does not look like the injected bundle", { src: `${ORIGIN}/other/bundle.js` }],
    ["a src that is not a URL", { src: "::::" }],
  ])("falls back to origin + /chatbot for %s", (_label, script) => {
    expect(deriveChatbotBase(script, ORIGIN)).toBe(`${ORIGIN}/chatbot`);
  });
});

describe("hostBasePrefix", () => {
  it.each([
    [`${ORIGIN}/chatbot`, ""],
    [`${ORIGIN}/prod/chatbot`, "/prod"],
    ["not a url", ""],
  ])("reads %s as prefix %s", (base, expected) => {
    expect(hostBasePrefix(base)).toBe(expected);
  });
});
