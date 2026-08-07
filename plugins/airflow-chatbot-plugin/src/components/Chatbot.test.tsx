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
import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ColorModeProvider } from "src/context/colorMode";

import { Chatbot } from "./Chatbot";
import { localSystem } from "../theme";

const stubHealthFetch = () =>
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      json: () =>
        Promise.resolve({
          llm: { configured: true },
          mcp: { reachable: true, toolset_importable: true, unreachable: [] },
        }),
      ok: true,
      status: 200,
    } as unknown as Response),
  );

afterEach(() => {
  sessionStorage.clear();
  vi.unstubAllGlobals();
});

describe("Chatbot focus management", () => {
  it("focuses the input on open and hands focus back to the trigger on close", async () => {
    stubHealthFetch();
    render(
      <ChakraProvider value={localSystem}>
        <ColorModeProvider>
          <Chatbot />
        </ColorModeProvider>
      </ChakraProvider>,
    );
    // Let the initial health poll settle inside act.
    await act(async () => {
      await Promise.resolve();
    });

    fireEvent.click(screen.getByLabelText("Open Airy"));

    expect(screen.getByRole("dialog")).not.toBeNull();
    expect(document.activeElement).toBe(screen.getByPlaceholderText(/Ask anything/u));

    fireEvent.click(screen.getByLabelText("Close chat"));

    expect(screen.queryByRole("dialog")).toBeNull();
    expect(document.activeElement).toBe(screen.getByLabelText("Open Airy"));
  });
});
