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

import { useEffect, useState } from "react";

/**
 * Detect color mode from the document's class attribute.
 * This works with Airflow's theme system which uses next-themes
 * and sets class="dark" or class="light" on the <html> element.
 */
const getColorModeFromDOM = (): "dark" | "light" => {
  if (typeof document === "undefined") return "dark";
  return document.documentElement.classList.contains("dark") ? "dark" : "light";
};

export const useColorMode = () => {
  const [colorMode, setColorMode] = useState<"dark" | "light">(getColorModeFromDOM);

  useEffect(() => {
    // Watch for class changes on <html> element to detect theme switches
    const observer = new MutationObserver(() => {
      setColorMode(getColorModeFromDOM());
    });

    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class"],
    });

    // Set initial value
    setColorMode(getColorModeFromDOM());

    return () => observer.disconnect();
  }, []);

  return {
    colorMode,
    selectedTheme: colorMode,
    setColorMode: () => {}, // No-op since we follow Airflow's theme
  };
};
