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
import { createSystem, defaultConfig } from "@chakra-ui/react";

// Airflow logo blue. Without this token every `brand.500` usage (buttons,
// spinners, focus rings) resolves to an empty CSS var and renders transparent.
export const localSystem = createSystem(defaultConfig, {
  theme: {
    tokens: {
      colors: {
        brand: { 500: { value: "#017cee" } },
      },
    },
  },
});
