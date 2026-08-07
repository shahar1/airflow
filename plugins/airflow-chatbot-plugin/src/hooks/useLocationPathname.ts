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

const LOCATION_CHANGED_EVENT = "airy:location-changed";

let historyPatched = false;

/**
 * The host UI is a SPA: it navigates via history.pushState, which fires no
 * event this separate React root could hear. Wrapping the two silent methods
 * (once, however many hooks subscribe) turns every route change into one.
 */
const patchHistoryOnce = () => {
  if (historyPatched) return;
  historyPatched = true;
  for (const method of ["pushState", "replaceState"] as const) {
    const original = globalThis.history[method].bind(globalThis.history);
    globalThis.history[method] = (...args: Parameters<History["pushState"]>) => {
      original(...args);
      globalThis.dispatchEvent(new Event(LOCATION_CHANGED_EVENT));
    };
  }
};

/** The current pathname, kept fresh across SPA navigation in the host UI. */
export const useLocationPathname = (): string => {
  const [pathname, setPathname] = useState(() => globalThis.location.pathname);

  useEffect(() => {
    patchHistoryOnce();
    const update = () => setPathname(globalThis.location.pathname);
    // The path may have changed between first render and this subscription
    // (and mounting is exactly when the drawer opens — resample then).
    update();
    globalThis.addEventListener("popstate", update);
    globalThis.addEventListener(LOCATION_CHANGED_EVENT, update);
    return () => {
      globalThis.removeEventListener("popstate", update);
      globalThis.removeEventListener(LOCATION_CHANGED_EVENT, update);
    };
  }, []);

  return pathname;
};
