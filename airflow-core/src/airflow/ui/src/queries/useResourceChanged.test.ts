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
import { QueryClient } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";

import { RESOURCE_CHANGED_EVENT, listenForResourceChanges } from "src/queries/useResourceChanged";

const DAG_ID = "sales_summary";

type InvalidateSpy = { mock: { calls: Array<[{ queryKey: Array<unknown> }]> } };

const listening = () => {
  const client = new QueryClient();
  const invalidate = vi.spyOn(client, "invalidateQueries").mockResolvedValue() as unknown as InvalidateSpy;
  const stop = listenForResourceChanges(client);

  return { invalidate, stop };
};

const emit = (detail: unknown) =>
  globalThis.dispatchEvent(new CustomEvent(RESOURCE_CHANGED_EVENT, { detail }));

/** The first element of every key an invalidation was asked for. */
const invalidatedKeys = (invalidate: InvalidateSpy): Array<string> =>
  invalidate.mock.calls.map((call) => String(call[0].queryKey[0]));

let stopListening: (() => void) | undefined;

afterEach(() => {
  stopListening?.();
  stopListening = undefined;
});

describe("listenForResourceChanges", () => {
  it("refetches the definition views when a Dag's source changed", () => {
    const { invalidate, stop } = listening();

    stopListening = stop;
    emit({ updates: [{ dag_id: DAG_ID, kind: "dag_definition", version_number: 2 }] });

    const keys = invalidatedKeys(invalidate);

    expect(keys).toContain("DagSourceServiceGetDagSource");
    expect(keys).toContain("StructureServiceStructureData");
    expect(keys).toContain("TaskServiceGetTasks");
    expect(keys).toContain("DagVersionServiceGetDagVersions");
    expect(keys).toContain("GridServiceGetGridRuns");
  });

  it("refetches run and task-instance views when an instance was cleared", () => {
    const { invalidate, stop } = listening();

    stopListening = stop;
    emit({
      updates: [{ dag_id: DAG_ID, dag_run_id: "manual__1", kind: "task_instances", task_ids: ["report"] }],
    });

    const keys = invalidatedKeys(invalidate);

    expect(keys).toContain("DagRunServiceGetDagRun");
    expect(keys).toContain("TaskInstanceServiceGetTaskInstances");
    expect(keys).toContain("TaskInstanceServiceGetLog");
    expect(keys).toContain("GridServiceGetGridRuns");
    // The same set the native clear mutation invalidates: the Gantt and the
    // cleared instance's own detail query go stale too.
    expect(keys).toContain("GanttServiceGetGanttData");
    expect(keys).toContain("TaskInstanceServiceGetMappedTaskInstance");
    // A cached "what would this clear do" answer is stale the moment it does it.
    expect(keys).toContain("clearTaskInstanceDryRun");
    expect(keys).toContain("patchTaskInstanceDryRun");
  });

  it("invalidates every map index of a cleared mapped task", () => {
    const { invalidate, stop } = listening();

    stopListening = stop;
    emit({
      updates: [{ dag_id: DAG_ID, dag_run_id: "manual__1", kind: "task_instances", task_ids: ["report"] }],
    });

    const mapped = invalidate.mock.calls
      .map((call) => call[0].queryKey)
      .find((key) => key[0] === "TaskInstanceServiceGetMappedTaskInstance");

    // No mapIndex in the key: a partial match covers -1 and every fan-out index.
    expect(mapped?.[1]).toEqual({ dagId: DAG_ID, dagRunId: "manual__1", taskId: "report" });
  });

  it("scopes the invalidation to the Dag the event names", () => {
    const { invalidate, stop } = listening();

    stopListening = stop;
    emit({ updates: [{ dag_id: DAG_ID, kind: "dag_definition" }] });

    const scoped = invalidate.mock.calls
      .map((call) => call[0].queryKey[1])
      .filter((params): params is { dagId?: string } => typeof params === "object" && params !== null);

    expect(scoped.length).toBeGreaterThan(0);
    expect(scoped.every((params) => params.dagId === undefined || params.dagId === DAG_ID)).toBe(true);
  });

  it.each([
    ["no detail", undefined],
    ["no updates", { updates: undefined }],
    ["an unknown kind", { updates: [{ dag_id: DAG_ID, kind: "everything" }] }],
    ["no dag", { updates: [{ kind: "dag_definition" }] }],
  ])("invalidates nothing for %s", (_label, detail) => {
    const { invalidate, stop } = listening();

    stopListening = stop;
    emit(detail);

    expect(invalidate).not.toHaveBeenCalled();
  });

  it("stops listening once unsubscribed", () => {
    const { invalidate, stop } = listening();

    stop();
    emit({ updates: [{ dag_id: DAG_ID, kind: "dag_definition" }] });

    expect(invalidate).not.toHaveBeenCalled();
  });
});
