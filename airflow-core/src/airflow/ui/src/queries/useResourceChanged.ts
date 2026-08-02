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
import type { QueryClient } from "@tanstack/react-query";

import {
  UseDagRunServiceGetDagRunKeyFn,
  useDagRunServiceGetDagRunsKey,
  UseDagSourceServiceGetDagSourceKeyFn,
  UseDagVersionServiceGetDagVersionsKeyFn,
  UseGanttServiceGetGanttDataKeyFn,
  UseTaskServiceGetTasksKeyFn,
  useTaskInstanceServiceGetMappedTaskInstanceKey,
  useTaskInstanceServiceGetTaskInstancesKey,
  UseStructureServiceStructureDataKeyFn,
} from "openapi/queries";

import { gridQueryKeys, tiPerAttemptQueryKeys } from "./gridViewQueryKeys";
import { useClearTaskInstancesDryRunKey } from "./useClearTaskInstancesDryRun";
import { usePatchTaskInstanceDryRunKey } from "./usePatchTaskInstanceDryRun";

/**
 * A mutation landed outside this React tree — refetch what it changed.
 *
 * Plugins mounted by the chatbot injection middleware are their own React
 * roots, outside the `QueryClientProvider`, so they cannot invalidate the host
 * cache directly. They dispatch this event instead. It carries no data and
 * grants no authority: it only asks queries the signed-in user is already
 * allowed to run to run again, so a page whose data did not really change ends
 * up exactly where it was.
 */
export const RESOURCE_CHANGED_EVENT = "airflow:resource-changed:v1";

type ResourceUpdate = {
  dag_id: string;
  dag_run_id?: string;
  kind: string;
  task_ids?: Array<string>;
};

/** Everything that shows a Dag's *definition* — code, tasks, graph, version. */
const definitionKeys = (dagId: string) => [
  ...gridQueryKeys(dagId),
  UseDagSourceServiceGetDagSourceKeyFn({ dagId }, [{ dagId }]),
  UseDagVersionServiceGetDagVersionsKeyFn({ dagId }, [{ dagId }]),
  UseTaskServiceGetTasksKeyFn({ dagId }, [{ dagId }]),
  UseStructureServiceStructureDataKeyFn({ dagId }, [{ dagId }]),
];

/**
 * Everything that shows run and task-instance *state*.
 *
 * The same set `useClearTaskInstances` invalidates after its own mutation — an
 * out-of-band clear leaves exactly the same caches stale as an in-band one.
 */
const runKeys = (update: ResourceUpdate) => {
  const { dag_id: dagId, dag_run_id: dagRunId, task_ids: taskIds } = update;

  if (dagRunId === undefined) {
    return [...gridQueryKeys(dagId), ...tiPerAttemptQueryKeys, [useDagRunServiceGetDagRunsKey]];
  }

  return [
    ...gridQueryKeys(dagId),
    ...tiPerAttemptQueryKeys,
    [useDagRunServiceGetDagRunsKey],
    [useTaskInstanceServiceGetTaskInstancesKey],
    UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }),
    UseGanttServiceGetGanttDataKeyFn({ dagId, runId: dagRunId }),
    // A cached preview of what a clear *would* do is answered from a state the
    // clear has already left behind.
    [useClearTaskInstancesDryRunKey, dagId],
    [usePatchTaskInstanceDryRunKey, dagId, dagRunId],
    // The detail panel of a cleared instance reads its own query — without this
    // it keeps showing the state the clear just replaced. No `mapIndex`: the
    // partial key matches every map index of a mapped task, which is what a
    // clear by task id affects.
    ...(taskIds ?? []).map((taskId) => [
      useTaskInstanceServiceGetMappedTaskInstanceKey,
      { dagId, dagRunId, taskId },
    ]),
  ];
};

const isUpdate = (value: unknown): value is ResourceUpdate =>
  typeof value === "object" &&
  value !== null &&
  typeof (value as ResourceUpdate).dag_id === "string" &&
  (value as ResourceUpdate).dag_id !== "" &&
  typeof (value as ResourceUpdate).kind === "string";

const keysFor = (update: ResourceUpdate): Array<unknown> => {
  if (update.kind === "dag_definition") {
    return definitionKeys(update.dag_id);
  }
  if (update.kind === "dag_run" || update.kind === "task_instances") {
    return runKeys(update);
  }

  return [];
};

/**
 * Start listening for out-of-band mutations. Returns the unsubscribe function.
 *
 * Invalidation is idempotent, so a replayed event is harmless — it costs one
 * refetch of data the user is looking at anyway.
 */
export const listenForResourceChanges = (client: QueryClient): (() => void) => {
  const onChanged = (event: Event) => {
    const detail: unknown = (event as CustomEvent<unknown>).detail;
    const updates: unknown = (detail as { updates?: unknown } | null)?.updates;

    if (!Array.isArray(updates)) {
      return;
    }

    for (const queryKey of updates.filter(isUpdate).flatMap(keysFor)) {
      void client.invalidateQueries({ queryKey: queryKey as Array<unknown> });
    }
  };

  globalThis.addEventListener(RESOURCE_CHANGED_EVENT, onChanged);

  return () => globalThis.removeEventListener(RESOURCE_CHANGED_EVENT, onChanged);
};
