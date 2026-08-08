<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Deferred semantic changes — do NOT implement during extraction](#deferred-semantic-changes--do-not-implement-during-extraction)
  - [The four known-open Gate 4 v1 defects](#the-four-known-open-gate-4-v1-defects)
  - [Found by the Phase B lifecycle audit](#found-by-the-phase-b-lifecycle-audit)
  - [Platform input to the redesign (not ours to fix)](#platform-input-to-the-redesign-not-ours-to-fix)
  - [Why these are deferred rather than fixed now](#why-these-are-deferred-rather-than-fixed-now)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Deferred semantic changes — do NOT implement during extraction

Fifteen behaviour changes identified during the Phase B read/completeness audit. Every one is a
**logic change**. None may be executed during the move-only structural extraction; they belong to
the separately-authorised typed completeness redesign.

If an extraction step appears to require one of these, that is the signal to **stop at the previous
checkpoint and report**, not to make the change.

## The four known-open Gate 4 v1 defects

| id | site | defect |
|---|---|---|
| **D1** | `_recorded_output` ~`:5798` | derives completeness from the route's claimed `total_entries`, never comparing `len(rows)` handed over. The correct derivation already exists as `_read_is_complete`. |
| **D2** | `diagnose_dag` ~`:3217` | asserts "no task instance in it failed" without consulting `omitted`. |
| **D3** | `compare_dag_runs` ~`:6551` | emits per-task `worker_dispatched: False` and instance counts manufactured by a cap. Preserved deliberately in `baselines/representative-outputs/compare_dag_runs__gate0_forge_ordered.json` so extraction can be checked for having **preserved** it rather than quietly repairing it. |
| **D4** | `duration_in_line_with_history` ~`:6084` | passes off a clamped sample with no completeness note. |

## Found by the Phase B lifecycle audit

| id | site | defect |
|---|---|---|
| **D5** | `_event_history` `:1850` | status computed over `fetched`, but callers receive `kept` after a `dag_id` filter that discards rows. A read with rejected rows reports `checked`. **This is also the deferred cycle C-1** (`reading ↔ evidence` via `:1755 → _compact_event:1453`) — during extraction it is avoided by a single documented `BOUNDARY_EXCEPTIONS` entry, never by breaking it with a logic change. |
| **D6** | `_task_comparison` `:2528` | same class. |
| **D7** | `_tasks` `:400` | drops completeness by construction (`return _tasks_reading(dag_id)[0]`) across 5 call sites. |
| **D8–D15** | backfill reads, token eviction, `find_failure_clusters:6660`, `_tail`, `_find_import_errors`, the plan/gate rule-set gap, cross-written completeness keys, field-level clamps | recorded in `docs/read-completeness-inventory.md`. |

## Platform input to the redesign (not ours to fix)

`GET /api/v2/eventLogs` reports a **post-filter** `total_entries`. Measured on this deployment: the
route returns 58 `patch_task_instance` / 45 `post_clear_task_instances` / 2008 total where the
database holds 80 / 47 / 2496. The 22 omitted rows resolve exactly, with no remainder, to `dag_id`
values that no longer resolve.

Consequence: **a short read is indistinguishable from a complete one at the call site**, so
`_event_history` is blind to this class of omission by construction. This is the same shape as D1,
occurring in the platform rather than in this server.

**Operational rule:** the protected audit-row invariants must be measured with `psql`, never through
the REST route. The authoritative non-`g4*` counts are **`patch_task_instance` = 66** and
**`post_clear_task_instances` = 4**.

## Why these are deferred rather than fixed now

Gate 4 v1 failed permanently across four rounds with one repeating shape: the sites that were
*named* got repaired correctly and durably, and the class reappeared at a site that was not named.
Fixing these individually would repeat that. The redesign makes the absence-shaped helpers
**structurally unable** to report an absence over an incomplete read, which is what turns the
convention into an invariant.
