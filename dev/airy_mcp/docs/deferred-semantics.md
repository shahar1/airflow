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
  - [Where D1-D15 stand after the typed completeness layer](#where-d1-d15-stand-after-the-typed-completeness-layer)
  - [Keys the completeness layer added](#keys-the-completeness-layer-added)
  - [The one public schema change, declared](#the-one-public-schema-change-declared)

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


## Where D1-D15 stand after the typed completeness layer

All fifteen are **no longer deferred**: each has a regression in `test_server.py`
named `test_d<N>_...`, and each of those fails on the pre-redesign tree. What
this document describes is therefore the history of the decision, not a list of
open work.

Two of them landed in a shape worth writing down, because the obvious reading of
the entry above is not what the code does:

* **D4 - a median over a clamped sample.** The sample size this tool asks for
  (`RUN_HISTORY_LIMIT`) is *not* a shortfall. A median is a statistic over a
  sample and does not need the population, so a page that comes back AT the
  limit is the sample that was requested and the leg answers from it, with the
  sample size named in the detail. A page that comes back UNDER the limit while
  the route accounts for more is a read that fell short, and that one still
  withholds the leg. Charging the tool's own sample size to the reading made
  `duration_in_line_with_history` unanswerable for every task with more than ten
  runs - a permanent null rather than a caution.

* **D14 - no read writes its completeness onto another read.**
  `instances_without_attribution` is gone from the event history. The four
  `attribution_payload_*` keys deliberately remain there: every
  `last_state_change` they measure is that same read's projection onto one
  instance, so they are one read accounting for the size of its own projection
  rather than one read's number written onto another's payload.

## Keys the completeness layer added

Machine-readable coverage that did not exist before, and that a reader has to
know is three-valued or is a disclosure rather than a finding:

| key | on | means |
| --- | --- | --- |
| `task_instances_read_whole` | each run of `compare_dag_runs` | the run's instance list was read to the end |
| `task_list_read_whole` | `diagnose_dag.tasks` | `/tasks` was read to the end |
| `asset_catalog_read_whole` | `get_blast_radius` | `/assets` was read to the end |
| `failures_read_whole` | `find_failure_clusters` | the fleet scan covered the window AND every log was readable |
| `failures_unreadable` | `find_failure_clusters` | the failures whose log could not be read, by instance |
| `logs_read_as_a_tail` | `find_failure_clusters` | how many signatures were drawn from a cut log |
| `runs_read_whole` / `runs_omitted` | `diagnose_dag.run_history` | the run window |
| `planned_runs_read_whole` | `plan_backfill` | the dry-run preview |
| `log_tail_truncated` | each entry of `diagnose_dag.failures` | the named error line may not be the cause |
| `reads_not_read_whole` | each instance of `verify_task_instance_recovery` | every read behind that instance's legs that came back short |
| `instances_not_located` | `verify_task_instance_recovery` | asked-for instances that were not among the rows read, over a run list that was not read whole - as distinct from `instances_not_found`, which is a settled absence |
| `surviving_runs_read_whole` / `surviving_runs_unread` | an abandoned backfill | whether the survivor list is the whole of it |
| `partial_external_effect_possible` | `apply_task_instance_clear` | re-asked at the gate off the attempt history it re-read immediately before the write |

Nullable fields whose docstring contract is two-valued, and where `null` is
therefore explained in the payload rather than in the docstring: `compare_dag_runs`'
`run_a_worker_field` / `run_b_worker_field` and `run_a_instances` /
`run_b_instances` (see its `scope`), and `diagnose_dag`'s
`no_task_instance_failed`.

## The one public schema change, declared

The frozen tool schemas in [`baselines/tool-schemas.json`](../baselines/tool-schemas.json) are the
record of what this server's API looked like before the typed completeness redesign. **The baseline
is not edited.** One tool's public surface has diverged from it, and this section is that
divergence written down, because the authorising instruction requires any public change to be
explicit, minimal, tested and documented.

**`compare_dag_runs`, docstring only.** Nothing else about the tool moved: the name, the four
parameters, their kinds, their annotations, their defaults and the return annotation are all
byte-identical to the freeze. `source_sha256` moved because the body changed; only the docstring is
a change to what a *caller* is told.

| | frozen | now |
| --- | --- | --- |
| `docstring_sha256` | `cc8194f12dd1d2d64c0936efa3dbd2bd791a15da98cfcc56f77ee4ecb89ad9dd` | `cedadf3171501531ebbe07a1dbebbca5f025e6225443b06fc214fd6000dd828d` |
| length | 853 | 1371 |

**What was added, and why it is essential rather than cosmetic.** `run_a_worker_field` /
`run_b_worker_field` became three-valued (commit `951992da15`): `null` now means "that run's rows do
not settle the question", which is a different answer from `false` ("no instance of that task on
that run recorded a hostname or pid"). The frozen docstring defines only true and false. A model
handed a `null` under a two-valued contract reads it as false — which is precisely the reading these
fields exist to prevent, and the reason the completeness layer made them three-valued at all. A
value a caller cannot interpret is not a smaller change than a documented one; it is the same change
with the documentation missing.

**What the added text says**, and what the test below pins:

* the two flags are THREE-valued: true, false and null;
* `null` is NOT false;
* the two reasons a run does not settle it — an incomplete instance list, or no instance of that
  task at all — and that `*_worker_field_note` says which;
* that where one flag is null, whether the task stopped being dispatched is **not established
  either way**, rather than resolved as "added or removed". (The closing sentence originally
  resolved it that way, which is false for the truncation cause; the `scope` string beside it was
  already correct.)

**Held to it:** `test_the_compare_dag_runs_docstring_declares_its_three_valued_contract` in
`test_server.py` recomputes `docstring_sha256` the same way the freeze did, asserts it equals the
value in the table above and differs from the frozen one, and asserts each claim above appears in
the text. A silent drift in either direction — the docstring changing again, or the three-valued
contract being written back to two — fails that test.

**No other tool's docstring, signature or schema has diverged from the freeze**, which is asserted
by the schema tests that read `baselines/tool-schemas.json` directly.
