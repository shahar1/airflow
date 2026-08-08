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

- [Phase B — READ / COMPLETENESS INVENTORY](#phase-b--read--completeness-inventory)
  - [0. The one-paragraph finding](#0-the-one-paragraph-finding)
  - [1. Every bounded / list read](#1-every-bounded--list-read)
  - [2. Every clamp and pagination loop](#2-every-clamp-and-pagination-loop)
  - [3. Every completeness calculation, by lifecycle](#3-every-completeness-calculation-by-lifecycle)
  - [4. Absence-shaped helpers](#4-absence-shaped-helpers)
  - [5. Pre-mutation consumers — what the write path depends on](#5-pre-mutation-consumers--what-the-write-path-depends-on)
  - [6. Dependency graph](#6-dependency-graph)
  - [7. The four KNOWN-OPEN defects, located (inventoried, not fixed)](#7-the-four-known-open-defects-located-inventoried-not-fixed)
  - [8. What this inventory says about the shape of a fix](#8-what-this-inventory-says-about-the-shape-of-a-fix)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Phase B — READ / COMPLETENESS INVENTORY

**Analysis only.** Nothing in the repo was moved, edited, renamed, split or deleted.
No `Reading` abstraction was implemented. No extraction was begun.

- Subject: `/home/shahar/repos/apache/airflow/dev/airy_mcp/server.py` @ `b933d63ec3`
- 7122 lines, **157 top-level definitions** (155 functions, 2 classes), **14 registered MCP tools**
- Test suite re-run to confirm the tree is untouched: **659 passed**
- `git status --porcelain` at finish: only `dev/g4adj3/`, `dev/rust_serde_poc/`, `dev/serde_poc_bench.py`

Artefacts in this directory:

| file | what it is |
|---|---|
| `README.md` | this report |
| `dependency-graph.json` | nodes = defs, edges = calls + value-references |
| `dependency-graph.md` | SCCs, fan-in / fan-out leaders, cycles, per-tool footprint |
| `clamps.json` | 98 located slice / cap / omitted-count / pagination sites |
| `build_graph.py` | the AST generator that produces all three, re-runnable |

Regenerate with `python3 build_graph.py` (defaults point at the same paths).

---

## 0. The one-paragraph finding

There are **24 bounded or list reads**. Every one of them is bounded. **Eleven** derive a
completeness fact of some kind; **seven** derive nothing at all; **six** derive one and then
lose it before the consumer. The derivations are written out longhand at **nine independent
sites** in **four mutually incompatible forms**, and only one of those forms — `_read_is_complete`
(`:5155`) — is named, tested and correct. Nothing in the types or the call convention forces a
site to pick the right one, so the four wrong forms are not bugs that were introduced; they are
what a site produces when it derives completeness from whatever numbers happen to be in scope.

The four forms actually in the file:

| form | expression | sites | correct? |
|---|---|---|---|
| **A** kept vs `max(delivered, claimed)` | `_read_is_complete` | `:5155`, and inline at `:4393-4397` | **yes** |
| **B** delivered vs claimed | `len(fetched) >= total` | `:1850`, `:865`, `:628`, `:2528`, `:2571`, `:6660`, `:4576` | no — blind to this tool's own clamps and drops |
| **C** kept vs claimed | `len(entries) >= total` | `:5798` | no — blind to what the route delivered |
| **D** none | value taken, count never read | `:400`, `:3490`, `:5860`, `:6738`, `:6977`, `:7060`, `:355` | no |

Form B and form C each provably disagree with form A on inputs the API can produce. Verified by
evaluating the real `_read_is_complete` against the two inline expressions:

```
kept  delivered  claimed | _read_is_complete | _recorded_output:5798 | _event_history:1850
  10         20        3 |             False |                  True | True     <- C and B both wrong
   7         10       10 |             False |                 False | True     <- B wrong
  95        100      100 |             False |                 False | True     <- B wrong
```

---

## 1. Every bounded / list read

24 reads. `bound` is what actually stops it; `completeness` is what the site knows afterwards.

### 1.1 Paginating reads (2)

| # | read | line | bound | completeness known | consumers |
|---|---|---|---|---|---|
| R1 | `_run_task_instances` — `GET /dags/<d>/dagRuns/<r>/taskInstances` | `:365` | `TASK_INSTANCE_PAGE`=100 per page, ceiling `TASK_INSTANCE_SCAN_LIMIT`=500; loop breaks at `:380` | **yes, as a bare int** — returns `max(total - len(tis), 0)` at `:382`. Callers must remember to look at tuple slot 1 | `diagnose_dag:3101`, `_version_drift:4239`, `plan_task_instance_clear:4926`, `verify_task_instance_recovery:6383`, `compare_dag_runs:6508` |
| R2 | `_event_history` — `GET /eventLogs` | `:1755` | `EVENT_SCAN_PAGE`=100, `max_pages`=3 (`:1804`), ceiling `EVENT_SCAN_LIMIT`=300; break at `:1827` | **partially wrong** — `status` at `:1850` is form B over `fetched`, but the rows the caller gets are `kept` (`:1837`), after a `dag_id` filter that drops rows and counts them into `rows_rejected` (`:1855`). A read with rejected rows reports `checked` | `_attribution_reader:1961` → `_last_state_change:1571`; `_run_health:2403`; `_audit_transitions:5817`; `_verify_instance:6105` |

### 1.2 Single-page reads that DO derive completeness (5)

| # | read | line | bound | completeness known | consumers |
|---|---|---|---|---|---|
| R3 | `_tasks_reading` — `GET /dags/<d>/tasks` | `:385` | route's own default page; **not paginated** | **raw numbers only** — returns `(rows, total)` at `:395`. The comparison is made at exactly one of its two call sites | `_version_drift:4245` (compares, `:4246`), `_tasks:400` (**drops it**) |
| R4 | `_find_import_errors` — `GET /importErrors` | `:584` | `limit=100` (`:597`), not paginated | **as a prose check entry** — `total > len(errors)` at `:628` appends `import_errors_truncated` | `diagnose_dag:3183` → `checks` list → summary prose only |
| R5 | `_attempt_history` — `GET .../tries` | `:841` | route not paginated; whole page taken | **yes, as a status string** — `partial` at `:865` (form B), `empty` at `:864`, `unavailable` at `:857`; `attempts_recorded` carried | `_check_dispatch_evidence:2202`, and via `_attempt_reading` at `_recovery_evidence:4438`, `_containment_gate:5381`, `_verify_instance:5963` |
| R6 | `_version_context` — `GET /dagVersions` | `:4525` | `limit=DAG_VERSION_SCAN`=100 (`:4567`) | **yes** — `versions_status` at `:4576` (form B), and correctly **gated**: `original_version_listed` is only computed when `checked` (`:4577`) | `plan_task_instance_clear:5081` — advisory only, never a gate |
| R7 | `_task_comparison` — `GET /dags/<d>/dagRuns/~/taskInstances` per task id | `:2487` | `limit=RUN_HISTORY_LIMIT`=10 per task; task ids sliced to `TASK_COMPARISON_LIMIT`=5 (`:2496`) | **derived and then orphaned** — `rows_omitted[task_id]` at `:2528`, `task_ids_omitted` at `:2500`. Emitted at `:2542`; **no consumer reads either** | `_run_history:2579` → `_augment_dispatch_findings:2805` → `_compared_rows_by_run:2621`, `_recurrence_clause:2634`, `_contrast_clause:2663` |

### 1.3 Single-page reads that derive NOTHING (7)

| # | read | line | bound | completeness known | consumers |
|---|---|---|---|---|---|
| R8 | `_tasks` | `:398` | inherits R3 | **none — dropped by construction**: `return _tasks_reading(dag_id)[0]` | `_change_impact:3403`, `_find_unaddressed_findings:3476`, `_mapped_in_closure:4642`, `_resolve_task:4142`, `diagnose_dag:3162` |
| R9 | `_duration_baseline` — `GET /dagRuns/~/taskInstances` | `:5833` | `limit=RUN_HISTORY_LIMIT`=10 (`:5858`) | **none** — `total_entries` never read; the `except` at `:5861` also returns the partial sample under the *first* source string, hiding that the second leg failed | `_verify_instance:6070` → `duration_in_line_with_history` check `:6084-6091` |
| R10 | `_build_asset_note` — `GET /assets` | `:3483` | `limit=100` (`:3490`) | **none** | `plan_dag_code_changes:3722` → the approval card's asset note |
| R11 | `get_blast_radius` — `GET /assets` | `:7053` | `limit=100` (`:7060`) | **none** — four empty lists mean "no declared edge" *and* "catalog truncated" | tool result; `scope` prose at `:7082` covers only the first meaning |
| R12 | `_backfill_runs` — `GET /backfills/<id>/dag_runs` | `:6975` | `limit=MAX_BACKFILL_RUNS+1`=51 (`:6976`); the `+1` overflow sentinel is never checked | **none** | `run_backfill:6952` (**authorizes `created: True` vs abandon**), `_abandon_backfill:6998` |
| R13 | `_dry_run_backfill` — `POST /backfills/dry_run` | `:6732` | route-side; `resp.get("backfills", [])` | **none** | `plan_backfill:6838` (count vs `MAX_BACKFILL_RUNS`), `run_backfill:6919` (**identity compare authorizes the write**) |
| R14 | `_tail` — log body | `:355` | `LOG_TAIL_LINES`=40 then `LOG_TAIL_CHARS`=4000 | **none, ever** — no caller can tell a 40-line log from a 4000-line one | `diagnose_dag:3251`, `_attempt_log:4342`, `find_failure_clusters:6680` |

### 1.4 Reads whose completeness is derived with the wrong numbers (2)

| # | read | line | bound | completeness known | consumers |
|---|---|---|---|---|---|
| R15 | `_recorded_output` — `GET .../xcomEntries` | `:5760` | **not paginated, no `limit` sent**; local clamp `rows[:RECOVERY_ATTEMPT_LIMIT]`=10 at `:5790` | **form C, wrong** — `status` at `:5798` is `len(entries) >= total`. `len(rows)` (delivered) is used only as the *default* for `total` at `:5785` and is never compared. `entries_omitted` at `:5801` is likewise `total - kept` | `_verify_instance:6141` → `recorded_output_post_dates_clear` (`:6177`, `:6191`), `_downstream_dating_check:6206` (`:5917`, `:5929`) |
| R16 | `_event_history` status | `:1850` | see R2 | **form B, wrong w.r.t. the filter** — see R2 | see R2 |

### 1.5 Deliberately-bounded lookups (4)

| # | read | line | bound | completeness known | consumers |
|---|---|---|---|---|---|
| R17 | `_latest_version` — `GET /dagVersions` | `:320` | `limit=1` | n/a by design; **but returns `None` on an empty page** — absence-shaped | `_force_reparse:346`, `apply_dag_code_changes:3825`, `revert_dag_code:3619` |
| R18 | `_resolve_run` — `GET /dagRuns` | `:4098` | `limit=1` or `2` (`:4104`) | n/a by design; `len(runs) < wanted` → `(None, msg)` at `:4111` | 6 tools |
| R19 | `_recent_runs` — `GET /dagRuns` | `:2596` | `limit=RUN_HISTORY_LIMIT`=10 (`:2603`) | **raw numbers** — `(runs, total)` at `:2606`; `_run_history:2571` turns it into `runs_omitted` (form B), reported only | `diagnose_dag:3047`, `:3056` |
| R20 | `_expandable_probe` — `GET .../listMapped` | `:4595` | `limit=1` | **tri-state and correct** — `True` / `False` only on the explicit `is not mapped` detail / `None` otherwise (`:4615-4619`) | `_mapped_in_closure:4648` |

### 1.6 Fleet / budget-bounded reads (4)

| # | read | line | bound | completeness known | consumers |
|---|---|---|---|---|---|
| R21 | `find_failure_clusters` — `POST /dags/~/dagRuns/~/taskInstances/list` | `:6635` | `page_limit=FAILURE_SCAN_LIMIT`=50 (`:6652`) | ~~**computed at the wrong moment**~~ — **fixed**: the scan is a `Reading`, the allowlist filter is a `.filter()` on it, and `failures_omitted` is the reading's own number, so it describes the list the clusters were built from. A log that cannot be read now joins `failures_unreadable` instead of taking the tool down, and `scope` stops asserting the window holds no failure whenever coverage is short | tool result |
| R22 | `_check_dispatch_evidence` `/tries` budget | `:2145` | `TRIES_PROBE_LIMIT`=20 (`:2203`); beyond it `status: "not_checked"` | **yes** — `attempt_history_unchecked` at `:2243` → `run_health` blocker `:2388` | `diagnose_dag:3104` |
| R23 | `diagnose_dag` failed-log loop | `:3231` | `DIAGNOSIS_LOG_BUDGET_CHARS`=12000 | **yes** — `logs_omitted` at `:3262` → summary tail `:3000` | own result |
| R24 | `_project_task_instances` detail projection | `:2261` | `TASK_INSTANCE_DETAIL_LIMIT`=200 (`:2285`) | **yes** — returns `len(tis) - len(detailed)` at `:2340` → `task_instance_detail_reduced` on the diagnosis. `instances_without_attribution` on the event history is **gone**: it was this projection's number written onto a different read's payload (D14) | `diagnose_dag:3107` |

---

## 2. Every clamp and pagination loop

`clamps.json` holds all 98 located sites (33 slices, 45 limit applications, 13 omitted-count
idioms, 7 loops). The ones that change what a conclusion may say:

### 2.1 Pagination loops (2 real, 5 local)

| loop | line | ceiling | terminates on |
|---|---|---|---|
| `_run_task_instances` | `:369` | `TASK_INSTANCE_SCAN_LIMIT`=500 | empty page **or** `len(tis) >= min(total, 500)` (`:380`) |
| `_event_history` | `:1806` | `max_pages`=3 × `EVENT_SCAN_PAGE`=100 | empty page **or** `len(fetched) >= min(total, 300)` (`:1827`) |

The other five `while` loops are graph walks (`_display_order:426`, `_downstream_task_ids:2727`)
and store evictions (`_issue_token:6753`, `_record_approved_set:6794`) — not reads.

### 2.2 Clamps that discard rows a conclusion is then drawn over

| clamp | line | constant | value | reported as | read by an absence-based conclusion? |
|---|---|---|---|---|---|
| `rows[:…]` in `_attempt_rows` | `:4367` | `RECOVERY_ATTEMPT_LIMIT` | 10 | **`_attempt_reading:4396` re-derives `partial`** | yes — and correctly handled |
| `rows[:…]` in `_recorded_output` | `:5790` | `RECOVERY_ATTEMPT_LIMIT` | 10 | `entries_omitted` (form C) | **yes — `:6191`, `:5929`** |
| `dag_id` filter in `_event_history` | `:1837` | — | — | `rows_rejected:1855`, **not in `status`** | **yes — `_last_state_change:1578` `_ATTR_NONE`** |
| `task_ids[:…]` in `_task_comparison` | `:2496` | `TASK_COMPARISON_LIMIT` | 5 | `task_ids_omitted` | yes — `_contrast_clause` returns `""` |
| per-task `limit` in `_task_comparison` | `:2522` | `RUN_HISTORY_LIMIT` | 10 | `rows_omitted` (unread) | **yes — `_recurrence_clause:2646`, `_contrast_clause:2681`** |
| `runs[:…]` in `_run_history` | `:2561` | `RUN_HISTORY_LIMIT` | 10 | `runs_omitted` | yes — `order` at `:2795` bounds the recurrence streak |
| `ordered[:…]` in `_project_task_instances` | `:2285` | `TASK_INSTANCE_DETAIL_LIMIT` | 200 | `detail_reduced` | no — undetailed rows get `_ATTR_UNKNOWN` |
| `matched[:…]` in `_last_state_change` | `:1706` | `EVENT_HISTORY_PER_INSTANCE` | 2 | `events_omitted_for_instance` | no — headline is `matched[0]` |
| `run_scoped[:…]` | `:1857` | `RUN_SCOPED_EVENT_LIMIT` | 10 | `run_scoped_events_omitted` | no |
| `checks[:…]` in `_check_dispatch_evidence` | `:2224` | `DISPATCH_FINDING_LIMIT` | 25 | fold entry + `run_health` blocker | no |
| `unknown[:…]`/`strangers[:…]` | `:537`, `:563` | `STATIC_CHECK_LIMIT` | 25 | fold entry + `run_health` blocker | no |
| `withheld[:…]` | `:1447` | `EXTRA_KEY_LIMIT` | 20 | `extra_keys_omitted` | no |
| `value[:…]` in `_bounded_extra_value` | `:1388` | `EXTRA_LIST_LIMIT` | 20 | `extra_truncated` | no |
| `worker_fieldless[:…]` | `:2254` | `COVERAGE_NAME_LIMIT` | 5 | named vs total in prose `:2914` | no |
| `streak[:…]`, `reached[:…]` | `:2651`, `:2762` | 4 / 6 | | "and N more" | no |
| `_tail` line + char clamps | `:358`, `:362` | 40 / 4000 | | **nothing** | yes — `_extract_error_line`, `_error_signature` |
| `[-RECOVERY_LOG_TAIL_CHARS:]` | `:4342` | 600 | | **nothing** | yes — `log_for_new_attempt:6098` |
| `[-budget:]` diagnosis logs | `:3251` | 12000 | | `logs_omitted` | no |
| `_quoted` / `_fenced` / `_clamped_operator` / `_clamped_event_text` | `:722`, `:734`, `:740`, `:1257` | 120 / 80 / 120 / var | | ellipsis, or `(value, True)` | no — display only |
| `_enforce_attribution_ceiling` reduction | `:1892-1901` | `ATTRIBUTION_PAYLOAD_LIMIT_CHARS`=400000 | | `size_reduced`, `attribution_reduced_for_size`, `attribution_payload_over_limit` | no |
| summary entry budget | `:2977-2983` | `DIAGNOSIS_SUMMARY_BUDGET_CHARS`=12000 | | `unlisted` in tail `:3005` | no |
| token / approved-set eviction | `:6753`, `:6794` | 20 / 20 | | **nothing** | **yes — see §4.18** |

---

## 3. Every completeness calculation, by lifecycle

Exact line numbers. A single site can appear in more than one class.

### 3.1 created

| line | what |
|---|---|
| `:376` | `total = resp.get("total_entries", len(page))` — the claimed universe, defaulted to the page |
| `:382` | `max(total - len(tis), 0)` — R1's omitted count |
| `:395` | `(rows, total)` — R3's raw pair |
| `:565`, `:581` | `suppressed` static checks |
| `:627-637` | `total > len(errors)` → `import_errors_truncated` |
| `:855`, `:864`, `:868-869` | `attempts_recorded` + `empty`/`partial`/`checked` |
| `:1387`, `:1429-1434`, `:1448-1449` | `extra_truncated`, `extra_keys_omitted` |
| `:1749` | `events_omitted_for_instance` |
| `:1824`, `:1850`, `:1853` | `total`, `status`, `events_omitted` |
| `:1855` | `rows_rejected = len(fetched) - len(kept)` — **created and never used** |
| `:1858` | `run_scoped_events_omitted` |
| `:1902-1907` | payload bytes / limit / reduced / `attribution_payload_over_limit` |
| `:2222`, `:2255` | `dispatch_findings_suppressed` |
| `:2240-2256` | the whole `coverage` dict (8 completeness numbers) |
| `:2340` | `len(tis) - len(detailed)` |
| `:2500`, `:2528` | `task_ids_omitted`, `rows_omitted` |
| `:2571` | `runs_omitted` |
| `:3262` | `logs_omitted` |
| `:4393-4397` | **`_attempt_reading` — form A written inline**: `recorded` raised to `returned`, then `status → partial` when `len(rows) < returned` |
| `:4570`, `:4576` | `versions_status` |
| `:4653` | `settled` |
| `:5155-5168` | **`_read_is_complete` — the canonical form A**, the only named one |
| `:5798`, `:5801` | `_recorded_output` status + `entries_omitted` (**form C**) |
| `:6660` | `failures_omitted` (**before the filter**) |

### 3.2 inferred (derived from numbers that are not "what was kept")

| line | what | why it is an inference and not a measurement |
|---|---|---|
| `:865` | `total > len(rows)` | correct at this layer, but this layer is not where the clamp is — `_attempt_rows:4367` clamps afterwards, which is the whole reason `_attempt_reading` exists |
| `:1850` | `"checked" if len(fetched) >= total` | `fetched` is pre-filter; the caller receives `kept` |
| `:2528` | `total_entries - len(returned)` | `returned` is then filtered by run window at `:2531`, and the filtered count is what `tasks[task_id]` holds |
| `:4576` | `"checked" if len(known) >= total` | no local clamp here, so B ≡ A; safe by accident of there being no clamp |
| `:5785` | `total = resp.get("total_entries", len(rows))` | when the key is missing, the claimed universe becomes the delivered count — the two numbers form C needs to keep apart are silently fused |
| `:5798` | `"checked" if len(entries) >= total` | `entries` is post-clamp, `total` is the claim; `len(rows)` is never in the comparison |
| `:6660` | `total_entries - len(tis)` | `tis` is rebound at `:6665` |

### 3.3 overwritten

| line | what |
|---|---|
| `:4393-4394` | `recorded` overwritten upward when the source claimed fewer than it delivered — the one place a lying total is corrected |
| `:4396-4397` | **`status` "checked" → "partial"**, and `error` defaulted to `_HISTORY_CLAMPED`. The good overwrite: it re-states R5's completeness in terms of what this reading kept |
| `:1902-1907` | four `history[...]` keys rewritten by `_enforce_attribution_ceiling` after the fact |
| `:1914` | `events_omitted_for_instance` overwritten with `events_recorded` |
| `:1923-1928` | `extra_keys_omitted` incremented, `extra_truncated` forced `True` |
| ~~`:3115`~~ | ~~`event_history["instances_without_attribution"] = detail_reduced`~~ — **removed**; the count travels as `task_instance_detail_reduced` on the diagnosis, and `test_d14` asserts the key is absent |
| `:3180` | `coverage["static_checks_suppressed"]` written into `coverage` from outside `_check_dispatch_evidence`, which initialised it to `0` at `:2256` |
| ~~`:6665`~~ | ~~`tis` rebound after `failures_omitted` was computed off the old binding~~ — **fixed**; the filter is a `.filter()` on the `Reading` and the count is the reading's own |

### 3.4 dropped

| line | what is lost | consequence |
|---|---|---|
| **`:400`** | `_tasks` returns `_tasks_reading(dag_id)[0]` — **the total is discarded for 5 call sites** | a short `/tasks` page is invisible to `_change_impact:3403` (which computes `removed_task_ids` and issues/withholds the code-change token), `_resolve_task:4142` / `_display_order` (a positional resolve over a subset graph looks unambiguous), `_mapped_in_closure:4642`, `_find_unaddressed_findings:3476`, `diagnose_dag:3162` |
| `:2542` | `rows_omitted` is written into the result and read by nobody | `_recurrence_clause` counts a "consecutive run(s)" streak and `_contrast_clause` concludes "no dispatched row elsewhere" over a 10-row clamp |
| `:2500` | `task_ids_omitted` likewise emitted, never read | |
| `:3114` | `event_history.pop("rows", None)` | the derived `status` survives, but no downstream reader can re-derive |
| `:4640-4645` | `contextlib.suppress(...)` around the `/tasks` `is_mapped` marker | the read's failure never reaches `settled:4653`; mitigated by the authoritative probe, which is why this is a design note not a defect |
| `:5861-5862` | `_duration_baseline` `except` returns `samples, source` with the pre-failure source string | the sample is silently one-legged |
| `:5860` | `resp["task_instances"]` — `total_entries` never read | see R9 |
| `:6738`, `:6977` | `resp.get(..., [])` — no count | see R12, R13 |
| `:3490`, `:7060` | `["assets"]` at `limit=100` — no count | see R10, R11 |
| `:355-362` | `_tail` clamps to 40 lines / 4000 chars and returns a bare `str` | no caller can tell truncated from whole |
| `:1855` | `rows_rejected` is computed and then never consulted by anything | |

### 3.5 converted to bool / None

| line | what | shape |
|---|---|---|
| `:2023-2028` | `earlier_attempt_executed` | `checked` → bool; `partial` + positive → `True`; else `None` |
| `:2433` | **`run_health["clean"] = not blockers`** | 15 completeness facts → one bool |
| `:4443-4448` | `earlier_executed` | `partial` + negative → `None` |
| `:4457` | `history_rules_out_partial = status == "checked" and earlier_executed is False` | tri-state → bool that gates a warning |
| `:4459-4460`, `:4470-4471`, `:4483-4484`, `:4494-4495` | `dispatched` / `partial_possible` | `bool \| None` |
| `:4576-4577` | `versions_status` → `original_version_listed` | string → bool, correctly gated |
| `:4653` | `settled = not unprobed` | list → bool |
| `:5241` | `_read_is_complete(...)` | three ints → **the bool that refuses the write** |
| `:5917`, `:5929` | `_downstream_dating_check` | `partial` + negative → `None`; otherwise the raw `consistent` bool |
| `:6051` | `_later_than(ended, started or "") is True` | `bool \| None` → bool, **`None` collapses to `False`** |
| `:6098` | `{"present": True, "empty": False, "no_logs_reported": False}.get(log["status"])` | 4 statuses → `bool \| None` |
| `:6177`, `:6191` | `recorded_output_post_dates_clear` | `partial` + no fresh → `None`; otherwise `bool(fresh)` |
| `:6208-6217` | `verdict` | two lists → `"verified"` / `"unverified"` |
| `:6418-6420` | **`verified`** | `bool(results) and not missing and not omitted` ∧ instance-set ∧ all verdicts |
| `:5639` | `mapped_settled` | three conjuncts → bool |

### 3.6 consumed to authorize a write

There is exactly **one** place where a completeness fact stops a mutation, and it is Phase A's:

| line | rule | what it consumes |
|---|---|---|
| **`:5241`** | **R1** | `_read_is_complete(len(now), delivered, claimed)` over the clear preview |
| `:5255` | R1b | `_identities(now) != plan["affected"]` |
| `:5276` | R2 | live states in `_IN_FLIGHT_TARGET_STATES` |
| `:5298` | R2b | state / try_number drift vs the plan |
| `:5316` | R3/R4 | `_version_drift` → R1's `omitted` (`:4240`) **and** R3's totals (`:4246`) |
| `:5339` | R5 | `set(expansion["tasks"]) - set(plan["mapped_tasks"])` |
| `:5356` | R6 | `expansion["settled"]` |
| `:5384` | R7 | `_attempt_reading(...)["status"] == "checked"` over the target's `/tries` |
| `:5554` | — | the single call site |
| **`:5561`** | — | the single mutating write |

Everything else that gates a mutation gates it on *identity or digest*, never on completeness:

| line | write | gate |
|---|---|---|
| `:3857` | `_write_if_unchanged` (dag code) | token, arg equality, asset-note equality `:3807`, flock `:3826`, parsed-source drift `:3828`, md5 vs plan `:3834`, `compile()` `:3844`. `_change_impact`'s reliance on R8 is **not** re-checked |
| `:3636` | `_write_if_unchanged` (revert) | token, diff equality `:3596`, flock, drift, md5 vs plan `:3625` |
| `:3639` | `backup.unlink()` | same transaction |
| `:4048` | `PATCH /dags/<d>` unpause | `_redeem_token("unpause")` |
| `:4053` | `POST /dagRuns` | `_validate_conf` |
| `:6948` | `POST /backfills` | token, arg equality, `_same_runs` ×2, `count > MAX_BACKFILL_RUNS`. R13 has no completeness |
| `:6990` | `PUT /backfills/<id>/cancel` | `_same_runs` over R12, which has no completeness |
| `:338` | `PUT /parseDagFile` | none — post-write reparse |

Plan-time refusals that *do* consume completeness (they withhold the token, so they are
pre-mutation in effect):

| line | what |
|---|---|
| `:4979-4987` | `plan_task_instance_clear` refuses when R1's `omitted` is non-zero |
| `:5014-5016` | refuses on `_version_drift` error (R1 + R3 incompleteness) |
| `:3725-3735` | `plan_dag_code_changes` withholds the token on `impact["blocking"]`; `_change_impact:3409` fails closed when `_tasks` **raises** — but not when it merely returns short |
| `:6856` | `plan_backfill` withholds past `MAX_BACKFILL_RUNS` |

---

## 4. Absence-shaped helpers

Everything that can return a negative meaning "not present". The column that matters is the last
one: **can it currently return `False` (or `None`/`[]`/`""` read as absence) on an incomplete read?**

| # | helper | line | negative | `False` on incomplete read? |
|---|---|---|---|---|
| 1 | `_read_is_complete` | `:5155` | `False` | **by design — this is the one whose `False` is the refusal** |
| 2 | `_carries_worker_field` | `:2616` | `False` | **yes** — over `_task_comparison` rows clamped to 10 with `rows_omitted` unread (`:2646`, `:2681`), and over `compare_dag_runs`' possibly-`omitted` instance list (`:6533`) |
| 3 | `_carries_execution_fields` | `:4401` | `False` | **yes** — `:4443` over `_attempt_reading` rows (guarded by status), `:5866` over R9 which has no status at all |
| 4 | `_event_association` | `:1473` | `None` | **yes** — `None` for every row, over a `kept` list that silently dropped `rows_rejected` rows, becomes `_ATTR_NONE` at `:1578` |
| 5 | `_is_never_dispatched_attempt` | `:744` | `False` | **yes structurally** (`:790` returns `False` when keys are absent) — compensated by `incomplete` at `:2161-2172`, not by the helper |
| 6 | `_tries_probe_tier` | `:807` | `None` | **yes structurally** (`:828`) — "no probe needed" and "couldn't tell" are the same `None`; compensated at `:2250` |
| 7 | `_find_import_errors` | `:584` | `[]` | **yes** — the `except` at `:599-600` returns a bare `[]`, indistinguishable from "no import errors" |
| 8 | `_downstream_task_ids` | `:2722` | `[]` | **yes** — edges come from R8, whose total is dropped |
| 9 | `_compared_rows_by_run` | `:2621` | `{}` | **yes** — R7's clamp |
| 10 | `_contrast_clause` | `:2663` | `""` | **yes** — `""` means "no dispatched row elsewhere", concluded over 5 task ids × 10 rows |
| 11 | `_recurrence_clause` | `:2634` | `""` | **yes** — a streak that runs off the end of a 10-run window reads as a streak that ended |
| 12 | `_impact_clause` | `:2737` | (never empty) | no |
| 13 | `_latest_version` | `:320` | `None` | marginal — `limit=1`, so `None` really is "no versions" |
| 14 | `_expandable_probe` | `:4595` | `False` | **no — deliberately correct**: `False` only on the explicit `is not mapped` detail (`:4615`); everything else is `None` |
| 15 | `_later_than` | `:5741` | `None` | no by itself; **but `:6051` collapses its `None` to `False`** inside the `legs` dict |
| 16 | `_approved_set_record` | `:6802` | `None` | **yes via eviction** — see #18 |
| 17 | `_peek_token` / `_redeem_token` | `:6760`, `:6776` | `None` | **yes via eviction** — see #18 |
| 18 | `_issue_token` eviction | `:6753` | — | `_TOKEN_MAX`=20 evicts the **oldest** token; `_APPROVED_SET_MAX`=20 (`:6794`) does the same to baselines. "No plan for this clear" (`:5483`) and "this server did not redeem a clear plan for it" (`:6261`) can both mean *evicted*, which is a bounded read of an in-memory store with no completeness at all |
| 19 | `_has_rest_audit_marker` | `:1261` | `False` | no — documented at `:1275-1278`; absence never removes what the name established |
| 20 | `_is_request_settable_targeting` | `:1317` | `False` | no — the union at `:1326` exists precisely to stop a lost `extra` producing `False` |
| 21 | `_is_json_object` | `:1414` | `False` | no |
| 22 | `_classification` | `:1502` | `None` | no |
| 23 | `_mentions_task` | `:3373` | `False` | no — regex over the complete patched source |
| 24 | `_plan_target` | `:4317` | `""` | no — `""` propagates to `target_resolved=False` at `:6410` |
| 25 | `_version_drift` | `:4230` | `(None, None)` | no — the second slot separates "no drift" from "couldn't tell" (`:4241`, `:4247`) |
| 26 | `_api_detail` | `:4829` | `""` | no — `""` routes to unknown-outcome, not to refused (`:5572`) |
| 27 | `_resolve_run` | `:4098` | `(None, msg)` | no — 403 kept distinct from 404 at `:4118-4132` |
| 28 | `_resolve_task` | `:4136` | `(None, "", err)` | inherits R8's dropped total |
| 29 | `_normalized_changes` | `:3291` | `None` | no |
| 30 | `_patch` | `:3306` | `(None, err)` | no |
| 31 | `_validate_conf` | `:3949` | `None` = valid | no |
| 32 | `_clear_flag_error` | `:4841` | `None` = ok | no |
| 33 | `_explain_unknown_dag` | `:175` | `None` = re-raise | no |
| 34 | `_same_runs` | `:6727` | `False` | inherits R12/R13's absent completeness |
| 35 | `_discard_same_source_plans` | `:6807` | `False` | no |
| 36 | `_backfill_runs` | `:6975` | `[]` | **yes** — `resp.get(..., [])` on a shapeless body |
| 37 | `_dry_run_backfill` | `:6732` | `[]` | **yes** — same shape |
| 38 | `_compute_asset_edges` | `:7021` | 4 × `[]` | **yes** — over a `limit=100` page with no total |
| 39 | `_attempt_rows` | `:4355` | `[]` | no by itself — `_attempt_reading` wraps it |
| 40 | `_strip_context_rows` / `_strip_extra_projection` | `:1910`, `:1919` | `False` = nothing stripped | no |
| 41 | `_recorded_output` | `:5760` | `total_entries: 0` on the three failure arms (`:5776-5787`) | guarded — `_verify_instance:6148-6157` and `_downstream_dating_check:5907` test the status before the zero |
| 42 | `_audit_transitions` | `:5805` | `pair_recorded` **absent** on the non-checked arm (`:5815`) | guarded at `:6106` |
| 43 | `_check` | `:5736` | `passed=None` | by design — `None` never counts as a pass (`:6209`, `:6217`) |
| 44 | `_display_order` | `:403` | `set(range(n))` on a short order | fails **closed** for a cycle (`:434-438`) — but a short `/tasks` page yields a *consistent* order over a subset with **no** ambiguity flagged |

**Count that answers the brief's question directly: 14 helpers can currently return a negative on
an incomplete read** (#2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #16/#17/#18, #36, #37, #38).

---

## 5. Pre-mutation consumers — what the write path depends on

### 5.1 The contained path (`apply_task_instance_clear`)

Ordered, `:5478` → `:5561`:

```
_peek_token("clear")            :5478   in-memory, evictable (§4.18)
_clear_flag_error               :5485   type check, no read
asked != planned                :5506   7-tuple equality against the plan
_redeem_token("clear")          :5525   approval spent
_clear_body(dry_run=True)       :5533
POST clearTaskInstances         :5544   the preview
_affected(preview)              :5545   -> now
_containment_gate               :5554 ─┬ R1  _read_is_complete(len(now), delivered, claimed)   :5241
                                       ├ R1b _identities(now) vs plan["affected"]              :5255
                                       ├ R2  in-flight states                                  :5276
                                       ├ R2b state/try_number drift                            :5298
                                       ├ R3/R4 _version_drift  -> R1 omitted + R3 totals       :5316
                                       ├ R5  newly-expandable                                  :5339
                                       ├ R6  expansion["settled"]                              :5356
                                       └ R7  _attempt_reading status == checked                :5384
POST clearTaskInstances         :5561   THE WRITE
```

Reads the *plan* rested on that the gate does **not** re-ask, and whose completeness therefore
never reaches the write:

| plan-time read | line | why it matters |
|---|---|---|
| `_recovery_evidence` → `_attempt_log` | `:5029` → `:4519` | the log clamp (`RECOVERY_LOG_TAIL_CHARS`) behind `current_attempt_dispatched` |
| `_version_context` | `:5081` | `versions_status: "partial"` is shown to the operator and never gates |
| `_recovery_warnings` | `:5083` | `partial_possible` is derived from `history_rules_out_partial:4457`, re-asked at R7 only for the *target* |
| `_clear_flags` | `:5075` | display |

Post-write, `:5629-5641` builds `mapped_tasks` / `mapped_settled` from the plan ∪ the pre-write
probe ∪ the write's own rows — deliberately **no** fresh HTTP call (`:5623-5628`).

### 5.2 The uncontained paths

| write | line | completeness consumed | completeness *relied on* but not consumed |
|---|---|---|---|
| `apply_dag_code_changes` | `:3857` | none | R8 via `_change_impact:3403` — `removed_task_ids` is computed against a task list whose total was dropped, and the plan-time `impact` is not recomputed at apply (`:3831-3834` substitutes the digest) |
| `revert_dag_code` | `:3636` | none | — (revert restores a byte-exact backup; no list read behind it) |
| `rerun_dag` | `:4048`, `:4053` | none | — |
| `run_backfill` | `:6948` | none | R13 (`_dry_run_backfill`) authorizes by identity with no count; R12 (`_backfill_runs`, `limit=51`, `+1` sentinel never checked) decides `created: True` vs `_abandon_backfill` |
| `_abandon_backfill` | `:6990` | none | R12 again, inside a bare `except` (`:7001`) |
| `_force_reparse` | `:338` | none | post-write |

---

## 6. Dependency graph

Full data in `dependency-graph.json`; narrative in `dependency-graph.md`.

- **157 nodes**, **365 edges**, **14 MCP tools**.
- **0 non-trivial strongly connected components.** The call graph is a DAG apart from one
  self-recursive def: `_bounded_extra_value` (`:1379`, depth-limited by its own `depth` parameter).
  There is no mutual recursion anywhere in the module.

- **0 defs unreachable from an MCP tool** (excluding `main`). Nothing is dead.

Fan-in leaders (most depended upon — the extraction blast radius):

| def | line | fan-in |
|---|---|---|
| `_api` | `:156` | 31 |
| `_dag_url` | `:170` | 21 |
| `_quoted` | `:703` | 14 |
| `_explain_error` | `:188` | 12 |
| `_explain_unknown_dag` | `:175` | 10 |
| `_ti_where` | `:685` | 10 |
| `_fenced` | `:725` | 9 |
| `DagFileError` | `:146` | 8 |
| `DagFileDriftError` | `:244` | 7 |
| `_ti_key` | `:681` | 7 |

Fan-out leaders (the orchestrators):

| def | line | fan-out | tool |
|---|---|---|---|
| `diagnose_dag` | `:3009` | 28 | yes |
| `plan_task_instance_clear` | `:4867` | 18 | yes |
| `apply_dag_code_changes` | `:3765` | 17 | yes |
| `revert_dag_code` | `:3575` | 16 | yes |
| `apply_task_instance_clear` | `:5444` | 15 | yes |
| `plan_dag_code_changes` | `:3655` | 12 | yes |
| `_containment_gate` | `:5218` | 11 | — |
| `_last_state_change` | `:1558` | 11 | — |
| `_verify_instance` | `:5938` | 11 | — |

The three defs that would carry a `Reading` type sit at fan-in 5–31 (`_run_task_instances` 5,
`_tasks_reading` 2 but `_tasks` 5, `_api` 31), and the reads that most need one
(`_recorded_output`, `_duration_baseline`, `_backfill_runs`, `_dry_run_backfill`,
`_compute_asset_edges`) all have fan-in ≤ 2 — i.e. the wrong derivations are in the *leaves*, not
in the shared core.

---

## 7. The four KNOWN-OPEN defects, located (inventoried, not fixed)

| # | defect | site | mechanism | what it can currently assert |
|---|---|---|---|---|
| 1 | `_recorded_output` derives completeness from the route's claimed total | `:5798` | form C: `len(entries) >= total`. `len(rows)` enters only as the default at `:5785`. `_read_is_complete` (`:5155`) is the correct derivation and is not called | `recorded_output_post_dates_clear: passed=False` (`:6191`) and `output_post_dates_the_task_it_reports_on: passed=False` (`:5929`) over records `rows[:10]` discarded |
| 2 | `diagnose_dag` asserts "no task instance in it failed" without consulting `omitted` | `:3217-3224` | `failed` is computed at `:3202` from `tis`; `omitted` (R1) is written to `result["task_instances_omitted"]` at `:3145-3146` and to `run_health.clean_blockers` at `:2384-2385`, but the `diagnosis` string is built from `failed` alone | the literal key called `diagnosis` says "no task instance in it failed" for a run whose instance list stopped at 500 |
| 3 | `compare_dag_runs` emits `worker_dispatched: False` and counts manufactured by a cap | `:6516-6535`, `:6551-6556` | `omitted` is captured at `:6508` and stored only as a per-run note (`:6517`); `info["worker_dispatched"]` starts `False` (`:6523`) and `info["count"]` counts only what was read. Neither `run_a_worker_field` / `run_b_worker_field` (`:6551-6552`) nor `run_a_instances` / `run_b_instances` (`:6555-6556`) is qualified by it | `False` for a task whose worker-bearing instance was past the 500 ceiling; an instance count that is the ceiling, presented as the fan-out |
| 4 | `duration_in_line_with_history` passes off a clamped sample with no completeness note | `:5833-5871` → `:6084-6091` | R9 reads `limit=RUN_HISTORY_LIMIT`=10 and never reads `total_entries`; the `except` at `:5861` returns the partial sample under the pre-failure `source` string | `passed=True/False` on a median over ≤10 samples, with `source` prose that names both legs whether or not the second one ran |

Two further sites in the same class, found by this inventory and **not** in the brief:

| # | site | mechanism |
|---|---|---|
| 5 | `_event_history` status ignores `rows_rejected` | `:1850` vs `:1837`/`:1855`. `status` is form B over `fetched`; the caller receives `kept`. A read that dropped rows reports `checked`, and `_last_state_change:1578` then returns `_ATTR_NONE` — an absence claim — over rows it discarded |
| 6 | `_task_comparison`'s `rows_omitted` has no consumer | created `:2528`, emitted `:2542`, read nowhere. `_recurrence_clause:2646` and `_contrast_clause:2681` both draw absence conclusions over the clamped rows |

---

## 8. What this inventory says about the shape of a fix

Stated as observation, not as a proposal to implement.

1. The correct derivation already exists, is named, and is used at **two** of the **nine** sites
   that need it (`:5155` itself and the inline re-derivation at `:4393-4397`). The remaining seven
   are the same arithmetic written from memory.

2. The three numbers form A needs — **kept**, **delivered**, **claimed** — are all in scope at
   every one of the nine sites. Not one of them is unavailable; five of the nine simply never bind
   `delivered` to a name (`_recorded_output:5785` fuses it into the default for `claimed`).

3. Completeness survives as a **bare `int`** (`omitted`), a **bare `str`** (`status`), a **bare
   `bool`** (`settled`), a **tuple slot** (`_tasks_reading[1]`), and a **dict key nobody reads**
   (`rows_omitted`). There is no shape a consumer is obliged to unpack, which is why `_tasks:400`
   can drop it with a `[0]` and why `rows_omitted` can exist for 5000 lines without a reader.

4. The 14 absence-shaped helpers in §4 that can return `False` on an incomplete read all take
   **rows**, never a reading. A helper handed `list[dict]` has no way to refuse.

5. The single containment gate proves the pattern works: eight rules, one call site, one write, and
   the refusal names the *read* rather than the conclusion. It covers one of the seven writes.
