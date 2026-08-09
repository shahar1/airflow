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

- [Phase B — TARGET MODULE ARCHITECTURE and Gate 4 v2 acceptance criteria](#phase-b--target-module-architecture-and-gate-4-v2-acceptance-criteria)
  - [0. Two environment constraints that decide the shape before anything else](#0-two-environment-constraints-that-decide-the-shape-before-anything-else)
  - [1. Target module architecture — 10 modules](#1-target-module-architecture--10-modules)
  - [2. Dependency-ordered extraction sequence](#2-dependency-ordered-extraction-sequence)
  - [3. Logic-change deferral list — executed in the typed-redesign stage, never during extraction](#3-logic-change-deferral-list--executed-in-the-typed-redesign-stage-never-during-extraction)
  - [4. The `Reading` sketch — design only, nothing committed](#4-the-reading-sketch--design-only-nothing-committed)
  - [5. Proposed Gate 4 v2 acceptance criteria](#5-proposed-gate-4-v2-acceptance-criteria)
  - [6. Risk register for the extraction](#6-risk-register-for-the-extraction)
  - [Summary](#summary)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Phase B — TARGET MODULE ARCHITECTURE and Gate 4 v2 acceptance criteria

**Analysis only. No production code was moved, edited, renamed, split or deleted. No `Reading`
was implemented. No extraction was begun.** Everything below is a proposal; the only files written
are in this directory.

- Subject: `/home/shahar/repos/apache/airflow/dev/airy_mcp/server.py`@`b933d63ec3`
- 7122 lines, **157 top-level definitions**, **365 edges**, **14 registered MCP tools**
- Inputs: `/tmp/airy-gauntlet/phaseB/freeze/` (characterization freeze),
  `/tmp/airy-gauntlet/phaseB/inventory/` (read/completeness inventory + dependency graph)

- Derivation scripts for the numbers in §1–§2: `partition.py`,`partition_v2.py`,`partition_v3.py`
  in this directory (they read `inventory/dependency-graph.json` and write nothing)

`git status --porcelain`at finish:`?? dev/g4adj3/`,`?? dev/rust_serde_poc/`,
`?? dev/serde_poc_bench.py` — the three pre-existing untracked paths, unchanged.

---

## 0. Two environment constraints that decide the shape before anything else

These are not preferences. They are measured facts about how this module is loaded, and each one
kills a whole family of otherwise-obvious designs.

### C1 — the sidecar is launched as a **script**, not a package

```
scripts/in_container/bin/generate_mprocs_config.py:239
scripts/in_container/bin/run_tmux:176
    python /opt/airflow/dev/airy_mcp/server.py --port 8001
```

`sys.path[0]`is`/opt/airflow/dev/airy_mcp`. There is no package context, so **relative imports
(`from . import reading`) raise`ImportError` in production**, and turning the directory into a
package (`__init__.py`,`python -m airy_mcp.server`) is a change to two launcher files outside
`dev/airy_mcp/` and to the deployed process command line — i.e. a redeploy, which is forbidden.

> **Consequence:** the target is **flat sibling modules in `dev/airy_mcp/`, imported absolutely**
> (`import reading`), with`server.py`keeping its name and its`main()`. No package. No
> `__init__.py`. No relative imports.

The test suite reaches the same conclusion from the other side: `test_server.py:38` is a bare
`import server`, resolved by pytest's rootdir insertion. A package would break all 659 sidecar node
ids at *collection*, which is the one thing the freeze forbids.

### C2 — the test suite's control point is the `server` module object

`test_server.py`performs **46`monkeypatch.setattr(server, "<name>", …)` calls over 24 distinct
names**, and references **94 distinct `server.X` attributes** in total.

| patched name | count | target module |
|---|---|---|
| `_api`| 10 |`transport` |
| `TASK_INSTANCE_DETAIL_LIMIT`| 4 |`evidence` |
| `_run_task_instances`,`_expandable_probe`| 3 each |`reading` |
| `DIAGNOSIS_LOG_BUDGET_CHARS`,`TASK_INSTANCE_SCAN_LIMIT`,`_read_reviewed_file`,`MAX_BACKFILL_RUNS`,`EVENT_SCAN_PAGE`,`EVENT_SCAN_LIMIT`| 2 each |`diagnosis`/`reading`/`dagsource`/`codechange`/`evidence` |
| `DAGS_DIR`,`REPARSE_TIMEOUT_S`,`TASK_INSTANCE_PAGE`,`_tasks`,`FAILURE_SCAN_LIMIT`,`_TOKEN_TTL_S`,`TRIES_PROBE_LIMIT`,`DISPATCH_FINDING_LIMIT`,`DIAGNOSIS_SUMMARY_BUDGET_CHARS`,`API_URL`,`TASK_COMPARISON_LIMIT`,`DISPATCH_CONTRAST_RUN_LIMIT`,`DISPATCH_IMPACT_TASK_LIMIT`,`_mapped_in_closure` | 1 each | various |

The distinction that matters, and it is absolute:

- **A name that is only *read* through `server.X`is safe under re-export.**`server.py` keeping
  `from primitives import _quoted, _fenced, …` preserves every one of the 94 read references.

- **A name that is *patched* on `server`is NOT safe under re-export.**`monkeypatch.setattr(server,
  "_api", fake)`rebinds`server._api`only. A feature module that did`from transport import _api`
  holds its own binding and never sees the fake — it calls the **real** `_api`, which calls
  `_login()`, which issues`httpx.post` to a live Airflow.

That last sentence is the single most dangerous thing in this whole extraction, and it is why §6
opens with it.

> **Consequence:** every call into a patchable collaborator must be a **late-bound module-attribute
> lookup** — `transport.api(...)`,`reading.run_task_instances(...)`— never`from x import y`.
> One module object, one binding, one patch point. Repointing `monkeypatch.setattr(server, "_api", …)`
> to `monkeypatch.setattr(transport, "_api", …)` changes a test **body**, not a test **identity**,
> so the freeze's contract (1185 node ids) survives it. That is exactly why the freeze froze node
> ids and not file bytes.

---

## 1. Target module architecture — 10 modules

Ordered by layer. Every edge in the table below is derived from the real call graph, not asserted:
run `python3 partition_v3.py` to reproduce.

```
                          server.py           (composition root, MCP registration, facade)
                              │
        ┌──────────┬──────────┼──────────┬───────────┐
     recovery   codechange  diagnosis    │           │
        │           │          │         │           │
        └───────────┴────┬─────┴─────────┘           │
                    evidence · dagsource · approvals │
                         └────────┬──────────────────┘
                               reading            ← the boundary
                                  │
                          transport · primitives
```

**Derived result: the module graph is a DAG.** Zero non-trivial strongly connected components, zero
upward edges under this layering. (`partition_v3.py`output:`non-trivial module SCCs: NONE`,
`upward (back) edges: NONE`.) That is not luck — §2 shows the one placement decision that buys it.

| module | defs | def-lines | tools | fan-in (modules) |
|---|---:|---:|---:|---:|
| `transport.py` | 6 | 61 | 0 | 6 |
| `primitives.py` | 19 | 118 | 0 | 6 |
| `reading.py` | 23 | 567 | 0 | 5 |
| `dagsource.py` | 28 | 506 | 0 | 3 |
| `evidence.py` | 24 | 1139 | 0 | 2 |
| `approvals.py` | 8 | 82 | 0 | 2 |
| `diagnosis.py` | 17 | 916 | **4** | 0 |
| `codechange.py` | 11 | 721 | **7** | 0 |
| `recovery.py` | 20 | 1766 | **3** | 0 |
| `server.py` | 1 | 8 | — | — |
| | **157** | **5884** | **14** | |

(5884 def-lines of 7122 file lines; the remaining ~1240 are module constants, the `_U*`/`_L*` legend
blocks at `:872-1240`, and section banners, which travel with the module that reads them.)

---

### 1.1 `transport.py` — the HTTP conversation with Airflow, and the words for its failures

**Responsibility.** Turn a method + path into a decoded response body, re-authenticating once on
401, and turn an exception into a string that is safe to relay.

**Moves in** (6 defs): `_login:150`,`_api:156`,`_dag_url:170`,`_explain_unknown_dag:175`,
`_explain_error:188`,`_api_detail:4829`. Module state`_token:145`; constants`API_URL:63`,
`USERNAME:64`,`PASSWORD:65`.

**Public surface.** `api(method, path, **kwargs)`,`dag_url(dag_id, suffix="")`,
`explain_error(e)`,`explain_unknown_dag(dag_id, e)`,`api_detail(e)`.
(During extraction the underscore names are kept verbatim — renaming is a separate, later change.)

**Must NOT know about.** Pagination. `total_entries`. Clamps. Any Airflow *concept* — it knows
paths and status codes, never what a task instance is. It must not import any other module in this
tree; its outgoing module edge count is **0**.

**Why it is not merged into `reading`.**`_api` has fan-in 31 and is the single most-patched name in
the suite. Isolating it makes the "who is allowed to talk to Airflow" question a one-line grep.

---

### 1.2 `primitives.py` — pure functions over one row or one string

**Responsibility.** Neutralise author-controlled text, key and describe a single task instance, and
answer single-row predicates — with no I/O and no notion of a list.

**Moves in** (19 defs): `_ti_key:681`,`_ti_where:685`,`_quoted:703`,`_fenced:725`,
`_clamped_operator:737`,`_clamped_event_text:1254`,`_clip_at_word:6589`,`_now_iso:4313`,
`_later_than:5741`,`_check:5736`,`_attribution_detail:1239`,`_attribution_sentence:1244`,
`_tagged_log:4412`,`_run_version:4092`,`_carries_worker_field:2616`,
`_carries_execution_fields:4401`,`_parsed_extra:1337`,`_parsed_extra_of:1404`,
`_is_json_object:1414`. Constants`_PROSE_UNSAFE:697`,`_FORGEABLE_NUMBERING:698`,
`_PROSE_LINE_BREAKS:701`,`OPERATOR_CLAMP_CHARS:106`,`EVENT_*_CLAMP_CHARS:130-141`.

**Public surface.** The 19 functions, all total, all deterministic, none raising.

**Must NOT know about.** HTTP. Lists. Completeness. It has **zero outgoing module edges** — the
only module in the tree with none.

**Completeness enforcement.** Vacuous and deliberately so: `primitives` never sees more than one
row, so there is nothing for it to count. `_carries_worker_field`and`_carries_execution_fields`
live here precisely because they are *row* predicates; the inventory's finding that they "take rows,
not a reading, so they have no way to refuse" (§4 #2/#3) is resolved not by changing them but by
making the **caller** unable to reach a negative except through `reading.none_match`. See §4.

**Note.** `_run_version:4092`,`_carries_worker_field:2616`and`_carries_execution_fields:4401`
are placed here for a graph reason, not an aesthetic one: leaving them in `codechange` /
`evidence`produces the`diagnosis → codechange`and`reading → evidence` back-edges that
`partition.py`reports. Moving these three (plus`_parsed_extra`,`_parsed_extra_of`,
`_is_json_object`) removes 4 of the 5 upward edges, move-only.

---

### 1.3 `reading.py` — **the boundary.** Every bounded read, and the only producer of completeness

**Responsibility.** Issue every list-shaped read against Airflow, follow its pages, apply every
clamp, and hand back a `Reading` whose completeness has already been derived — so that no caller
ever holds rows and a count at the same time.

**Moves in** (23 defs): `_read_is_complete:5155`,`_tail:355`,`_run_task_instances:365`,
`_tasks_reading:385`,`_tasks:398`,`_latest_version:320`,`_find_import_errors:584`,
`_attempt_history:841`,`_attempt_rows:4355`,`_attempt_reading:4377`,`_attempt_log:4326`,
`_recent_runs:2596`,`_task_comparison:2487`,`_recorded_output:5760`,`_duration_baseline:5833`,
`_version_context:4525`,`_expandable_probe:4595`,`_resolve_run:4098`,`_backfill_runs:6975`,
`_dry_run_backfill:6732`,`_build_asset_note:3483`,`_compute_asset_edges:7021`,
`_audit_transitions:5805`. Constants: every scan/page/limit in`:66-143` that bounds a list
(`TASK_INSTANCE_PAGE`,`TASK_INSTANCE_SCAN_LIMIT`,`EVENT_SCAN_PAGE`,`EVENT_SCAN_LIMIT`,
`RUN_HISTORY_LIMIT`,`TASK_COMPARISON_LIMIT`,`FAILURE_SCAN_LIMIT`,`RECOVERY_ATTEMPT_LIMIT`,
`DAG_VERSION_SCAN`,`LOG_TAIL_LINES`,`LOG_TAIL_CHARS`,`RECOVERY_LOG_TAIL_CHARS`,
`MAX_BACKFILL_RUNS`).

Four reads currently issued **inline inside tool bodies** relocate here as part of their tool's
wave (they are list reads by any definition, and three of them are `total_entries`-bearing):

| current site | what it reads | new home |
|---|---|---|
| `plan_task_instance_clear:4930`|`POST /clearTaskInstances`dry-run preview (`total_entries`) |`reading.read_clear_preview` |
| `apply_task_instance_clear:5544`| the same preview, immediately pre-write |`reading.read_clear_preview` |
| `find_failure_clusters:6656`|`POST /dags/~/dagRuns/~/taskInstances/list`,`page_limit=50`|`reading.read_failure_scan` |
| `get_blast_radius:7060`|`GET /assets?limit=100`|`reading.read_asset_catalog` |
| `diagnose_dag:3235`,`find_failure_clusters:6669`| task-instance log bodies (clamped by`_tail`) |`reading.read_log_tail` |

**Public surface.** One function per read, each returning `Reading`(or`Reading`-plus-context for
the two that carry a resolved id). Plus the `Reading`/`Verdict` types themselves and the two
combinators `find`/`none_match`. **`_read_is_complete` is NOT exported** — it becomes
`Reading.complete`, private to the type. That single decision is what turns the convention into an
invariant: there is no longer a function a feature module could call with the wrong three numbers.

**Must NOT know about.** What a diagnosis, a plan, a token or a clear is. It knows routes, page
sizes and clamps. It may import `transport`and`primitives` and nothing else. Derived: its only
outgoing edges are `reading → transport`(35) and`reading → primitives` (6).

**How the boundary is enforced structurally.** See §1.11.

---

### 1.4 `dagsource.py` — the Dag's source file: locate it, jail it, lock it, write it, read it

**Responsibility.** Everything that treats the Dag as *bytes on disk or text to parse*, including
the write jail and the atomic replace.

**Moves in** (28 defs): `DagFileError:146`,`DagFileDriftError:244`,`_dag_path:211`,
`_parsed_source:228`,`_exclusive:249`,`_write_if_unchanged:265`,`_backup_path:291`,
`_read_reviewed_file:304`,`_force_reparse:327`,`_display_order:403`,`_reachable:442`,
`_ambiguous_positions:460`,`_changed_lines_touch_assets:498`,`_referenced_task_ids:506`,
`_declared_task_ids:510`,`_static_checks:517`,`_definition_updates:3279`,
`_normalized_changes:3291`,`_patch:3306`,`_definition_count:3327`,`_decorator_root:3345`,
`_taskflow_definition_count:3354`,`_mentions_task:3373`,`_change_impact:3386`,
`_find_unaddressed_findings:3465`,`_build_revert_diff:3506`,`_resolve_task:4136`,
`_downstream_task_ids:2722`. Constants`DAGS_DIR:66`,`REPARSE_TIMEOUT_S:68`,
`STATIC_CHECK_LIMIT:100`, the asset/decorator regexes at`:486-496`.

**Public surface.** `dag_path`,`parsed_source`,`read_reviewed_file`,`write_if_unchanged`,
`backup_path`,`force_reparse`,`display_order`,`ambiguous_positions`,`static_checks`,
`change_impact`,`find_unaddressed_findings`,`build_revert_diff`,`resolve_task`,
`downstream_task_ids`,`normalized_changes`,`patch`, and the two exception classes.

**Must NOT know about.** Task instances, runs, events, tokens. It may issue the three **point**
reads it already owns (`_dag_path:217`→`GET /dags/<id>`,`_parsed_source:235` →
`GET /dagSources/<id>`) and the one write (`_force_reparse:338`→`PUT /parseDagFile`) — all three
are enumerated in the boundary allowlist as *non-list* routes.

**Completeness enforcement.** `dagsource`never calls`transport.api` for a paginated route, so it
never receives a `total_entries`to compare. Its one list dependency is`_tasks:398` (3 call sites:
`_change_impact:3403`,`_find_unaddressed_findings:3476`,`_resolve_task:4142`), which after the
extraction is `reading.read_tasks() -> Reading`.`_change_impact`and`_display_order` therefore
**receive a `Reading`, not a list**, and the D6/D7 deferrals below make their negatives
(`removed_task_ids`, an unambiguous topological order) go through`Verdict`.

---

### 1.5 `evidence.py` — what the platform's own records establish about an attempt

**Responsibility.** Two kinds of evidence about whether work actually happened — the event-log
reading (who is recorded as having asked for a state) and the dispatch reading (whether an attempt
carries the fields only a worker writes) — plus their shared vocabulary of what neither can
establish.

**Moves in** (24 defs): `_has_rest_audit_marker:1261`,`_classify_event:1283`,
`_is_request_settable_targeting:1317`,`_recorded_principal_kind:1329`,`_bounded_extra_value:1379`,
`_projected_extra:1424`,`_compact_event:1453`,`_event_association:1473`,`_classification:1502`,
`_bare_attribution:1517`,`_last_state_change:1558`,`_event_history:1755`,
`_attribution_bytes:1866`,`_enforce_attribution_ceiling:1871`,`_strip_context_rows:1910`,
`_strip_extra_projection:1919`,`_prune_unknowns_legend:1932`,`_attribution_reader:1950`,
`_is_never_dispatched_attempt:744`,`_tries_probe_tier:807`,`_dispatch_finding:1976`,
`_check_dispatch_evidence:2145`,`_project_task_instances:2261`,`_run_health:2352`.
Constants: the whole `_U1`–`_U15`/`_L1`–`_Ln`legend block`:872-1240`,`_DISPATCH_EVIDENCE_KEYS:645`,
`_TASK_INSTANCE_DETAIL_KEYS:657`,`_HISTORY_*:676-678`,`TASK_INSTANCE_DETAIL_LIMIT:87`,
`DISPATCH_FINDING_LIMIT:96`,`TRIES_PROBE_LIMIT:89`,`EVENT_HISTORY_PER_INSTANCE:126`,
`RUN_SCOPED_EVENT_LIMIT:127`,`COVERAGE_NAME_LIMIT:111`,`ATTRIBUTION_PAYLOAD_LIMIT_CHARS`.

**Public surface.** `event_history(dag_id, run_id, audit_scope)`,`attribution_reader(...)`,
`last_state_change(...)`,`check_dispatch_evidence(...)`,`project_task_instances(...)`,
`run_health(...)`,`is_never_dispatched_attempt(...)`,`dispatch_finding(...)`, and the
`_UNKNOWNS` legend.

**Must NOT know about.** Dag source text, tokens, plans, writes, or any tool's result shape.

**Completeness enforcement.** `evidence` holds **one** declared boundary exception —
`_event_history:1755`still calls`transport.api` directly, because breaking it apart is a logic
change (deferral **D5**). Until D5 lands the exception is a named entry in
`BOUNDARY_EXCEPTIONS` (§1.11), which the boundary check requires to be non-growing. Everything else
in `evidence`receives`Reading`s:`_check_dispatch_evidence:2202` takes
`reading.read_attempt_history(...) -> Reading`, and its 8-number`coverage`dict at`:2240-2256`
becomes 8 `Verdict`s rather than 8 hand-derived ints.

---

### 1.6 `approvals.py` — what the user was shown, and the single-use right it grants

**Responsibility.** Issue, peek at, redeem and evict the plan tokens and the approved-instance-set
baselines, and decide whether two run lists are the same list.

**Moves in** (8 defs): `_run_identity:6704`,`_same_runs:6727`,`_issue_token:6749`,
`_peek_token:6760`,`_redeem_token:6776`,`_record_approved_set:6793`,`_approved_set_record:6802`,
`_discard_same_source_plans:6807`. State`_issued_tokens:6744`,`_approved_clear_sets`; constants
`_TOKEN_TTL_S:6745`,`_TOKEN_MAX:6746`,`_APPROVED_SET_MAX`.

**Public surface.** `issue_token(kind, payload)`,`peek_token(kind, token)`,
`redeem_token(kind, token)`,`record_approved_set(...)`,`approved_set_record(...)`,
`discard_same_source_plans(...)`,`same_runs(a, b)`.

**Must NOT know about.** HTTP, Airflow, rows, completeness of a *read*. It is a bounded in-memory
store and nothing else.

**Completeness enforcement.** Inverted here, and this is the one place the inventory found a
completeness problem that is *not* about a list read: `_issue_token:6753` evicts the oldest token at
`_TOKEN_MAX`=20 and`_record_approved_set:6794` does the same at 20, so "no reviewed plan for this
clear" (`:5483`) and "this server did not redeem a clear plan for it" (`:6261`) can both mean
**evicted**. The store is a bounded read with no completeness signal at all. `approvals` is
therefore required to return `Verdict.UNKNOWN`— not`None` — when a lookup misses **and** the store
is at capacity or has evicted since process start. That is deferral **D9**; during extraction the
behaviour is preserved exactly and the eviction counter is merely recorded.

---

### 1.7 `diagnosis.py` — what is wrong with this run, and the prose that says so

**Responsibility.** The four read-only tools and the whole narrative layer they share.

**Moves in** (17 defs, **4 tools**): `_comparison_task_ids:2471`,`_run_history:2547`,
`_compared_rows_by_run:2621`,`_recurrence_clause:2634`,`_contrast_clause:2663`,
`_impact_clause:2737`,`_augment_dispatch_findings:2773`,`_summarize_failure:2851`,
`_census_clause:2863`,`_coverage_clauses:2877`,`_build_diagnosis_summary:2936`,
**`diagnose_dag:3009`**,`_extract_error_line:6598`,`_error_signature:6624`,
**`find_failure_clusters:6635`**, **`compare_dag_runs:6473`**, **`get_blast_radius:7053`**.
Constants `DIAGNOSIS_LOG_BUDGET_CHARS:76`,`DIAGNOSIS_SUMMARY_BUDGET_CHARS:103`,
`DISPATCH_CONTRAST_RUN_LIMIT:2611`,`DISPATCH_IMPACT_TASK_LIMIT:2614`.

**Public surface.** The four tool functions. Everything else is private to the module.

**Must NOT know about.** How to reach Airflow (its 8 current `transport` edges become
`reading` calls in wave 7), how a token works, how a file is written.

**Completeness enforcement — structural, three ways.**

1. `diagnosis`does not import`transport`. After wave 7 the string`transport.` does not appear in
   the file, so `total_entries` never enters its namespace. Checked by the import assertion.

2. Every list it holds arrives as a `Reading`whose`_delivered`/`_claimed` are private. There are
   no two numbers in scope to write a comparison over — the arithmetic that produced the seven wrong
   inline forms is not expressible here.

3. Every negative it emits (`_contrast_clause`'s`""`,`_recurrence_clause`'s`""`,
   `_compared_rows_by_run`'s`{}`,`compare_dag_runs`'`worker_dispatched: False`) must come from
   `reading.none_match(...) -> Verdict`and be rendered with`Verdict.as_field()`, which returns
   `None`on an incomplete read.`Verdict.__bool__`raises`TypeError`, so`if verdict:` and
   `bool(verdict)` are compile-time-visible and runtime-loud.

---

### 1.8 `codechange.py` — changes that are not a clear

**Responsibility.** The seven tools that repair source, revert it, trigger a fresh run, or create a
backfill, together with their conf validation.

**Moves in** (11 defs, **7 tools**): **`plan_dag_code_changes:3655`**,
**`apply_dag_code_changes:3765`**, **`plan_revert_dag_code:3517`**, **`revert_dag_code:3575`**,
**`rerun_dag:3991`**, **`plan_backfill:6829`**, **`run_backfill:6875`**,`_abandon_backfill:6980`,
`_describe_bounds:3911`,`_describe_params:3933`,`_validate_conf:3949`. Constants
`MAX_BACKFILL_RUNS:70`, the apply-time drift sentence at`:3757`.

**Public surface.** The seven tool functions.

**Must NOT know about.** Event-log epistemics, dispatch evidence, diagnosis prose.

**Completeness enforcement.** It keeps 5 enumerated **write** calls (`rerun_dag:4048`,`:4053`,
`run_backfill:6948`,`_abandon_backfill:6990`, and`apply_dag_code_changes`' filesystem write via
`dagsource`) plus 4 enumerated **point** reads (`GET /dags/<id>`at`:3531`,`:3606`,`:3818`,
`:4013`and`GET /dags/<id>/details`at`:4022`). It has **no list read of its own** after
`_dry_run_backfill:6732`and`_backfill_runs:6975`move to`reading`. Because those two currently
derive nothing at all (inventory R12/R13), `run_backfill:6952`'s`created: True` decision and
`_abandon_backfill:6998`begin consuming a`Reading` the moment the move lands — but they must
**not** change their answer during extraction. The behaviour change is deferral **D8**.

---

### 1.9 `recovery.py` — clear an instance that already exists, and prove the work came back

**Responsibility.** The plan → containment gate → apply → verify pipeline for a task-instance
clear. This is the module the whole gauntlet is about.

**Moves in** (20 defs, **3 tools**): `_clear_body:4165`,`_affected:4213`,`_identities:4225`,
`_version_drift:4230`,`_plan_target:4317`,`_recovery_evidence:4425`,`_mapped_in_closure:4622`,
`_recovery_warnings:4656`,`_clear_flags:4719`,`_clear_flag_error:4841`,
**`plan_task_instance_clear:4867`**,`_incomplete_read:5171`,`_expired_evidence:5196`,
`_containment_gate:5218`,`_clear_outcome_unknown:5401`, **`apply_task_instance_clear:5444`**,
`_downstream_dating_check:5881`,`_verify_instance:5938`,`_approved_instance_set_check:6238`,
**`verify_task_instance_recovery:6315`**. Constants: the containment banner`:5126-5140`,
`_NOTHING_CLEARED:5143`,`_DO_NOT_BYPASS:5146`,`_NO_EXPANSION:5152`,
`_IN_FLIGHT_TARGET_STATES:4802`, the clear-route status sets at`:4808-4826`,
`RECOVERY_ATTEMPT_LIMIT`(shared with`reading`— it lives in`reading`,`recovery` reads it).

**Public surface.** The three tool functions, plus `containment_gate(...)` exported **only** so a
test can call it directly (the suite exercises it).

**Must NOT know about.** Dag source text beyond what `dagsource.resolve_task` hands it; diagnosis
prose; how a backfill works.

**Completeness enforcement.** This module keeps exactly **two** `transport` calls — both writes /
write-previews on the mutating path (`apply_task_instance_clear:5544`and`:5561`) — and after wave 8
the `:5544`preview is`reading.read_clear_preview(...) -> Reading`, so the gate's R1 at`:5241`
stops calling `_read_is_complete(len(now), delivered, claimed)` with three loose ints and starts
reading `preview.complete`. The single mutating write at`:5561` is the only remaining bare
`transport.api`in the module, and it is`dry_run=False` — a write, not a read.

`_incomplete_read:5171`and`_expired_evidence:5196` — the two refusal shapes that name the *read*
rather than the conclusion — stay here and become the template every other module's UNKNOWN
rendering points at.

---

### 1.10 `server.py` — composition root and facade

**Responsibility.** Import the nine modules, re-export every public name the test suite and any
external caller reference through `server.X`, register the 14 tools with`FastMCP`, and run.

**Keeps** (1 def): `main:7111`, the`mcp = FastMCP("airy-selfheal")`instance at`:143`, the
registration loop at `:7090-7108`, and the module docstring.

**Public surface.** The 14 tools plus a re-export block covering all **94** `server.X` names the
suite touches.

**Must NOT know about.** Anything. It contains no logic — after the extraction it is imports,
re-exports, the registration tuple, and `main()`. Target size: under 200 lines.

---

### 1.11 How "feature modules may consume completeness but never recreate it" is enforced

Four mechanisms. All four are properties of the code's shape, and all four are mechanically
checkable — none is a convention or a review note.

| # | mechanism | what it makes impossible | how it is checked |
|---|---|---|---|
| **S1** | **No transport handle.** Feature modules (`diagnosis`,`recovery`,`codechange`,`evidence`,`dagsource`) do not`import transport`; the name`transport`is absent from their namespace except at sites listed in`BOUNDARY_EXCEPTIONS`. | A feature module cannot obtain a raw response body, so it never sees`total_entries`. There is nothing to count. | AST scan over each module for`Import`/`ImportFrom`of`transport`and for any`Attribute`access`transport.*`; every hit must match a line in`BOUNDARY_EXCEPTIONS`. Prek hook, runs on every commit. |
| **S2** | **The numbers are private to the type.** `Reading`stores`_delivered`and`_claimed`; the only public completeness member is the`complete`property.`_read_is_complete`is deleted as a free function. | The seven wrong inline forms (B at`:1850`,`:865`,`:628`,`:2528`,`:2571`,`:6660`,`:4576`; C at`:5798`) are not expressible: a consumer has no second number to compare against. | mypy (`Reading`fields are name-mangled /`_`-prefixed and not in`__all__`), plus an AST scan that the identifiers`total_entries`,`_delivered`,`_claimed`appear in no module other than`reading.py`. |
| **S3** | **Negatives cannot be spelled by hand.** `Verdict`is a three-state tagged union with`__bool__`raising`TypeError`.`Verdict.as_field()`is the only conversion to a JSON value and returns`None`for`UNKNOWN`. |`worker_dispatched: False`(`:6551`),`passed: False`(`:6191`,`:5929`),`""`from`_contrast_clause:2663`,`{}`from`_compared_rows_by_run:2621`cannot be produced over an incomplete read — the field comes out`None`. | Runtime:`__bool__`raises, so any surviving truthiness test fails a test immediately. Static: an AST scan asserting that every dict key in the negative-claim registry is assigned from a`.as_field()` call. |
| **S4** | **Clamping is a method of the type, not a slice.** `Reading.clamp(n)`returns a **new**`Reading`with`rows`reduced and`complete`recomputed. A bare`reading.rows[:10]`yields a plain`tuple`, and`find`/`none_match`take a`Reading`, not a sequence — so a sliced list can no longer be used to draw a negative at all. | The Round-3 FATAL exactly:`_recorded_output:5790`clamping`rows[:RECOVERY_ATTEMPT_LIMIT]`**after**`:5798`computed the status;`_attempt_rows:4367`clamping after`_attempt_history:865`. | mypy rejects`find(tuple, …)`. Plus a property test: for every`Reading`-producing function,`f(...).clamp(k).complete`is`False`whenever`k < delivered`. |

**S2 deserves the emphasis.** The inventory's finding (§8.3) is that completeness currently survives
as a bare `int`, a bare`str`, a bare`bool`, a tuple slot and a dict key nobody reads — *"there is
no shape a consumer is obliged to unpack, which is why `_tasks:400`can drop it with a`[0]`."*
Making `complete`a property of the only type that carries rows is what removes the`[0]`: after the
move there is no tuple to index, so `_tasks:398`'s one-line body cannot drop anything.

---

## 2. Dependency-ordered extraction sequence

Nine waves. Each wave is **move-only**: whole definitions relocate, call expressions change from
`f(...)`to`mod.f(...)`, and nothing else. After every wave the full verification set in §6 runs.

Ordering is by two derived criteria, in this priority: **(a) outgoing module edges — a module with
none can leave first; (b) how many of its names the test suite monkeypatches on `server`** — because
per §0/C2 a patched name is the only kind that can silently stop controlling the code.

| wave | module | out-edges | patched names it takes | why here |
|---:|---|---:|---:|---|
| 1 | `primitives.py` | **0** | **0** | Zero dependencies, zero patch exposure. 19 pure functions, 118 lines. The smallest possible blast radius in the whole file, and it proves the sibling-import + re-export mechanism (C1/C2) before anything load-bearing rides on it. |
| 2 | `transport.py`| 0 | **1** (`_api`, ×10 sites;`API_URL` ×1) | Also zero dependencies, but it carries the single most-patched name. Doing it second means the re-export mechanism is already proven, so a failure here is unambiguously about the patch repointing and not about the packaging. |
| 3 | `approvals.py`| 1 (`primitives`) | 1 (`_TOKEN_TTL_S`) | Self-contained in-memory store; 8 defs, 82 lines; consumed only by`codechange`and`recovery`. Its one patched name fails **loud** if repointing is missed (`test_expired_tokens_are_not_redeemable:3279`asserts`is None` and would get a payload). |
| 4 | `reading.py`| 2 (`transport`,`primitives`) | 4 (`_run_task_instances`×3,`_tasks`×1,`_expandable_probe`×3, plus 8 scan/page constants) | **The point of the whole exercise.** Everything below it is now in place. Extracted *without*`_event_history` (see cycle C-1). Largest single wave at 23 defs / 567 lines — but its consumers have not moved yet, so their call sites are the only churn. |
| 5 | `dagsource.py`| 3 (`transport`,`primitives`,`reading`) | 3 (`_read_reviewed_file`×2,`DAGS_DIR`,`REPARSE_TIMEOUT_S`) | Depends on`reading`only for`_tasks`(3 sites) and`_latest_version` (1 site), all now available. Carries the write jail, so it moves as one piece with its lock and its atomic replace. |
| 6 | `evidence.py`| 4 (`transport`†,`primitives`,`reading`) | 2 (`TASK_INSTANCE_DETAIL_LIMIT`×4,`EVENT_SCAN_*`×4) | † the one declared exception (`_event_history`). 24 defs / 1139 lines including the`_U*`/`_L*` legend block, which is constants and moves with zero call-site churn. |
| 7 | `diagnosis.py`| 5 | 3 (`DIAGNOSIS_LOG_BUDGET_CHARS`×2,`DIAGNOSIS_SUMMARY_BUDGET_CHARS`,`FAILURE_SCAN_LIMIT`,`TASK_COMPARISON_LIMIT`,`DISPATCH_*_LIMIT`) | First tool-bearing module. Read-only tools only — nothing here can mutate, so a mistake in this wave cannot write. That is deliberate: it exercises the tool-relocation mechanics on the safe half. |
| 8 | `codechange.py`| 4 | 1 (`MAX_BACKFILL_RUNS`×2) | Seven tools, five of them writing. Moves after`diagnosis` has proven tool relocation. |
| 9 | `recovery.py`| 6 | 1 (`_mapped_in_closure`) | **Last, deliberately.** It holds the only pre-mutation containment gate in the module. *(Superseded 2026-08-09: this line also called it "the only mutating write the gauntlet certified". It is not — `recovery.py`'s tools are **withdrawn** and uncertified; the certified writes are `apply_dag_code_changes`, `revert_dag_code` and `rerun_dag` in `codechange.py`.)* Everything it needs already sits in its final home, so its wave is a pure lift with no ordering pressure on anything else. |

**Wave 10 (not a move):** `server.py` shrinks to the composition root. This is the wave where the
re-export block is finalised and the 94-name facade is asserted complete.

### 2.1 Cycles the graph shows

`partition.py` (the naïve placement) reports **two** non-trivial module SCCs' worth of back-edges;
`partition_v3.py` (the proposed placement) reports **none**. The difference is three decisions:

| # | cycle / back-edge | edges | break | move-only? |
|---|---|---|---|---|
| **C-1** | `reading ↔ evidence`—`_event_history:1755`calls`_compact_event:1453`, while`_check_dispatch_evidence:2145`calls`_attempt_history:841`| 1 up | **Placement.**`_event_history`is assigned to`evidence.py`, not`reading.py`, and keeps its direct`transport.api`call as the single entry in`BOUNDARY_EXCEPTIONS`. | **Yes — move-only.** The proper break (split the read from the row shaping) is a **logic change** and is deferred as **D5**. Until D5, the cycle is not broken by moving code; it is avoided by not creating it. |
| **C-2** | `reading → evidence`—`_duration_baseline:5833`calls`_carries_execution_fields:4401`| 1 up | Move`_carries_execution_fields`(a 2-line row predicate) to`primitives`. | **Yes — move-only.** |
| **C-3** | `diagnosis → codechange`—`_run_history:2547`,`compare_dag_runs:6473`and`diagnose_dag:3009`all call`_run_version:4092`| 3 up | Move`_run_version`(a 3-line dict accessor) to`primitives`. | **Yes — move-only.** |
| **C-4** | `reading → evidence`—`_event_history:1755`calls`_parsed_extra:1337`and`_parsed_extra_of:1404`| 2 up | Move both (pure JSON parsing) to`primitives`. Subsumed by C-1's placement but recorded because it recurs the moment D5 splits`_event_history`. | **Yes — move-only.** |

**Within-module recursion:** `_bounded_extra_value:1379` is self-recursive, depth-limited by its own
`depth` parameter. It is not a cycle between modules and needs no break.

**No cycle in this file requires a logic change to break during extraction.** The one that would
(C-1) is avoided by placement, and its real fix is deferred. That is the honest statement: the
extraction can be completed as a pure move.

### 2.2 What each wave verifies before the next begins

1. `git diff --stat` shows only file moves plus qualified call expressions — **no changed
   conditional, no changed literal, no changed return shape**. A `--word-diff` review of every hunk.

2. Sidecar suite: **659 collected, 659 passed**, and the node-id list is byte-identical to
   `freeze/test-identities.txt` (the ids, not the bodies).

3. Plugin **246**, frontend **280** — untouched, re-run as a tripwire.
4. `prek run mypy-dev --all-files`→ Passed.`ruff check`+`ruff format --check` clean.
5. All 12 normalized captures in `freeze/representative-outputs/` reproduce **byte-identical** under
   the same normalization rules.

6. The boundary check (S1) passes and `BOUNDARY_EXCEPTIONS` has not grown.
7. `git checkout -- uv.lock` if the check run re-resolved it (freeze §8 documents this).

---

## 3. Logic-change deferral list — executed in the typed-redesign stage, never during extraction

Fifteen items. Each is a **behaviour** change: it makes some tool answer differently on some input.
None may be applied while a wave is in flight, and none may be smuggled in as "tidying".

The rule for the whole list: **during extraction the wrong answer must be preserved exactly.**
`compare_dag_runs__gate0_forge_ordered.json` in the freeze is the capture that exhibits D3 in the
payload (`run_a_worker_field`/`run_b_worker_field` false on two tasks); if it stops exhibiting it
during a move wave, the wave changed behaviour and must be reverted.

### 3.1 The four KNOWN-OPEN defects from the brief

| id | site | defect | intended fix |
|---|---|---|---|
| **D1** | `_recorded_output``server.py:5798`(with`:5785`,`:5801`) | Form **C**:`status = "checked" if len(entries) >= total`.`entries`is post-clamp (`rows[:RECOVERY_ATTEMPT_LIMIT]`at`:5790`);`len(rows)`— what the route delivered — enters only as the *default* for`total`at`:5785`and is never compared.`_read_is_complete:5155`exists and is not called. Can assert`recorded_output_post_dates_clear: passed=False`(`:6191`) and`output_post_dates_the_task_it_reports_on: passed=False`(`:5929`) over records it discarded. | Return`Reading(rows=kept, delivered=len(rows), claimed=resp.get("total_entries"))`.`complete`is then form **A** by construction.`:6191`and`:5929`obtain their booleans from`reading.none_match(...).as_field()`, which yields`None`on`complete is False`. |
| **D2** | `diagnose_dag``server.py:3217-3224`| Builds the key literally named`diagnosis`from`failed`(`:3202`) alone and asserts *"no task instance in it failed"*.`omitted`is available at`:3145-3146`and is already a`run_health``clean_blocker`at`:2384-2385`. A run whose instance list stopped at`TASK_INSTANCE_SCAN_LIMIT`=500 gets the all-clear. | The instance list arrives as a`Reading`. "No task instance failed" is`none_match(reading, is_failed)`—`ABSENT`only when`reading.complete`, otherwise`UNKNOWN`, which renders as the "was not read whole" sentence instead of the all-clear. |
| **D3** | `compare_dag_runs``server.py:6516-6535`,`:6551-6556`|`omitted`is captured at`:6508`, stored only as a per-run note at`:6517`.`info["worker_dispatched"]`is initialised`False`at`:6523`;`run_a_worker_field`/`run_b_worker_field`(`:6551-6552`) and`run_a_instances`/`run_b_instances`(`:6555-6556`) are unqualified by it.`False`for a task whose worker-bearing instance sat past the 500 ceiling; an instance count that is the ceiling, presented as the fan-out. | Both fields become`Verdict.as_field()`over the run's`Reading`; the counts become`{"counted": n, "complete": bool}`or`None`. Preserve the *positive* direction unchanged — a measured`True`on a complete read must stay`True`. |
| **D4** | `_duration_baseline``server.py:5833-5871`→`duration_in_line_with_history``:6084-6091`| Reads`limit=RUN_HISTORY_LIMIT`=10 and never reads`total_entries`(form **D**). The`except`at`:5861-5862`additionally returns the partial sample under the **pre-failure**`source`string, so a one-legged sample is described as two-legged. | Return a`Reading`; the median check renders`passed=None`when`not complete`. Split the`source` string so the second leg is named only if it ran. |

### 3.2 The two further sites of the same class found by the inventory

| id | site | defect | intended fix |
|---|---|---|---|
| **D5** | `_event_history``server.py:1850`vs`:1837`/`:1855`|`status`is form **B** over`fetched`; the caller receives`kept`after a`dag_id`filter (`:1837`) that discards rows into`rows_rejected`(`:1855`) — **created and consulted by nothing**. A read that dropped rows reports`checked`, and`_last_state_change:1578`then returns`_ATTR_NONE`(an absence claim) over rows it discarded. | Split into`reading.read_event_log(...) -> Reading`(pages, dag_id filter as a`Reading.filter`that reduces`kept`) and`evidence.shape_event_history(reading)`(the`_compact_event`projection, the legend, the run-scoped slice). **This is also the fix that breaks cycle C-1 and deletes the sole`BOUNDARY_EXCEPTIONS` entry.** |
| **D6** | `_task_comparison``server.py:2528`/`:2542`|`rows_omitted`is created and emitted and read by **no consumer**, while`_recurrence_clause:2646`counts a consecutive-run streak and`_contrast_clause:2681`concludes "no dispatched row elsewhere" over a 10-row-per-task, 5-task clamp.`task_ids_omitted:2500`likewise. | Both clauses take a`Reading`; their`""`returns become`Verdict.UNKNOWN` rendered as the "not read whole" sentence. |

### 3.3 The rest, from the inventory's §3 lifecycle table

| id | site | defect | intended fix |
|---|---|---|---|
| **D7** | `_tasks``server.py:400`|`return _tasks_reading(dag_id)[0]`— the total is discarded for **5** call sites. A short`/tasks`page is invisible to`_change_impact:3403`(which issues or withholds the code-change token),`_resolve_task:4142`/`_display_order:403`(a positional resolve over a subset graph looks unambiguous),`_mapped_in_closure:4642`,`_find_unaddressed_findings:3476`,`diagnose_dag:3162`. | Delete`_tasks`; the five callers take the`Reading`.`_display_order`must flag ambiguity when`not complete`— it currently fails closed for a *cycle* (`:434-438`) but produces a confident order over a subset. |
| **D8** | `_backfill_runs:6975-6977`,`_dry_run_backfill:6732-6738`| Form **D**.`_backfill_runs`asks`limit=MAX_BACKFILL_RUNS+1`=51 — an overflow sentinel that is **never checked** — and`run_backfill:6952`uses the result to authorise`created: True`versus`_abandon_backfill`.`_dry_run_backfill`'s identity compare at`:6919`authorises the write. | Both return`Reading`s;`run_backfill`refuses (pre-mutation,`_incomplete_read`-shaped) rather than abandoning on an incomplete list. |
| **D9** | `_issue_token:6753`,`_record_approved_set:6794`| Bounded in-memory stores that evict the **oldest** entry at 20 with no completeness signal. "No reviewed plan for this clear" (`:5483`) and "this server did not redeem a clear plan for it" (`:6261`) can each mean *evicted*. | Record an eviction counter; a miss while the counter is non-zero is`UNKNOWN`, not absence. |
| **D10** | `find_failure_clusters:6660`|`failures_omitted`is computed **before** the allowlist filter rebinds`tis`at`:6663-6665`, so it does not describe the list finally used. | Derive from the post-filter`Reading`. |
| **D11** | `_tail:355-362`,`_attempt_log:4342`| Clamp to 40 lines / 4000 chars (and`[-600:]`) and return a bare`str`. No caller can tell a truncated log from a whole one;`_extract_error_line`,`_error_signature`and`log_for_new_attempt:6098`all conclude over it. | Return`(text, truncated)`;`log_for_new_attempt`renders`None` when truncated and no marker was found. |
| **D12** | `_find_import_errors:599-600`| The`except`arm returns a bare`[]`, indistinguishable from "no import errors".`:628`reports truncation only as prose. | Return a`Reading`with`error` set; the diagnosis renders UNKNOWN. |
| **D13** | `_containment_gate`/`_identities:4225-4227`(Round-4 MAJOR 2) |`_identities`discards`state`and`try_number`, and the **plan's own in-flight refusal is never re-asked** before the POST. Airflow refuses only`running`, so`queued`/`scheduled`clear through — the double-dispatch case the plan's rule exists to prevent. The gate's R2/R2b at`:5276`/`:5298` cover part of this; the *plan-time* rule set and the *gate* rule set are not the same set. | A single declared registry of write preconditions; the gate iterates it. §5 criterion **N7** makes the two sets provably equal. |
| **D14** | `_enforce_attribution_ceiling:3115`|`event_history["instances_without_attribution"] = detail_reduced`writes a **projection** completeness number onto the **event-history** payload, where a reader attributes it to the event scan. Likewise`coverage["static_checks_suppressed"]`at`:3180`, written from outside`_check_dispatch_evidence`which initialised it at`:2256`. | Each completeness number is owned by the`Reading` it describes; no cross-writing. **Landed:**`instances_without_attribution`is gone and the count travels as`task_instance_detail_reduced`on the diagnosis. The four`attribution_payload_*`keys still sit on`event_history`, deliberately: every`last_state_change`they measure is that same read's projection onto one instance, so it is one read accounting for its own projection rather than cross-writing. |
| **D15** | `_projected_extra:1448-1449`,`_bounded_extra_value:1387`|`extra_truncated`/`extra_keys_omitted`are a *field-level* completeness with the same ad-hoc shape as the list-level one. Out of scope for`Reading`as sketched, but the same class. | A`Clamped[T]` sibling type, or an explicit decision to leave field-level clamps as display-only and prove no conclusion is drawn over them. |

**Count: 15 deferred logic changes** (D1–D15), of which 4 are the brief's KNOWN-OPEN defects, 2 are
same-class sites the inventory found, and 9 are the residue of the lifecycle audit.

---

## 4. The `Reading` sketch — design only, nothing committed

### 4.1 The type

```text
# reading.py — sketch, NOT committed

@dataclass(frozen=True, slots=True)
class Reading:
    """Rows that were kept, and whether they are all of them.

    Three numbers, because three different things truncate a list: the route's
    own paging (_claimed above _delivered), this tool's clamps (kept below
    _delivered), and a source whose count is lower than what it handed over.
    """
    rows: tuple[Mapping[str, Any], ...]   # what was KEPT — the only rows anyone sees
    route: str                            # named in every refusal
    _delivered: int                       # len() of what the route handed over, BEFORE our clamps
    _claimed: int | None                  # the route's total_entries, or None if it did not say
    error: str | None = None              # set when the read did not happen at all

    @property
    def complete(self) -> bool:
        if self.error is not None:
            return False
        universe = self._delivered if self._claimed is None else max(self._claimed, self._delivered)
        return len(self.rows) >= universe

    def clamp(self, n: int) -> Reading:
        """A clamp is a method, never a slice: completeness is recomputed."""
        return replace(self, rows=self.rows[:n])

    def filter(self, keep: Callable[[Mapping], bool]) -> Reading:
        """A discard is a method too — dropped rows reduce kept, exactly like a clamp."""
        return replace(self, rows=tuple(r for r in self.rows if keep(r)))
```

The derivation is the one the brief names and the one `_read_is_complete:5155` and
`_attempt_reading:4393-4397` already implement:

```
complete  ==  len(rows_kept) >= max(claimed_total, len(rows_returned))
```

`clamp`and`filter`are the whole of S4:`_recorded_output:5790`(`rows[:10]`),
`_attempt_rows:4367`(`rows[:10]`) and`_event_history:1837`(the`dag_id` discard) become method
calls that **recompute**, so the Round-3 FATAL — a clamp applied after the status was computed —
cannot be written.

### 4.2 The three outcomes

```text
class Outcome(Enum):
    PRESENT = "present"   # a row satisfying the question was found
    ABSENT  = "absent"    # no such row, AND the list was read whole
    UNKNOWN = "unknown"   # no such row, but the list was NOT read whole

@dataclass(frozen=True, slots=True)
class Verdict:
    outcome: Outcome
    row: Mapping[str, Any] | None = None
    route: str = ""
    why: str = ""                     # for UNKNOWN: which read fell short, and how far

    def __bool__(self) -> NoReturn:
        raise TypeError(
            "a Verdict is three-valued; use .as_field(), .is_present() or match on .outcome"
        )

    def as_field(self) -> bool | None:
        """The ONLY conversion to a JSON value. UNKNOWN is None, never False."""
        return {Outcome.PRESENT: True, Outcome.ABSENT: False}.get(self.outcome)
```

### 4.3 The two combinators — the only way from rows to a negative

```text
def find(reading: Reading, question: Callable[[Mapping], bool], why: str) -> Verdict:
    hit = next((r for r in reading.rows if question(r)), None)
    if hit is not None:
        return Verdict(Outcome.PRESENT, row=hit, route=reading.route)   # presence survives truncation
    if not reading.complete:
        return Verdict(Outcome.UNKNOWN, route=reading.route, why=why)
    return Verdict(Outcome.ABSENT, route=reading.route)

def none_match(reading, question, why) -> Verdict:   # the inverse, same guard
    ...
```

`find`takes a`Reading`— **not** a`Sequence`. That is the structural claim.

### 4.4 Why absence-shaped helpers become *structurally* unable to return ABSENT on an incomplete read

The inventory catalogues **45** absence-shaped helpers, of which **14 can currently return a
negative on an incomplete read**, and diagnoses why: *"the 14 helpers that can return `False` on an
incomplete read all take **rows**, never a reading. A helper handed `list[dict]` has no way to
refuse."* The sketch removes each of the four ways a negative can currently be reached:

1. **A negative cannot be constructed outside `find`/`none_match`.**`Outcome.ABSENT` is produced
   at exactly two places in the tree, both inside `reading.py`. What sits behind the completeness
   guard is **the branch that claims the whole universe was examined**, and in the two combinators
   that is a different branch: in `find` it is ABSENT ("nothing matched, and everything was
   looked at"), and in `none_match` it is PRESENT ("nothing matched, and everything was looked
   at") — `none_match`'s ABSENT is a counterexample row that was *found*, which is a presence and
   survives truncation. The earlier wording said both ABSENT branches sit behind the guard, which
   is false of `none_match` and describes an invariant the code does not hold.
   There is no`Verdict(Outcome.ABSENT)` call site anywhere else — checkable by an AST scan, which
   now guards the OUTCOME rather than one call shape (`reading.Verdict(reading.Outcome("absent"))`
   and `replace(verdict, outcome=...)` both passed the one-line version).

2. **A negative cannot be reached by ignoring the type.** `Verdict.__bool__` raises. The two idioms
   that produce today's silent hard negatives — `if not rows_matching:` and
   `{"worker_dispatched": bool(...)}` — either explode at runtime or fail to serialise. There is no
   quiet path.

3. **A negative cannot be reached by slicing around the type.** `reading.rows[:10]` is a plain
   `tuple`, and`find` does not accept one. A caller that clamps by slicing loses the ability to
   draw *any* conclusion — which is the correct incentive, since slicing is exactly what
   `_recorded_output:5790` did.

4. **A negative cannot be reached by re-deriving completeness.** `_delivered`and`_claimed` are
   private and absent from `__all__`;`_read_is_complete` no longer exists as a free function. The
   three numbers form A needs are, per the inventory, *in scope at all nine sites today* — after
   this they are in scope at exactly one.

Concretely, for the load-bearing helpers the inventory names:

| helper | today | after |
|---|---|---|
| `_carries_worker_field:2616`|`bool(row.get("hostname")) or row.get("pid") is not None`— over`_task_comparison`rows clamped to 10 with`rows_omitted`unread | stays a **row** predicate in`primitives`; the *aggregate* question becomes`none_match(comparison_reading, carries_worker_field, …)`, so the aggregate`False`is unreachable while`not complete` |
| `_carries_execution_fields:4401`| same shape, used at`:5866`over`_duration_baseline` which has no status at all | same |
| `_event_association:1473`|`None`for every row over a`kept`list that silently dropped`rows_rejected`rows →`_ATTR_NONE`at`:1578`|`reading.filter(...)`makes the discard reduce`kept`, so`complete`goes`False`and the association is`UNKNOWN` (D5) |
| `_compared_rows_by_run:2621`/`_contrast_clause:2663`/`_recurrence_clause:2634`|`{}`/`""`/`""`read as absence |`Verdict.UNKNOWN` rendered as the "not read whole" sentence (D6) |
| `_display_order:403`| a confident, unambiguous topological order over a subset when`/tasks`was short | takes the`Reading`; ambiguity flagged when`not complete` (D7) |
| `_find_import_errors:599`| bare`[]`from the`except`arm |`Reading(error=…)`,`complete`is`False` (D12) |
| `_backfill_runs:6975`/`_dry_run_backfill:6732`/`_compute_asset_edges:7021`|`[]`on a shapeless body / a`limit=100`page with no total |`Reading`s; four empty lists in`get_blast_radius` stop meaning both "no declared edge" and "catalog truncated" (D8) |

**The one behaviour that must not change:** presence-based conclusions survive a truncated list.
`find`returns`PRESENT`before it ever consults`complete`. The file states this rule itself, in
comments at `:866-867`and`:6178-6179` — *"Presence-based conclusions survive a truncated list;
absence-based ones do not"* — and Round 4 specifically verified there was **no over-correction** — `rows=10 sought=99`
still `False`on a *complete* read, presence under a clamped read still`True`. Criterion **N5b**
below preserves that in both directions.

---

## 5. Proposed Gate 4 v2 acceptance criteria

### 5.0 Inherited unchanged

Every original Gate 4 criterion carries over verbatim, with no weakening, reinterpretation or
removal. Restated from the record so a fresh adjudicator can check them without reconstructing them:

| id | inherited criterion (from GAUNTLET.md §Gate 4) | still mechanically checkable as |
|---|---|---|
| **I1** | Recovery restores the work, not the colour — a cleared instance comes back with a real `try_number`,`hostname`,`pid`,`duration`, the artefact XCom written, and a downstream regenerated so it post-dates what it reports on | live re-run on`g4adj3_recovery/filing`; assert the five fields non-null and the timestamp ordering |
| **I2** | Write authorisation binds — bad token, widened task set, flipped `only_failed`, replay after a successful apply all refused, with the plan surviving where it should | the existing suite cases plus the live replay probe |
| **I3** | Mapped task-group detection resolves via the `listMapped` probe; the plan carries "MAY CREATE" and "NOT closed"; the apply returns the not-established strings | live, on the mapped-group fixture |
| **I4** | No network call after the write — every post-write call forced to 500 and the result still reads `cleared: True, mutation_applied: True, error: None` | the existing forced-500 test |
| **I5** | The set leg cannot be lied to — honest list, post-clear reality, and a one-item lie return byte-identical output with `baseline_source` naming the server-side record | the existing three-way test |
| **I6** | Pre-mutation 4xx says "not applied"; mid-flight failure still says "unknown" | both directions, existing tests |
| **I7** | Binding constraints: no metadata/audit mutation, no fabricated evidence, no permission bypass, no destructive git/filesystem action, no push/PR/issue/email/disclosure, no restart or redeploy | `git status --porcelain`; protected audit counts |

### 5.1 New criteria — one per round-failure mode, plus the extraction's own

Every criterion below states its check as something a script can run.

---

**N1 — Exactly one completeness derivation exists in the tree.**
*Catches: the root cause of all four rounds — nine sites, four mutually incompatible forms.*

Check: an AST scan asserts that the expression pattern `len(X) >= Y`/`Y > len(X)` /
`max(Y - len(X), 0)`where`Y`derives from`total_entries` occurs in **exactly one** place —
`Reading.complete`. Baseline today: **9 sites, 4 forms** (A at`:5155`and`:4393`; B at`:1850`,
`:865`,`:628`,`:2528`,`:2571`,`:6660`,`:4576`; C at`:5798`; D — absent — at`:400`,`:3490`,
`:5860`,`:6738`,`:6977`,`:7060`,`:355`). Target: **1 site, 1 form.** The scan prints the census;
a census above 1 fails the gate. Run as a prek hook.

---

**N2 — Nothing outside the read layer talks to the transport.**
*Catches: the "one more site" pattern — a new read added anywhere re-creates the class.*

Check: an AST scan over the nine modules for `import transport`/`transport.*`. Every hit must
appear verbatim in `dev/airy_mcp/BOUNDARY_EXCEPTIONS`, a checked-in file whose every line carries a
deferral id. The file **must not grow**: the check compares against the committed line count and
fails on any increase. Baseline at the end of extraction: **1 read exception**
(`evidence._event_history:1755`, D5), **3 point-read exceptions** (`dagsource:217`,`:235`;
`codechange``GET /dags/<id>`×4), **6 write exceptions** (`dagsource:338`,`codechange:4048`,
`:4053`,`:6948`,`:6990`,`recovery:5561`). Target after D5: **0 read exceptions.**

---

**N3 — Every list read returns a `Reading`.**
*Catches: Round 4 MAJOR 1 (`_recorded_output` deriving from the claimed total) and Round 3's FATAL.*

Check: for every function in `reading.py`whose body contains a`transport.api` call, mypy asserts
the return annotation is `Reading` or a tuple containing one. Additionally: a scan asserts no
function in `reading.py`returns a bare`list`/`tuple[list, int]`/`dict`carrying a`status`
string. Baseline: **24 bounded reads, 11 deriving completeness, 7 deriving nothing, 6 deriving and
losing it.** Target: **24 / 24 / 0 / 0.**

---

**N4 — No hard negative is spelled by hand.**
*Catches: Round 4 MAJOR 3/4 — `worker_dispatched: False`, "no task instance failed".*

Check: (a) a runtime test asserts `bool(Verdict(...))`raises`TypeError`; (b) an AST scan asserts
every assignment into a key in the *negative-claim registry* — a checked-in list seeded with
`worker_dispatched`,`passed`,`settled`,`dispatched`,`clean`,`earlier_attempt_executed`,
`original_version_listed`,`recorded_output_post_dates_clear`,`verified`,`cleared_matches_plan`,
`mapped_tasks_settled`— has a`.as_field()` call on its right-hand side. Baseline: **15 sites
convert completeness to bool/None by hand** (inventory §3.5). Target: **0 hand conversions.**

---

**N5 — The truncation sweep: truncating a read may never flip a claim.**
*This is the criterion that would have caught **all four rounds** without naming a single site.*

Check: a property test that, for every one of the 14 registered tools × every read that tool
reaches, runs the tool twice against the `FakeAirflow` double —

- **run C** with the read complete;
- **run T** with the identical data but the read truncated, in each of the three ways the API can
  produce: `claimed > delivered`(route paged),`delivered > kept`(our clamp),`claimed < delivered`
  (a lying total);

— and asserts, over a recursive walk of both result JSONs:

- **N5a (no manufactured negative):** no key goes `True → False`, no key goes non-empty → empty, and
  no key goes non-null → `False`. A key may go`True → None`,`False → None`, or value →`None`.
  Any other transition fails.

- **N5b (no over-correction):** no key goes `False → None` when the read in run T is still
  `complete`(the exact-limit and limit-plus-one cases), and no key goes`True → None` at all —
  presence survives truncation.

- **N5c (the omission is named):** the result of run T contains, somewhere, the route name of the
  read that was truncated.

Every one of D1–D4 fails N5a today. `_recorded_output` fails it with 15 records / 10 read; the
`/tries`12-attempt case fails it with the sought row last;`diagnose_dag` fails it at the 500
ceiling; `compare_dag_runs`fails it on`worker_dispatched`. The test is generated from the tool
registry, so **a tool added later is swept automatically** — which is the structural answer to
"iteration will keep finding one more site."

---

**N6 — Every write is dominated by a pre-mutation gate.**
*Catches: the gauntlet's own observation that the containment gate covers 1 of 7 writes.*

Check: an AST dominance analysis — for every `transport.api` call with a method in
`{POST, PATCH, PUT, DELETE}` that is not in the read-search allowlist, plus every
`dagsource.write_if_unchanged`and`Path.unlink`, assert that every path from the enclosing
function's entry to that call passes through a call to a function registered in the
`WRITE_GATES`registry. Baseline: **1 of 7 writes gated on completeness** (`:5561`via`:5554`);
the other six gate on identity or digest only (`:3857`,`:3636`,`:4048`,`:4053`,`:6948`,
`:6990`,`:338`). Target: **7 of 7.**

---

**N7 — The gate re-asks every fact the plan refused on.**
*Catches: Round 4 MAJOR 2 — the plan's in-flight refusal never re-asked; `_identities` discarding
`state`and`try_number`.*

Check: both the plan-time refusal predicates and the gate rules are entries in one declared registry
with an `asked_at: {"plan", "gate"}` field. A test asserts
`{r for r in REGISTRY if "plan" in r.asked_at} ⊆ {r for r in REGISTRY if "gate" in r.asked_at}`,
and a second test asserts every rule's predicate is actually invoked when the gate runs (by
instrumenting the registry). Today the two sets differ by at least four members — the inventory's
"NOT RE-ASKED BY THE GATE" list: `_recovery_evidence:5029`(and its`_attempt_log` clamp at
`:4519`),`_version_context:5081`,`_recovery_warnings:5083`,`_clear_flags:5075`.
Baseline: **plan-time rules not re-asked = 4.** Target: **0.**

---

**N8 — Freeze conformance: the extraction changed nothing observable.**
*Catches: an extraction that quietly repaired or broke a defect while claiming to be move-only.*

Check, all mechanical:

- Sidecar / plugin / frontend node-id lists byte-identical to `freeze/test-identities.txt`
  (**1185** ids: 659 + 246 + 280). Counts alone are not enough — Gate 4 v1 checked counts.

- All **12** normalized captures in `freeze/representative-outputs/` reproduce byte-identical under
  the recorded rules, **including** `compare_dag_runs__gate0_forge_ordered.json`, which must still
  exhibit D3 (`run_a_worker_field`/`run_b_worker_field` false on two tasks) at the end of
  extraction and must **stop** exhibiting it only in the D3 commit of the redesign stage.

- `tool-schemas.json`: the 14 signatures and the sha256 of each docstring unchanged through the
  extraction.

- `prek run mypy-dev --all-files`Passed;`ruff check`and`ruff format --check` clean.
- `git checkout -- uv.lock` afterwards if the check run re-resolved it (freeze §8).

---

**N9 — The live decisive checks from Gates 1–4 re-run green.**

Check: the Gate 1 detector fires on `g0r1_ordered_001`and stays silent on`gate0_drift_001` /
`gate0_drift_reconcile`/`gate0_empty_noop`; the Gate 2 forgery sweep still yields only
`audited_other_state_action`/`no_event_found`/`other_recorded_event` /
`platform_execution_event`; the Gate 3 payload checks; the Gate 4 I1–I6 above. Scripted, not
performed by hand.

---

**N10 — Protected audit invariants, measured at the database.**

Check: non-`g4*``patch_task_instance`= **66** and`post_clear_task_instances` = **4**, **read via
psql, not the REST API.** The freeze established that `GET /api/v2/eventLogs` reports the
*post-filter* `total_entries`and omits 22 rows whose`dag_id` no longer resolves (58/45/2008 over
the wire vs 80/47/2496 in the DB), so a REST measurement of this invariant is itself an instance of
the class under repair. Any adjudicator re-measuring must use psql.

---

**N11 — The three tool-count / doc facts are reconciled.** ⚠️ **SUPERSEDED 2026-08-09.**

~~Check: `dev/airy_mcp/README.md`'s tool table lists **14** tools, including
`verify_task_instance_recovery`(`server.py:6315`, added by`fe6333c55c`). The freeze found the doc
still lists 13. A test asserts the registration tuple and the README table agree.~~

A 14-tool README is no longer a passing criterion — it is now a **failing** one. Five tools were
withdrawn from the registered surface (`verify_task_instance_recovery` among them); the
registration tuple, `TOOL_POLICY` and the README table each carry **ten** names, and
`test_every_withdrawn_tool_is_unreachable_and_still_implemented` is what holds that shut. The
part of N11 that survives is the *shape* of the check: the registration tuple and the README
table must agree, whatever the number is.

---

**Passing bar.** N1–N11 all green, **and** I1–I7 all green, **and** the deferral list is empty of
D1–D8 (D9–D15 may remain open with tracking). N5 is the criterion with veto power: it is the one
that generalises, and a Gate 4 v2 that passes everything except N5 has reproduced Gate 4 v1.

---

## 6. Risk register for the extraction

Ordered by how quietly the failure happens. "Quiet" is the axis that matters: a loud failure costs
an hour, and this file's entire history is quiet failures.

| # | risk | how it changes behaviour silently | specific guard |
|---:|---|---|---|
| **R1** | **A monkeypatch stops reaching the code under test.** `test_server.py:363`does`monkeypatch.setattr(server, "_api", fake)`. If any module does`from transport import _api`, that module keeps its own binding and calls the **real**`_api`→`_login()`→`httpx.post`at`AIRFLOW_API_URL`. | Worst case in the file. A test that "passes" may have hit the live deployment; a bare`except`(`_mapped_in_closure:4640`,`_abandon_backfill:7001`,`_duration_baseline:5861`,`_event_history:1829`) can swallow the connection error and return the *fallback* answer, so the assertion still holds for the wrong reason. | **(a)** A session-scoped autouse fixture that replaces`httpx.request`/`httpx.post`with a raiser, so *any* escape to the network is loud. Adding a fixture adds no node id, so`freeze/test-identities.txt`is preserved. **(b)** Call convention:`transport.api(...)`, never`from transport import api`— enforced by the N2 AST scan. **(c)** A per-wave canary test that patches the new target and asserts the moved function observes it. **(d)** Diff review of every one of the 46`setattr` sites against the 24-name table in §0/C2. |
| **R2** | **A module-level constant is patched in one place and read in another.** e.g. `TASK_INSTANCE_PAGE`moves to`reading`but`monkeypatch.setattr(server, "TASK_INSTANCE_PAGE", 2)`(`:749`) still patches`server`. | The pagination test silently exercises the *unpatched* 100-per-page path and passes for the wrong reason — the loop under test never iterates. 14 of the 24 patched names are constants. | Each of the 14 constants gets a **negative canary**: a test that patches the new home to an absurd value and asserts the tool result changes. If the canary does not change, the patch is not reaching. Run in the same wave that moves the constant. |
| **R3** | **A `from X import *`or a re-export shadows a later rebinding.**`server.py`'s facade re-exports 94 names; a name re-exported by value freezes at import time. |`server._issued_tokens`re-exported by value would give the autouse`fresh_token_store`fixture (`test_server.py:373`) a *different dict* from the one`approvals`mutates — tokens leak across tests and a plan from test A authorises test B. | Mutable module state (`_issued_tokens`,`_approved_clear_sets`,`_token`) is **never** re-exported by value.`server.py`exposes it as a property or the fixture is repointed to`approvals`. A dedicated test asserts`server._issued_tokens is approvals._issued_tokens`. |
| **R4** | **A move quietly repairs a defect.** Re-typing `_recorded_output`'s return while moving it, or "fixing" the obvious`len(rows)`omission at`:5785`in passing. | The extraction reports "move-only, all green" while D1 silently closed — and the redesign stage then cannot demonstrate the fix, and the adjudicator cannot tell which stage changed the behaviour. |`freeze/representative-outputs/compare_dag_runs__gate0_forge_ordered.json` must still exhibit D3 at the end of extraction — the freeze author chose it for exactly this. Plus: the N5 truncation sweep is run at the end of extraction and must still **FAIL** on D1–D4 with the same failure set as the baseline. A wave that reduces the N5 failure set has changed behaviour. |
| **R5** | **A tool's docstring changes.** The docstrings are the model's instructions and the freeze hashed each one. Reflowing a docstring while moving a function changes what a small model relays. | The demo's summary prose changes with no code change visible in a diff review. | `freeze/tool-schemas.json` carries a sha256 per docstring; N8 asserts all 14 unchanged through the extraction. |
| **R6** | **`_read_is_complete`is deleted before its callers move.** It is used at`_containment_gate:5241`— the only place a completeness fact stops a mutation. | The write loses its guard silently:`_read_is_complete`returning something truthy by accident (e.g. a moved-but-not-imported name resolving to a module) would authorise a clear over a partial preview. |`_read_is_complete`moves **whole and unchanged** in wave 4 and is not touched again until the redesign. A dedicated test asserts`recovery`refuses on`kept < max(delivered, claimed)` for all three truncation shapes, and it is run after **every** wave, not just wave 4. |
| **R7** | **Import order changes when module state initialises.** `_token = None`at`:145`,`_issued_tokens`at`:6744`. A circular import at load time resolves to a partially-initialised module. | A tool that works in the suite fails in production at first call, or worse re-authenticates per call. | The module graph is a **DAG** with zero SCCs (derived, §1). A test asserts`import server`succeeds from a cold interpreter and that`python dev/airy_mcp/server.py --help`exits 0 — the latter is the only check that exercises the real C1 script-mode`sys.path`. |
| **R8** | **The production launch command breaks.** Anything that turns `dev/airy_mcp/`into a package breaks`python /opt/airflow/dev/airy_mcp/server.py --port 8001`. | Discovered only at demo time, and fixing it means a redeploy, which is forbidden. | Flat siblings, absolute imports, no`__init__.py`(C1). Checked by the`--help`exit-0 test in R7 and by asserting no`__init__.py`exists under`dev/airy_mcp/`. |
| **R9** | **`uv.lock`churn is committed.** Running`uv run`/`prek run mypy-dev`re-resolves the workspace and rewrote two provider versions (clickhousedb 1.0.1→1.0.0, opensearch 1.11.2→1.12.0). | An unrelated dependency change rides in on a "move-only" commit. | Freeze §8 documents it; every wave ends with`git status --porcelain`and`git checkout -- uv.lock`if it appears.`git diff --stat`on the wave commit must list only`dev/airy_mcp/` paths. |
| **R10** | **A live capture mutates the deployment.** Re-running the representative outputs for N8 issues real calls. | Audit rows appear; the protected 66/4 invariant moves; the evaluation fixtures unpause or trigger. | Reuse the freeze's own interlock: `_api`wrapped so anything but a GET raises before the wire, with the two audited exceptions already disclosed —`plan_task_instance_clear`(adds no row:`action_logging`skips`post_clear_task_instances`when the body carries`dry_run`,`api_fastapi/logging/decorators.py:149,167`) and`find_failure_clusters`(a POST-as-GET search that appends exactly 2`get_task_instances_batch`rows — not a protected event). Verify 66/4 **via psql** before and after (N10). Never unpause`claims_settlement_export`,`freight_ledger_reconcile`,`subscriber_usage_rating`. |
| **R11** | **`dev/g4adj3/`or the rust-POC paths are disturbed.** | Evidence for a closed gate is lost, and the working-tree proof stops being provable. |`git status --porcelain`must show exactly`?? dev/g4adj3/`,`?? dev/rust_serde_poc/`,`?? dev/serde_poc_bench.py`after every wave.`dev/g4adj3/` file count and hashes asserted unchanged (10 files). |
| **R12** | **`BOUNDARY_EXCEPTIONS` grows.** The easy way to make a wave green is to add a line. | The boundary becomes a convention again, which is the exact failure mode the redesign exists to fix. | The N2 check compares against the committed line count and fails on any increase. Every line carries a deferral id, and a line with no matching open deferral fails the check. |

---

## Summary

**Module list (10):** `transport`·`primitives`·`reading`·`dagsource`·`evidence` ·
`approvals`·`diagnosis`·`codechange`·`recovery`·`server` (composition root).
`reading` is the boundary: it owns transport pagination and is the sole producer of completeness;
the five feature modules consume it and are structurally prevented from recreating it by four
mechanisms (no transport handle · private `_delivered`/`_claimed`·`Verdict.__bool__` raising ·
`clamp`/`filter` as methods rather than slices).

**Extraction order (9 waves):** `primitives`→`transport`→`approvals`→`reading`→`dagsource`
→ `evidence`→`diagnosis`→`codechange`→`recovery`, then`server` shrinks to the facade.
`primitives` goes first: zero outgoing module edges and **zero** monkeypatched names — the smallest
blast radius in the file.

**Cycles:** the naïve placement shows **4 back-edge groups (5 edges)**; three (C-2, C-3, C-4) are
broken **move-only** by relocating six pure row/JSON helpers to `primitives`. The fourth (**C-1**,
`reading ↔ evidence`via`_event_history:1755 → _compact_event:1453`) requires a **logic change** and
is therefore **deferred (D5)**; it is avoided during extraction by placing `_event_history` in
`evidence`behind the single`BOUNDARY_EXCEPTIONS` entry. With those placements the module graph is
a **DAG: zero non-trivial SCCs, zero upward edges** (derived by `partition_v3.py`).

**Deferral count: 15** logic changes (D1–D15) — the brief's four KNOWN-OPEN defects (D1
`_recorded_output:5798`, D2`diagnose_dag:3217`, D3`compare_dag_runs:6551`, D4
`duration_in_line_with_history:6084`), two same-class sites the inventory found (D5`_event_history`,
D6 `_task_comparison`), and nine from the lifecycle audit. **None may be executed during extraction.**

**Gate 4 v2:** inherits I1–I7 unchanged and adds N1–N11, all mechanically checkable. **N5** — the
truncation sweep, generated from the tool registry, asserting that truncating a read may never flip
a claim in either direction — is the criterion with veto power and the one that would have caught
all four rounds without naming a single site.

**Working tree:** `git status --porcelain`shows exactly`?? dev/g4adj3/`,`?? dev/rust_serde_poc/`,
`?? dev/serde_poc_bench.py` — unchanged. No production code was moved, edited, renamed, split or
deleted; no `Reading` was implemented; no extraction was begun.
