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

- [Normalization rules applied to the frozen representative outputs](#normalization-rules-applied-to-the-frozen-representative-outputs)
  - [How to use these for a post-extraction diff](#how-to-use-these-for-a-post-extraction-diff)
  - [Value-level rules (applied to every string, at any depth, including dict keys)](#value-level-rules-applied-to-every-string-at-any-depth-including-dict-keys)
  - [Key-level rules (applied by field name, and they override the value rules)](#key-level-rules-applied-by-field-name-and-they-override-the-value-rules)
  - [What is deliberately NOT normalized](#what-is-deliberately-not-normalized)
  - [Read-only interlock (how these were taken without writing)](#read-only-interlock-how-these-were-taken-without-writing)
  - [Inventory](#inventory)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Normalization rules applied to the frozen representative outputs

Frozen at `b933d63ec34555feb6bb390dcc1752b014a69bfc`, against the live Airflow
3.4.0 deployment at `http://localhost:28080`.

These files exist so a later **move-only** extraction can be diffed against them.
A diff is only meaningful if everything that legitimately varies between two runs
of the same call has already been collapsed to a stable placeholder. Everything
below is collapsed; everything not listed below is **load-bearing** and a diff on
it means behaviour moved.

Producer: `/tmp/airy-gauntlet/phaseB/capture_outputs.py` (live captures) and
`/tmp/airy-gauntlet/phaseB/capture_refusal.py` (the offline refusal). Re-running
either regenerates the files in place.

## How to use these for a post-extraction diff

```bash
python /tmp/airy-gauntlet/phaseB/capture_outputs.py   # writes into a scratch copy first
diff -ru /tmp/airy-gauntlet/phaseB/freeze/representative-outputs <new-capture-dir>
```

Two caveats on interpreting the diff:

1. **The live deployment is not frozen.** These captures read mutable state. A
   new run of a fixture, a new audit row, or a Dag reparse changes the *inputs*,
   so a non-empty diff is not automatically a regression — check whether the
   changed field is derived from state that moved. The `gate0_*` fixtures are the
   most stable targets because they are forged, one-shot and not scheduled.

2. **`http_calls` is part of the contract.** The recorded request sequence is how
   an extraction that quietly adds, drops or reorders a read gets caught, even
   when the returned payload happens to look the same.

## Value-level rules (applied to every string, at any depth, including dict keys)

Applied in this order; earlier substitutions win.

| # | Pattern | Placeholder | Why it varies |
|---|---|---|---|
| 1 | RFC-4122 UUID | `<UUID>` | run/ti/version identity, regenerated per run |
| 2 | 64 lowercase hex chars | `<SHA256>` | content digests |
| 3 | 40 lowercase hex chars | `<SHA1>` | git/bundle versions |
| 4 | 32 lowercase hex chars | `<MD5>`|`source_digest`, the Dag-source hash |
| 5 | ISO-8601 datetime (`YYYY-MM-DD[T ]hh:mm:ss[.ffffff][±hh:mm\|Z]`) |`<TS>` | wall-clock |
| 6 | bare `YYYY-MM-DD`not already inside a placeholder |`<DATE>` | logical dates in run ids and log lines |
| 7 | `0x`+ 6 or more hex chars |`<ADDR>` | Python object addresses in tracebacks / log tails |
| 8 | `pid=`/`pid:`/`process:`+ digits | `pid=<PID>` | worker pid inside prose and log tails |
| 9 | `host=`/`hostname:`+ token | `host=<HOST>` | worker hostname inside prose and log tails |
| 10 | `<n> seconds\|secs\|ms\|minutes`|`<ELAPSED>` | durations rendered into prose |
| 11 | 12 lowercase hex chars | `<CONTAINERID>` | Docker short container id, which is what the worker hostname is here |

Rule 5 runs before rule 6 so a full timestamp never degrades into `<DATE>` plus a
time fragment. Rule 11 runs last because it would otherwise eat the tail of a
longer hex string; rules 2–4 have already consumed those.

## Key-level rules (applied by field name, and they override the value rules)

| Field names | Placeholder |
|---|---|
| `start_date`,`end_date`,`logical_date`,`run_after`,`data_interval_start`,`data_interval_end`,`queued_at`,`queued_when`,`scheduled_when`,`last_scheduling_decision`,`when`,`timestamp`,`created_at`,`updated_at`,`last_parsed_time`,`last_parsed`,`next_dagrun`,`next_dagrun_create_after`,`last_expired`,`triggerer_start`,`generated_at`,`as_of`,`captured_at`,`run_started_at`,`run_ended_at`,`last_updated`|`<TS>` |
| `duration`,`duration_seconds`,`seconds`,`elapsed`,`run_duration`,`median_duration`,`baseline_duration`,`delta`,`delta_seconds`,`duration_delta`,`a_duration`,`b_duration`,`longest_duration`,`run_a`,`run_b`|`<DURATION>` |
| `hostname`,`host`,`worker`,`worker_hostname`,`external_executor_id`|`<HOST>` |
| `pid`,`process_id`|`<PID>` |
| `plan_token`,`unpause_token`,`token`,`revert_token`|`<TOKEN>` |
| `source_digest`,`digest`,`md5`,`sha256`,`file_token`,`content_hash`|`<DIGEST>` |
| `id`,`log_id`,`event_id`,`backfill_id`,`dag_version_id`,`version_id`,`trigger_id`,`job_id`,`ti_id`,`task_instance_id`,`bundle_version`|`<ID>` |

`run_a`/`run_b`carry a **run-summary dict** at the top level of`compare_dag_runs`
and a **float duration** inside `task_durations[]`. The rule is type-gated, so the
dicts recurse normally and only the floats collapse. Their `dag_run_id`and`state`
therefore survive, which is what makes the two runs identifiable in a diff.

A `null`is left as`null` rather than replaced — the difference between "absent"
and "present but volatile" is exactly what several of the known-open defects turn
on, so it must survive normalization.

## What is deliberately NOT normalized

These vary between *tools* but not between *runs of the same tool*, and they are
the substance a diff has to be able to see:

- `dag_id`,`task_id`,`map_index`,`dag_run_id` — the named fixture run ids
  (`g0r1_ordered_001`,`gate0_drift_001`) are stable by construction. Generated
  `manual__<ts>`ids normalize through value rule 5 to`manual__<TS>`, which is
  the intended behaviour: the shape survives, the instant does not.

- Every `state`,`try_number`,`run_state`,`planned`,`cleared`,
  `mutation_applied`,`settled`,`clean` — the booleans and enums that carry the
  finding.

- All prose: `summary`,`diagnosis`,`error`,`next_step`,`warnings`,`limits`,
  `dag_version_limits`,`checks`. The deterministic summary is the product; a
  word changing in it is a behaviour change.

- Counts: `total_entries`,`events_scanned`,`events_omitted`,`failures_omitted`,
  `rows_rejected`,`attribution_payload_bytes`. **`attribution_payload_bytes` is
  intentionally left raw** even though it is a byte count, because it is derived
  from the payload the tool built and is therefore a fingerprint of that payload.

- The full Dag `source` text.
- `http_calls` — the recorded request sequence, see the caveat above.

## Read-only interlock (how these were taken without writing)

`server._api` was wrapped for the whole live capture. The wrapper raises before
reaching the wire on anything except:

1. any `GET`;
2. `POST /dags/<dag>/clearTaskInstances`**with`dry_run: true` in the body** —
   Airflow's own `action_logging` skips the audit row for exactly this case
   (`airflow-core/src/airflow/api_fastapi/logging/decorators.py:149,167`:
   `skip_dry_run_events = {"clear_dag_run", "post_clear_task_instances"}`), which
   is why `plan_task_instance_clear` can be captured live without moving the
   protected `post_clear_task_instances` count;

3. `POST /dags/~/dagRuns/~/taskInstances/list` — the POST-as-GET batch search
   route that `find_failure_clusters` uses. It reads rows and writes none, but it
   *does* carry `action_logging`, so it appends a`get_task_instances_batch` audit
   row per call. Two such rows were appended (13 → 15) and are disclosed in
   `../baseline-counts.txt`. It cannot touch either protected event.

Every recorded `http_calls` list in these files is the wrapper's own log, so the
absence of any other non-GET verb is checkable from the artifacts themselves.

The containment refusal in `containment_refusal.json` did not use the interlock at
all: `server._api`was replaced by a raiser, and the empty`http_calls` list is
the proof that neither refusal branch reached the network. Both branches return
from `_containment_gate`before the mutating write at`server.py:5561`.

## Inventory

| File | Call | Live? |
|---|---|---|
| `diagnose_dag__gate0_forge_ordered.json`|`diagnose_dag('gate0_forge_ordered', 'g0r1_ordered_001')` — the forged control | live |
| `diagnose_dag__gate0_drift_reconcile.json`|`diagnose_dag('gate0_drift_reconcile', 'gate0_drift_001')` — the healthy control | live |
| `diagnose_dag__gate0_empty_noop.json`|`diagnose_dag('gate0_empty_noop', 'gate0_r_gate0_empty_noop_001')` | live |
| `diagnose_dag__eval_fixture_claims_settlement_export.json`|`diagnose_dag('claims_settlement_export')` — paused evaluation fixture, **not unpaused, not triggered** | live |
| `compare_dag_runs__claims_settlement_export.json`|`compare_dag_runs('claims_settlement_export', 'previous', 'latest')`— two healthy runs,`latest`/`previous` resolution | live |
| `compare_dag_runs__gate0_forge_ordered.json`|`compare_dag_runs('gate0_forge_ordered', 'gate0_ordered_bulk_001', 'g0r1_ordered_001')`— the forged control, and the one capture that exhibits defect D3 (`run_a_worker_field`/`run_b_worker_field` false) | live |
| `get_blast_radius__gate0_forge_ordered.json`|`get_blast_radius('gate0_forge_ordered')` | live |
| `get_blast_radius__incident_triage.json`|`get_blast_radius('incident_triage')` | live |
| `find_failure_clusters__24h.json`|`find_failure_clusters(hours=24)` | live |
| `find_failure_clusters__720h_scoped.json`|`find_failure_clusters(hours=720, dag_ids=[...])` — the caller-scoped form | live |
| `plan_task_instance_clear__dryrun__gate0_version_drop.json`|`plan_task_instance_clear('gate0_version_drop', dag_run_id='gate0_drop_001', position=1)` — plan form only | live, dry-run only |
| `containment_refusal.json`|`_containment_gate(...)` R1 (truncated read) and R2 (expired evidence) | offline, fakes |
