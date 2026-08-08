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

- [Airy self-healing MCP (summit demo)](#airy-self-healing-mcp-summit-demo)
  - [Setup (Breeze)](#setup-breeze)
  - [Showcase: incident_triage and incident_digest](#showcase-incident_triage-and-incident_digest)
  - [Demo run-book](#demo-run-book)
  - [Deliberate shortcuts](#deliberate-shortcuts)
  - [Tests](#tests)
  - [The Dag-processor question, and the long-term answer](#the-dag-processor-question-and-the-long-term-answer)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->
# Airy self-healing MCP (summit demo)

A second MCP sidecar with the **write-capable** tools that
`astro-airflow-mcp` deliberately does not have. Every mutation is planned first:
the planning tool is read-only and returns a single-use `plan_token`, and the
writing tool refuses without it — so the approval card shows the change the user
is actually approving.

| Tool | What it does |
|---|---|
| `diagnose_dag(dag_id, dag_run_id="")` | one run — the exact `dag_run_id` if given, else the first failed of the last 5 (else the newest) → **every** task instance and state, the log tail of **every** failed *or retrying* one (`up_for_retry` counts, marked `still_retrying`), the task graph in topological order, the full Dag source, static `checks` (an XCom `task_ids` that matches no task, source/graph disagreement, and **import errors** for the Dag's file — the failure mode that never produces a failed run), and a deterministic `summary` that enumerates every failure and every check as a numbered list. The summary is built server-side, never by the model: a small model reliably repeats a numbered list it was handed, and just as reliably drops one finding out of two it has to assemble itself |
| `plan_dag_code_changes(dag_id, changes)` | read-only: applies every `{old, new}` in memory, compiles, returns the combined diff plus the graph impact — a change that orphans a task's edges or leaves a live reference behind is `blocking` and gets **no** token. Removals are found against the live graph, so deleting a TaskFlow task (or just its `@task` decorator) counts, even though it declares no `task_id`. A patch that touches assets, inlets/outlets or the schedule also carries an `asset_note` naming this Dag's own produced/consumed assets (never other Dags' ids — only `get_blast_radius` is authorized for those) |
| `apply_dag_code_changes(dag_id, changes, plan_token, asset_note="")` | writes all the planned edits as one file write, one backup, one reparse, one new Dag version. A plan that carried an `asset_note` must have it repeated verbatim here — the tool refuses without it, so the approval card always shows what the change can knock over |
| `plan_revert_dag_code(dag_id)` | read-only: previews a revert as the diff between the backup and the current file, plus a relayable `summary` and a single-use `plan_token` (kind `revert`). No backup → no token, just a relayable "nothing to revert" |
| `revert_dag_code(dag_id, plan_token, diff)` | restores the **original** file, discarding every fix (rehearse the demo from the chat). Requires the token from `plan_revert_dag_code` *and* the same `diff` repeated in the arguments, so the confirmation card shows exactly what reverting discards; at execution it re-verifies that the backup is still there and the current bytes still hash to what the plan was made from — drift aborts |
| `compare_dag_runs(dag_id, run_a, run_b)` | per-task duration deltas and conf changes; takes exact run ids or `latest`/`previous` ("compare the last two runs" is `previous` vs `latest`). A mapped task is aggregated to one row per task — instance count plus the *longest* instance's duration, not whichever `map_index` the API listed last. Names the differing Dag versions but does **not** diff them, since an older version may hold a co-located Dag the caller was never authorized against |
| `find_failure_clusters(hours, dag_ids)` | recent failed task instances grouped by normalised error signature; `dag_ids` is set by the caller's permissions, not by the model. Log fetches pass each instance's `map_index`, so a mapped failure is signed by its own log rather than the unmapped instance's; `failures_omitted` reports how many failures the scan the clusters were built from did not cover — counted off the reading after the allowlist filter reduced it, not off the page before — so "3 clusters" never quietly means "of the 50 I looked at". A log that cannot be read joins `failures_unreadable` rather than taking the tool down, and whenever coverage is short `scope` stops claiming the window holds no failed instance |
| `plan_backfill(dag_id, from, to)` | dry-run preview — read-only; returns every planned run and the `plan_token` that authorizes creating them |
| `run_backfill(dag_id, from, to, plan_token, planned_runs)` | creates the backfill, only for a plan the user reviewed and that still produces the same runs, capped at `AIRY_MCP_MAX_BACKFILL_RUNS` (50) |
| `get_blast_radius(dag_id)` | assets this Dag produces/consumes and the Dags up- and downstream of them |
| `plan_task_instance_clear(dag_id, task_id/position, dag_run_id)` | read-only: resolves `latest` to an exact run and a position to a task id (refused where a branch means the graph does not fix the order), dry-runs the clear, and returns the exact instances it would affect. Takes the downstream with it by default, as Airflow's own clear dialog does — `include_downstream=False` holds it to the one task |
| `apply_task_instance_clear(dag_id, dag_run_id, task_ids, plan_token)` | re-runs the **existing** instances in their own run: repeats the dry run *and* the task-set check, aborts if either moved, and never creates a Dag run. After the clear it compares what actually cleared against the plan: `cleared_matches_plan`, and on drift a `cleared_delta` (`missing`/`extra`) plus a warning — truthful reporting of an outcome that moved, not a rollback; the clear itself did happen |
| `verify_task_instance_recovery(dag_id, dag_run_id, instances, prior_attempts, target_task_id, cleared_after, audit_scope, xcom_scope)` | read-only: the step that makes a clear a *recovery* rather than a colour change. Call it after `apply_task_instance_clear` with the arguments that call handed back in `verify_with.args`. Reports each leg separately — a new attempt exists, the earlier attempt survived in `/tries`, state, execution fields read together, duration against this instance's own history, the new attempt's log, the audit transitions this API can see, and whether output was recorded after the clear; a downstream instance must record output *after the target's re-run finished*, which catches a completion notice that survived from before. The re-expansion baseline is taken from what **this server** recorded when it redeemed the plan token, never from `instances`, so a mapped task the scheduler re-expanded shows as an addition by identity. Without `target_task_id` every role is `unclassified` and the dating leg reports "not established". `external_system_checked` is always false: `verified` means Airflow's own record is consistent with a re-run, not that the work outside Airflow is correct. `audit_scope`/`xcom_scope` are set by the caller's permissions, not by the model, and **degrade rather than gate** — without them those legs report "not established" and the rest of the reading stands |
| `rerun_dag(dag_id, conf=None, note="", unpause=False, unpause_token="")` | triggers a **new** run. `conf` is validated against the Dag's own `params` schema (from `GET /dags/{dag_id}/details`): unknown keys are refused with the full list of valid params, types and defaults; enum and type violations are refused; a Dag without params accepts only an empty conf — and validation runs *before* the unpause flow, so a bad conf never burns an `unpause_token`. `note` is attached to the created run (default "Triggered via Airy"). A paused Dag first returns a warning and a token, and only a second call carrying it may unpause |

Two conventions run through the whole surface. Any tool's first GET translates
a 403/404 into `Dag 'x' does not exist or you cannot see it` — deliberately the
same words for both, so an unauthorized caller cannot use the error to confirm
an id exists. And results carry a pre-digested `summary` wherever a small model
must relay a finding completely.

This deliberately goes past both of the AIPs it borrows its framing from.
**AIP-91** (Draft, not yet voted) phase 1 is **GET-only**: per-user
authorization via a JWT pass-through proxy, with writes explicitly rejected.
**AIP-101** (Draft) is the embedded UI assistant that rides entirely on AIP-91
and never exceeds the signed-in user's permissions. Everything write-capable
here is beyond both — an experiment in the "what if the assistant could close
the loop" end state, not a proposal for either — and the UI says so: the
drawer carries an **Experimental** badge.

## Setup (Breeze)

```bash
# 1. the demo Dags: sales_summary plus the incident showcase pair (both
#    incident files must land in the write jail so Airy can patch the malformed
#    record — AIRY_MCP_DAGS_DIR defaults to /files/dags)
cp dev/airy_mcp/demo_dag.py files/dags/sales_summary.py
cp dev/airy_mcp/incident_triage_dag.py dev/airy_mcp/incident_digest_dag.py files/dags/

# 2. the plugin (files/plugins is what Breeze actually loads)
cd plugins/airflow-chatbot-plugin && pnpm install && pnpm deploy:breeze && cd -

# 3. demo timing — add to files/airflow-breeze-config/environment_variables.env
#    so a fix lands in seconds instead of ~30s (see "Timing" below):
#      AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL=0
#      AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=0

# 4. the showcase Dags' LLM connection — same env file. Required whenever
#    OPENAI_API_KEY is set (the Breeze demo env sets it): the engine gate
#    routes to @task.llm when *either* the connection or the key exists, but
#    @task.llm itself resolves the pydanticai_default connection, so key
#    without connection fails at connection lookup. The password may be
#    omitted — the hook then falls back to OPENAI_API_KEY.
#      AIRFLOW_CONN_PYDANTICAI_DEFAULT='{"conn_type": "pydanticai", "password": "<key>", "extra": {"model": "openai:gpt-4o-mini"}}'
```

The image already ships `fastmcp-slim[client]` at the version in `uv.lock` (it
comes with pydantic-ai's MCP extra), so the sidecar only needs the **server**
half. `files/airflow-breeze-config/init.sh` installs it when
`ENABLE_AIRY_MCP=true`, and the launchers run `pip install
'fastmcp-slim[server]'` if `import fastmcp.server` fails — deliberately with no
version specifier, so pip adds the extra's dependencies and leaves the
installed version alone.

Do **not** `pip install fastmcp` instead — in `init.sh` or anywhere else: the
meta-package resolves to the latest release and drags `mcp` (1.28.1 → 1.29.0)
and `uvicorn` (0.51 → 0.52) off Airflow's pins. Verified: `fastmcp-slim[server]`
keeps `fastmcp-slim`, `mcp`, `uvicorn`, `httpx`, `pydantic` and `starlette`
exactly where `uv.lock` has them.

The sidecar binds **127.0.0.1** by default: the transport is unauthenticated and
`apply_dag_code_changes` writes Python that Airflow then executes. Do not expose it.

### Headless start

`breeze start-airflow` drives a terminal multiplexer, and the default
(`mprocs`) panics under a pty that reports no size — exactly what a headless
shell (an agent, CI, `nohup`) hands it. Give the pty a size and use tmux:

```bash
script -qec "stty rows 50 cols 200; breeze start-airflow --backend postgres --terminal-multiplexer tmux" /dev/null
```

### Timing

`[core] min_serialized_dag_update_interval` (default 30 s) makes the Dag
processor skip re-serialising a Dag that changed less than 30 s ago — so a fix
applied right after a previous change would appear not to land. `REPARSE_TIMEOUT_S`
is 45 s so it outlasts that window even unconfigured — but 45 s of dead air is
not a demo. **Treat the two env vars above as mandatory**, not optional.

`ENABLE_AIRY_MCP=true` starts both sidecars (`astro-airflow-mcp` on :8000,
this one on :8001). The plugin reads a comma-separated `airy_mcp_url` Variable
and defaults to attaching both — but it TCP-probes each one first and attaches
only those listening. That is load-bearing, not tidiness: pydantic-ai raises out
of `agent.run()` if *any* attached toolset fails to initialise, so a dead sidecar
would otherwise take down the whole chat rather than just its own tools.
(Verified against pydantic-ai 2.13.0; a sidecar that is listening but broken
still errors.)

### Operational controls

All read at request time from Airflow Variables — no restart needed:

- **`airy_read_only`** — global kill-switch: `true` withholds every write tool
  from every user, and the prompt then says an admin disabled writes rather
  than blaming the user's permissions. Fail-closed: a Variable store that
  cannot be read counts as "on". Enforced server-side in the toolset gate, not
  just in the prompt — including when `/confirm` resumes an already-approved
  write, so flipping the switch mid-approval makes the resume fail rather than
  execute.
- **`airy_disabled_tools`** — comma-separated tool names withheld entirely.
  Unknown names match nothing; disabling an `apply_*` does not disable its
  `plan_*`, or vice versa. Fail-closed: when the list cannot be read, every
  write tool is withheld.
- **`airy_model`** — the model name, default `gpt-4o-mini` (the demo key is
  restricted to exactly that model; set this Variable when using a stronger
  key).
- **`airy_mcp_url`** — comma-separated MCP endpoints. **The write-capable
  sidecar must be the *last* entry** (the default puts `:8001` last): nothing
  in MCP names which server carries which tool without connecting to it, so
  the plugin identifies the write sidecar by position. Any override of this
  Variable must keep that convention.

`GET /chatbot/health` reports `read_only` (the kill-switch) and
`write_tools_available` (write sidecar reachable and the MCP extra
importable). They are orthogonal — the drawer's read-only badge combines them
and words the two causes differently. SSE `error` frames carry a
machine-readable `code` (`llm_auth | rate_limited | mcp_unreachable |
cancelled | internal`) and a `retryable` flag; the raw exception text stays in
the server log. A `{"type": "ping"}` frame goes out after ~15 s of frame
silence so proxies do not sever a stream that is quietly waiting on a long
tool call.

## Showcase: incident_triage and incident_digest

`incident_triage` is the richer showcase next to `sales_summary` — the
three-task sales Dag stays as-is because its one-screen shape is what makes
the code-fix loop legible. The incident pair exercises everything else:

- **Params validated at trigger time** — `window_hours` (integer 1–168),
  `severity_threshold` (enum over low/medium/high/critical), `skip_invalid`
  (boolean, default `False`). This gives `rerun_dag`'s conf validation a real
  schema to refuse against.
- **Deterministic fixture ingest** — incidents are built inline, seeded by the
  calendar date of the logical date (no network, no external files); the same
  date always produces the same batch.
- **Dynamic task mapping** — classification expands over the normalized
  incidents.
- **Schema-validated `@task.llm` with a deterministic offline fallback** — a
  gate task routes to `@task.llm` (connection `pydanticai_default`,
  `output_type` a pydantic model whose severity is a `Literal`) when the
  connection or `OPENAI_API_KEY` exists, else to a rule-based classifier
  producing the *same* schema; every assessment is re-validated with
  `model_validate` before it touches control flow. The executive summary has
  the same llm/offline split.
- **Severity routing into task groups** — a branch sends the batch to the
  `page` group when any incident meets the threshold, else to the `digest`
  group.
- **An Asset-linked consumer** — `publish_report` renders a markdown report
  with `outlets=[Asset("incident_report")]` and attaches it to the asset
  event's extra; `incident_digest` is scheduled on that asset and logs the
  report in full. That makes `get_blast_radius` demoable for the first time:
  `incident_triage` → `incident_report` → `incident_digest`.

**The staged failure.** One fixture record, `INC-4419`, always carries the
malformed timestamp `2026-02-30T99:99:99+00:00` (the `LEGACY_FEED_TIMESTAMP`
constant — exactly one occurrence in the file, so the string-replace patch
applies cleanly). With the default `skip_invalid=False` the `normalize` task
fails loudly, and its error names the record and both recoveries:

1. **Re-trigger with conf** — `rerun_dag` with `{"skip_invalid": true}`; Airy
   turns the natural-language ask into typed conf that the params schema
   validates.
2. **Fix the feed and clear** — `plan`/`apply_dag_code_changes` replacing the
   malformed literal with a parseable timestamp, then
   `plan`/`apply_task_instance_clear` on `normalize`.

## Demo run-book

The `sales_summary` Dag carries **two** bugs. Only one of them has failed
anything yet —
`report` never runs while `summarize` is failing — and finding just that one is
the failure mode this demo is built to avoid. One diagnosis reports both.

1. Trigger `sales_summary` — it fails on `summarize`.
2. **"What's wrong with sales_summary?"** → `diagnose_dag` → Airy reports two
   findings and distinguishes them:
   - **confirmed by the log**: `summarize`, `KeyError: 'ammount'`;
   - **latent blocker**: `report` pulls `task_ids='summarise'`, which matches no
     task in the Dag, so it would pull `None` the moment it runs. This one comes
     from the tool's static `checks`, not from a log — nothing has run it yet.
3. Airy calls `plan_dag_code_changes` with **both** edits, then proposes one
   `apply_dag_code_changes` card showing both hunks. Approve it → one write,
   one backup, `reparsed — Dag version 1 → 2`, and the open Dag view refreshes
   itself.
4. Click **Re-run…** → `rerun_dag` → all three tasks go green.

Both fixes are single unique strings: `"column": "ammount"` → `"amount"`, and
`task_ids='summarise'` → `'summarize'`. A second plan made *after* the first
edit landed would have been computed against source that no longer exists, which
is why they go in one call.

### Clearing an existing task instance

Run this one from its own starting state, not after the repair above: once both
bugs are fixed, `report` never fails, so there is nothing to clear.

1. From a run where `extract` and `summarize` succeeded and `report` failed, fix
   only the `summarise` typo and let it reparse — do **not** trigger a new run.
2. **"Clear the third task in the latest run."** → `plan_task_instance_clear`
   resolves `latest` to an exact run id and "third" to `report` from the
   *topological* order (`GET /dags/{id}/tasks` sorts alphabetically, which would
   have answered `summarize`), then dry-runs the clear. A position is refused
   whenever more than one task could legitimately occupy it — worked out from
   each task's ancestor and descendant counts, so a task on a short branch that
   could float anywhere after its parent makes those positions unanswerable
   too. The task a diamond rejoins at is still fixed, and still answerable.
3. The card names the Dag, the exact run, `report`, whether the downstream goes
   with it, that it runs on the latest parsed code, and that it creates no Dag
   run. Approve → the same instances are re-queued, each previous attempt is
   kept in task-instance history, and the Grid updates without a reload.

Clearing takes the downstream with it because clearing alone usually does not
finish the job: a task sitting in `upstream_failed` is never rescheduled, and one
that already succeeded keeps the XCom value the re-run exists to replace. So
clearing `summarize` on its own would leave `report` exactly as broken as it was.
That is not silent — the plan lists every instance before the card is shown.

Reset between rehearsals: ask Airy to *"revert sales_summary"* — now a planned
flow, so `plan_revert_dag_code` shows the diff before `revert_dag_code` is
approved — or use the file-level reset below.

### Incident triage beats

1. Trigger `incident_triage` with defaults → `normalize` fails within seconds;
   its log names `INC-4419`, the malformed value, and both recoveries.
2. **"What's wrong with incident_triage?"** → `diagnose_dag` → the `summary`
   carries the confirmed failure straight from the log.
3. **Recovery A** — *"re-run it, but skip the invalid records"* → `rerun_dag`
   with conf `{"skip_invalid": true}`. For a refusal beat first, ask for
   `severity_threshold: "urgent"` or `window_hours: "yesterday"` — both are
   refused with the full catalog of valid params, types and defaults.
4. **Recovery B** (rehearse from a fresh failure, not after A) — *"fix the
   feed"* → `plan_dag_code_changes` replacing the malformed timestamp (it occurs
   exactly once), then `plan`/`apply_task_instance_clear` on `normalize`. The
   repaired record parses but falls outside the default 24 h window, so the
   run succeeds with one record visible in the report's "dropped" line.
5. A successful run's `publish_report` emits the `incident_report` asset event
   → an `incident_digest` run starts within seconds and logs the full markdown
   report.
6. **"What breaks if incident_triage breaks?"** → `get_blast_radius` →
   `incident_report` and `incident_digest`.

### Deterministic reset

Between rehearsals, put the sources back and drop the backups Airy's patches
leave behind:

```bash
cp dev/airy_mcp/demo_dag.py files/dags/sales_summary.py
rm -f files/dags/sales_summary.py.airy-bak
# same pattern if incident_triage was patched:
cp dev/airy_mcp/incident_triage_dag.py files/dags/incident_triage_dag.py
rm -f files/dags/incident_triage_dag.py.airy-bak
```

Clear the conversation from the drawer (the clear button arms on the first
click and clears on the second). Dag-run history survives all of this — it
lives in the metadata DB — so a truly blank slate is `breeze down` and a fresh
start.

## Deliberate shortcuts

Declared up front, all of them cheap to replace:

1. **`[ACTION: …]` marker for buttons.** The system prompt asks Airy to end a
   reply with `[ACTION: <text>]` lines; the UI strips them and renders chips that
   send the text as the next user message. They are for *questions* only — a
   change is proposed by calling its write tool, never by a chip asking the user
   to ask again. *Real answer:* structured UI parts streamed over SSE, so the
   button carries a typed tool call instead of a round-trip through the model.
2. **Service-account execution.** Every `/chatbot` route requires a logged-in
   Airflow user, and every tool call is authorized against the *specific* Dag in
   its arguments before it reaches a sidecar — and again when `/confirm` resumes
   an approved call, so approving a write against one Dag cannot execute against
   another. `TOOL_POLICY` is an **allowlist**, and a tool absent from it is refused
   rather than guessed at — a denylist of writer names fails open the moment a
   sidecar gains a mutating tool, which would then be treated as a read, needing
   neither write permission nor a confirmation. `WRITE_TOOLS` is derived from the
   policy, so a tool cannot be added without classifying it. The practical cost:
   the read-only `astro-airflow-mcp` sidecar's tools are not reachable until
   someone enumerates them there.

   A tool is a bundle of REST calls, so it is authorized as one:
   `_tool_access_requirements` maps each tool to the `(method, DagAccessEntity)`
   pairs Airflow's own routes demand, and every one has to pass. `diagnose_dag`
   therefore needs `RUN`, `TASK_INSTANCE`, `TASK_LOGS`, `CODE` and `TASK`, not
   just Dag-level read; triggering a run is `POST` on `RUN`, not edit on the Dag;
   clearing a task instance is `PUT` on `TASK_INSTANCE`, which Airflow's route
   demands of its *dry run* too, so the read-only planner is held to it as well.
   Write tools are offered per permission, not as one "may you edit a Dag?": a
   user who may clear task instances but not rewrite Dag files is offered exactly
   the clear. (Under the simple auth manager both answers come from the role, so
   this only bites under FAB.) The
   Dag's `team_name` is passed in `DagDetails`, because a team-scoped auth
   manager answers a different question without it.

   Three cases reach past the named Dag and are authorized accordingly. A source
   file can define several Dags, so **patching** one requires edit on all of them
   (the reparse re-reads the file — the same reason Airflow's `/parseDagFile`
   authorizes by file), and **reading** source requires every co-located Dag to
   be readable (the rule `/dagSources` enforces by returning `REDACTED_SOURCE`).
   `get_blast_radius` derives its answer from the asset table, so it also needs
   `is_authorized_asset` **and** `is_authorized_asset_alias` — `GET /assets`
   demands both, an alias being just another name for an asset. Tools absent from the policy are refused, so the read-only
   sidecar's tools are unreachable until someone classifies them there.

   A tool that names no `dag_id` would speak for the whole fleet, and it is
   **narrowed, never gated**: the plugin computes the Dags that clear the tool's
   requirements and writes them into the call's `dag_ids` argument, overwriting
   whatever the model asked for (arguments that are not a dict are refused, since
   the rewrite would otherwise be silently dropped). A preflight "may you read
   everything?" would only be a snapshot, and the sidecar's admin-backed scan
   runs after it, so a Dag created in between would come back unauthorized.
   Narrowing has no such window. `find_failure_clusters` then queries the
   **batch** `POST .../taskInstances/list`, the only variant that filters by
   `dag_ids` — the wildcard `GET` ignores it, so its 50-row page would fill up
   with failures from Dags the caller cannot see and hide the ones they can —
   and re-filters the rows before fetching a single log. A fleet-wide tool that
   takes no allowlist is refused outright, even for a full reader: there is no
   version of that call that is not a snapshot.

   Underneath, the sidecar still *executes* as one admin service account
   (loopback-only), so in-Airflow audit trails attribute actions to that account,
   not the human. *Real answer:* AIP-91's identity propagation — pass the user's
   JWT through and let RBAC decide per call.
3. ~~No confirmation on the write itself.~~ Now server-enforced: write tools are
   approval-required in pydantic-ai, so the run suspends and the UI shows
   Confirm/Reject buttons backed by a TTL'd, user-bound nonce on
   `POST /chatbot/confirm`. The record is *not* discarded when the stream starts:
   it moves `pending → executing → done` and keeps the frames it emitted, so a
   browser that disconnects after the write landed can ask again with the same
   nonce and be told what happened instead of silently repeating it — and the
   card exposes that as a **Check outcome** button, because a guarantee the UI
   cannot reach is not a guarantee. A cancelled stream lands in `interrupted`,
   not `done`: the tool may have run, and a partial transcript is not an outcome.
   Replays of anything unfinished carry an `unsettled` frame, since every SSE
   stream ends with `done` and the drawer would otherwise read that as
   settlement. Remaining
   shortcuts: that store is in-memory and per-process (a restart loses the
   outcome, and a second api-server worker never had it), one verdict covers a
   whole suspension batch, and there is still no audit-log entry per applied
   patch.
4. **Full-file string replace instead of a real patch.** Requires `old` to be
   unique. Fine for a one-line fix, not for multi-hunk edits.

   Source access is bound to the snapshot it was authorized against. Permission
   is granted over the Dags Airflow has *parsed* out of one file **version**, so
   the plugin pins that version's **content hash** into the tool's arguments and
   the sidecar refuses anything else — "latest" could have grown a Dag nobody was
   checked against between the two reads, and a version *number* would not catch
   it either, because `DagCode.update_source_code` rewrites the latest version's
   source in place. `diagnose_dag` therefore returns `/dagSources`
   content, never the bytes on disk, and a write refuses outright when the two
   differ. The whole read-check-write runs under an `flock` on the Dag file and
   re-compares immediately before replacing it, so an edit landing mid-patch is
   refused rather than overwritten by a buffer computed from bytes that have
   moved. A writer that does not take the lock — a human in an editor — is
   outside what a file-backed bundle can defend.
5. **Consent that outlives the click, proved with tokens.** Every mutation is
   reachable only through the plan that was shown, and some change Airflow
   beyond the thing the user thinks they approved:
   - `apply_dag_code_changes` needs the `plan_token` from
     `plan_dag_code_changes`, and its `changes` must be the ones planned — they
     are in the *arguments* so the card shows every hunk, and the token is what
     proves the diff was not invented. At execution the source is re-hashed
     against the digest the plan was made from: same bytes, or no write.
   - `apply_task_instance_clear` needs the `plan_token` from
     `plan_task_instance_clear`, and repeats both checks at the moment of
     execution: an instance that started running since the preview aborts it
     rather than being killed by an approval given for a failed one, and a Dag
     that gained a task since the preview aborts it too — clearing re-queues the
     run, and the scheduler then reconciles a re-queued run against the latest
     version, which would create instances nobody reviewed. That is why the
     check does not depend on `run_on_latest_version`.
   - `plan_backfill` returns **every** planned run with both halves of its
     identity, and issues no token at all above the cap — a token must not
     authorize runs the preview was too abbreviated to have shown, and a
     partitioned Dag has no logical date to show in the first place.
   - `run_backfill` must repeat the plan back in `planned_runs` — the runs are in
     the *arguments* so the confirmation card spells out every run it will create,
     and the token is what proves the list was not invented. Mismatched, and it
     refuses.
   - `run_backfill` needs the single-use `plan_token` from `plan_backfill`, its
     arguments must match that plan exactly, and the dry run is repeated at the
     moment of execution — schedule or state drift since the preview aborts it.
     `AIRY_MCP_MAX_BACKFILL_RUNS` (50) still caps the size. Preview and create
     are two REST calls and so cannot be atomic from outside Airflow; what
     actually got created is therefore read back and compared by run *identity*
     — `(logical_date, partition_key)`, since the same count can be different
     runs; the date is canonicalised by parsing, the partition key compared
     exactly, and slots Airflow could not fill (no `dag_run_id`, or an
     `exception_reason`) do not count as created. A backfill that does not match
     is cancelled. Cancelling is a
     compensating action, not a rollback: it pauses the backfill and fails its
     *queued* runs, so anything the scheduler already picked up is reported in
     `surviving_runs` rather than quietly implied to be gone. *Real answer:* an
     expected-plan precondition on `POST /backfills` itself.
   - `rerun_dag` on a paused Dag returns a warning plus an `unpause_token`
     instead of unpausing. Only a second call carrying that token may unpause,
     and the confirmation card retitles itself to "Re-run and resume this Dag's
     schedule" so the lasting effect is in the line people actually read.

   One in-memory, per-process token store holds all four kinds (source change,
   clear, backfill, unpause), like the pending-approval store. A token proves the
   warning was issued and the plan was shown, not that a human read either — the
   confirmation card is what covers that.

6. **Refreshing the page around the chat.** A write that lands changes what the
   Dag view behind the drawer is showing, and that view will not notice: core
   queries have a five-minute stale time, window-focus refetch is off, and the
   Grid only polls while a run is active — so a terminal run cleared from the
   chat, or a source edit, sits stale until a manual reload.

   The chatbot cannot fix that from inside: it is injected as its own React root,
   outside Airflow's `QueryClientProvider`, so `useQueryClient()` is not
   reachable. So each write tool returns `mutation_applied` plus `ui_updates`,
   the plugin turns a *clean* result into one `resource_changed` SSE frame, the
   browser dispatches `airflow:resource-changed:v1`, and a module-scope listener
   in the host UI's `main.tsx` invalidates the matching queries — the same key
   set `useClearTaskInstances` invalidates after its own mutation.

   The event carries no data and no authority: it asks queries the signed-in
   user may already run to run again. Nothing is inferred from prose or from a
   tool's name — a refused, denied or failed write emits no frame, and a source
   edit whose reparse has not produced a new version emits none either, because
   nothing the Graph or Code view shows has changed yet. *Real answer:* a
   first-class plugin API for cache invalidation, rather than a custom event two
   bundles have to agree on.

### Known residual limitations

Found in review and accepted for the demo, distinct from the settled
shortcuts above:

- `GET /assets` reads one 100-row page, so `get_blast_radius` and the
  `asset_note` can understate a fleet-sized asset catalog (import errors now
  carry an explicit truncation note; assets do not yet). Blast radius is one
  hop, not transitive.
- Plan tokens have no session owner, so planning the same Dag from two
  conversations inside the 15-minute TTL discards the older conversation's
  token (its apply then refuses safely, and the guard's refusal text stays
  neutral about who planned first).
- The backend validates `rerun_dag` conf only when it is a dict; a non-dict
  conf reaches the sidecar, whose own input schema is the backstop.
- `gpt-4o-mini` (the demo key's only model) occasionally plans a partial fix;
  the plan result's `unaddressed_findings` field and the prompt rules push it
  to re-plan, and the demo script's explicit phrasing avoids the detour. The
  `airy_model` Variable upgrades behavior wholesale on a better key.
- The frontend vitest suite has once failed en masse on a cold cache and
  passed on every rerun; if a smoke check fails wholesale, run it twice.

## Tests

```bash
uv run --project airflow-core pytest dev/airy_mcp/test_server.py -q           # 205 tests
uv run --project airflow-core pytest dev/airy_mcp/test_incident_triage.py -q  # 48 tests
```

The plugin and UI suites:

```bash
cd plugins/airflow-chatbot-plugin && uv run --project ../../airflow-core pytest test_chat_stream.py -q  # 211 tests
cd plugins/airflow-chatbot-plugin && pnpm exec vitest run                                               # 265 tests
cd airflow-core/src/airflow/ui && pnpm exec vitest run src/queries/useResourceChanged.test.ts
```

`testpaths = ["tests"]` in the root `pyproject.toml` means **nothing collects
these suites automatically** — no CI job runs them, and they are invisible
unless someone types the path. Anyone editing `server.py` or the showcase Dags
has to run them by hand. Moving the files to `dev/airy_mcp/tests/` (with a
`conftest.py` for the import) is the first thing to do if any of this is
adopted.

## The Dag-processor question, and the long-term answer

We do **not** disable the processor while patching. In Breeze the bundle is a
*local folder*, so the file on disk is the single source of truth: nothing
overwrites the edit, and the reparse is what makes it take effect. The only real
problem is latency (bundle refresh + `min_file_process_interval` ≈ 30 s of dead
air), so the source tools force a priority reparse and wait for the Dag version
to bump.

**That stops being true for a git bundle** — the next refresh checks the ref out
again and the patch vanishes. A production version needs somewhere writable to
put the change:

- **A dedicated writable "MCP bundle"**, higher priority than the source bundle,
  holding only agent-authored overrides. The source tools write there and the Dag
  moves bundles for as long as the override lives. Needs: a bundle
  implementation that accepts writes; a documented precedence rule when the same
  `dag_id` exists in two bundles (today that is a collision, not an override);
  and a way to retire the override once the fix lands upstream.
- **Or — better — don't write to a bundle at all**: open a PR against the source
  repo and let the existing git bundle pick the change up after merge. Slower on
  stage, but it is the only version that survives contact with a real deployment,
  and it keeps Dag code under review.

Either way the tool contract stays as it is here; only the write target changes.
