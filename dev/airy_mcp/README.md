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
| `diagnose_dag(dag_id)` | latest failed run → **every** task instance and state, the log tail of **every** failed one, the task graph in topological order, the full Dag source, and static `checks` (an XCom `task_ids` that matches no task, source/graph disagreement) |
| `plan_dag_code_changes(dag_id, changes)` | read-only: applies every `{old, new}` in memory, compiles, returns the combined diff plus the graph impact — a change that orphans a task's edges or leaves a live reference behind is `blocking` and gets **no** token. Removals are found against the live graph, so deleting a TaskFlow task (or just its `@task` decorator) counts, even though it declares no `task_id` |
| `apply_dag_code_changes(dag_id, changes, plan_token)` | writes all the planned edits as one file write, one backup, one reparse, one new Dag version |
| `revert_dag_code(dag_id)` | restores the **original** file, discarding every fix (rehearse the demo from the chat) |
| `compare_dag_runs(dag_id, run_a, run_b)` | per-task duration deltas and conf changes; names the differing Dag versions but does **not** diff them, since an older version may hold a co-located Dag the caller was never authorized against |
| `find_failure_clusters(hours, dag_ids)` | recent failed task instances grouped by normalised error signature; `dag_ids` is set by the caller's permissions, not by the model |
| `plan_backfill(dag_id, from, to)` | dry-run preview — read-only; returns every planned run and the `plan_token` that authorizes creating them |
| `run_backfill(dag_id, from, to, plan_token, planned_runs)` | creates the backfill, only for a plan the user reviewed and that still produces the same runs, capped at `AIRY_MCP_MAX_BACKFILL_RUNS` (50) |
| `get_blast_radius(dag_id)` | assets this Dag produces/consumes and the Dags up- and downstream of them |
| `plan_task_instance_clear(dag_id, task_id/position, dag_run_id)` | read-only: resolves `latest` to an exact run and a position to a task id (refused where a branch means the graph does not fix the order), dry-runs the clear, and returns the exact instances it would affect. Takes the downstream with it by default, as Airflow's own clear dialog does — `include_downstream=False` holds it to the one task |
| `apply_task_instance_clear(dag_id, dag_run_id, task_ids, plan_token)` | re-runs the **existing** instances in their own run: repeats the dry run *and* the task-set check, aborts if either moved, and never creates a Dag run |
| `rerun_dag(dag_id, unpause=False, unpause_token="")` | triggers a **new** run; a paused Dag first returns a warning and a token, and only a second call carrying it may unpause |

This goes past AIP-91 phase 1 (read-only) on purpose — it is the "what if the
assistant could close the loop" end state, not a proposal for phase 1.

## Setup (Breeze)

```bash
# 1. the demo Dag
cp dev/airy_mcp/demo_dag.py files/dags/sales_summary.py

# 2. the plugin (files/plugins is what Breeze actually loads)
cd plugins/airflow-chatbot-plugin && pnpm install && pnpm build
mkdir -p www/dist && cp dist/* www/dist/ && cd -
cp plugins/airflow-chatbot-plugin/airflow_chatbot_plugin.py files/plugins/
cp -r plugins/airflow-chatbot-plugin/www files/plugins/

# 3. demo timing — add to files/airflow-breeze-config/environment_variables.env
#    so a fix lands in seconds instead of ~30s (see "Timing" below):
#      AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL=0
#      AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=0
```

The image already ships `fastmcp-slim[client]` at the version in `uv.lock` (it
comes with pydantic-ai's MCP extra), so the sidecar only needs the **server**
half. The launchers run `pip install 'fastmcp-slim[server]'` if
`import fastmcp.server` fails — deliberately with no version specifier, so pip
adds the extra's dependencies and leaves the installed version alone.

Do **not** `pip install fastmcp` instead: the meta-package resolves to the latest
release and drags `mcp` (1.28.1 → 1.29.0) and `uvicorn` (0.51 → 0.52) off
Airflow's pins. Verified: `fastmcp-slim[server]` keeps `fastmcp-slim`, `mcp`,
`uvicorn`, `httpx`, `pydantic` and `starlette` exactly where `uv.lock` has them.

The sidecar binds **127.0.0.1** by default: the transport is unauthenticated and
`apply_dag_code_changes` writes Python that Airflow then executes. Do not expose it.

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

## Demo run-book

The Dag carries **two** bugs. Only one of them has failed anything yet —
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

Reset between rehearsals: ask Airy to *"revert sales_summary"* (or
`mv files/dags/sales_summary.py.airy-bak files/dags/sales_summary.py`).

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

3. **Refreshing the page around the chat.** A write that lands changes what the
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

## Tests

```bash
uv run --project airflow-core pytest dev/airy_mcp/test_server.py -q
```

The plugin and UI suites:

```bash
cd plugins/airflow-chatbot-plugin && uv run --project ../../airflow-core pytest test_chat_stream.py -q
cd plugins/airflow-chatbot-plugin && pnpm exec vitest run
cd airflow-core/src/airflow/ui && pnpm exec vitest run src/queries/useResourceChanged.test.ts
```

`testpaths = ["tests"]` in the root `pyproject.toml` means **nothing collects the
MCP suite automatically** — no CI job runs it, and it is invisible unless someone
types the path. Anyone editing `server.py` has to run it by hand. Moving the file
to `dev/airy_mcp/tests/` (with a `conftest.py` for the import) is the first thing
to do if any of this is adopted.

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
