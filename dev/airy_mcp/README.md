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

A second MCP sidecar with the three **write-capable** tools that
`astro-airflow-mcp` deliberately does not have:

| Tool | What it does |
|---|---|
| `diagnose_dag(dag_id)` | latest failed run → failing task + log tail + full Dag source |
| `fix_dag_code(dag_id, old, new)` | patches the source file (`old` must be unique), forces a reparse, returns a unified diff |
| `revert_dag_code(dag_id)` | restores the pre-fix backup (rehearse the demo from the chat) |
| `rerun_dag(dag_id)` | unpauses if needed and triggers a **new** run |

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
`fix_dag_code` writes Python that Airflow then executes. Do not expose it.

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

The Dag carries **two** bugs, and the second only surfaces once the first is
fixed — `report` never runs while `summarize` is failing. That is the point: a
clean one-shot repair looks rehearsed, a recovery looks real.

1. Trigger `sales_summary` — it fails on `summarize`.
2. **"What's wrong with sales_summary?"** → `diagnose_dag` → Airy names the task,
   the `KeyError: 'ammount'`, and the offending `op_kwargs` line.
3. Click **Apply the fix…** → `fix_dag_code` → diff + `reparsed — Dag version 1 → 2`.
4. Click **Re-run…** → `rerun_dag` → `summarize` goes green and **`report` fails**
   with `ValueError: invalid literal for int() with base 10: 'None'`.
5. Ask again → `diagnose_dag` → the XCom pull references `task_ids='summarise'`,
   which matches no task, so the pull returned `None`.
6. Fix and re-run → green.

Both fixes are single unique strings, so `fix_dag_code` applies each cleanly:
`"column": "ammount"` → `"amount"`, and `task_ids='summarise'` → `'summarize'`.

Reset between rehearsals: ask Airy to *"revert sales_summary"* (or
`mv files/dags/sales_summary.py.airy-bak files/dags/sales_summary.py`).

## Deliberate shortcuts

Declared up front, all of them cheap to replace:

1. **`[ACTION: …]` marker for buttons.** The system prompt asks Airy to end a
   reply with `[ACTION: <text>]` lines; the UI strips them and renders chips that
   send the text as the next user message. *Real answer:* structured UI parts
   streamed over SSE, so the button carries a typed tool call instead of a
   round-trip through the model.
2. **Service-account auth.** The sidecar logs in as one admin user, so tool calls
   are not attributed to the human in the chat. *Real answer:* AIP-91's identity
   propagation — pass the user's JWT through and let RBAC decide.
3. **No confirmation on the write itself.** The click *is* the gate; the tool
   trusts whatever the model passes — beyond refusing a patch that would not
   `compile()`. *Real answer:* a dry-run/confirm pair, plus an audit-log entry
   per applied patch.
4. **Full-file string replace instead of a real patch.** Requires `old` to be
   unique. Fine for a one-line fix, not for multi-hunk edits.

## Tests

```bash
uv run --project airflow-core pytest dev/airy_mcp/test_server.py -q
```

`testpaths = ["tests"]` in the root `pyproject.toml` means **nothing collects this
suite automatically** — no CI job runs it, and it is invisible unless someone
types the path. Anyone editing `server.py` has to run it by hand. Moving the file
to `dev/airy_mcp/tests/` (with a `conftest.py` for the import) is the first thing
to do if any of this is adopted.

## The Dag-processor question, and the long-term answer

We do **not** disable the processor while patching. In Breeze the bundle is a
*local folder*, so the file on disk is the single source of truth: nothing
overwrites the edit, and the reparse is what makes it take effect. The only real
problem is latency (bundle refresh + `min_file_process_interval` ≈ 30 s of dead
air), so `fix_dag_code` forces a priority reparse and waits for the Dag version
to bump.

**That stops being true for a git bundle** — the next refresh checks the ref out
again and the patch vanishes. A production version needs somewhere writable to
put the change:

- **A dedicated writable "MCP bundle"**, higher priority than the source bundle,
  holding only agent-authored overrides. `fix_dag_code` writes there and the Dag
  moves bundles for as long as the override lives. Needs: a bundle
  implementation that accepts writes; a documented precedence rule when the same
  `dag_id` exists in two bundles (today that is a collision, not an override);
  and a way to retire the override once the fix lands upstream.
- **Or — better — don't write to a bundle at all**: open a PR against the source
  repo and let the existing git bundle pick the change up after merge. Slower on
  stage, but it is the only version that survives contact with a real deployment,
  and it keeps Dag code under review.

Either way the tool contract stays as it is here; only the write target changes.
