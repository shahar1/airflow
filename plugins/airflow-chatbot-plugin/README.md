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

# Airflow Chatbot Plugin — "Airy"

An LLM-powered assistant embedded in the Airflow UI as a floating button and
slide-out drawer. The Python side (`airflow_chatbot_plugin.py`) runs a
PydanticAI agent against MCP sidecars that expose Airflow tools; the React
side (`src/`) streams the conversation — tool calls included — and renders
approval cards for every write.

**Experimental, and deliberately beyond the AIPs it is framed by.** AIP-91
(Draft) phase 1 is GET-only with per-user JWT pass-through; AIP-101 (Draft)
is the embedded assistant riding entirely on AIP-91. Everything write-capable
here goes past both, and the drawer says so with an **Experimental** badge.
The write-capable sidecar, the demo run-book, and the full list of deliberate
shortcuts live in [`dev/airy_mcp/README.md`](../../dev/airy_mcp/README.md).

## Features

- **Streaming with visible tool calls** — the drawer shows each tool call and
  its (clipped) result as it happens, not a spinner.
- **Approval cards with diffs and truthful receipts** — write tools suspend
  the run until the user clicks Confirm/Reject; the card shows the planned
  diff/arguments because the tools require them repeated in the call. A
  refused or denied write is never painted green, a dropped connection ends
  in "outcome unknown" (with a **Check outcome** button that replays the
  server's record), and only a result carrying `mutation_applied: true`
  triggers a page refresh.
- **Stop generation** — a streaming answer can be stopped; an approved write
  that is already executing deliberately cannot (hanging up the reader would
  not undo it).
- **Persistence** — the transcript survives reloads via `sessionStorage`,
  written on a 500 ms trailing throttle plus turn-end and `beforeunload`
  flushes.
- **Page context** — the current path is sent with each turn so "this Dag"
  resolves; an SPA-navigation hook resamples it without a full page load.
- **Suggested prompts** — page-aware starter chips on the empty state; while
  disconnected they fill the input instead of firing doomed requests.
- **Per-message copy** — a copy control on every assistant bubble, with
  honest Copied/Copy-failed feedback.
- **Entity links** — relative markdown links to Dags/runs/tasks resolve
  against the host's base path and open in the same tab; external links get
  `target="_blank" rel="noopener noreferrer"`; backticked Dag ids are
  linkified only when the transcript itself proved them (tool args, confirm
  cards, `resource_changed` frames) — never guessed from prose.
- **Error taxonomy with retry** — `error` frames carry a machine-readable
  `code` and `retryable` flag; the drawer renders friendly copy per code and
  offers Retry only when retrying can help. Raw exception text stays in the
  server log.
- **Read-only and Experimental badges** — the read-only badge distinguishes
  "an admin disabled writes" from "the write sidecar is down".
- **Accessibility** — the drawer is a labelled `role="dialog"` (modal with a
  focus trap on mobile), focus moves in on open and back to the launcher on
  close, the resize handle is a keyboard-operable `role="separator"`, the
  transcript is a `role="log"` with visually-hidden speaker prefixes and a
  polite completion announcement, Enter respects IME composition, and the
  input stays editable while streaming (only send is gated).
- **UI refresh after writes** — a landed write emits a `resource_changed`
  frame; the bundle dispatches `airflow:resource-changed:v1` and the host UI
  invalidates the matching queries, so the Grid/Code view updates without a
  reload.

## How it is wired

`AirflowChatbotPlugin` registers two things on the api-server:

1. A FastAPI sub-app under **`/chatbot`** (every route requires a logged-in
   Airflow user; only the static bundle mount is public):

   | Route | What it does |
   |---|---|
   | `GET /chatbot/` | liveness JSON |
   | `GET /chatbot/health` | LLM key presence/source, MCP reachability, `read_only` (the `airy_read_only` kill-switch) and `write_tools_available` (write sidecar reachable + MCP extra importable) |
   | `POST /chatbot/chat` | one turn, answered as server-sent events |
   | `POST /chatbot/confirm` | approve/reject a suspended write by nonce; a repeated nonce **replays** the recorded outcome instead of executing again |
   | `/chatbot/static/*` | the built bundle |

2. A root middleware that injects
   `<div id="airflow-chatbot-root"></div><script src="{base}/chatbot/static/main.iife.js?v={mtime}">`
   into the UI's HTML pages (skipping API/static/login paths and compressed
   bodies). The `?v=` query is the bundle's mtime, stat'ed per request, so a
   rebuilt bundle takes effect on browser reload without an api-server
   restart. The frontend derives its base URL from that script tag's `src`
   (`src/basePath.ts`), so path-prefixed deployments work; a
   `data-chatbot-base` attribute on the script tag overrides it.

SSE frames are `data: {json}` with a `type` of `text`, `tool`, `tool_result`,
`confirm_required`, `resource_changed`, `unsettled`, `error`
(`code`: `llm_auth | rate_limited | mcp_unreachable | cancelled | internal`,
plus `message` and `retryable`), `ping` (emitted after ~15 s of frame
silence; clients ignore unknown types), and a terminating `done`.

Server-side, every tool call is authorized as the signed-in user against an
allowlist (`TOOL_POLICY`) per Dag and per underlying REST permission; write
tools are additionally gated behind the confirm flow, the `airy_read_only`
kill-switch, and `airy_disabled_tools`. Replayed history is capped (newest 20
user/assistant pairs and ≤32k characters).

## Configuration

- **LLM API key** — Airflow Connection `openai_default` (key in the
  *password* field, encrypted at rest, checked first) or the
  `OPENAI_API_KEY` environment variable.
- **Airflow Variables** (read per request, no restart needed):
  - `airy_model` — model name, default `gpt-4o-mini`.
  - `airy_mcp_url` — comma-separated MCP endpoints, default
    `http://localhost:8000/mcp,http://localhost:8001/mcp`; empty disables
    MCP. **The last entry must be the write-capable sidecar** — the plugin
    identifies it by position. Each endpoint is TCP-probed and only listening
    ones are attached.
  - `airy_read_only` — `true` withholds every write tool from every user.
    Fail-closed: an unreadable Variable store counts as on, and the switch is
    enforced again when `/confirm` resumes an approved write.
  - `airy_disabled_tools` — comma-separated tool names withheld entirely
    (unknown names are ignored; disabling an `apply_*` does not disable its
    `plan_*`). Fail-closed: unreadable list ⇒ no write tools.

## Development

Prerequisites: Node.js >= 22, pnpm.

```bash
cd plugins/airflow-chatbot-plugin
pnpm install
pnpm dev        # dev server with hot reload at http://localhost:5173
pnpm test       # vitest
pnpm lint       # eslint --quiet + tsc
```

## Build and deploy

`pnpm build` produces `dist/main.iife.js` — a single self-contained IIFE
bundle (React included, CSS injected by JS). The api-server serves it from
`www/dist/`, so copy it there after building.

**In Breeze** the plugins folder is `files/plugins/` (every compose variant
sets `AIRFLOW__CORE__PLUGINS_FOLDER=/files/plugins`, and this source
directory is *not* mounted into the container). One command builds and copies
everything:

```bash
cd plugins/airflow-chatbot-plugin && pnpm deploy:breeze
```

which runs `vite build`, copies `dist/*` to `www/dist/`, and copies
`airflow_chatbot_plugin.py` plus `www/` into `files/plugins/`. Re-run it
after every edit or the container keeps serving the previous copy. A change
to `airflow_chatbot_plugin.py` needs an api-server restart; a bundle-only
change takes effect on browser reload (the injected script busts the cache by
mtime).

Outside Breeze, copy the plugin directory (`airflow_chatbot_plugin.py` and
`www/`) into `$AIRFLOW_HOME/plugins/` and restart the api-server.

## Project structure

```
airflow-chatbot-plugin/
├── src/                          # React UI source
│   ├── basePath.ts               # backend base URL derived from the injected script tag
│   ├── components/
│   │   ├── Chatbot.tsx           # orchestrator (health polling, focus handoff)
│   │   ├── ChatButton.tsx        # floating action button
│   │   ├── ChatDrawer.tsx        # drawer: dialog semantics, resize, badges, two-step clear
│   │   ├── ChatInput.tsx         # autogrow textarea; send gated while busy, never disabled
│   │   ├── MessageList.tsx       # transcript: markdown, tool chips, approval cards, copy
│   │   └── icons/
│   ├── hooks/
│   │   ├── useChat.ts            # SSE streaming, confirm flow, persistence, health
│   │   └── useLocationPathname.ts  # SPA-navigation page-context hook
│   ├── context/colorMode/        # theme/dark-mode context
│   ├── main.tsx                  # entry point (IIFE, auto-mounts)
│   └── dev.tsx                   # development entry point
├── www/dist/                     # built bundle the api-server serves (generated)
├── airflow_chatbot_plugin.py     # backend: FastAPI sub-app, agent, authz, SSE
├── test_chat_stream.py           # backend test suite
└── package.json
```

## Tests

```bash
cd plugins/airflow-chatbot-plugin && uv run --project ../../airflow-core pytest test_chat_stream.py -q  # 191 tests
cd plugins/airflow-chatbot-plugin && pnpm exec vitest run                                               # 251 tests
```

Neither suite is collected by CI (`testpaths = ["tests"]` in the root
`pyproject.toml` excludes the backend suite) — run them by hand when touching
this plugin.

## License

Licensed under the Apache License, Version 2.0.
