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

- [Airy summit demo — short card](#airy-summit-demo--short-card)
  - [Setup — is the environment ready? (T-10 min)](#setup--is-the-environment-ready-t-10-min)
  - [The beats](#the-beats)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Airy summit demo — short card

Condensed from `files/airy-demo-script-2026-08.md` — that file has the verbatim payloads,
caveats and fallbacks; the runbook has reset (§6), failure playbook (§7) and pre-flight (§8).

**Fixture:** `eod_settlement_demo` — nightly card-batch settlement for point-of-sale
stores. `collect_card_captures` → `settle_card_batch` → `record_settlement` →
`notify_back_office`. The bug: `settle_card_batch` is still an `EmptyOperator`, so
`submit_settlement()` never runs — the run is green, the ledger line and the back-office
notification are written from a receipt that does not exist.

**The stage claim (and nothing wider), spoken as the first line of the Close:** *"Airy
used runtime state unavailable to a repository-only agent, safely corrected the workflow
under approval, and checked what the replacement run recorded."* The thirty-seconds-of-
retail explainer is spoken in the Opening.

**Scope note:** beat 3's attribution ambiguity is a property of Airflow's record
**today**, not of Airy and not a timeless limitation. The Opening passage below says this
aloud before beat 1, and the Q&A after the close handles the follow-up question.

**Never say:** "Airy wrote this fix" (the patch is pasted in — say *"I gave it the patch"*) ·
"proved the money moved" (`external_system_checked` is always false) · "who marked it
success" · "can backfill the other nights" · "production-ready" (say *prototype*) ·
"the version pin guarantees the bytes" · "a model without runtime access would get this
wrong" (the claims ledger refuses that: it proposes the same fix — concede it at beat 4b,
then pivot to the incident).

---

## Setup — is the environment ready? (T-10 min)

Run from the repo root. Any failure → fix with the referenced runbook section, re-run, only
then rehearse. First time after the rename (or after `breeze down`): the Dag will not exist
yet — stage it from scratch with runbook §4, then come back here.

**These checks verify; they do not restore.** After any rehearsal (Airy applied the patch,
extra runs exist), converge back to the initial state with one command, then re-run the
checks:

```bash
files/airy-demo-2026-08/reset_demo.sh
```

It restores the stub, deletes every run except the keeper, reparses and verifies. It never
touches the keeper run itself — if the keeper is missing or dirty it stops and says so.

```bash
# the container name changes on every Breeze restart — always look it up
C=$(docker ps --format '{{.Names}}' | grep '^breeze-airflow-run')
cd /home/shahar/repos/apache/airflow
API=files/airy-demo-2026-08/api.sh

# 1. Deployed artefacts are the build you think they are (runbook §8.0)
cmp plugins/airflow-chatbot-plugin/airflow_chatbot_plugin.py \
    files/plugins/airflow_chatbot_plugin.py && echo "plugin .py: IN SYNC"
find plugins/airflow-chatbot-plugin/src -type f \
     -newer files/plugins/www/dist/main.iife.js -print | sed 's/^/STALE BUNDLE: /'
cmp plugins/airflow-chatbot-plugin/www/dist/main.iife.js \
    files/plugins/www/dist/main.iife.js && echo "bundle: DEPLOYED == BUILT"
git rev-parse HEAD | grep -q '^04cdece57a16d6d5e4fe55e31161ee60ec8dee3b$' \
  && echo "HEAD: PINNED SHA MATCHES" || echo "HEAD MISMATCH — reconcile before rehearsing"
# any hit → cd plugins/airflow-chatbot-plugin && pnpm deploy:breeze, then restart the
# API server (runbook §8.0) and hard-reload the browser

# 2. Airy is up and writable
$API GET "/chatbot/health"
# expect: read_only: false, write_tools_available: true, unreachable: []
# else: files/airy-readonly.sh false; check the :8001 sidecar (runbook §7, last rows)

# 3. The fixture is in stub form on disk, no leftover backup
docker exec $C grep -n 'post = ' /files/dags/eod_settlement_demo.py
# expect exactly: 102:    post = EmptyOperator(task_id="settle_card_batch")
docker exec $C ls /files/dags/eod_settlement_demo.py.airy-bak 2>/dev/null \
  && echo "BAK PRESENT — run the reset" || echo "bak: absent"
# wrong line or bak present → full reset, runbook §6 (cp the pristine INSIDE the container)

# 4. Parsed clean, exactly one green keeper run
$API GET "/api/v2/importErrors?limit=5" | python3 -c \
  'import json,sys;print("import errors:", json.load(sys.stdin)["total_entries"])'   # 0
$API GET "/api/v2/dags/eod_settlement_demo/dagRuns?limit=10" | python3 -c '
import json, sys
d = json.load(sys.stdin)
runs = d.get("dag_runs")
print([(r["dag_run_id"], r["state"]) for r in runs] if runs is not None else f"API ERROR: {d}")'
# expect exactly: [('eod_20260814', 'success')]
# [] → stage the keeper run (runbook §4 steps 3-4); extra runs → delete them (§6 step 2)

# 5. The staged run shows the incident signature
$API GET "/api/v2/dags/eod_settlement_demo/dagRuns/eod_20260814/taskInstances?limit=50" \
| python3 -c '
import json, sys
d = json.load(sys.stdin)
tis = d.get("task_instances")
if tis is None:
    print("KEEPER RUN ABSENT — stage it (runbook §4):", d.get("detail", d))
else:
    for ti in sorted(tis, key=lambda t: t["task_id"]):
        print({k: ti.get(k) for k in ("task_id", "state", "duration", "hostname", "operator")})'
# expect 4 success rows; settle_card_batch: duration 0.0, hostname "", operator EmptyOperator
```

Not commands, still mandatory: clear the drawer conversation (click clear twice); open two
browser tabs — Grid on `eod_20260814`, `notify_back_office`'s log; re-read the
NEVER SAY list above.

---

## The beats

Every beat has two parts. **Do** is what you click, type or wait for — anything to be
typed into Airy is quoted verbatim and is never spoken. **Say** is one continuous spoken
passage; read it as written.

### Opening — before beat 1

**Do:** Grid tab on screen, all four squares green. Nothing typed yet.

**Say:**

> Thirty seconds of retail context first. When you pay by card at a Point of Sale, the
> payment is authorized during the day, but the money does not actually move to the store
> yet. At close of day, the store submits that day's card transactions as a batch to the
> payment processor for settlement. Settlement is the step where the store actually gets
> paid. The back office is where the store reconciles what the Point of Sale says
> happened against the processor's receipt and keeps the books straight. The pipeline on
> screen is that end-of-day settlement job. If settlement is skipped, the store simply
> does not get paid — while every technical dashboard still looks green.
>
> One thing before we start. Everything you're about to see is grounded in the evidence
> Airflow exposes today. And that record does not always carry enough run-scoped
> provenance to tie every task-state change to one specific actor or mechanism. Airy is
> designed to respect that boundary: it tells you what the record proves, and it does not
> guess past it. If Airflow adds richer provenance in the future, that just gives Airy
> better evidence and makes these questions easier to answer. That's the direction we
> designed for.

### 1 — The problem

**Do:** show the Grid — one run, four green squares. Switch to `notify_back_office`'s
log, search `PENDING`, leave the highlighted line on screen. Then type into Airy:

> The eod_settlement_demo run last night is green, but the back office says last
> night's card batch was never settled. What happened?

**Say:**

> Here is the situation. Last night's settlement run is green. Four tasks, four green
> squares. But this morning the back office says the store was never paid — the card
> batch never settled. And look at the log. The pipeline told the back office the batch
> was settled and posted. It wasn't. Nothing is red, so nothing paged anyone. This is the
> failure mode monitoring is worst at. So let's ask Airy what happened.

### 2 — Airy names the task, from runtime evidence

**Do:** let Airy answer the beat-1 question — it names `settle_card_batch`. Then type,
verbatim:

> Look at the operator recorded on that task instance, and list the dispatch fields —
> hostname, pid, queued_when, scheduled_when, duration — for all four tasks side by side.

Wait for the four-row table; keep it on screen while you speak.

**Say:**

> Airy went to the runtime record and named a task: settle_card_batch. Now I've asked it
> to show the evidence, and this table is the whole story. When Airflow really dispatches
> a task, it leaves fingerprints — a hostname, a process id, a queued time, a scheduled
> time. Three of these four tasks have all of them. This one has none. No hostname. No
> process id. Never queued, never scheduled. Zero seconds of duration. And the operator
> recorded on that row is EmptyOperator — a placeholder. Nothing ran, because there was
> nothing to run. That is a diagnosis from the runtime record — you cannot see any of
> this in the repository.

### 3 — What Airy will not claim (attribution)

**Do:** nothing new on screen — this is spoken over the same table and Airy's
attribution text. If the drawer paraphrases the caveat, trust the tool text, not the
model's summary.

**Say:**

> At this point you are probably asking: so who marked it green? Here is where Airy is
> different — it will not answer that with a name. The event log for this run holds
> exactly one row — the trigger itself — and nothing for settle_card_batch. And Airy
> says, explicitly, that this absence is not evidence that someone wrote to the
> database, because several different mechanisms leave exactly this nothing:
>
> - a **bulk state change** — changes the state, records nothing;
> - the scheduler's own **EmptyOperator fast path** — marks a task green without ever
>   dispatching it;
> - a **CLI-created attempt** — its rows carry no run id, so a run-scoped query cannot
>   even reach them.
>
> Airy lists those alternatives and picks none. And the limit cuts both ways. If there
> were an audit row, it would only prove a request was received and logged — not that it
> was authorised, not that it changed the state. With no row, nothing proves tampering
> either. What settles this case is a different field entirely: the operator recorded on
> the run says EmptyOperator, and the source code says EmptyOperator. Record and code
> agree — nothing had to force anything. Airy did not tell us who did this, and it is not
> going to. It tells us what the record can and cannot carry.
>
> And to be clear — that is Airflow's observability today, not a permanent fact. If a
> future Airflow logs manual state overrides explicitly, that list collapses to a named
> mechanism and this whole question gets easier — which would be a good thing. A better
> record means more settled answers. What stays is the discipline: saying what is proven
> and what is not.

### 4 — The patch (supplied by you)

**Do:** type into Airy, verbatim — the patch text is deliberately part of the message:

> In eod_settlement_demo the settle_card_batch task is still an EmptyOperator, so the
> submit_settlement function the file already defines never runs. Plan and then apply exactly
> this one source change, and show me the diff before anything is written:
>
> old:
>     post = EmptyOperator(task_id="settle_card_batch")
>
> new:
>     post = PythonOperator(task_id="settle_card_batch", python_callable=submit_settlement)
>
> Once the change is applied, trigger one replacement run using EXACTLY the arguments the
> apply hands back in its replacement_run block, and add the note "Replacement run for the
> unsettled card batch".

**Say:**

> I want to be completely honest about what I just did: the patch text is mine. I gave it
> the exact change. This deployment deliberately runs a small model — gpt-4o-mini — and
> measured on this rig, it does not reliably author patches. A frontier model likely
> would. But that is the design point. The safety story does not care who wrote the
> patch. Whatever gets proposed — by me, by a small model, by a frontier model — is
> checked against the real bytes on disk, shown as a diff, approved by a human, rolled
> back if it breaks the file, and verified at the end. The approval card is the contract,
> not the chat.

### 4b — own the pushback, unprompted

**Do:** nothing — speak while the plan card renders.

**Say:**

> Some of you are thinking: my coding agent finds this bug. You're right. The
> EmptyOperator is sitting right there in the file. A repository-only model proposes this
> exact one-line change — it can even predict that PENDING line in the log. As a code
> review, it is completely correct. But a code review is dateless. It is true of every
> checkout of this file, on any day, whether or not anything ever went wrong. It cannot
> tell you that this fired last night. That this specific run went green. That the back
> office was actually told a batch settled when no receipt exists. That exactly one task
> row has no hostname, no process id, and zero duration. And it cannot check, after the
> fix, whether the replacement run actually recorded the receipt. The fix is trivial on
> purpose. You are not watching the patch — you are watching the evidence, the approvals,
> and the check at the end.

### 5–6 — The card, then the approval

**Do:** point at the amber **Approval required** card — the diff is open by default, the
badge reads "Writes the Dag file · reparses immediately", *Technical details* holds the
plan token. When the passage ends, click **Apply to Dag source file**.

**Say:**

> Nothing has been written yet. This approval card is the contract. It says what lastingly
> changes: it writes a file on the Airflow host and reparses it immediately, and there is
> no review step after this button. It shows the diff itself, not a description of the
> diff. And it carries a single-use token, minted against the exact bytes that produced
> this diff — if the file moves underneath us before I click, the apply refuses rather
> than write something nobody reviewed. One click, one decision, exactly this write.

### 7 — Apply and validate

**Do:** wait for the result — `applied: true`, imports clean, task graph as predicted,
and a `replacement_run` block carrying `run_id`, `expected_dag_version` and
`verify_task_id`. The card collapses to the receipt "Edited Dag code · approved by you".

**Say:**

> It wrote the file, took a backup, forced a reparse, and then asked the Dag processor
> two questions a compile cannot answer: does this file still import, and is the Dag that
> came out the Dag the diff predicted. Both clean. And notice — nothing has run. Changing
> the code did not trigger anything.

**If the apply rolls back instead, say:**

> Applied: false — and the file on disk is byte-for-byte the original again. It would
> have been very easy to build a demo that always reports success. This is the honest
> branch, and it is the guardrail working.

### 8–9 — A separate trigger, a separate approval

**Do:** type into Airy:

> Now trigger one replacement run of eod_settlement_demo, with the note
> "Replacement run for the unsettled card batch".

A second **Approval required** card appears — `run_id` and `expected_dag_version` each on
their own line. Speak, then click **Re-run Dag**.

**Say:**

> Notice what did not happen. Approving the code change ran nothing. To get a replacement
> run, I have to ask again — and Airy has to ask me again, with a different tool, a
> different approval card, and a different nonce. Two mutations, two approvals. Approving
> one
> never authorises the other. Now look at the approval card. The run has an id we chose,
> so a
> retry finds the run it already created instead of making a second one. And it carries
> the Dag version the change was approved against — if the Dag moves before this goes
> out, the trigger refuses rather than run code nobody approved. One more thing, and it
> matters: the payload calls itself NOT VERIFIED. It created a run. It read nothing about
> what that run did. Creating is not verifying.

### 10 — The check

**Do:** wait for the replacement run to finish, then type into Airy:

> did the replacement run actually record the settlement?

The model calls `verify_replacement_run` on the exact run it triggered. Have the
replacement run's `notify_back_office` log ready in the second tab.

**Say:**

> Before the fix, this same check came back occurred: false. The run had finished, every
> output record was read, and there was no receipt. And Airy's own wording matters here:
> it recorded nothing — not "the work did not happen". Now, on the replacement run:
> occurred: true. The task recorded its output — a real settlement receipt with a
> confirmation id, and the back-office notification finally names it instead of PENDING.
> One honest boundary to finish on: external_system_checked is false, and it is always
> false. This is Airflow's record of the task's output. Nobody here observed the payment
> processor. We verified what the run recorded — not that the money moved.

### Close — unconditionally

**Say:**

> So, what you just saw: Airy used runtime state unavailable to a repository-only agent,
> safely corrected the workflow under approval, and checked what the replacement run
> recorded. The differentiator is not the fix. It is runtime state — which instance, on
> which run, with which evidence — and the check at the end.

Then stop.

### Q&A — if asked: "What if open-source Airflow adds better logging for manual task overrides?"

**Say** *(20–30 seconds)*:

> We would welcome it — this attribution example would get easier, or disappear. Airy
> reasons over whatever the runtime records. Today that record has gaps, so it lists
> alternatives and refuses to pick one. Give it a row that says who changed the state,
> through which interface, and it reports that as fact instead of a shortlist. Better
> observability makes Airy sharper, not obsolete. And one thing survives any logging
> improvement: a row proves a request was received and logged — not that it was
> authorised, and not that it changed the state.

---

**If something refuses:** a refusal is the guardrail working — read its sentence aloud and
re-ask (playbook, runbook §7). **Ids on screen beat ids on paper:** version numbers,
`EOD-…` batch and `PROC-…` confirmation ids change every day — read them off the screen.
