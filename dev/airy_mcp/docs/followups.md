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

- [Documentation follow-ups — deliberately NOT mixed into extraction commits](#documentation-follow-ups--deliberately-not-mixed-into-extraction-commits)
  - [F-1 — the README tool table is stale: 14 tools registered, 13 documented](#f-1--the-readme-tool-table-is-stale-14-tools-registered-13-documented)
  - [F-2 — architecture and inventory documents now live in the repo](#f-2--architecture-and-inventory-documents-now-live-in-the-repo)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Documentation follow-ups — deliberately NOT mixed into extraction commits

## F-1 — the README tool table is stale: 14 tools registered, 13 documented

`server.py` registers **14** MCP tools. Verified by parsing the registration loop rather than by
counting the table:

```
diagnose_dag, compare_dag_runs, find_failure_clusters, plan_backfill, run_backfill,
get_blast_radius, plan_dag_code_changes, apply_dag_code_changes, plan_task_instance_clear,
apply_task_instance_clear, verify_task_instance_recovery, plan_revert_dag_code,
revert_dag_code, rerun_dag
```

`dev/airy_mcp/README.md`'s tool table lists **13**. The missing entry is
**`verify_task_instance_recovery`** (`server.py:6315`), added by commit `fe6333c55c`.

**The code is correct; the documentation is stale.** Left uncorrected on purpose: fixing it inside a
move-only extraction commit would put a substantive documentation change in a diff whose entire
value is that it contains none.

**Action:** correct the README tool table in its own commit, after the extraction is verified.
It should also carry the tool's read/write classification and its authorization requirements, since
`verify_task_instance_recovery` is the one tool whose XCom access requirement was made degradable
rather than mandatory.

## F-2 — architecture and inventory documents now live in the repo

Phase B produced its planning artifacts under `/tmp`. The authoritative copies are now tracked:

- `docs/extraction-plan.md` — 10-module target architecture, full definition assignment,
  dependency-ordered extraction sequence, cycle analysis, the `Reading`/`Verdict` sketch (design
  only), and the proposed Gate 4 v2 criteria.
- `docs/read-completeness-inventory.md` — every bounded read, clamp, pagination loop, completeness
  lifecycle site and absence-shaped helper, with line numbers.
- `docs/deferred-semantics.md` — the fifteen deferrals, none implementable during extraction.
- `baselines/` — the pre-extraction behaviour contract: collected test identities, the 14 tool
  schemas captured via `inspect.signature`, normalized representative outputs with their
  normalization rules, and the AST dependency graph plus its generator.

Bulky transcripts and local evidence deliberately remain outside the repo under
`/tmp/airy-gauntlet/`.
