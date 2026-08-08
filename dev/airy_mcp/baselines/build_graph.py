# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Phase B inventory generator — ANALYSIS ONLY, writes nothing into the repo.

Builds, by AST analysis of ``dev/airy_mcp/server.py``:

* ``dependency-graph.json``  nodes = top-level defs, edges = calls (incl.
  references passed as values, e.g. ``mcp.tool(_tool)`` and callables handed to
  ``_attribution_reader``);
* ``dependency-graph.md``    SCCs, fan-in / fan-out leaders, cycles;
* ``clamps.json``            every slice / cap / pagination loop, located.

Reproducible: run it again against the same commit and the output is identical.

    python3 build_graph.py [<server.py>] [<outdir>]
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

SERVER = Path(
    sys.argv[1] if len(sys.argv) > 1 else "/home/shahar/repos/apache/airflow/dev/airy_mcp/server.py"
)
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else "/tmp/airy-gauntlet/phaseB/inventory")

SOURCE = SERVER.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)
LINES = SOURCE.splitlines()

# --------------------------------------------------------------------------
# Nodes: every top-level def / class.
# --------------------------------------------------------------------------
DEFS: dict[str, dict] = {}
for node in TREE.body:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        DEFS[node.name] = {
            "name": node.name,
            "line": node.lineno,
            "end_line": getattr(node, "end_lineno", node.lineno),
            "kind": "class" if isinstance(node, ast.ClassDef) else "def",
            "public": not node.name.startswith("_"),
        }

# The MCP-registered tools: the module-level ``for _tool in (...)`` at the bottom.
# Module-level only — ``for strip in (_strip_context_rows, …)`` inside
# ``_enforce_attribution_ceiling`` is the same shape and is not a registration.
TOOLS: set[str] = set()
for node in TREE.body:
    if isinstance(node, ast.For) and isinstance(node.iter, ast.Tuple):
        for element in node.iter.elts:
            if isinstance(element, ast.Name) and element.id in DEFS:
                TOOLS.add(element.id)


def resolve(node: ast.AST) -> str | None:
    """The top-level def a call/reference names, or None."""
    if isinstance(node, ast.Name) and node.id in DEFS:
        return node.id
    return None


class Edges(ast.NodeVisitor):
    """Calls and value-references from one top-level def to another."""

    def __init__(self, owner: str) -> None:
        self.owner = owner
        self.calls: set[str] = set()
        self.refs: set[str] = set()

    def visit_Call(self, node: ast.Call) -> None:
        target = resolve(node.func)
        if target:
            self.calls.add(target)
        # A def handed as an argument is a real dependency: _attribution_reader's
        # closure, the strip functions in _enforce_attribution_ceiling.
        for arg in [*node.args, *(kw.value for kw in node.keywords)]:
            ref = resolve(arg)
            if ref:
                self.refs.add(ref)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load) and node.id in DEFS:
            self.refs.add(node.id)
        self.generic_visit(node)


EDGES: dict[str, dict[str, list[str]]] = {}
for node in TREE.body:
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        continue
    visitor = Edges(node.name)
    for child in node.body:
        visitor.visit(child)
    # decorators / bases too
    for extra in [*getattr(node, "decorator_list", []), *getattr(node, "bases", [])]:
        visitor.visit(extra)
    calls = sorted(visitor.calls)
    refs = sorted(visitor.refs - visitor.calls)
    EDGES[node.name] = {"calls": calls, "references": refs}

ALL_OUT = {name: sorted(set(e["calls"]) | set(e["references"])) for name, e in EDGES.items()}

# --------------------------------------------------------------------------
# Fan-in / fan-out.
# --------------------------------------------------------------------------
fan_in: dict[str, list[str]] = {name: [] for name in DEFS}
for src, targets in ALL_OUT.items():
    for tgt in targets:
        if tgt != src:
            fan_in[tgt].append(src)
for name in fan_in:
    fan_in[name] = sorted(set(fan_in[name]))


# --------------------------------------------------------------------------
# Tarjan SCC (iterative — the graph is small but recursion depth is a landmine).
# --------------------------------------------------------------------------
def strongly_connected(graph: dict[str, list[str]]) -> list[list[str]]:
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    result: list[list[str]] = []
    counter = 0

    for root in graph:
        if root in index:
            continue
        work = [(root, iter(graph.get(root, [])))]
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            node, children = work[-1]
            advanced = False
            for child in children:
                if child not in graph:
                    continue
                if child not in index:
                    index[child] = low[child] = counter
                    counter += 1
                    stack.append(child)
                    on_stack.add(child)
                    work.append((child, iter(graph.get(child, []))))
                    advanced = True
                    break
                if child in on_stack:
                    low[node] = min(low[node], index[child])
            if advanced:
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
            if low[node] == index[node]:
                component = []
                while True:
                    top = stack.pop()
                    on_stack.discard(top)
                    component.append(top)
                    if top == node:
                        break
                result.append(sorted(component))
    return result


SCCS = strongly_connected(ALL_OUT)
NONTRIVIAL = [c for c in SCCS if len(c) > 1]
SELF_LOOPS = sorted(name for name, targets in ALL_OUT.items() if name in targets)

# --------------------------------------------------------------------------
# Reachability from the 14 MCP tools — what is live, what is not.
# --------------------------------------------------------------------------
reachable: set[str] = set()
queue = sorted(TOOLS)
while queue:
    current = queue.pop()
    if current in reachable:
        continue
    reachable.add(current)
    queue.extend(ALL_OUT.get(current, []))
UNREACHED = sorted(set(DEFS) - reachable - {"main"})

# --------------------------------------------------------------------------
# Clamps: every slice, cap, pagination loop.
# --------------------------------------------------------------------------
LIMIT_NAMES = {
    name.id
    for node in TREE.body
    if isinstance(node, ast.Assign)
    for name in node.targets
    if isinstance(name, ast.Name)
    and any(
        token in name.id for token in ("LIMIT", "SCAN", "PAGE", "CLAMP", "BUDGET", "MAX", "CHARS", "FLOOR")
    )
}


def owner_of(line: int) -> str:
    best = "<module>"
    for name, info in DEFS.items():
        if info["line"] <= line <= info["end_line"]:
            best = name
    return best


CLAMPS: list[dict] = []
for ast_node in ast.walk(TREE):
    line = getattr(ast_node, "lineno", None)
    if line is None:
        continue
    if isinstance(ast_node, ast.Subscript) and isinstance(ast_node.slice, ast.Slice):
        CLAMPS.append(
            {
                "kind": "slice",
                "line": line,
                "owner": owner_of(line),
                "text": LINES[line - 1].strip()[:180],
            }
        )
    elif isinstance(ast_node, ast.Call) and isinstance(ast_node.func, ast.Name) and ast_node.func.id == "max":
        # ``max(total - len(rows), 0)`` — the omitted-count idiom.
        text = LINES[line - 1].strip()
        if "len(" in text and "-" in text:
            CLAMPS.append(
                {"kind": "omitted_count", "line": line, "owner": owner_of(line), "text": text[:180]}
            )
    elif isinstance(ast_node, ast.While):
        CLAMPS.append(
            {
                "kind": "pagination_loop",
                "line": line,
                "owner": owner_of(line),
                "text": LINES[line - 1].strip(),
            }
        )

for name in sorted(LIMIT_NAMES):
    for i, text in enumerate(LINES, 1):
        if name in text and ("limit" in text.lower() or "params" in text or "[:" in text or "budget" in text):
            owner = owner_of(i)
            if owner == "<module>":
                continue
            CLAMPS.append(
                {
                    "kind": "limit_applied",
                    "line": i,
                    "owner": owner,
                    "text": text.strip()[:180],
                    "limit": name,
                }
            )

CLAMPS.sort(key=lambda c: (c["line"], c["kind"]))

# --------------------------------------------------------------------------
# Emit.
# --------------------------------------------------------------------------
OUT.mkdir(parents=True, exist_ok=True)

graph = {
    "source": str(SERVER),
    "node_count": len(DEFS),
    "edge_count": sum(len(v) for v in ALL_OUT.values()),
    "mcp_tools": sorted(TOOLS),
    "nodes": [
        {
            **DEFS[name],
            "is_mcp_tool": name in TOOLS,
            "fan_out": len(ALL_OUT.get(name, [])),
            "fan_in": len(fan_in.get(name, [])),
            "calls": EDGES.get(name, {}).get("calls", []),
            "references": EDGES.get(name, {}).get("references", []),
            "called_by": fan_in.get(name, []),
        }
        for name in sorted(DEFS, key=lambda n: DEFS[n]["line"])
    ],
    "edges": [
        {"from": src, "to": tgt, "kind": "call" if tgt in EDGES[src]["calls"] else "reference"}
        for src in sorted(ALL_OUT)
        for tgt in ALL_OUT[src]
    ],
    "strongly_connected_components": {
        "total": len(SCCS),
        "nontrivial": NONTRIVIAL,
        "self_recursive": SELF_LOOPS,
    },
    "unreachable_from_tools": UNREACHED,
}
(OUT / "dependency-graph.json").write_text(json.dumps(graph, indent=2) + "\n", encoding="utf-8")
(OUT / "clamps.json").write_text(json.dumps(CLAMPS, indent=2) + "\n", encoding="utf-8")

by_fan_in = sorted(DEFS, key=lambda n: (-len(fan_in[n]), n))
by_fan_out = sorted(DEFS, key=lambda n: (-len(ALL_OUT.get(n, [])), n))

md = [
    "# Dependency graph — `dev/airy_mcp/server.py`",
    "",
    f"Generated by `build_graph.py` from `{SERVER}`. Nodes = top-level defs and classes; "
    "edges = calls plus value-references (a def handed as an argument is a real dependency).",
    "",
    f"- **{len(DEFS)}** top-level definitions ({sum(1 for d in DEFS.values() if d['kind'] == 'class')} classes, "
    f"{sum(1 for d in DEFS.values() if d['kind'] == 'def')} functions)",
    f"- **{graph['edge_count']}** edges",
    f"- **{len(TOOLS)}** registered MCP tools",
    f"- **{len(NONTRIVIAL)}** non-trivial strongly connected components; "
    f"**{len(SELF_LOOPS)}** self-recursive defs",
    "",
    "## Cycles / strongly connected components",
    "",
]
if NONTRIVIAL:
    for component in NONTRIVIAL:
        md.append(f"- **mutual recursion**: {', '.join(f'`{n}`' for n in component)}")
else:
    md.append("No mutual-recursion cycles: the call graph is a DAG apart from the self-recursive defs below.")
md += ["", "Self-recursive:"]
md += [f"- `{n}` (line {DEFS[n]['line']})" for n in SELF_LOOPS] or ["- none"]

md += [
    "",
    "## Fan-in leaders (most depended upon)",
    "",
    "| def | line | fan-in | callers |",
    "|---|---|---|---|",
]
for name in by_fan_in[:20]:
    callers = ", ".join(f"`{c}`" for c in fan_in[name][:8])
    if len(fan_in[name]) > 8:
        callers += f", … (+{len(fan_in[name]) - 8})"
    md.append(f"| `{name}` | {DEFS[name]['line']} | {len(fan_in[name])} | {callers or '—'} |")

md += [
    "",
    "## Fan-out leaders (most dependent)",
    "",
    "| def | line | fan-out | MCP tool |",
    "|---|---|---|---|",
]
for name in by_fan_out[:20]:
    md.append(
        f"| `{name}` | {DEFS[name]['line']} | {len(ALL_OUT.get(name, []))} | {'yes' if name in TOOLS else ''} |"
    )

md += [
    "",
    "## MCP tools and their transitive footprint",
    "",
    "| tool | line | direct | transitive |",
    "|---|---|---|---|",
]
for tool in sorted(TOOLS, key=lambda t: DEFS[t]["line"]):
    seen: set[str] = set()
    stack = list(ALL_OUT.get(tool, []))
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        stack.extend(ALL_OUT.get(current, []))
    md.append(f"| `{tool}` | {DEFS[tool]['line']} | {len(ALL_OUT.get(tool, []))} | {len(seen)} |")

md += ["", "## Not reachable from any MCP tool", ""]
md += [f"- `{n}` (line {DEFS[n]['line']})" for n in UNREACHED] or ["- none"]
md.append("")
(OUT / "dependency-graph.md").write_text("\n".join(md), encoding="utf-8")

print(
    f"nodes={len(DEFS)} edges={graph['edge_count']} tools={len(TOOLS)} sccs={len(NONTRIVIAL)} clamps={len(CLAMPS)}"
)
