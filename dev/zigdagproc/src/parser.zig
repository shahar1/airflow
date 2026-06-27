// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Static Dag extraction over a tree-sitter-python AST (NO Python executed).
//! Recognised subset:
//!   with DAG(dag_id="..", schedule=".."|None, start_date=datetime(Y,M,D), ...):
//!       <var> = EmptyOperator(task_id="..")        a >> b >> c   a << b   a >> [b, c]
//!   @dag(...) def f(): @task def g(): ...; g()     (TaskFlow call graph)
//! Real Python syntax (whitespace, comments, multi-line, nesting) is handled by
//! tree-sitter; only the *semantic* subset above is extracted.

const std = @import("std");
const ts = @import("tree_sitter");
const dag = @import("dag.zig");

// Provided by the vendored tree-sitter-python grammar (vendor/tree-sitter-python).
extern fn tree_sitter_python() callconv(.c) *const anyopaque;

const Node = ts.Node;
const List = std.ArrayList;
const eql = std.mem.eql;

pub const Error = error{ ParseFailed, NoDagId, NoSupportedTasks } || std.mem.Allocator.Error;

pub fn parse(alloc: std.mem.Allocator, src: []const u8) Error!dag.Spec {
    const parser = ts.Parser.create();
    defer parser.destroy();
    parser.setLanguage(ts.Language.fromRaw(tree_sitter_python())) catch return error.ParseFailed;
    const tree = parser.parseString(src, null) orelse return error.ParseFailed;
    defer tree.destroy();

    var ctx = Ctx{
        .alloc = alloc,
        .src = src,
        .var_to_task = std.StringHashMap([]const u8).init(alloc),
        .task_funcs = std.StringHashMap([]const u8).init(alloc),
        .consts = std.StringHashMap([]const u8).init(alloc),
    };
    try walk(&ctx, tree.rootNode());

    if (ctx.dag_id == null) return error.NoDagId;
    // No tasks we recognise -> not in our subset; skip rather than write a bogus empty Dag.
    if (ctx.tasks.items.len == 0) return error.NoSupportedTasks;
    return .{
        .dag_id = ctx.dag_id.?,
        .schedule = if (ctx.have_schedule) ctx.schedule else null,
        .start_y = ctx.sy,
        .start_m = ctx.sm,
        .start_d = ctx.sd,
        .tasks = ctx.tasks.items,
        .deps = ctx.deps.items,
    };
}

const Ctx = struct {
    alloc: std.mem.Allocator,
    src: []const u8,
    dag_id: ?[]const u8 = null,
    schedule: ?[]const u8 = null,
    have_schedule: bool = false,
    sy: i64 = 1970,
    sm: i64 = 1,
    sd: i64 = 1,
    tasks: List(dag.TaskSpec) = .empty,
    deps: List(dag.Dep) = .empty,
    var_to_task: std.StringHashMap([]const u8),
    dag_func_name: []const u8 = "", // the @dag-decorated function's name (TaskFlow)
    task_funcs: std.StringHashMap([]const u8), // @task func name -> task_id
    consts: std.StringHashMap([]const u8), // module-level NAME = "literal" (e.g. DAG_ID)
};

/// Resolve a string-or-identifier node to a string value (constant propagation).
fn resolveStr(ctx: *Ctx, node: Node) ?[]const u8 {
    if (eql(u8, node.kind(), "string")) return stringContent(node, ctx.src);
    if (eql(u8, node.kind(), "identifier")) return ctx.consts.get(text(node, ctx.src));
    return null;
}

// ---------------------------------------------------------------------------
// Node helpers
// ---------------------------------------------------------------------------
fn text(node: Node, src: []const u8) []const u8 {
    return src[node.startByte()..node.endByte()];
}

/// The content of a `string` node, without the surrounding quotes/prefix.
fn stringContent(node: Node, src: []const u8) []const u8 {
    var i: u32 = 0;
    while (i < node.namedChildCount()) : (i += 1) {
        const ch = node.namedChild(i).?;
        if (eql(u8, ch.kind(), "string_content")) return text(ch, src);
    }
    return "";
}

/// If `node` is a shift expression, return ">>" or "<<", else null.
fn shiftOp(node: Node, src: []const u8) ?[]const u8 {
    var i: u32 = 0;
    while (i < node.childCount()) : (i += 1) {
        const t = text(node.child(i).?, src);
        if (eql(u8, t, ">>") or eql(u8, t, "<<")) return t;
    }
    return null;
}

/// The callee name of a `call`: a bare identifier, or the last attribute of `a.b.Name`.
fn callName(call: Node, src: []const u8) []const u8 {
    const f = call.childByFieldName("function") orelse return "";
    if (eql(u8, f.kind(), "attribute")) {
        if (f.childByFieldName("attribute")) |attr| return text(attr, src);
    }
    return text(f, src);
}

/// `@task` -> "task", `@dag(...)` -> "dag".
fn decoratorName(deco: Node, src: []const u8) []const u8 {
    const e = deco.namedChild(0) orelse return "";
    if (eql(u8, e.kind(), "call")) return callName(e, src);
    if (eql(u8, e.kind(), "attribute")) {
        if (e.childByFieldName("attribute")) |a| return text(a, src);
    }
    return text(e, src);
}

/// Expand Airflow's cron preset aliases (@daily -> "0 0 * * *", ...). Non-cron
/// presets (@once, @continuous) pass through and route to the Python fallback.
fn normalizeCron(s: []const u8) []const u8 {
    const Preset = struct { alias: []const u8, cron: []const u8 };
    const presets = [_]Preset{
        .{ .alias = "@yearly", .cron = "0 0 1 1 *" },
        .{ .alias = "@annually", .cron = "0 0 1 1 *" },
        .{ .alias = "@monthly", .cron = "0 0 1 * *" },
        .{ .alias = "@weekly", .cron = "0 0 * * 0" },
        .{ .alias = "@daily", .cron = "0 0 * * *" },
        .{ .alias = "@midnight", .cron = "0 0 * * *" },
        .{ .alias = "@hourly", .cron = "0 * * * *" },
    };
    for (presets) |p| if (eql(u8, s, p.alias)) return p.cron;
    return s;
}

// ---------------------------------------------------------------------------
// DAG(...) / EmptyOperator / dependency extraction
// ---------------------------------------------------------------------------
fn handleDagCall(ctx: *Ctx, call: Node) void {
    const args = call.childByFieldName("arguments") orelse return;
    var i: u32 = 0;
    while (i < args.namedChildCount()) : (i += 1) {
        const arg = args.namedChild(i).?;
        if (eql(u8, arg.kind(), "keyword_argument")) {
            const name_node = arg.childByFieldName("name") orelse continue;
            const value = arg.childByFieldName("value") orelse continue;
            const key = text(name_node, ctx.src);
            if (eql(u8, key, "dag_id")) {
                if (resolveStr(ctx, value)) |v| ctx.dag_id = v;
            } else if (eql(u8, key, "schedule")) {
                ctx.have_schedule = true;
                ctx.schedule = if (resolveStr(ctx, value)) |v| normalizeCron(v) else null;
            } else if (eql(u8, key, "start_date")) {
                parseStartDate(ctx, value);
            }
        } else if (ctx.dag_id == null and !eql(u8, arg.kind(), "keyword_argument")) {
            if (resolveStr(ctx, arg)) |v| ctx.dag_id = v; // positional dag_id (literal or constant)
        }
    }
}

fn parseStartDate(ctx: *Ctx, value: Node) void {
    if (!eql(u8, value.kind(), "call")) return; // expects datetime(Y, M, D)
    const args = value.childByFieldName("arguments") orelse return;
    var nums: [3]i64 = .{ 1970, 1, 1 };
    var ni: usize = 0;
    var i: u32 = 0;
    while (i < args.namedChildCount() and ni < 3) : (i += 1) {
        const a = args.namedChild(i).?;
        if (eql(u8, a.kind(), "integer")) {
            nums[ni] = std.fmt.parseInt(i64, text(a, ctx.src), 10) catch nums[ni];
            ni += 1;
        }
    }
    ctx.sy = nums[0];
    ctx.sm = nums[1];
    ctx.sd = nums[2];
}

fn handleAssignment(ctx: *Ctx, node: Node) !void {
    const left = node.childByFieldName("left") orelse return;
    const right = node.childByFieldName("right") orelse return;
    if (!eql(u8, left.kind(), "identifier")) return;
    // NAME = "literal" -> remember it (constant propagation for dag_id/schedule).
    if (eql(u8, right.kind(), "string")) {
        try ctx.consts.put(text(left, ctx.src), stringContent(right, ctx.src));
        return;
    }
    if (!eql(u8, right.kind(), "call")) return;
    if (!eql(u8, callName(right, ctx.src), "EmptyOperator")) return;
    const args = right.childByFieldName("arguments") orelse return;
    var i: u32 = 0;
    while (i < args.namedChildCount()) : (i += 1) {
        const arg = args.namedChild(i).?;
        if (!eql(u8, arg.kind(), "keyword_argument")) continue;
        const name_node = arg.childByFieldName("name") orelse continue;
        const value = arg.childByFieldName("value") orelse continue;
        if (eql(u8, text(name_node, ctx.src), "task_id") and eql(u8, value.kind(), "string")) {
            const tid = stringContent(value, ctx.src);
            try ctx.tasks.append(ctx.alloc, .{ .task_id = tid });
            try ctx.var_to_task.put(text(left, ctx.src), tid);
        }
    }
}

/// Flatten a left-associative chain of one shift operator into ordered operands.
fn collectOperands(node: Node, op: []const u8, src: []const u8, list: *List(Node), alloc: std.mem.Allocator) !void {
    if (eql(u8, node.kind(), "binary_operator")) {
        if (shiftOp(node, src)) |this_op| {
            if (eql(u8, this_op, op)) {
                try collectOperands(node.childByFieldName("left").?, op, src, list, alloc);
                try list.append(alloc, node.childByFieldName("right").?);
                return;
            }
        }
    }
    try list.append(alloc, node);
}

/// Resolve an operand (identifier or `[a, b]` list) to the task_ids it names.
fn taskIdsOf(ctx: *Ctx, node: Node, out: *List([]const u8)) !void {
    if (eql(u8, node.kind(), "list")) {
        var i: u32 = 0;
        while (i < node.namedChildCount()) : (i += 1) try taskIdsOf(ctx, node.namedChild(i).?, out);
    } else if (ctx.var_to_task.get(text(node, ctx.src))) |tid| {
        try out.append(ctx.alloc, tid);
    }
}

fn handleDepChain(ctx: *Ctx, node: Node, op: []const u8) !void {
    var operands: List(Node) = .empty;
    try collectOperands(node, op, ctx.src, &operands, ctx.alloc);
    if (operands.items.len < 2) return;
    var i: usize = 0;
    while (i + 1 < operands.items.len) : (i += 1) {
        var lefts: List([]const u8) = .empty;
        var rights: List([]const u8) = .empty;
        try taskIdsOf(ctx, operands.items[i], &lefts);
        try taskIdsOf(ctx, operands.items[i + 1], &rights);
        for (lefts.items) |l| for (rights.items) |r| {
            if (eql(u8, op, ">>")) {
                try ctx.deps.append(ctx.alloc, .{ .up = l, .down = r });
            } else {
                try ctx.deps.append(ctx.alloc, .{ .up = r, .down = l });
            }
        };
    }
}

// ---------------------------------------------------------------------------
// TaskFlow (@dag / @task)
// ---------------------------------------------------------------------------
/// Resolve an expression to the task_id it "produces": a variable bound to a task,
/// or a call to a @task function (recording that call's deps/op_args).
fn resolveProduces(ctx: *Ctx, node: Node) !?[]const u8 {
    const k = node.kind();
    if (eql(u8, k, "identifier")) return ctx.var_to_task.get(text(node, ctx.src));
    if (eql(u8, k, "call")) {
        const tid = ctx.task_funcs.get(callName(node, ctx.src)) orelse return null;
        if (node.childByFieldName("arguments")) |args| {
            var i: u32 = 0;
            while (i < args.namedChildCount()) : (i += 1) {
                const arg = args.namedChild(i).?;
                if (eql(u8, arg.kind(), "keyword_argument")) continue; // op_kwargs: skipped
                if (try resolveProduces(ctx, arg)) |producer| {
                    try ctx.deps.append(ctx.alloc, .{ .up = producer, .down = tid });
                }
            }
        }
        return tid;
    }
    return null;
}

fn registerTaskFunc(ctx: *Ctx, decorated: Node) !void {
    const def = decorated.childByFieldName("definition") orelse return;
    if (!eql(u8, def.kind(), "function_definition")) return;
    const fname = text(def.childByFieldName("name") orelse return, ctx.src);
    var is_task = false;
    var i: u32 = 0;
    while (i < decorated.namedChildCount()) : (i += 1) {
        const ch = decorated.namedChild(i).?;
        if (eql(u8, ch.kind(), "decorator") and eql(u8, decoratorName(ch, ctx.src), "task")) is_task = true;
    }
    if (!is_task) return;
    const callable = try std.fmt.allocPrint(ctx.alloc, "{s}.<locals>.{s}", .{ ctx.dag_func_name, fname });
    try ctx.tasks.append(ctx.alloc, .{ .task_id = fname, .kind = .decorated, .callable_name = callable });
    try ctx.task_funcs.put(fname, fname);
}

fn handleDagFunction(ctx: *Ctx, deco: Node, def: Node, fname: []const u8) !void {
    ctx.dag_func_name = fname;
    const deco_expr = deco.namedChild(0).?;
    if (eql(u8, deco_expr.kind(), "call")) handleDagCall(ctx, deco_expr);
    if (ctx.dag_id == null) ctx.dag_id = fname; // default dag_id = function name

    const body = def.childByFieldName("body") orelse return;
    var i: u32 = 0;
    while (i < body.namedChildCount()) : (i += 1) { // pass 1: register tasks (so calls resolve)
        const st = body.namedChild(i).?;
        if (eql(u8, st.kind(), "decorated_definition")) try registerTaskFunc(ctx, st);
    }
    i = 0;
    while (i < body.namedChildCount()) : (i += 1) { // pass 2: resolve the call graph
        const st = body.namedChild(i).?;
        // a statement is either bare (call/assignment) or wrapped in expression_statement
        const ex = if (eql(u8, st.kind(), "expression_statement")) (st.namedChild(0) orelse continue) else st;
        if (eql(u8, ex.kind(), "assignment")) {
            const r = ex.childByFieldName("right") orelse continue;
            const left = ex.childByFieldName("left") orelse continue;
            if (try resolveProduces(ctx, r)) |tid| {
                if (eql(u8, left.kind(), "identifier")) try ctx.var_to_task.put(text(left, ctx.src), tid);
            }
        } else {
            _ = try resolveProduces(ctx, ex);
        }
    }
}

fn walk(ctx: *Ctx, node: Node) !void {
    const k = node.kind();
    if (eql(u8, k, "decorated_definition")) {
        if (node.childByFieldName("definition")) |def| {
            if (eql(u8, def.kind(), "function_definition")) {
                var i: u32 = 0;
                while (i < node.namedChildCount()) : (i += 1) {
                    const ch = node.namedChild(i).?;
                    if (eql(u8, ch.kind(), "decorator") and eql(u8, decoratorName(ch, ctx.src), "dag")) {
                        try handleDagFunction(ctx, ch, def, text(def.childByFieldName("name").?, ctx.src));
                        return; // @dag body handled wholesale; don't descend
                    }
                }
            }
        }
    } else if (eql(u8, k, "call")) {
        if (eql(u8, callName(node, ctx.src), "DAG")) handleDagCall(ctx, node);
    } else if (eql(u8, k, "assignment")) {
        try handleAssignment(ctx, node);
    } else if (eql(u8, k, "binary_operator")) {
        if (shiftOp(node, ctx.src)) |op| {
            try handleDepChain(ctx, node, op);
            return; // the whole chain is consumed; don't descend into it
        }
    }
    var i: u32 = 0;
    while (i < node.namedChildCount()) : (i += 1) try walk(ctx, node.namedChild(i).?);
}

test "parse python dag subset" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const spec = try parse(arena.allocator(),
        \\with DAG(dag_id="t", schedule="0 0 * * *", start_date=datetime(2024, 1, 1)):
        \\    a = EmptyOperator(task_id="a")
        \\    b = EmptyOperator(task_id="b")
        \\    a >> b
    );
    try std.testing.expectEqualStrings("t", spec.dag_id);
    try std.testing.expectEqualStrings("0 0 * * *", spec.schedule.?);
    try std.testing.expectEqual(@as(i64, 2024), spec.start_y);
    try std.testing.expectEqual(@as(usize, 2), spec.tasks.len);
    try std.testing.expectEqual(@as(usize, 1), spec.deps.len);
    try std.testing.expectEqualStrings("a", spec.deps[0].up);
    try std.testing.expectEqualStrings("b", spec.deps[0].down);
}

test "parse taskflow @dag/@task call graph" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const spec = try parse(arena.allocator(),
        \\@dag(dag_id="tf", schedule="@daily", start_date=datetime(2024, 1, 1))
        \\def tf():
        \\    @task
        \\    def extract():
        \\        return 1
        \\    @task
        \\    def load(x):
        \\        print(x)
        \\    load(extract())
        \\tf()
    );
    try std.testing.expectEqualStrings("tf", spec.dag_id);
    try std.testing.expectEqualStrings("0 0 * * *", spec.schedule.?); // @daily expanded
    try std.testing.expectEqual(@as(usize, 2), spec.tasks.len);
    try std.testing.expectEqual(dag.TaskKind.decorated, spec.tasks[0].kind);
    try std.testing.expectEqualStrings("tf.<locals>.extract", spec.tasks[0].callable_name);
    try std.testing.expectEqual(@as(usize, 1), spec.deps.len);
    try std.testing.expectEqualStrings("extract", spec.deps[0].up);
    try std.testing.expectEqualStrings("load", spec.deps[0].down);
}
