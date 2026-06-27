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

//! DagSpec -> Airflow v3 serialized blob (the `serialized_dag.data` column) + a
//! `dag_hash` that is byte-identical to `SerializedDagModel.hash`.

const std = @import("std");
const dag = @import("dag.zig");

const SERIALIZER_VERSION = 3;
const Writer = std.Io.Writer;

pub const Serialized = struct {
    data: []u8, // the v3 blob WITH fileloc -> serialized_dag.data column
    dag_hash: [32]u8,
    dag_id: []const u8,
};

pub fn md5Hex(input: []const u8, out: *[32]u8) void {
    var digest: [16]u8 = undefined;
    std.crypto.hash.Md5.hash(input, &digest, .{});
    const hex = "0123456789abcdef";
    for (digest, 0..) |byte, i| {
        out[i * 2] = hex[byte >> 4];
        out[i * 2 + 1] = hex[byte & 0x0f];
    }
}

pub fn serialize(alloc: std.mem.Allocator, spec: dag.Spec, fileloc: []const u8, dags_folder: []const u8) !Serialized {
    const ids = try sortedTaskIds(alloc, spec);

    var data_aw: Writer.Allocating = .init(alloc);
    try emit(&data_aw.writer, spec, fileloc, dags_folder, ids, true);

    // Hash input: our emitted blob WITHOUT fileloc, re-serialized canonically to match Python.
    var raw_aw: Writer.Allocating = .init(alloc);
    defer raw_aw.deinit();
    try emit(&raw_aw.writer, spec, fileloc, dags_folder, ids, false);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_aw.written(), .{});
    defer parsed.deinit();
    var canon_aw: Writer.Allocating = .init(alloc);
    defer canon_aw.deinit();
    try writeCanonicalJson(&canon_aw.writer, parsed.value, alloc);

    var hash: [32]u8 = undefined;
    md5Hex(canon_aw.written(), &hash);

    return .{ .data = try data_aw.toOwnedSlice(), .dag_hash = hash, .dag_id = spec.dag_id };
}

// ---------------------------------------------------------------------------
// Primitive writers
// ---------------------------------------------------------------------------
fn writeJsonString(w: *Writer, s: []const u8) !void {
    try w.writeByte('"');
    for (s) |ch| switch (ch) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        '\t' => try w.writeAll("\\t"),
        else => try w.writeByte(ch),
    };
    try w.writeByte('"');
}

/// Match Python repr for our values: integral floats print "<n>.0".
fn writeFloat(w: *Writer, v: f64) !void {
    if (v == @trunc(v)) try w.print("{d:.1}", .{v}) else try w.print("{d}", .{v});
}

/// Unix epoch (UTC midnight) for Y/M/D — Howard Hinnant's days_from_civil.
fn epochFromYMD(y: i64, m: i64, d: i64) f64 {
    var yy = y;
    yy -= @intFromBool(m <= 2);
    const era = @divFloor(if (yy >= 0) yy else yy - 399, 400);
    const yoe = yy - era * 400;
    const doy = @divTrunc(153 * (m + (if (m > 2) @as(i64, -3) else 9)) + 2, 5) + d - 1;
    const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + doy;
    const days = era * 146097 + doe - 719468;
    return @as(f64, @floatFromInt(days * 86400));
}

fn lessThanStr(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

fn sortedTaskIds(alloc: std.mem.Allocator, spec: dag.Spec) ![]const []const u8 {
    const ids = try alloc.alloc([]const u8, spec.tasks.len);
    for (spec.tasks, 0..) |t, i| ids[i] = t.task_id;
    std.mem.sort([]const u8, ids, {}, lessThanStr);
    return ids;
}

/// Emit `,"downstream_task_ids":[...]` (only when non-empty), in dependency order.
fn writeDownstream(w: *Writer, spec: dag.Spec, task_id: []const u8) !void {
    var first = true;
    for (spec.deps) |dep| {
        if (!std.mem.eql(u8, dep.up, task_id)) continue;
        try w.writeAll(if (first) ",\"downstream_task_ids\":[" else ",");
        first = false;
        try writeJsonString(w, dep.down);
    }
    if (!first) try w.writeByte(']');
}

// ---------------------------------------------------------------------------
// The v3 blob, canonical (alphabetically sorted) key order. Arrays keep emit
// order. `include_fileloc=false` is the pre-hash form (Airflow pops fileloc).
// ---------------------------------------------------------------------------
fn emit(
    w: *Writer,
    spec: dag.Spec,
    fileloc: []const u8,
    dags_folder: []const u8,
    sorted_task_ids: []const []const u8,
    include_fileloc: bool,
) !void {
    try w.print("{{\"__version\":{d},\"dag\":{{", .{SERIALIZER_VERSION});

    try w.writeAll("\"_processor_dags_folder\":");
    try writeJsonString(w, dags_folder);
    try w.writeAll(",\"allowed_run_types\":null,\"catchup\":false,\"dag_dependencies\":[],\"dag_id\":");
    try writeJsonString(w, spec.dag_id);
    try w.writeAll(",\"deadline\":null,\"disable_bundle_versioning\":false,\"edge_info\":{}");
    if (include_fileloc) {
        try w.writeAll(",\"fileloc\":");
        try writeJsonString(w, fileloc);
    }
    try w.writeAll(",\"max_active_runs\":16,\"max_active_tasks\":16,\"max_consecutive_failed_dag_runs\":0,\"params\":[]");
    // relative_fileloc (path relative to the bundle root) is part of the hash, unlike fileloc.
    try w.writeAll(",\"relative_fileloc\":");
    try writeJsonString(w, std.fs.path.basename(fileloc));
    try w.writeAll(",\"start_date\":");
    try writeFloat(w, epochFromYMD(spec.start_y, spec.start_m, spec.start_d));

    try w.writeAll(",\"task_group\":{\"_group_id\":null,\"children\":{");
    for (sorted_task_ids, 0..) |tid, i| {
        if (i != 0) try w.writeByte(',');
        try writeJsonString(w, tid);
        try w.writeAll(":[\"operator\",");
        try writeJsonString(w, tid);
        try w.writeByte(']');
    }
    try w.writeAll("},\"downstream_group_ids\":[],\"downstream_task_ids\":[]," ++
        "\"group_display_name\":\"\",\"prefix_group_id\":true,\"tooltip\":\"\"," ++
        "\"ui_color\":\"CornflowerBlue\",\"ui_fgcolor\":\"#000\"," ++
        "\"upstream_group_ids\":[],\"upstream_task_ids\":[]}");

    try w.writeAll(",\"tasks\":[");
    for (spec.tasks, 0..) |t, ti| {
        if (ti != 0) try w.writeByte(',');
        try emitTask(w, spec, t);
    }
    try w.writeByte(']');

    if (spec.schedule) |cron| {
        try w.writeAll(",\"timetable\":{\"__type\":\"airflow.timetables.trigger.CronTriggerTimetable\"," ++
            "\"__var\":{\"expression\":");
        try writeJsonString(w, cron);
        try w.writeAll(",\"interval\":0.0,\"run_immediately\":false,\"timezone\":\"UTC\"}}");
    } else {
        try w.writeAll(",\"timetable\":{\"__type\":\"airflow.timetables.simple.NullTimetable\",\"__var\":{}}");
    }
    try w.writeAll(",\"timezone\":\"UTC\"}}");
}

fn emitTask(w: *Writer, spec: dag.Spec, t: dag.TaskSpec) !void {
    try w.writeAll("{\"__type\":\"operator\",\"__var\":{");
    switch (t.kind) {
        .empty => {
            try w.writeAll("\"_is_empty\":true,\"_needs_expansion\":false," ++
                "\"_task_module\":\"airflow.providers.standard.operators.empty\"");
            try writeDownstream(w, spec, t.task_id);
            try w.writeAll(",\"has_retry_policy\":false,\"retry_delay\":300.0,\"task_id\":");
            try writeJsonString(w, t.task_id);
            try w.writeAll(",\"task_type\":\"EmptyOperator\",\"template_fields\":[],\"ui_color\":\"#e8f7e4\"}}");
        },
        .decorated => {
            try w.writeAll("\"_needs_expansion\":false,\"_operator_name\":\"@task\"," ++
                "\"_task_module\":\"airflow.providers.standard.decorators.python\"");
            try writeDownstream(w, spec, t.task_id);
            // op_args: one xcom_pull template per upstream (deps with down==task_id), in order.
            try w.writeAll(",\"has_retry_policy\":false,\"op_args\":[");
            var first_arg = true;
            for (spec.deps) |dep| {
                if (!std.mem.eql(u8, dep.down, t.task_id)) continue;
                if (!first_arg) try w.writeByte(',');
                first_arg = false;
                try w.writeAll("\"{{ task_instance.xcom_pull(task_ids='");
                try w.writeAll(dep.up); // task_ids/dag_id are plain identifiers (no JSON escape needed)
                try w.writeAll("', dag_id='");
                try w.writeAll(spec.dag_id);
                try w.writeAll("', key='return_value') }}\"");
            }
            try w.writeAll("],\"op_kwargs\":{},\"python_callable_name\":");
            try writeJsonString(w, t.callable_name);
            try w.writeAll(",\"retry_delay\":300.0,\"task_id\":");
            try writeJsonString(w, t.task_id);
            try w.writeAll(",\"task_type\":\"_PythonDecoratedOperator\"," ++
                "\"template_fields\":[\"templates_dict\",\"op_args\",\"op_kwargs\"]," ++
                "\"template_fields_renderers\":{\"op_args\":\"py\",\"op_kwargs\":\"py\",\"templates_dict\":\"json\"}," ++
                "\"ui_color\":\"#ffefeb\"}}");
        },
    }
}

// ---------------------------------------------------------------------------
// Canonical re-serialization for dag_hash parity with Python's
// SerializedDagModel.hash: _sort_serialized_dag_dict (sort dict keys; sort a
// list of operators by __var.task_id; sort a list of strings; else preserve) +
// json.dumps defaults (", " / ": " separators).
// ---------------------------------------------------------------------------
fn jsonTaskId(v: std.json.Value) ?[]const u8 {
    if (v != .object) return null;
    const var_ = v.object.get("__var") orelse return null;
    if (var_ != .object) return null;
    const tid = var_.object.get("task_id") orelse return null;
    return if (tid == .string) tid.string else null;
}

fn lessByTaskId(items: []std.json.Value, a: usize, b: usize) bool {
    return std.mem.lessThan(u8, jsonTaskId(items[a]).?, jsonTaskId(items[b]).?);
}

fn lessByString(items: []std.json.Value, a: usize, b: usize) bool {
    return std.mem.lessThan(u8, items[a].string, items[b].string);
}

fn writeCanonicalJson(w: *Writer, v: std.json.Value, alloc: std.mem.Allocator) !void {
    switch (v) {
        .null => try w.writeAll("null"),
        .bool => |b| try w.writeAll(if (b) "true" else "false"),
        .integer => |i| try w.print("{d}", .{i}),
        .float => |f| try writeFloat(w, f),
        .number_string => |s| try w.writeAll(s),
        .string => |s| try writeJsonString(w, s),
        .array => |arr| {
            const items = arr.items;
            const order = try alloc.alloc(usize, items.len);
            defer alloc.free(order);
            for (order, 0..) |*o, i| o.* = i;
            var all_ops = items.len > 0;
            var all_str = items.len > 0;
            for (items) |it| {
                if (jsonTaskId(it) == null) all_ops = false;
                if (it != .string) all_str = false;
            }
            if (all_ops) std.mem.sort(usize, order, items, lessByTaskId) //
            else if (all_str) std.mem.sort(usize, order, items, lessByString);
            try w.writeByte('[');
            for (order, 0..) |idx, n| {
                if (n != 0) try w.writeAll(", ");
                try writeCanonicalJson(w, items[idx], alloc);
            }
            try w.writeByte(']');
        },
        .object => |obj| {
            const keys = try alloc.alloc([]const u8, obj.count());
            defer alloc.free(keys);
            var it = obj.iterator();
            var i: usize = 0;
            while (it.next()) |e| : (i += 1) keys[i] = e.key_ptr.*;
            std.mem.sort([]const u8, keys, {}, lessThanStr);
            try w.writeByte('{');
            for (keys, 0..) |k, n| {
                if (n != 0) try w.writeAll(", ");
                try writeJsonString(w, k);
                try w.writeAll(": ");
                try writeCanonicalJson(w, obj.get(k).?, alloc);
            }
            try w.writeByte('}');
        },
    }
}

test "epoch from ymd" {
    try std.testing.expectEqual(@as(f64, 1704067200.0), epochFromYMD(2024, 1, 1));
}
