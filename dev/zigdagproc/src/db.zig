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

//! Postgres persistence via libpq: upsert the dag, version-bump on hash change,
//! no-op when unchanged, mark stale on file removal. Mirrors Airflow's model so
//! a Zig-written dag is interchangeable with one the Python processor wrote.

const std = @import("std");
const c = @import("libpq");
const serializer = @import("serializer.zig");
const cron = @import("cron.zig");

pub const Conn = ?*c.PGconn;
pub const Error = error{ Connect, Query } || std.mem.Allocator.Error;
pub const WriteResult = enum { written, unchanged };

// File-scope PRNG for the random tail of UUIDv7. Uniqueness is carried by the
// embedded millisecond timestamp, so a clock-seeded Xoshiro is sufficient (not
// used for anything security-bearing).
var prng: std.Random.DefaultPrng = .init(0);

pub fn seedUuid(seed: u64) void {
    prng = .init(seed);
}

/// UUIDv7 (time-ordered) as a canonical 36-char string — Airflow uses uuid6.uuid7().
fn uuid7(ms: u64, out: *[36]u8) void {
    var b: [16]u8 = undefined;
    prng.random().bytes(&b);
    b[0] = @truncate(ms >> 40);
    b[1] = @truncate(ms >> 32);
    b[2] = @truncate(ms >> 24);
    b[3] = @truncate(ms >> 16);
    b[4] = @truncate(ms >> 8);
    b[5] = @truncate(ms);
    b[6] = 0x70 | (b[6] & 0x0f); // version 7
    b[8] = 0x80 | (b[8] & 0x3f); // variant 10
    const hex = "0123456789abcdef";
    var i: usize = 0;
    var o: usize = 0;
    while (i < 16) : (i += 1) {
        if (o == 8 or o == 13 or o == 18 or o == 23) {
            out[o] = '-';
            o += 1;
        }
        out[o] = hex[b[i] >> 4];
        out[o + 1] = hex[b[i] & 0x0f];
        o += 2;
    }
}

pub fn connect(dsn: []const u8) Error!Conn {
    const dsn_z = try std.heap.page_allocator.allocSentinel(u8, dsn.len, 0);
    defer std.heap.page_allocator.free(dsn_z);
    @memcpy(dsn_z, dsn);
    const conn = c.PQconnectdb(dsn_z.ptr);
    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        std.debug.print("DB connection failed: {s}\n", .{c.PQerrorMessage(conn)});
        c.PQfinish(conn);
        return error.Connect;
    }
    return conn;
}

pub fn finish(conn: Conn) void {
    c.PQfinish(conn);
}

// ---------------------------------------------------------------------------
// libpq command helpers
// ---------------------------------------------------------------------------
fn check(res: ?*c.PGresult, conn: Conn, want: c_uint) Error!void {
    defer if (res != null) c.PQclear(res);
    if (c.PQresultStatus(res) != want) {
        std.debug.print("  pg error: {s}\n", .{c.PQerrorMessage(conn)});
        return error.Query;
    }
}

fn exec(conn: Conn, sql: [*:0]const u8) Error!void {
    try check(c.PQexec(conn, sql), conn, c.PGRES_COMMAND_OK);
}

fn execParams(conn: Conn, sql: [*:0]const u8, params: []const ?[*:0]const u8) Error!void {
    const res = c.PQexecParams(conn, sql, @intCast(params.len), null, @ptrCast(params.ptr), null, null, 0);
    try check(res, conn, c.PGRES_COMMAND_OK);
}

fn exists(conn: Conn, sql: [*:0]const u8, params: []const ?[*:0]const u8) Error!bool {
    const res = c.PQexecParams(conn, sql, @intCast(params.len), null, @ptrCast(params.ptr), null, null, 0);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        std.debug.print("  pg error: {s}\n", .{c.PQerrorMessage(conn)});
        if (res != null) c.PQclear(res);
        return error.Query;
    }
    const n = c.PQntuples(res);
    c.PQclear(res);
    return n > 0;
}

fn dupZ(a: std.mem.Allocator, str: []const u8) Error![*:0]const u8 {
    const b = try a.allocSentinel(u8, str.len, 0);
    @memcpy(b, str);
    return b.ptr;
}

/// Mark all dags from a (now-removed) file stale — the scheduler then ignores them.
pub fn markStale(alloc: std.mem.Allocator, conn: Conn, fileloc: []const u8) Error!void {
    try execParams(conn, "UPDATE dag SET is_stale = true WHERE fileloc = $1", &.{try dupZ(alloc, fileloc)});
}

pub fn write(
    alloc: std.mem.Allocator,
    conn: Conn,
    bundle_name: []const u8,
    s: serializer.Serialized,
    fileloc: []const u8,
    source_code: []const u8,
    schedule: ?[]const u8, // cron expression, or null for NullTimetable
    now_ms: u64,
) Error!WriteResult {
    const now_unix: i64 = @intCast(now_ms / 1000);
    const is_cron = schedule != null;

    // Scheduling fields the scheduler needs: max_active_runs (so a queued run can
    // start) and next_dagrun* (so scheduled runs fire). next_dagrun is the next
    // cron fire; the scheduler advances it from there.
    const timetable_type = if (is_cron) "CronTriggerTimetable" else "NullTimetable";
    const summary = if (is_cron) schedule.? else "Never, external triggers only";
    var ts_buf: [25]u8 = undefined;
    const next_dr: ?[*:0]const u8 = if (is_cron) blk: {
        const t = cron.nextFire(schedule.?, now_unix) orelse break :blk null;
        break :blk try dupZ(alloc, cron.formatUtc(t, &ts_buf));
    } else null;

    const dag_id_z = try dupZ(alloc, s.dag_id);
    const bundle_z = try dupZ(alloc, bundle_name);
    const fileloc_z = try dupZ(alloc, fileloc);
    const tt_z = try dupZ(alloc, timetable_type);
    const summary_z = try dupZ(alloc, summary);
    const periodic_z = try dupZ(alloc, if (is_cron) "true" else "false");
    const relfileloc_z = try dupZ(alloc, std.fs.path.basename(fileloc));
    const data_z = try dupZ(alloc, s.data);
    const src_z = try dupZ(alloc, source_code);
    const hash_z = try dupZ(alloc, &s.dag_hash);

    var src_hash: [32]u8 = undefined;
    serializer.md5Hex(source_code, &src_hash);
    const src_hash_z = try dupZ(alloc, &src_hash);

    var dv_id: [36]u8 = undefined;
    var sd_id: [36]u8 = undefined;
    var dc_id: [36]u8 = undefined;
    uuid7(now_ms, &dv_id);
    uuid7(now_ms, &sd_id);
    uuid7(now_ms, &dc_id);
    const dv_id_z = try dupZ(alloc, &dv_id);
    const sd_id_z = try dupZ(alloc, &sd_id);
    const dc_id_z = try dupZ(alloc, &dc_id);

    try exec(conn, "BEGIN");
    errdefer exec(conn, "ROLLBACK") catch {};

    try execParams(conn,
        \\INSERT INTO dag_bundle (name, active) VALUES ($1, true) ON CONFLICT (name) DO NOTHING
    , &.{bundle_z});

    // Upsert the dag row — never delete it (task_instances reference old dag_versions
    // with a RESTRICT FK, so a cascading delete would fail once a Dag has run).
    // max_active_runs + next_dagrun* are what let the scheduler start manual runs and
    // fire scheduled ones; the scheduler recomputes next_dagrun after each run.
    try execParams(conn,
        \\INSERT INTO dag
        \\  (dag_id, bundle_name, max_active_tasks, max_active_runs, max_consecutive_failed_dag_runs,
        \\   has_task_concurrency_limits, is_stale, is_paused, partition_mapper_info,
        \\   timetable_type, timetable_summary, timetable_periodic, fileloc, relative_fileloc, owners,
        \\   next_dagrun, next_dagrun_create_after, next_dagrun_data_interval_start, next_dagrun_data_interval_end)
        \\VALUES ($1, $2, 16, 16, 0, false, false, false, '[]'::json,
        \\        $3, $5, $6::boolean, $4, $7, 'airflow',
        \\        $8::timestamptz, $8::timestamptz, $8::timestamptz, $8::timestamptz)
        \\ON CONFLICT (dag_id) DO UPDATE SET
        \\  bundle_name = EXCLUDED.bundle_name,
        \\  max_active_runs = EXCLUDED.max_active_runs,
        \\  timetable_type = EXCLUDED.timetable_type,
        \\  timetable_summary = EXCLUDED.timetable_summary,
        \\  timetable_periodic = EXCLUDED.timetable_periodic,
        \\  fileloc = EXCLUDED.fileloc,
        \\  relative_fileloc = EXCLUDED.relative_fileloc,
        \\  next_dagrun = EXCLUDED.next_dagrun,
        \\  next_dagrun_create_after = EXCLUDED.next_dagrun_create_after,
        \\  next_dagrun_data_interval_start = EXCLUDED.next_dagrun_data_interval_start,
        \\  next_dagrun_data_interval_end = EXCLUDED.next_dagrun_data_interval_end,
        \\  is_stale = false
    , &.{ dag_id_z, bundle_z, tt_z, fileloc_z, summary_z, periodic_z, relfileloc_z, next_dr });

    // Unchanged content (a serialized_dag with this hash already exists) -> no-op.
    if (try exists(conn, "SELECT 1 FROM serialized_dag WHERE dag_id = $1 AND dag_hash = $2", &.{ dag_id_z, hash_z })) {
        try exec(conn, "COMMIT");
        return .unchanged;
    }

    // Changed/new -> bump to the next version_number and write the new rows.
    try execParams(conn,
        \\INSERT INTO dag_version (id, dag_id, version_number, bundle_name, created_at, last_updated)
        \\VALUES ($1::uuid, $2::varchar,
        \\  (SELECT COALESCE(MAX(version_number), 0) + 1 FROM dag_version WHERE dag_id = $2::varchar),
        \\  $3::varchar, now(), now())
    , &.{ dv_id_z, dag_id_z, bundle_z });

    try execParams(conn,
        \\INSERT INTO serialized_dag (id, dag_id, dag_hash, dag_version_id, data, created_at, last_updated)
        \\VALUES ($1, $2, $3, $4, $5::json, now(), now())
    , &.{ sd_id_z, dag_id_z, hash_z, dv_id_z, data_z });

    try execParams(conn,
        \\INSERT INTO dag_code
        \\  (id, dag_id, fileloc, source_code, source_code_hash, dag_version_id, created_at, last_updated)
        \\VALUES ($1, $2, $3, $4, $5, $6, now(), now())
    , &.{ dc_id_z, dag_id_z, fileloc_z, src_z, src_hash_z, dv_id_z });

    try exec(conn, "COMMIT");
    return .written;
}
