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

//! zigdagproc — a native-Zig Dag processor for Apache Airflow.
//!
//! Statically parses real Python Dag files over a tree-sitter AST (NO Python is
//! executed — no `import airflow`, no GIL), serialises each to the exact Airflow
//! v3 blob with a byte-identical `dag_hash`, and writes the rows to Postgres via
//! libpq. An inotify watcher reacts to file changes; SIGTERM/SIGINT shut it down
//! cleanly. It is interchangeable with the Python dag-processor.

const std = @import("std");
const config = @import("config.zig");
const parser = @import("parser.zig");
const serializer = @import("serializer.zig");
const db = @import("db.zig");
const watcher = @import("watcher.zig");

const Io = std.Io;

fn isPy(name: []const u8) bool {
    return std.mem.endsWith(u8, name, ".py");
}

fn nowMs(io: Io) u64 {
    return @intCast(@divTrunc(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_ms));
}

/// `.fallback` => the file is outside the Zig subset and a Python fallback is configured.
const Outcome = enum { done, fallback };

/// Parse + serialise one file, then (per mode) emit it, drop it (bench), or write it.
fn processFile(parent: std.mem.Allocator, io: Io, conn: db.Conn, cfg: config.Config, path: []const u8) !Outcome {
    var arena = std.heap.ArenaAllocator.init(parent); // one arena per file -> bounded memory
    defer arena.deinit();
    const alloc = arena.allocator();

    const source = Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(4 * 1024 * 1024)) catch |e| {
        std.debug.print("  cannot read {s}: {s}\n", .{ path, @errorName(e) });
        return .done;
    };
    const spec = parser.parse(alloc, source) catch |e| {
        if (cfg.fallback.len > 0) return .fallback; // hand to the Python processor
        std.debug.print("  skip {s}: not in declarative subset ({s})\n", .{ path, @errorName(e) });
        return .done;
    };
    const s = try serializer.serialize(alloc, spec, path, cfg.procFolder());

    switch (cfg.mode) {
        .bench => {}, // parse+serialize+hash only
        .emit => {
            var buf: [4096]u8 = undefined;
            var fw = Io.File.stdout().writer(io, &buf);
            const w = &fw.interface;
            try w.writeAll(s.data);
            try w.print("\ndag_hash={s} dag_id={s}\n", .{ s.dag_hash, s.dag_id });
            try w.flush();
        },
        .once, .watch => {
            const result = try db.write(alloc, conn, cfg.bundle_name, s, path, source, spec.schedule, nowMs(io));
            std.debug.print("  {s} dag_id={s} hash={s} ({d} tasks)\n", .{ @tagName(result), s.dag_id, s.dag_hash, spec.tasks.len });
        },
    }
    return .done;
}

/// Hand a batch of out-of-subset files to the Python processor: `python3 <fallback> <file...>`.
/// One process => `import airflow` is paid once for the whole batch.
fn spawnFallback(alloc: std.mem.Allocator, io: Io, cfg: config.Config, files: []const []const u8) void {
    var argv: std.ArrayList([]const u8) = .empty;
    defer argv.deinit(alloc);
    argv.append(alloc, "python3") catch return;
    argv.append(alloc, cfg.fallback) catch return;
    for (files) |f| argv.append(alloc, f) catch return;
    std.debug.print("  fallback -> python for {d} unsupported file(s)\n", .{files.len});
    var child = std.process.spawn(io, .{ .argv = argv.items, .stdin = .ignore }) catch |e| {
        std.debug.print("  fallback spawn failed: {s}\n", .{@errorName(e)});
        return;
    };
    _ = child.wait(io) catch {};
}

/// Process every `.py` in the dags dir once; returns how many were seen.
fn scanOnce(alloc: std.mem.Allocator, io: Io, conn: db.Conn, cfg: config.Config) !usize {
    var dir = Io.Dir.cwd().openDir(io, cfg.dags_dir, .{ .iterate = true }) catch |e| {
        std.debug.print("cannot open dags dir '{s}': {s}\n", .{ cfg.dags_dir, @errorName(e) });
        return 0;
    };
    defer dir.close(io);
    var processed: usize = 0;
    var fb: std.ArrayList([]const u8) = .empty;
    defer {
        for (fb.items) |p| alloc.free(p);
        fb.deinit(alloc);
    }
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind != .file or !isPy(entry.name)) continue;
        processed += 1;
        const path = try std.fs.path.join(alloc, &.{ cfg.dags_dir, entry.name });
        defer alloc.free(path);
        if (try processFile(alloc, io, conn, cfg, path) == .fallback) try fb.append(alloc, try alloc.dupe(u8, path));
    }
    if (fb.items.len > 0) spawnFallback(alloc, io, cfg, fb.items);
    return processed;
}

/// Initial full scan, then react to inotify events until a shutdown signal.
fn watchLoop(alloc: std.mem.Allocator, io: Io, conn: db.Conn, cfg: config.Config) !void {
    _ = try scanOnce(alloc, io, conn, cfg);

    watcher.installSignalHandlers();
    const dir_z = try alloc.allocSentinel(u8, cfg.dags_dir.len, 0);
    defer alloc.free(dir_z);
    @memcpy(dir_z, cfg.dags_dir);
    var w = try watcher.Watcher.init(dir_z.ptr);
    defer w.deinit();
    std.debug.print("zigdagproc: watching '{s}' (inotify) — Ctrl-C / SIGTERM to stop\n", .{cfg.dags_dir});

    while (!watcher.shutdown.load(.acquire)) {
        if (!w.wait(1000)) continue; // timeout -> re-check shutdown
        while (w.next()) |ev| {
            if (!isPy(ev.name)) continue;
            var ev_arena = std.heap.ArenaAllocator.init(alloc);
            defer ev_arena.deinit();
            const ea = ev_arena.allocator();
            const path = try std.fs.path.join(ea, &.{ cfg.dags_dir, ev.name });
            switch (ev.kind) {
                .removed => {
                    db.markStale(ea, conn, path) catch |e| std.debug.print("  markStale {s}: {s}\n", .{ path, @errorName(e) });
                    std.debug.print("  removed {s}\n", .{path});
                },
                .changed => if (try processFile(ea, io, conn, cfg, path) == .fallback) spawnFallback(ea, io, cfg, &.{path}),
            }
        }
    }
    std.debug.print("zigdagproc: shutdown\n", .{});
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();
    const io = init.io;
    db.seedUuid(@bitCast(@as(i64, @truncate(Io.Timestamp.now(io, .real).nanoseconds))));

    const cfg = config.parse(init.minimal.args) catch return;

    switch (cfg.mode) {
        .emit => _ = try processFile(alloc, io, null, cfg, cfg.emit_file),
        .bench => {
            const t0 = Io.Timestamp.now(io, .real).nanoseconds;
            const n = try scanOnce(alloc, io, null, cfg);
            const dt_ms = @as(f64, @floatFromInt(Io.Timestamp.now(io, .real).nanoseconds - t0)) / 1e6;
            const per = if (n > 0) dt_ms / @as(f64, @floatFromInt(n)) else 0;
            const rate = if (dt_ms > 0) @as(f64, @floatFromInt(n)) / (dt_ms / 1000.0) else 0;
            std.debug.print("zig: {d} dags in {d:.1} ms = {d:.3} ms/dag, {d:.0} dags/sec\n", .{ n, dt_ms, per, rate });
        },
        .once => {
            const conn = try db.connect(cfg.dsn);
            defer db.finish(conn);
            _ = try scanOnce(alloc, io, conn, cfg);
        },
        .watch => {
            const conn = try db.connect(cfg.dsn);
            defer db.finish(conn);
            try watchLoop(alloc, io, conn, cfg);
        },
    }
}

test "serialize emits canonical blob and stable hash" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const spec = try parser.parse(a,
        \\with DAG(dag_id="t", schedule="0 0 * * *", start_date=datetime(2024, 1, 1)):
        \\    a = EmptyOperator(task_id="a")
        \\    b = EmptyOperator(task_id="b")
        \\    a >> b
    );
    const s = try serializer.serialize(a, spec, "/x/t.py", "/dags");
    try std.testing.expect(std.mem.startsWith(u8, s.data, "{\"__version\":3,\"dag\":{"));
    try std.testing.expect(std.mem.indexOf(u8, s.data, "\"downstream_task_ids\":[\"b\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, s.data, "\"start_date\":1704067200.0") != null);
    try std.testing.expectEqual(@as(usize, 32), s.dag_hash.len);
}

test {
    std.testing.refAllDecls(@This());
    _ = parser;
    _ = serializer;
}
