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

//! inotify directory watcher + graceful-shutdown signal handling.
//!
//! Like Airflow's DagFileProcessorManager, it reacts to file events: a created /
//! modified `.py` is (re)parsed, a removed one marks its Dags stale. inotify is
//! the Linux primitive (std has no high-level file watcher); we poll the inotify
//! fd with a timeout so SIGTERM/SIGINT is observed within ~1s for a clean exit.

const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;

/// Set by the signal handler; the run loop checks it to exit cleanly.
pub var shutdown = std.atomic.Value(bool).init(false);

fn onSignal(_: posix.SIG) callconv(.c) void {
    shutdown.store(true, .seq_cst);
}

pub fn installSignalHandlers() void {
    const act = posix.Sigaction{
        .handler = .{ .handler = onSignal },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(posix.SIG.TERM, &act, null);
    posix.sigaction(posix.SIG.INT, &act, null);
}

fn ok(rc: usize) bool {
    return @as(isize, @bitCast(rc)) >= 0;
}

pub const Kind = enum { changed, removed };
pub const Event = struct { name: []const u8, kind: Kind };

pub const Watcher = struct {
    fd: i32,
    buf: [8192]u8 align(@alignOf(linux.inotify_event)) = undefined,
    len: usize = 0,
    off: usize = 0,

    pub fn init(dir_z: [*:0]const u8) !Watcher {
        const ir = linux.inotify_init1(linux.IN.CLOEXEC);
        if (!ok(ir)) return error.InotifyInit;
        const fd: i32 = @intCast(ir);
        const mask = linux.IN.CLOSE_WRITE | linux.IN.MOVED_TO | linux.IN.CREATE |
            linux.IN.DELETE | linux.IN.MOVED_FROM;
        if (!ok(linux.inotify_add_watch(fd, dir_z, mask))) {
            _ = linux.close(fd);
            return error.InotifyWatch;
        }
        return .{ .fd = fd };
    }

    pub fn deinit(self: *Watcher) void {
        _ = linux.close(self.fd);
    }

    /// Block up to `timeout_ms` for events. Returns true when a batch is ready to
    /// drain via `next()`, false on timeout (caller re-checks `shutdown`).
    pub fn wait(self: *Watcher, timeout_ms: i32) bool {
        var pfd = [_]posix.pollfd{.{ .fd = self.fd, .events = posix.POLL.IN, .revents = 0 }};
        const ready = posix.poll(&pfd, timeout_ms) catch return false;
        if (ready == 0) return false;
        self.len = posix.read(self.fd, &self.buf) catch return false;
        self.off = 0;
        return self.len > 0;
    }

    /// Next event from the last `wait()` batch, or null when drained.
    pub fn next(self: *Watcher) ?Event {
        while (self.off < self.len) {
            const ev: *const linux.inotify_event = @ptrCast(@alignCast(&self.buf[self.off]));
            self.off += @sizeOf(linux.inotify_event) + ev.len;
            const name = ev.getName() orelse continue;
            const kind: Kind = if (ev.mask & (linux.IN.DELETE | linux.IN.MOVED_FROM) != 0) .removed else .changed;
            return .{ .name = name, .kind = kind };
        }
        return null;
    }
};
