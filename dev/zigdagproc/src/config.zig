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

//! CLI configuration and argument parsing.

const std = @import("std");

pub const Mode = enum { watch, once, emit, bench };

pub const Config = struct {
    mode: Mode = .watch,
    dags_dir: []const u8 = "dags",
    // Default targets the breeze-published Postgres (POSTGRES_HOST_PORT=25433).
    dsn: []const u8 = "host=127.0.0.1 port=25433 user=postgres password=airflow dbname=airflow",
    bundle_name: []const u8 = "zig-dags",
    interval_s: u64 = 2,
    emit_file: []const u8 = "",
    // Value emitted as `_processor_dags_folder` (part of dag_hash). Defaults to dags_dir;
    // set to the deployment's core.dags_folder (e.g. /files/dags) for hash parity with Python.
    proc_dags_folder: []const u8 = "",
    // Python fallback script for files outside the Zig subset. Empty => just skip them.
    // When set, unparseable files are handed to `python3 <fallback> <file...>`.
    fallback: []const u8 = "",

    /// The path used for the `_processor_dags_folder` field / hash (proc override or dags_dir).
    pub fn procFolder(self: Config) []const u8 {
        return if (self.proc_dags_folder.len > 0) self.proc_dags_folder else self.dags_dir;
    }
};

pub const ParseError = error{ MissingValue, BadArg };

pub fn parse(args_src: std.process.Args) ParseError!Config {
    var cfg = Config{};
    var it = args_src.iterate();
    _ = it.next(); // exe name
    while (it.next()) |arg| {
        if (std.mem.eql(u8, arg, "--watch")) {
            cfg.mode = .watch;
        } else if (std.mem.eql(u8, arg, "--bench")) {
            cfg.mode = .bench;
        } else if (std.mem.eql(u8, arg, "--once")) {
            cfg.mode = .once;
        } else if (std.mem.eql(u8, arg, "--emit")) {
            cfg.mode = .emit;
            cfg.emit_file = it.next() orelse return error.MissingValue;
        } else if (std.mem.eql(u8, arg, "--dags-dir")) {
            cfg.dags_dir = it.next() orelse return error.MissingValue;
        } else if (std.mem.eql(u8, arg, "--processor-dags-folder")) {
            cfg.proc_dags_folder = it.next() orelse return error.MissingValue;
        } else if (std.mem.eql(u8, arg, "--fallback")) {
            cfg.fallback = it.next() orelse return error.MissingValue;
        } else if (std.mem.eql(u8, arg, "--dsn")) {
            cfg.dsn = it.next() orelse return error.MissingValue;
        } else if (std.mem.eql(u8, arg, "--bundle")) {
            cfg.bundle_name = it.next() orelse return error.MissingValue;
        } else if (std.mem.eql(u8, arg, "--interval")) {
            cfg.interval_s = std.fmt.parseInt(u64, it.next() orelse return error.MissingValue, 10) catch return error.BadArg;
        } else {
            std.debug.print("unknown arg: {s}\n", .{arg});
            return error.BadArg;
        }
    }
    return cfg;
}
