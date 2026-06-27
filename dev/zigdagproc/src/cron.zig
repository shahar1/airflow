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

//! Minimal 5-field cron evaluator: the next fire time >= `after` (UTC), so the
//! processor can seed `next_dagrun` and the scheduler takes over from there.
//! Supports `*`, `n`, `a-b`, `a,b`, `*/n`, `a-b/n` (enough for the preset aliases
//! and common crons); anything unparseable yields null -> no scheduled runs.

const std = @import("std");

const Civil = struct { y: i64, mo: u32, d: u32, h: u32, mi: u32, dow: u32 };

fn civilFromUnix(t: i64) Civil {
    const days = @divFloor(t, 86400);
    const secs = @as(u32, @intCast(t - days * 86400));
    // Howard Hinnant civil_from_days
    const z = days + 719468;
    const era = @divFloor(if (z >= 0) z else z - 146096, 146097);
    const doe: i64 = z - era * 146097;
    const yoe = @divTrunc(doe - @divTrunc(doe, 1460) + @divTrunc(doe, 36524) - @divTrunc(doe, 146096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100));
    const mp = @divTrunc(5 * doy + 2, 153);
    const d = doy - @divTrunc(153 * mp + 2, 5) + 1;
    const mo = if (mp < 10) mp + 3 else mp - 9;
    // 1970-01-01 was Thursday; dow 0=Sunday
    const dow: u32 = @intCast(@mod(days + 4, 7));
    return .{
        .y = y + @intFromBool(mo <= 2),
        .mo = @intCast(mo),
        .d = @intCast(d),
        .h = secs / 3600,
        .mi = (secs % 3600) / 60,
        .dow = dow,
    };
}

/// Parse one cron field into a 64-bit set over [min, max].
fn fieldSet(spec: []const u8, min: u32, max: u32) ?u64 {
    var bits: u64 = 0;
    var parts = std.mem.tokenizeScalar(u8, spec, ',');
    while (parts.next()) |part| {
        var lo = min;
        var hi = max;
        var step: u32 = 1;
        var range = part;
        if (std.mem.indexOfScalar(u8, part, '/')) |s| {
            step = std.fmt.parseInt(u32, part[s + 1 ..], 10) catch return null;
            range = part[0..s];
        }
        if (std.mem.eql(u8, range, "*")) {
            // keep lo/hi
        } else if (std.mem.indexOfScalar(u8, range, '-')) |d| {
            lo = std.fmt.parseInt(u32, range[0..d], 10) catch return null;
            hi = std.fmt.parseInt(u32, range[d + 1 ..], 10) catch return null;
        } else {
            lo = std.fmt.parseInt(u32, range, 10) catch return null;
            hi = lo;
        }
        if (step == 0 or lo < min or hi > max or lo > hi) return null;
        var v = lo;
        while (v <= hi) : (v += step) bits |= @as(u64, 1) << @intCast(v);
    }
    return bits;
}

/// Format a unix time as a Postgres UTC timestamp: "YYYY-MM-DD HH:MM:00+00".
pub fn formatUtc(t: i64, buf: []u8) []const u8 {
    const c = civilFromUnix(t);
    const y: u32 = @intCast(c.y); // unsigned -> no leading '+' sign
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:00+00", .{ y, c.mo, c.d, c.h, c.mi }) catch buf[0..0];
}

/// Next fire time (unix seconds, minute-aligned) at or after `after`, or null.
pub fn nextFire(expr: []const u8, after: i64) ?i64 {
    var f = std.mem.tokenizeScalar(u8, expr, ' ');
    const mins = fieldSet(f.next() orelse return null, 0, 59) orelse return null;
    const hours = fieldSet(f.next() orelse return null, 0, 23) orelse return null;
    const doms = fieldSet(f.next() orelse return null, 1, 31) orelse return null;
    const mons = fieldSet(f.next() orelse return null, 1, 12) orelse return null;
    const dow_field = f.next() orelse return null;
    var dows = fieldSet(dow_field, 0, 7) orelse return null;
    if (dows & (@as(u64, 1) << 7) != 0) dows |= 1; // cron: 7 == Sunday == 0

    var t = @divFloor(after + 59, 60) * 60; // round up to the next minute
    const cap = t + 367 * 86400; // bound the search at ~1 year
    while (t < cap) : (t += 60) {
        const c = civilFromUnix(t);
        const bit = struct {
            fn in(set: u64, v: u32) bool {
                return set & (@as(u64, 1) << @intCast(v)) != 0;
            }
        }.in;
        // dom AND dow (each '*' is always-true) — close enough for presets/common crons.
        if (bit(mins, c.mi) and bit(hours, c.h) and bit(mons, c.mo) and bit(doms, c.d) and bit(dows, c.dow)) {
            return t;
        }
    }
    return null;
}

test "format utc" {
    var buf: [25]u8 = undefined;
    try std.testing.expectEqualStrings("2024-01-02 00:00:00+00", formatUtc(1704153600, &buf));
}

test "next fire daily" {
    // 2024-01-01 12:00:00 UTC -> next "0 0 * * *" is 2024-01-02 00:00:00
    const after: i64 = 1704110400; // 2024-01-01T12:00:00Z
    try std.testing.expectEqual(@as(?i64, 1704153600), nextFire("0 0 * * *", after));
}

test "next fire hourly step" {
    const after: i64 = 1704067200; // 2024-01-01T00:00:00Z
    // "0 */6 * * *" -> 00:00 matches immediately
    try std.testing.expectEqual(@as(?i64, 1704067200), nextFire("0 */6 * * *", after));
}
