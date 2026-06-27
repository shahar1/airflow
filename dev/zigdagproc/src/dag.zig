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

//! The declarative Dag data model: what the parser produces and the serializer consumes.

pub const TaskKind = enum { empty, decorated };

pub const TaskSpec = struct {
    task_id: []const u8,
    kind: TaskKind = .empty,
    callable_name: []const u8 = "", // decorated only: "<dag_func>.<locals>.<func>"
};

pub const Dep = struct { up: []const u8, down: []const u8 };

pub const Spec = struct {
    dag_id: []const u8,
    schedule: ?[]const u8, // null => NullTimetable; else a cron expression
    start_y: i64,
    start_m: i64,
    start_d: i64,
    tasks: []TaskSpec, // emit order
    deps: []Dep, // resolved (upstream task_id, downstream task_id)
};
