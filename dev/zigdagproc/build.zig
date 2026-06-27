const std = @import("std");

const PG_INCLUDE = "/usr/include/postgresql";

fn libpqModule(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) *std.Build.Module {
    const tc = b.addTranslateC(.{
        .root_source_file = b.path("src/c.h"),
        .target = target,
        .optimize = optimize,
    });
    tc.addIncludePath(.{ .cwd_relative = PG_INCLUDE });
    return tc.createModule();
}

/// The zig-tree-sitter binding is vendored (its build.zig + the tree-sitter core
/// build.zig are incompatible with this zig-dev: they use the removed `b.build_root`).
/// We build it as a local module and compile the tree-sitter runtime + the
/// tree-sitter-python grammar C ourselves.
fn tsBindingModule(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) *std.Build.Module {
    const build_opts = b.createModule(.{ .root_source_file = b.path("vendor/zig-tree-sitter/build_opts.zig") });
    const mod = b.createModule(.{
        .root_source_file = b.path("vendor/zig-tree-sitter/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("build", build_opts);
    return mod;
}

fn wire(b: *std.Build, mod: *std.Build.Module, ts_mod: *std.Build.Module, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) void {
    mod.linkSystemLibrary("pq", .{});
    mod.addImport("libpq", libpqModule(b, target, optimize));
    mod.addImport("tree_sitter", ts_mod);
    mod.addIncludePath(b.path("vendor/tree-sitter/lib/include"));
    mod.addIncludePath(b.path("vendor/tree-sitter/lib/src"));
    mod.addIncludePath(b.path("vendor/tree-sitter-python"));
    mod.addCSourceFiles(.{
        .files = &.{
            "vendor/tree-sitter/lib/src/lib.c", // amalgamated runtime
            "vendor/tree-sitter-python/parser.c",
            "vendor/tree-sitter-python/scanner.c",
        },
        .flags = &.{ "-std=gnu11", "-D_DEFAULT_SOURCE" },
    });
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const ts_mod = tsBindingModule(b, target, optimize);

    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    wire(b, exe_mod, ts_mod, target, optimize);

    const exe = b.addExecutable(.{ .name = "zigdagproc", .root_module = exe_mod });
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    run.step.dependOn(b.getInstallStep());
    run.addPassthruArgs();
    const run_step = b.step("run", "Run zigdagproc");
    run_step.dependOn(&run.step);

    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    wire(b, test_mod, ts_mod, target, optimize);
    const tests = b.addTest(.{ .root_module = test_mod });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
}
