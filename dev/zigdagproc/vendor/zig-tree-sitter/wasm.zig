// Wasm support stubbed out for the zigdagproc vendored build (no tree-sitter-c).
// The Parser wasm methods are @compileError-gated, so these opaque types only
// need to exist for signatures to resolve.
pub const WasmEngine = opaque {};
pub const WasmStore = opaque {};
