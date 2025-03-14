fn main() {
    println!("cargo:rustc-env=LD_LIBRARY_PATH={}", "/home/shahar/.local/share/uv/python/cpython-3.9.21-linux-x86_64-gnu/lib");
    println!("cargo:rustc-env=PYTHONHOME={}", "/home/shahar/.local/share/uv/python/cpython-3.9.21-linux-x86_64-gnu");
}