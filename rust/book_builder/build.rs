use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
};
fn sources(p: &Path, files: &mut Vec<PathBuf>) {
    for e in fs::read_dir(p).expect("source directory") {
        let p = e.expect("source entry").path();
        if p.is_dir() {
            sources(&p, files)
        } else if p.extension().is_some_and(|x| x == "rs") {
            files.push(p);
        }
    }
}
fn main() {
    let root = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let mut files = vec![
        root.join("Cargo.toml"),
        root.join("Cargo.lock"),
        root.join("build.rs"),
    ];
    sources(&root.join("src"), &mut files);
    files.sort();
    let mut hash = Sha256::new();
    for p in files {
        println!("cargo:rerun-if-changed={}", p.display());
        hash.update(p.strip_prefix(&root).unwrap().to_string_lossy().as_bytes());
        hash.update([0]);
        hash.update(fs::read(p).expect("source bytes"));
        hash.update([0]);
    }
    println!("cargo:rustc-env=OPTIMIZER_CODE_SHA={:x}", hash.finalize());
}
