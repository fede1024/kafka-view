use std::env::var;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;

fn main() -> Result<(), std::io::Error> {
    let outdir = var("OUT_DIR").unwrap();
    let rust_version_file = Path::new(&outdir).join("rust_version.rs");
    let output = Command::new(var("RUSTC").unwrap())
        .arg("--version")
        .output()?;
    let version = String::from_utf8_lossy(&output.stdout);

    let mut output_file = File::create(rust_version_file)?;
    output_file
        .write_all(format!("const RUST_VERSION: &str = \"{}\";", version.trim()).as_bytes())?;

    Ok(())
}
