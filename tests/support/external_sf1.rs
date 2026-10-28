//! Read only hash-verified external reference bytes. Missing inputs fail tests.
pub fn read(name: &str) -> String {
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/external_sf1.py");
    let python = std::env::var_os("ASSAY_REFERENCE_PYTHON").unwrap_or_else(|| "python3".into());
    let output = std::process::Command::new(python)
        .arg("-I")
        .arg("-B")
        .arg(script)
        .arg(name)
        .output()
        .expect("qualification requires Python 3 and the external SF1 verifier");
    assert!(
        output.status.success(),
        "external SF1 qualification failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("external SF1 input must be UTF-8")
}
