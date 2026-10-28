"""Review alpha archive contents, metadata, source linkage and binary path privacy."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from email.parser import BytesParser

LICENSES = {"LICENSE-MIT", "LICENSE-APACHE", "THIRD-PARTY-NOTICES", "THIRD-PARTY-LICENSES"}
FIXTURES = {"tests/fixtures/sqlite-real-conversion.tsv", "tests/fixtures/exact-ratio-rounding.json"}
EXTERNAL_INPUT_LOADER = {"scripts/external_sf1.py", "tests/support/external_sf1_hashes.json"}


def inspect(path, commit):
    subprocess.run([sys.executable, str(Path(__file__).with_name("check_session_links.py")),
                    "--tracked", str(Path(__file__).resolve().parents[1]), str(path)], check=True)
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            files = {name: archive.read(name) for name in archive.namelist() if not name.endswith("/")}
        metadata = [data for name, data in files.items() if name.endswith(".dist-info/METADATA")]
        assert len(metadata) == 1
        parsed = BytesParser().parsebytes(metadata[0])
        assert parsed["Name"] == "assaydb" and parsed["Version"] == "0.1.0a2"
        assert parsed["Requires-Python"] == ">=3.10"
        assert parsed["License-Expression"] == "MIT OR Apache-2.0"
        assert LICENSES <= {Path(name).name for name in files}
        native = [data for name, data in files.items() if name.endswith((".so", ".pyd", ".dylib"))]
        assert len(native) == 1
        # Inspect actual uncompressed bytes, including linker/debug strings.
        for data in native:
            assert not re.search(rb"/(?:Users|home)/[^/\x00\s]+/|/root/|[A-Za-z]:\\Users\\", data), "Home-directory path in native binary"
        assert all(name.startswith(("assaydb/", "assaydb-0.1.0a2.dist-info/")) for name in files)
        source = {"build_receipt_commit": commit, "note": "Wheel identity is linked by the CI build receipt and this hash"}
    else:
        with tarfile.open(path) as archive:
            members = archive.getmembers()
            assert all(member.isfile() or member.isdir() for member in members), "Nonregular archive entry"
            assert all(not Path(member.name).is_absolute() and ".." not in Path(member.name).parts for member in members)
            roots = {Path(member.name).parts[0] for member in members}
            assert len(roots) == 1
            files = {str(Path(member.name).relative_to(next(iter(roots)))): archive.extractfile(member).read()
                     for member in members if member.isfile()}
        assert not any(set(Path(name).parts) & {".git", ".venv", "target", "reports", "data", ".agent-notes"} for name in files)
        if path.suffix == ".crate":
            manifest = tomllib.loads(files["Cargo.toml"].decode())
            assert manifest["package"]["name"] == "assaydb"
            assert manifest["package"]["version"] == "0.1.0-alpha.2"
            assert manifest["package"]["license"] == "MIT OR Apache-2.0"
            assert LICENSES | FIXTURES | EXTERNAL_INPUT_LOADER | {"docs/CRATE-README.md"} <= files.keys()
            allowed = LICENSES | EXTERNAL_INPUT_LOADER | {"Cargo.toml", "Cargo.toml.orig", "Cargo.lock", "rust-toolchain.toml", ".cargo_vcs_info.json", "docs/CRATE-README.md"}
            # tests/support/ and tests/left_join_count/ hold .rs helper modules that the packaged integration tests
            # compile against; Cargo.toml `include` ships exactly these (SF10 engine merge, Oct 13).
            assert all(name in allowed or name.startswith(("src/", "tests/fixtures/")) or
                       (Path(name).parent == Path("tests") and name.endswith(".rs")) or
                       (Path(name).parent in (Path("tests/support"), Path("tests/left_join_count")) and name.endswith(".rs"))
                       for name in files)
            source = json.loads(files[".cargo_vcs_info.json"])
            assert source["git"]["sha1"] == commit and not source["git"].get("dirty", False)
        else:
            metadata = BytesParser().parsebytes(files["PKG-INFO"])
            assert metadata["Name"] == "assaydb" and metadata["Version"] == "0.1.0a2"
            receipts = [data for name, data in files.items() if name.endswith("source-manifest.json")]
            assert len(receipts) == 1
            source = json.loads(receipts[0])
            assert source["commit"] == commit
            decoder = "vendor/parquet-57.0.0-decoder-v1"
            workspace = tomllib.loads(files["Cargo.toml"].decode())
            assert workspace["patch"]["crates-io"]["parquet"]["path"] == decoder
            assert {decoder + "/" + name for name in
                    ("Cargo.toml", "src/lib.rs", "LICENSE.txt", "NOTICE.txt", "ASSAY-PROVENANCE.md")} <= files.keys()
            locked_decoder = [package for package in tomllib.loads(files["Cargo.lock"].decode())["package"]
                              if package["name"] == "parquet"]
            assert len(locked_decoder) == 1 and "source" not in locked_decoder[0]
            # Maturin relocates path dependencies under the archive root.
            for fixture in FIXTURES:
                assert any(name.endswith("engine/" + fixture) for name in files), fixture
            for license_name in LICENSES:
                assert any(Path(name).name == license_name for name in files), license_name
            for entry in source["files"]:
                matches = [data for name, data in files.items() if name == entry["destination"] or name.endswith("/" + entry["destination"])]
                # Cargo/maturin normalize manifests while building the sdist.
                if not entry["destination"].endswith("Cargo.toml"):
                    assert matches and any(hashlib.sha256(data).hexdigest() == entry["staged_sha256"] for data in matches), entry["destination"]
    return {"filename": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "source": source, "files": sorted(files), "checks": "passed"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("archives", type=Path, nargs="+")
    args = parser.parse_args()
    result = {"source_commit": args.commit, "artifacts": [inspect(path, args.commit) for path in args.archives]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")


def cli():
    try:
        main()
    except subprocess.CalledProcessError:
        raise SystemExit("Release content check failed (details redacted)") from None


if __name__ == "__main__":
    cli()
