"""Create a small, independently buildable source tree; never build or publish.

Run from a committed checkout under the shared activity lock. The destination
must not exist. No report, dataset, cache, private note, or symlink is copied.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[3]
PACKAGE = Path("bindings/python")
PACKAGE_DIRS = ("src", "python", "tests", "notebooks")
PACKAGE_FILES = ("Cargo.toml", "Cargo.lock", "pyproject.toml", "README.md", "BUILDING.md", "LICENSE-MIT", "LICENSE-APACHE", "THIRD-PARTY-NOTICES", "THIRD-PARTY-LICENSES")
ENGINE_FILES = ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "LICENSE-MIT", "LICENSE-APACHE", "THIRD-PARTY-NOTICES", "THIRD-PARTY-LICENSES", "docs/CRATE-README.md")
ENGINE_FIXTURES = ("tests/fixtures/sqlite-real-conversion.tsv", "tests/fixtures/exact-ratio-rounding.json")


def git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(root), *args])


def selected(path: str) -> bool:
    p = Path(path)
    if path in ENGINE_FILES or path.startswith("src/"):
        return True
    # Compile-time unit-test fixtures referenced by the engine, not benchmark data.
    if path in ENGINE_FIXTURES:
        return True
    if p.is_relative_to(PACKAGE):
        relative = p.relative_to(PACKAGE)
        return str(relative) in PACKAGE_FILES or relative.parts[0] in PACKAGE_DIRS
    return False


def release_profile(manifest: bytes) -> bytes:
    """Preserve the engine's exact release settings at the workspace root."""
    matches = list(re.finditer(rb"(?ms)^\[profile\.release\]\r?\n.*?(?=^\[|\Z)", manifest))
    if len(matches) != 1:
        raise RuntimeError("Expected one engine release profile")
    return matches[0].group().rstrip() + b"\n"


def stage(root: Path, output: Path) -> dict:
    root = root.resolve()
    paths = sorted(p for p in git(root, "ls-files", "-z").decode().split("\0") if p and selected(p))
    required = {str(PACKAGE / p) for p in PACKAGE_FILES} | set(ENGINE_FILES) | set(ENGINE_FIXTURES)
    if not required <= set(paths):
        raise RuntimeError(f"Commit required build inputs first: {sorted(required - set(paths))}")
    inputs: dict[str, bytes] = {}
    for name in paths:
        source = root / name
        if source.is_symlink() or not source.is_file():
            raise RuntimeError(f"Input must be a regular file: {name}")
        data = source.read_bytes()
        if data != git(root, "show", f"HEAD:{name}"):
            raise RuntimeError(f"Uncommitted input: {name}")
        inputs[name] = data
    engine_profile = release_profile(inputs["Cargo.toml"])
    output.mkdir(parents=True, exist_ok=False)
    entries = []
    for name, original in inputs.items():
        package_file = Path(name).is_relative_to(PACKAGE)
        target = Path(name).relative_to(PACKAGE) if package_file else Path("engine") / name
        data = original
        rewrite = None
        if name == str(PACKAGE / "Cargo.toml"):
            old = b'assaydb = { path = "../.." }'
            if data.count(old) != 1:
                raise RuntimeError("Binding engine dependency no longer matches staging rule")
            data = data.replace(old, b'assaydb = { path = "engine" }')
            profile_pattern = rb"(?ms)^\[profile\.release\]\r?\n.*?(?=^\[|\Z)"
            if re.search(profile_pattern, data):
                data = re.sub(profile_pattern, lambda _: engine_profile + b"\n", data)
            else:
                data = data.rstrip() + b"\n\n" + engine_profile
            rewrite = "Relocate engine dependency and preserve engine release profile at workspace root"
        elif name == "Cargo.toml":
            # Prevent Cargo's path-dependency packager from discovering unrelated
            # repository contents; this modifies only the isolated staging copy.
            included = ["src/**", *ENGINE_FIXTURES, *ENGINE_FILES]
            include_line = ("include = " + json.dumps(["/" + path for path in included]) + "\n").encode()
            data = re.sub(rb"(?m)^include = \[[^\n]*\]\r?\n", b"", data)
            data = data.replace(b"[package]\n", b"[package]\n" + include_line, 1)
            rewrite = "Limit staged Cargo package to exact engine build inputs"
        destination = output / target
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        entries.append({"source": name, "destination": str(target),
                        "source_sha256": hashlib.sha256(original).hexdigest(),
                        "staged_sha256": hashlib.sha256(data).hexdigest(), "rewrite": rewrite})
    # Use the engine's existing pinned toolchain for the binding build as well.
    (output / "rust-toolchain.toml").write_bytes(inputs["rust-toolchain.toml"])
    manifest = {"format": 1, "commit": git(root, "rev-parse", "HEAD").decode().strip(),
                "files": entries, "toolchain_copy": "engine/rust-toolchain.toml",
                "release_profile": {"source": "Cargo.toml:[profile.release]",
                                    "destination": "Cargo.toml:[profile.release]",
                                    "sha256": hashlib.sha256(engine_profile).hexdigest(),
                                    "reason": "Cargo ignores profiles in dependency members; the workspace root must retain the engine settings"}}
    (output / "source-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(stage(ROOT, args.output), indent=2))
