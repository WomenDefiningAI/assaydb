"""Install a local wheel into a fresh venv and retain portable check receipts."""
from __future__ import annotations

import argparse
import hashlib
import email
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import zipfile

PACKAGE = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args()
    wheel = args.wheel.resolve(strict=True)
    if wheel.suffix != ".whl":
        raise ValueError("Supply exactly one local wheel")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    receipt = {"wheel": wheel.name, "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(), "commands": []}
    with zipfile.ZipFile(wheel) as archive:
        metadata_paths = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        if len(metadata_paths) != 1:
            raise RuntimeError("Expected exactly one wheel metadata record")
        metadata = email.message_from_bytes(archive.read(metadata_paths[0]))
        if metadata.get("License-Expression") != "MIT OR Apache-2.0":
            raise RuntimeError("Missing expected SPDX license expression")
        folder = metadata_paths[0].rsplit("/", 1)[0]
        licenses = {}
        for name in ("LICENSE-MIT", "LICENSE-APACHE", "THIRD-PARTY-NOTICES", "THIRD-PARTY-LICENSES"):
            data = archive.read(folder + "/licenses/" + name)
            if data != (PACKAGE / name).read_bytes():
                raise RuntimeError("Wheel license/notice content differs: " + name)
            licenses[name] = hashlib.sha256(data).hexdigest()
        receipt["licenses"] = licenses

    def run(label: str, command: list[str], cwd: Path, env: dict | None = None) -> None:
        started = time.monotonic()
        with (output / (label + ".log")).open("w") as log:
            result = subprocess.run(command, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT, close_fds=False, timeout=600)
        receipt["commands"].append({"label": label, "exit_code": result.returncode, "seconds": time.monotonic() - started})
        (output / "summary.json").write_text(json.dumps(receipt, indent=2) + "\n")
        if result.returncode:
            raise RuntimeError(f"{label} failed; see its retained log")

    with tempfile.TemporaryDirectory(prefix="assay-wheel-install-") as directory:
        directory = Path(directory)
        venv = directory / "venv"
        run("create-venv", [args.python, "-m", "venv", str(venv)], directory)
        python = str(venv / "bin/python")
        env = dict(os.environ, PATH=str(venv / "bin"), PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1")
        env.pop("PYTHONPATH", None)
        run("install", [python, "-m", "pip", "install", "--no-cache-dir", "--only-binary=:all:", str(wheel), "pandas", "polars", "duckdb"], directory, env)
        run("installed-tests", [python, str(PACKAGE / "scripts/probe_installed.py"), "--package", str(PACKAGE), "--output", str(output / "runtime.json")], directory, env)
        run("versions", [python, "-m", "pip", "freeze", "--all"], directory, env)
    receipt["status"] = "passed"
    (output / "summary.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
