"""Remap release-build source paths without changing the caller's toolchain.

Use with_engine_activity.py outside this wrapper on a shared host. Path remapping
complements release debug stripping; always scan the resulting binary as well.
"""
import argparse
import os
from pathlib import Path
import shlex
import subprocess
import tempfile


def release_environment():
    env = os.environ.copy()
    encoded = env.get("CARGO_ENCODED_RUSTFLAGS")
    flags = encoded.split("\x1f") if encoded else shlex.split(env.get("RUSTFLAGS", ""))
    prefixes = {"/root": "/build/user", "/github/home": "/build/user",
                "/tmp": "/build/tmp", "/private/tmp": "/build/tmp"}
    paths = [(str(Path.home()), "/build/user"), (tempfile.gettempdir(), "/build/tmp"),
             (str(Path.cwd()), "/build/source")]
    paths += [(env[key], target) for key, target in (
        ("CARGO_HOME", "/build/cargo"), ("RUSTUP_HOME", "/build/rustup"),
        ("CARGO_TARGET_DIR", "/build/target"), ("GITHUB_WORKSPACE", "/build/source"),
        ("RUNNER_TEMP", "/build/tmp")) if env.get(key)]
    for source, target in paths:
        prefixes[str(Path(source).absolute())] = target
        prefixes[str(Path(source).resolve())] = target
    # Rust uses the last matching mapping; put specific prefixes last.
    flags += [f"--remap-path-prefix={source}={prefixes[source]}"
              for source in sorted(prefixes, key=lambda value: (len(value), value))]
    env.pop("RUSTFLAGS", None)
    env["CARGO_ENCODED_RUSTFLAGS"] = "\x1f".join(flags)
    return env


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--github-env", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    environment = release_environment()
    if args.github_env:
        if args.command:
            parser.error("--github-env does not accept a command")
        value = environment["CARGO_ENCODED_RUSTFLAGS"]
        if "\n" in value or "\r" in value:
            parser.error("Release flag paths must not contain newlines")
        with Path(os.environ["GITHUB_ENV"]).open("a") as output:
            output.write("CARGO_ENCODED_RUSTFLAGS=" + value + "\n")
    else:
        if not args.command:
            parser.error("a build command is required")
        raise SystemExit(subprocess.run(args.command, env=environment).returncode)
