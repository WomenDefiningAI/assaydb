"""Remap release-build source paths without changing the caller's toolchain.

Use with_engine_activity.py outside this wrapper on a shared host. Path remapping
complements release debug stripping; always scan the resulting binary as well.
"""
import argparse
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile


def release_environment(*, wheel=False):
    env = os.environ.copy()
    # Opt in only for native macOS wheels. Other release builds keep Cargo profiles.
    if wheel and sys.platform == "darwin":
        target = env.get("CARGO_BUILD_TARGET", "")
        if target and not target.endswith("-apple-darwin"):
            raise ValueError("--wheel supports native macOS targets only on macOS")
        strip = env.get("CARGO_PROFILE_RELEASE_STRIP", "none")
        if strip != "none":
            raise ValueError("macOS --wheel requires CARGO_PROFILE_RELEASE_STRIP=none")
        env["CARGO_PROFILE_RELEASE_STRIP"] = "none"
    encoded = env.get("CARGO_ENCODED_RUSTFLAGS")
    flags = encoded.split("\x1f") if encoded else shlex.split(env.get("RUSTFLAGS", ""))
    if wheel and sys.platform == "darwin":
        # Suppress linker debug-map object paths without post-link stripping.
        flags += ["-C", "link-arg=-Wl,-S"]
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
    parser.add_argument("--wheel", action="store_true",
                        help="Use the native macOS wheel strip workaround; Linux is unchanged")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    environment = release_environment(wheel=args.wheel)
    if args.github_env:
        if args.command:
            parser.error("--github-env does not accept a command")
        value = environment["CARGO_ENCODED_RUSTFLAGS"]
        if "\n" in value or "\r" in value:
            parser.error("Release flag paths must not contain newlines")
        with Path(os.environ["GITHUB_ENV"]).open("a") as output:
            output.write("CARGO_ENCODED_RUSTFLAGS=" + value + "\n")
            if args.wheel and sys.platform == "darwin":
                output.write("CARGO_PROFILE_RELEASE_STRIP=none\n")
    else:
        if not args.command:
            parser.error("a build command is required")
        raise SystemExit(subprocess.run(args.command, env=environment).returncode)
