"""Serialize a command with the shared Assay activity lock.

Portable export adaptation: the existing lock helpers are self-contained here;
the development benchmark harness is not included.
"""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def lock_path():
    override = os.environ.get("ASSAY_ACTIVITY_LOCK_PATH")
    if override:
        return Path(override).resolve()
    git = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--git-common-dir"],
        capture_output=True, text=True,
    )
    if git.returncode == 0:
        common = Path(git.stdout.strip())
        directory = common if common.is_absolute() else ROOT / common
        return directory.resolve() / "engine-activity.lock"
    return ROOT / ".engine-activity.lock"


def inherited_lock(path):
    try:
        expected = path.stat()
    except FileNotFoundError:
        return None
    for entry in Path("/dev/fd").iterdir():
        try:
            fd = int(entry.name)
            actual = os.fstat(fd)
            if fd > 2 and (actual.st_dev, actual.st_ino) == (expected.st_dev, expected.st_ino):
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fd
        except (OSError, ValueError):
            pass
    return None


def main():
    if len(sys.argv) < 3:
        raise SystemExit("usage: with_engine_activity.py LABEL COMMAND [ARG ...]")
    path = lock_path()
    inherited = inherited_lock(path)
    if inherited is not None:
        return subprocess.run(sys.argv[2:], pass_fds=(inherited,)).returncode
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as lock:
        print("Waiting for exclusive activity:", sys.argv[1], flush=True)
        fcntl.flock(lock, fcntl.LOCK_EX)
        lock.seek(0)
        lock.truncate()
        json.dump({"pid": os.getpid(), "label": sys.argv[1], "started_unix": time.time()}, lock)
        lock.flush()
        print("Acquired exclusive activity:", sys.argv[1], flush=True)
        return subprocess.run(sys.argv[2:], pass_fds=(lock.fileno(),)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
