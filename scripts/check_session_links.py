"""Reject Claude Code session links in release inputs and unpacked packages.

Session visibility cannot be inferred from its URL. Keep all session links out of
shipped content, while allowing public documentation and the Code landing page.
Diagnostics deliberately omit matched values and member names.
"""
import argparse
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import stat
import subprocess
import tarfile
from urllib.parse import unquote_to_bytes
import zipfile
import zlib


SESSION_URL = re.compile(rb"https?://(?:www\.)?claude\.ai/code/[a-z0-9_-]+", re.I)
CHUNK_BYTES = 1024 * 1024
MAX_MEMBER_BYTES = 256 * 1024 * 1024
MAX_ARCHIVE_BYTES = 1024 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 100000


class RejectedContent(ValueError):
    """Only fixed, redacted diagnostics may cross the command-line boundary."""


def check_bytes(data):
    # Handle ordinary JSON slash escaping and percent-encoded URL spellings.
    normalized = unquote_to_bytes(data.replace(b"\\/", b"/"))
    if SESSION_URL.search(normalized):
        raise RejectedContent("Claude Code session URL in release content (value redacted)")


def check_name(name):
    check_bytes(os.fsencode(name))
    if (not name or "\\" in name or PureWindowsPath(name).drive
            or PurePosixPath(name).is_absolute() or ".." in PurePosixPath(name).parts):
        raise RejectedContent("Unsafe release member path (value redacted)")


def check_stream(stream):
    total, tail = 0, b""
    while chunk := stream.read(CHUNK_BYTES):
        total += len(chunk)
        if total > MAX_MEMBER_BYTES:
            raise RejectedContent("Release member exceeds scan limit (value redacted)")
        check_bytes(tail + chunk)
        # A URL prefix plus its first identifier character is under 256 bytes,
        # including slash escaping and percent encoding. Retain raw boundary bytes.
        tail = (tail + chunk)[-256:]


def check_budget(count, size):
    if count > MAX_ARCHIVE_MEMBERS or size > MAX_ARCHIVE_BYTES:
        raise RejectedContent("Release archive exceeds scan limit (value redacted)")


def check_metadata(metadata):
    for key, value in metadata.items():
        check_bytes(str(key).encode("utf-8", "surrogateescape"))
        check_bytes(str(value).encode("utf-8", "surrogateescape"))


def check_archive(path):
    check_bytes(path.name.encode())
    if path.is_symlink() or not path.is_file():
        raise RejectedContent("Nonregular release archive (value redacted)")
    total = 0
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            check_bytes(archive.comment)
            for count, member in enumerate(archive.infolist(), 1):
                total += member.file_size
                check_budget(count, total)
                if member.file_size > MAX_MEMBER_BYTES:
                    raise RejectedContent("Release member exceeds scan limit (value redacted)")
                check_name(member.filename)
                check_bytes(member.comment)
                check_bytes(member.extra)
                mode = stat.S_IFMT(member.external_attr >> 16)
                if mode not in (0, stat.S_IFREG, stat.S_IFDIR):
                    raise RejectedContent("Nonregular release member (value redacted)")
                if member.is_dir() and member.file_size != 0:
                    raise RejectedContent("Release directory contains payload (value redacted)")
                if not member.is_dir():
                    with archive.open(member) as stream:
                        check_stream(stream)
    else:
        with tarfile.open(path) as archive:
            check_metadata(archive.pax_headers)
            for count, member in enumerate(archive, 1):
                total += member.size
                check_budget(count, total)
                if member.size > MAX_MEMBER_BYTES:
                    raise RejectedContent("Release member exceeds scan limit (value redacted)")
                check_name(member.name)
                check_metadata(member.pax_headers)
                check_bytes(member.uname.encode("utf-8", "surrogateescape"))
                check_bytes(member.gname.encode("utf-8", "surrogateescape"))
                if not (member.isfile() or member.isdir()):
                    raise RejectedContent("Nonregular release member (value redacted)")
                if member.isdir() and member.size != 0:
                    raise RejectedContent("Release directory contains payload (value redacted)")
                if member.isfile():
                    with archive.extractfile(member) as stream:
                        check_stream(stream)


def check_tracked(root):
    root = root.resolve()
    names = subprocess.check_output(["git", "ls-files", "-z"], cwd=root,
                                    stderr=subprocess.DEVNULL).split(b"\0")
    for name in filter(None, names):
        relative = os.fsdecode(name)
        check_name(relative)
        path = root / relative
        if any(parent.is_symlink() for parent in (path, *path.parents) if parent != root):
            raise RejectedContent("Symlink in tracked release inputs (value redacted)")
        if not path.is_file():
            raise RejectedContent("Nonregular tracked release input (value redacted)")
        with path.open("rb") as stream:
            check_stream(stream)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tracked", type=Path, help="Check tracked release inputs in this checkout")
    parser.add_argument("archives", type=Path, nargs="*")
    args = parser.parse_args()
    if args.tracked is None and not args.archives:
        parser.error("Specify --tracked or at least one archive")
    try:
        if args.tracked is not None:
            check_tracked(args.tracked)
        for archive in args.archives:
            check_archive(archive)
    except RejectedContent as error:
        parser.exit(1, str(error) + "\n")
    except (OSError, ValueError, UnicodeError, EOFError, RuntimeError,
            subprocess.SubprocessError, tarfile.TarError, zipfile.BadZipFile, zlib.error):
        parser.exit(1, "Unable to inspect release content (details redacted)\n")


if __name__ == "__main__":
    main()
