"""Session-link leakage checks use synthetic identifiers, never real sessions."""
import importlib.util
import io
from pathlib import Path
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import quote_from_bytes
import zipfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/check_session_links.py"
spec = importlib.util.spec_from_file_location("session_links", SCRIPT)
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
SESSION = b"https://" + b"claude.ai/code/" + b"session_synthetic_test"


class SessionLinkTests(unittest.TestCase):
    def assert_cli_rejected(self, *args):
        result = subprocess.run([sys.executable, str(SCRIPT), *map(str, args)],
                                capture_output=True)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(b"redacted", result.stderr)
        for secret in (SESSION, b"session_synthetic_test", b"private-fixture", b"Traceback"):
            self.assertNotIn(secret, result.stdout + result.stderr)

    def test_raw_json_encoded_and_binary_links_rejected_without_values(self):
        for content in (SESSION, SESSION.upper(), SESSION.replace(b"/", b"\\/"),
                        quote_from_bytes(SESSION, safe="").encode(), b"\x00native\x00" + SESSION):
            with self.subTest(content_kind=type(content).__name__):
                with self.assertRaises(ValueError) as raised:
                    guard.check_bytes(content)
                self.assertNotIn("session_synthetic_test", str(raised.exception))

    def test_public_documentation_and_landing_page_allowed(self):
        guard.check_bytes(b"https://code.claude.com/docs/en/claude-code-on-the-web#share-sessions")
        guard.check_bytes(b"https://claude.ai/code https://claude.ai/code/\n")
        guard.check_bytes(b"https://example.org/code/session_synthetic_test")

    def test_stream_boundaries_and_limits(self):
        for content in (SESSION, SESSION.replace(b"/", b"\\/"),
                        quote_from_bytes(SESSION, safe="").encode()):
            for prefix in range(32):
                with patch.object(guard, "CHUNK_BYTES", 32):
                    with self.assertRaises(ValueError):
                        guard.check_stream(io.BytesIO(b"x" * prefix + content))
        with patch.object(guard, "MAX_MEMBER_BYTES", 8):
            with self.assertRaisesRegex(ValueError, "scan limit"):
                guard.check_stream(io.BytesIO(b"x" * 9))

    def test_archive_size_and_member_count_limits(self):
        with tempfile.TemporaryDirectory() as temp:
            for suffix in (".whl", ".crate"):
                path = Path(temp) / ("safe" + suffix)
                if suffix == ".whl":
                    with zipfile.ZipFile(path, "w") as archive:
                        archive.writestr("one", b"safe")
                        archive.writestr("two", b"safe")
                else:
                    with tarfile.open(path, "w:gz") as archive:
                        for name in ("one", "two"):
                            member = tarfile.TarInfo(name)
                            member.size = 4
                            archive.addfile(member, io.BytesIO(b"safe"))
                for limit, value in (("MAX_ARCHIVE_BYTES", 7), ("MAX_MEMBER_BYTES", 3),
                                     ("MAX_ARCHIVE_MEMBERS", 1)):
                    with patch.object(guard, limit, value):
                        with self.assertRaisesRegex(ValueError, "scan limit"):
                            guard.check_archive(path)

    def test_package_inspector_cli_redacts_encoded_archive_name(self):
        with tempfile.TemporaryDirectory() as temp:
            name = quote_from_bytes(SESSION, safe="") + ".whl"
            archive = Path(temp) / name
            with zipfile.ZipFile(archive, "w") as contents:
                contents.writestr("safe.txt", b"safe")
            output = Path(temp) / "review.json"
            result = subprocess.run([sys.executable, str(ROOT / "scripts/check_package_artifacts.py"),
                                     "--commit", "a" * 40, "--output", str(output), str(archive)],
                                    capture_output=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn(b"redacted", result.stderr)
            for secret in (name.encode(), b"session_synthetic_test", b"Traceback"):
                self.assertNotIn(secret, result.stdout + result.stderr)
            self.assertFalse(output.exists())

    def test_wheel_crate_and_sdist_contents_and_names_checked(self):
        with tempfile.TemporaryDirectory() as temp:
            for suffix in (".whl", ".crate", ".tar.gz"):
                for leak_in_name in (False, True):
                    path = Path(temp) / ("synthetic" + suffix)
                    name = SESSION.decode() if leak_in_name else "package/native.so"
                    data = b"harmless" if leak_in_name else b"\x00" + SESSION
                    if suffix == ".whl":
                        with zipfile.ZipFile(path, "w") as archive:
                            archive.writestr(name, data)
                    else:
                        with tarfile.open(path, "w:gz") as archive:
                            member = tarfile.TarInfo(name)
                            member.size = len(data)
                            archive.addfile(member, io.BytesIO(data))
                    result = subprocess.run([sys.executable, str(SCRIPT), str(path)],
                                            capture_output=True)
                    self.assertEqual(result.returncode, 1)
                    self.assertIn(b"value redacted", result.stderr)
                    self.assertNotIn(SESSION, result.stdout + result.stderr)
                    self.assertNotIn(b"session_synthetic_test", result.stdout + result.stderr)

    def test_metadata_links_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "private-fixture.whl"
            for location in ("archive", "member", "extra"):
                with zipfile.ZipFile(path, "w") as archive:
                    member = zipfile.ZipInfo("safe.txt")
                    if location == "archive":
                        archive.comment = SESSION
                    elif location == "member":
                        member.comment = SESSION
                    else:
                        member.extra = b"\xfe\xca" + len(SESSION).to_bytes(2, "little") + SESSION
                    archive.writestr(member, b"safe")
                self.assert_cli_rejected(path)
            path = Path(temp) / "private-fixture.tar.gz"
            for global_header in (False, True):
                headers = {"comment": SESSION.decode()}
                with tarfile.open(path, "w:gz", pax_headers=headers if global_header else None) as archive:
                    member = tarfile.TarInfo("safe.txt")
                    if not global_header:
                        member.pax_headers = headers
                    archive.addfile(member, io.BytesIO())
                self.assert_cli_rejected(path)

    def test_archive_links_and_special_members_rejected_without_extraction(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE):
                path = root / "private-fixture.crate"
                with tarfile.open(path, "w:gz") as archive:
                    member = tarfile.TarInfo("safe.txt")
                    member.type = kind
                    member.linkname = SESSION.decode()
                    archive.addfile(member)
                self.assert_cli_rejected(path)
            path = root / "private-fixture.whl"
            with zipfile.ZipFile(path, "w") as archive:
                member = zipfile.ZipInfo("safe.txt")
                member.create_system = 3
                member.external_attr = (stat.S_IFLNK | 0o777) << 16
                archive.writestr(member, b"outside")
            self.assert_cli_rejected(path)
            self.assertFalse((root / "safe.txt").exists())

    def test_unsafe_member_paths_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            for name in ("../private-fixture", "/private-fixture", "C:/private-fixture",
                         "folder\\private-fixture"):
                for suffix in (".whl", ".crate"):
                    path = Path(temp) / ("private-fixture" + suffix)
                    if suffix == ".whl":
                        with zipfile.ZipFile(path, "w") as archive:
                            archive.writestr(name, b"safe")
                    else:
                        with tarfile.open(path, "w:gz") as archive:
                            archive.addfile(tarfile.TarInfo(name), io.BytesIO())
                    self.assert_cli_rejected(path)

    def test_malformed_missing_and_symlink_archives_are_redacted(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "private-fixture.whl"
            self.assert_cli_rejected(path)
            path.write_bytes(b"not an archive")
            self.assert_cli_rejected(path)
            with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as archive:
                archive.writestr("private-fixture.txt", b"unique-content")
            path.write_bytes(path.read_bytes().replace(b"unique-content", b"broken-content"))
            self.assert_cli_rejected(path)
            link = root / "link.whl"
            link.symlink_to(path)
            self.assert_cli_rejected(link)
            self.assert_cli_rejected("--tracked", root)

    def test_tracked_files_checked_and_symlinks_never_read(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "repo"
            root.mkdir()
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            tracked = root / "safe.txt"
            tracked.write_bytes(b"safe")
            subprocess.run(["git", "-C", str(root), "add", "."], check=True)
            guard.check_tracked(root)
            tracked.write_bytes(SESSION)
            self.assert_cli_rejected("--tracked", root)
            tracked.unlink()
            # A missing external target proves the guard rejects the link before reading it.
            tracked.symlink_to(Path(temp) / "private-fixture-missing")
            with self.assertRaisesRegex(ValueError, "Symlink"):
                guard.check_tracked(root)
            self.assert_cli_rejected("--tracked", root)
            tracked.unlink()
            tracked.mkdir()
            self.assert_cli_rejected("--tracked", root)

    def test_tracked_symlink_parent_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "repo"
            root.mkdir()
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            folder = root / "folder"
            folder.mkdir()
            (folder / "safe.txt").write_bytes(b"safe")
            subprocess.run(["git", "-C", str(root), "add", "."], check=True)
            (folder / "safe.txt").unlink()
            folder.rmdir()
            folder.symlink_to(Path(temp) / "private-fixture-missing")
            with self.assertRaisesRegex(ValueError, "Symlink"):
                guard.check_tracked(root)

    def test_clean_zip_tar_and_directory_members_allowed(self):
        with tempfile.TemporaryDirectory() as temp:
            for suffix in (".whl", ".crate", ".tar.gz"):
                path = Path(temp) / ("clean" + suffix)
                if suffix == ".whl":
                    with zipfile.ZipFile(path, "w") as archive:
                        archive.writestr("package/", b"")
                        archive.writestr("package/safe.txt", b"safe")
                else:
                    with tarfile.open(path, "w:gz") as archive:
                        member = tarfile.TarInfo("package")
                        member.type = tarfile.DIRTYPE
                        archive.addfile(member)
                        member = tarfile.TarInfo("package/safe.txt")
                        member.size = 4
                        archive.addfile(member, io.BytesIO(b"safe"))
                guard.check_archive(path)

    def test_directory_payload_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            for suffix in (".whl", ".crate", ".tar.gz"):
                path = Path(temp) / ("private-fixture" + suffix)
                if suffix == ".whl":
                    with zipfile.ZipFile(path, "w") as archive:
                        archive.writestr("safe/", SESSION)
                else:
                    with tarfile.open(path, "w:gz") as archive:
                        member = tarfile.TarInfo("safe/")
                        member.type = tarfile.DIRTYPE
                        member.size = len(SESSION)
                        archive.addfile(member, io.BytesIO(SESSION))
                self.assert_cli_rejected(path)


if __name__ == "__main__":
    unittest.main()
