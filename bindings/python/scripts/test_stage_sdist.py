"""Pure fixture checks: no engine, subprocess build or installed package needed."""
import importlib.util
from pathlib import Path
import tempfile
import re
import tomllib
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("stage", Path(__file__).with_name("stage_sdist.py"))
stage = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(stage)


class StageTests(unittest.TestCase):
    def fixture(self, root):
        names = {str(stage.PACKAGE / p) for p in stage.PACKAGE_FILES} | set(stage.ENGINE_FILES)
        names |= set(stage.ENGINE_FIXTURES)
        names |= {"src/lib.rs", "reports/private.txt", ".private-review/private.txt", "data/test.parquet"}
        files = {name: b"fixture\n" for name in names}
        files["Cargo.toml"] = b'[package]\nname = "assaydb"\ninclude = ["src/**", "tests/**"]\n\n[profile.release]\ndebug = 1\nlto = "thin"\ncodegen-units = 1\n'
        files["bindings/python/Cargo.toml"] = b'assaydb = { path = "../.." }\n'
        for name, data in files.items():
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        def git(_root, *args):
            if args == ("ls-files", "-z"):
                return "\0".join(sorted(names)).encode() + b"\0"
            if args == ("rev-parse", "HEAD"):
                return b"fixture-commit\n"
            if args[0] == "show":
                return files[args[1].removeprefix("HEAD:")]
            raise AssertionError(args)
        return git

    def test_self_contained_allowlist_with_recorded_rewrites(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output = Path(directory) / "repository", Path(directory) / "stage"
            git = self.fixture(root)
            with patch.object(stage, "git", git):
                manifest = stage.stage(root, output)
            self.assertTrue((output / "engine/src/lib.rs").is_file())
            self.assertTrue((output / "engine/tests/fixtures/sqlite-real-conversion.tsv").is_file())
            self.assertTrue((output / "engine/tests/fixtures/exact-ratio-rounding.json").is_file())
            included = tomllib.loads((output / "engine/Cargo.toml").read_text())["package"]["include"]
            self.assertIn("/tests/fixtures/exact-ratio-rounding.json", included)
            self.assertIn(b'path = "engine"', (output / "Cargo.toml").read_bytes())
            self.assertEqual((output / "rust-toolchain.toml").read_bytes(), (root / "rust-toolchain.toml").read_bytes())
            names = [str(p.relative_to(output)) for p in output.rglob("*")]
            self.assertFalse(any("private" in name or "parquet" in name for name in names))
            changes = [entry for entry in manifest["files"] if entry["rewrite"]]
            self.assertEqual(len(changes), 2)
            for entry in changes:
                self.assertNotEqual(entry["source_sha256"], entry["staged_sha256"])
            with patch.object(stage, "git", git), self.assertRaises(FileExistsError):
                stage.stage(root, output)

    def test_engine_release_profile_is_effective_at_staged_workspace_root(self):
        # A dependency member's profile is ignored by Cargo. Test both a missing
        # root profile and an existing divergent profile, while retaining a
        # following unrelated table.
        for profile in (b"", b'[profile.release]\ndebug = 0\nlto = false\ncodegen-units = 16\n\n'):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as directory:
                root, output = Path(directory) / "repository", Path(directory) / "stage"
                original_git = self.fixture(root)
                name = "bindings/python/Cargo.toml"
                binding = b'[dependencies]\nassaydb = { path = "../.." }\n\n' + profile + b'[profile.dev]\ndebug = 0\n'
                (root / name).write_bytes(binding)
                def git(_root, *args):
                    if args == ("show", "HEAD:" + name):
                        return binding
                    return original_git(_root, *args)
                with patch.object(stage, "git", git):
                    manifest = stage.stage(root, output)
                staged = (output / "Cargo.toml").read_bytes()
                engine_profile = stage.release_profile((root / "Cargo.toml").read_bytes())
                self.assertEqual(stage.release_profile(staged), engine_profile)
                self.assertEqual(staged.count(b"[profile.release]"), 1)
                self.assertIn(b'[profile.dev]\ndebug = 0\n', staged)
                self.assertEqual(manifest["release_profile"]["source"], "Cargo.toml:[profile.release]")
                self.assertIn("workspace root", manifest["release_profile"]["reason"])

    def test_dirty_or_symlink_input_refused_before_stage_creation(self):
        for symlink in (False, True):
            with self.subTest(symlink=symlink), tempfile.TemporaryDirectory() as directory:
                root, output = Path(directory) / "repository", Path(directory) / "stage"
                git = self.fixture(root)
                source = root / "src/lib.rs"
                if symlink:
                    source.unlink()
                    source.symlink_to(root / "Cargo.toml")
                else:
                    source.write_bytes(b"dirty\n")
                with patch.object(stage, "git", git), self.assertRaises(RuntimeError):
                    stage.stage(root, output)
                self.assertFalse(output.exists())

    def test_actual_engine_compile_time_files_are_selected(self):
        # Catch a new include_str!/include_bytes! input omitted from the sdist.
        root = stage.ROOT
        found = []
        for source in (root / "src").rglob("*.rs"):
            for relative in re.findall(r'include_(?:str|bytes)!\s*\(\s*"([^"]+)"', source.read_text()):
                target = (source.parent / relative).resolve().relative_to(root)
                found.append(target)
                self.assertTrue((root / target).is_file(), str(target))
                self.assertTrue(stage.selected(target.as_posix()), str(target))
        self.assertTrue(found, "Expected to inspect real compile-time fixtures")

    def test_missing_required_fixture_refused_before_stage_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root, output = Path(directory) / "repository", Path(directory) / "stage"
            original_git = self.fixture(root)
            def git(_root, *args):
                data = original_git(_root, *args)
                if args == ("ls-files", "-z"):
                    return data.replace(b"tests/fixtures/exact-ratio-rounding.json\0", b"")
                return data
            with patch.object(stage, "git", git), self.assertRaisesRegex(RuntimeError, "exact-ratio-rounding"):
                stage.stage(root, output)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
