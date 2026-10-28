"""The crate must carry the external-input verifier, never arbitrary helpers."""
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "artifacts", Path(__file__).resolve().parents[2] / "scripts/check_package_artifacts.py")
artifacts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(artifacts)


class LoaderArchiveTests(unittest.TestCase):
    def inspect(self, *, missing=None, extra=None):
        names = artifacts.LICENSES | artifacts.FIXTURES | artifacts.EXTERNAL_INPUT_LOADER
        files = {name: b"fixture" for name in names}
        files.update({"Cargo.toml": b'[package]\nname="assaydb"\nversion="0.1.0-alpha.2"\nlicense="MIT OR Apache-2.0"\n',
                      ".cargo_vcs_info.json": json.dumps({"git": {"sha1": "fixture"}}).encode(),
                      "docs/CRATE-README.md": b"fixture"})
        if missing:
            del files[missing]
        if extra:
            files[extra] = b"unapproved"
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "fixture.crate"
            with tarfile.open(archive, "w:gz") as output:
                for name, data in files.items():
                    info = tarfile.TarInfo("fixture/" + name)
                    info.size = len(data)
                    output.addfile(info, io.BytesIO(data))
            # These synthetic fixtures exercise the archive policy; the real
            # privacy scanner is run separately on actual package artifacts.
            with patch.object(artifacts.subprocess, "run"):
                return artifacts.inspect(archive, "fixture")

    def test_exact_loader_files_are_required_and_accepted(self):
        self.assertEqual(self.inspect()["checks"], "passed")
        for name in artifacts.EXTERNAL_INPUT_LOADER:
            with self.subTest(name=name), self.assertRaises(AssertionError):
                self.inspect(missing=name)

    def test_other_scripts_and_support_data_stay_rejected(self):
        for name in ("scripts/unapproved.py", "tests/support/private.json"):
            with self.subTest(name=name), self.assertRaises(AssertionError):
                self.inspect(extra=name)
