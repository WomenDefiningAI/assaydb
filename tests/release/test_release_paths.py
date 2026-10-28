"""Scope and conflict controls for native macOS wheel release environments."""
import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "release_paths", Path(__file__).resolve().parents[2] / "scripts/with_release_paths.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class WheelEnvironmentTests(unittest.TestCase):
    def environment(self, platform, wheel=True, **env):
        with patch.dict(os.environ, env, clear=True), patch.object(release.sys, "platform", platform):
            return release.release_environment(wheel=wheel)

    def test_native_macos_wheel_keeps_remapping_and_disables_only_strip(self):
        env = self.environment("darwin", RUSTFLAGS="-C debuginfo=0")
        self.assertEqual(env["CARGO_PROFILE_RELEASE_STRIP"], "none")
        self.assertIn("--remap-path-prefix=", env["CARGO_ENCODED_RUSTFLAGS"])
        self.assertTrue(env["CARGO_ENCODED_RUSTFLAGS"].startswith("-C\x1fdebuginfo=0\x1f"))
        self.assertIn("link-arg=-Wl,-S", env["CARGO_ENCODED_RUSTFLAGS"])
        self.assertNotIn("RUSTFLAGS", env)
        self.assertNotIn("CARGO_PROFILE_RELEASE_DEBUG", env)

    def test_other_builds_preserve_existing_policy(self):
        for platform, wheel in [("linux", True), ("darwin", False), ("win32", True)]:
            with self.subTest(platform=platform, wheel=wheel):
                self.assertNotIn("CARGO_PROFILE_RELEASE_STRIP", self.environment(platform, wheel))
                self.assertNotIn("link-arg=-Wl,-S", self.environment(platform, wheel)["CARGO_ENCODED_RUSTFLAGS"])
                self.assertEqual(self.environment(platform, wheel,
                    CARGO_PROFILE_RELEASE_STRIP="debuginfo")["CARGO_PROFILE_RELEASE_STRIP"], "debuginfo")

    def test_macos_conflicting_strip_override_is_rejected(self):
        for strip in ("debuginfo", "symbols", "true", ""):
            with self.subTest(strip=strip), self.assertRaises(ValueError):
                self.environment("darwin", CARGO_PROFILE_RELEASE_STRIP=strip)
        self.assertEqual(self.environment("darwin", CARGO_PROFILE_RELEASE_STRIP="none")[
            "CARGO_PROFILE_RELEASE_STRIP"], "none")

    def test_macos_cross_target_is_rejected(self):
        with self.assertRaises(ValueError):
            self.environment("darwin", CARGO_BUILD_TARGET="x86_64-unknown-linux-gnu")
        self.assertEqual(self.environment("darwin", CARGO_BUILD_TARGET="aarch64-apple-darwin")[
            "CARGO_PROFILE_RELEASE_STRIP"], "none")

    def test_encoded_flags_take_precedence(self):
        env = self.environment("darwin", CARGO_ENCODED_RUSTFLAGS="-C\x1fopt-level=3", RUSTFLAGS="ignored")
        self.assertTrue(env["CARGO_ENCODED_RUSTFLAGS"].startswith("-C\x1fopt-level=3\x1f"))
        self.assertNotIn("ignored", env["CARGO_ENCODED_RUSTFLAGS"])


if __name__ == "__main__":
    unittest.main()
