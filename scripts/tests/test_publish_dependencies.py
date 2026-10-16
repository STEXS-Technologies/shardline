"""Publication must not silently drop fixes supplied by workspace overrides."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parent.parent / "check-publish-dependencies.py"


class PublishDependenciesTest(unittest.TestCase):
    def run_check(self, manifest: str, *arguments: str) -> subprocess.CompletedProcess:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "Cargo.toml"
            path.write_text(manifest, encoding="utf-8")
            return subprocess.run(
                [sys.executable, str(SCRIPT), str(path), *arguments],
                capture_output=True,
                text=True,
                check=False,
            )

    def test_released_dependencies_are_allowed(self):
        result = self.run_check('[workspace.dependencies]\nhyper = "1.12"\n')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_git_patch_is_rejected(self):
        result = self.run_check(
            '[patch.crates-io]\nhyper = { git = "https://example.test/hyper", rev = "abc" }\n'
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("patch.crates-io.hyper", result.stderr)

    def test_local_patch_is_rejected(self):
        result = self.run_check('[patch.crates-io]\nhyper = { path = "vendor/hyper" }\n')
        self.assertEqual(result.returncode, 1)

    def test_replacement_is_rejected(self):
        result = self.run_check('[replace]\n"hyper:1.12.0" = { path = "vendor/hyper" }\n')
        self.assertEqual(result.returncode, 1)
        self.assertIn("replace.hyper:1.12.0", result.stderr)

    def test_invalid_manifest_is_rejected(self):
        result = self.run_check('[patch.crates-io\n')
        self.assertNotEqual(result.returncode, 0)

    def test_reviewed_hyper_exception_warns_and_allows_exact_patch(self):
        manifest = (
            '[patch.crates-io]\n'
            'hyper = { git = "https://github.com/hyperium/hyper", '
            'rev = "97fff93e53f9e6a993f2d6a25d50883b7b8752a7" }\n'
        )
        self.assertEqual(self.run_check(manifest).returncode, 1)
        result = self.run_check(manifest, "--allow-reviewed-hyper-patch")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Crates.io consumers use released Hyper", result.stderr)
        result = self.run_check(
            manifest + 'other = { path = "vendor/other" }\n',
            "--allow-reviewed-hyper-patch",
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("patch.crates-io.other", result.stderr)

    def test_review_exception_does_not_allow_different_revision_or_source(self):
        for patch in [
            '{ git = "https://github.com/hyperium/hyper", rev = "other" }',
            '{ git = "https://example.test/hyper", rev = "97fff93e53f9e6a993f2d6a25d50883b7b8752a7" }',
            '{ path = "vendor/hyper" }',
        ]:
            result = self.run_check(
                f"[patch.crates-io]\nhyper = {patch}\n",
                "--allow-reviewed-hyper-patch",
            )
            self.assertEqual(result.returncode, 1, patch)


if __name__ == "__main__":
    unittest.main()
