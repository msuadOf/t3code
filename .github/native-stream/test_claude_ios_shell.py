import hashlib
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("claude_ios_shell", Path(__file__).with_name("claude-ios-shell.py"))
shell = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shell)


class ShellCompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.git("init", "-q")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "Test")
        for path in shell.ALLOWED | {"packages/native/module.swift", ".github/harness.py", "apps/mobile/app.config.ts"}:
            file = self.root / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("initial\n")
        for path in shell.INPUTS:
            target = self.root / path
            if not target.exists(): target.write_text("initial\n")
        self.base = self.commit()

    def tearDown(self):
        self.temp.cleanup()

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root).decode().strip()

    def runner(self, args):
        return subprocess.check_output(args, cwd=self.root).decode().strip()

    def commit(self):
        self.git("add", ".")
        self.git("commit", "-qm", "fixture")
        return self.git("rev-parse", "HEAD")

    def test_allows_only_current_thread_ts_changes_and_ignores_harness(self):
        path = sorted(shell.ALLOWED)[0]
        (self.root / path).write_text("current JS\n")
        (self.root / ".github/harness.py").write_text("new harness\n")
        current = self.commit()
        self.assertEqual(shell.compatibility(self.base, current, self.runner), [path])

    def test_native_changes_decline_reuse(self):
        for path in ["packages/native/module.swift", "apps/mobile/app.config.ts", "pnpm-lock.yaml"]:
            with self.subTest(path=path):
                self.git("reset", "--hard", self.base)
                (self.root / path).write_text("native changed\n")
                with self.assertRaises(ValueError):
                    shell.compatibility(self.base, self.commit(), self.runner)

    def test_removed_or_symlinked_allowlisted_files_decline_reuse(self):
        path = sorted(shell.ALLOWED)[0]
        (self.root / path).unlink()
        with self.assertRaises(ValueError):
            shell.compatibility(self.base, self.commit(), self.runner)
        self.git("reset", "--hard", self.base)
        (self.root / path).unlink()
        (self.root / path).symlink_to("/tmp/untrusted")
        with self.assertRaises(ValueError):
            shell.compatibility(self.base, self.commit(), self.runner)

    def test_digest_exactly_matches_existing_cache_format(self):
        raw = subprocess.check_output(["git", "rev-parse", *[self.base + ":" + p for p in shell.INPUTS]], cwd=self.root)
        expected = hashlib.sha256(raw + b"development:0\n").hexdigest()
        self.assertEqual(shell.source_digest(self.base, "development", "0", self.runner), expected)
        self.assertNotEqual(shell.source_digest(self.base, "preview", "0", self.runner), expected)
        self.assertNotEqual(shell.source_digest(self.base, "development", "1", self.runner), expected)

    def test_only_full_commit_sha_is_accepted(self):
        for invalid in ["main", "HEAD", "63e608cd", "--upload-pack=payload", "a" * 40 + "\n"]:
            with self.subTest(revision=invalid), self.assertRaises(ValueError):
                shell.compatibility(invalid, self.base, self.runner)


if __name__ == "__main__":
    unittest.main()
