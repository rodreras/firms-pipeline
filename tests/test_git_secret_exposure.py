import subprocess
import tempfile
import unittest
from pathlib import Path


class GitSecretExposureTests(unittest.TestCase):
    def test_gitignore_excludes_dotenv_but_allows_a_key_in_source(self):
        project = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as work:
            repo = Path(work)

            def git(*args):
                return subprocess.run(
                    ["git", "-c", f"core.hooksPath={repo / 'empty-hooks'}", *args],
                    cwd=repo,
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout

            git("init", "--quiet", "--template=")
            (repo / ".gitignore").write_bytes((project / ".gitignore").read_bytes())
            dummy = "FIRMS_MAP_KEY='synthetic-test-value'"
            (repo / ".env").write_text(dummy + "\n")
            (repo / "settings.py").write_text(dummy + "\n")
            git("add", "--all")
            staged = git("diff", "--cached", "--name-only").splitlines()
            self.assertNotIn(".env", staged)
            self.assertIn("settings.py", staged)
            self.assertIn(dummy, git("show", ":settings.py"))
