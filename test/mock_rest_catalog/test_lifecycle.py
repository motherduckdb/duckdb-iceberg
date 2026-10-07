"""Managed server ownership, process shutdown, and startup failure checks."""

import json
import secrets
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from mock_rest_catalog.config import load_config
from mock_rest_catalog.lifecycle import ROOT, control, start, stop
from mock_rest_catalog.server import MockServer


class LifecycleTests(unittest.TestCase):
    def test_managed_process_shutdown_removes_state_and_retains_logs(self):
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            directory = state_dir / "run"
            directory.mkdir()
            token = secrets.token_hex(32)
            with (directory / "server.log").open("w") as log:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "scripts.mock_rest_catalog.lifecycle",
                        "serve",
                        "--state-dir",
                        str(state_dir),
                        "--directory",
                        str(directory),
                        "--token",
                        token,
                        "--port",
                        "0",
                    ],
                    cwd=ROOT,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                )
            try:
                state_path = state_dir / "server.json"
                deadline = time.monotonic() + 5
                while not state_path.exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(state_path.exists(), (directory / "server.log").read_text())
                state = json.loads(state_path.read_text())
                self.assertEqual(control(state, "status"), {"unsupported": 0, "errors": 0})
                wrong_owner = dict(state, token="wrong")
                with self.assertRaises(HTTPError) as error:
                    control(wrong_owner, "stop")
                self.assertEqual(error.exception.code, 403)
                self.assertIsNone(process.poll())
                stop(state_dir)
                self.assertEqual(process.wait(timeout=5), 0)
                self.assertFalse(state_path.exists())
                self.assertTrue((directory / "warehouse").is_dir())
                self.assertTrue((directory / "requests.jsonl").exists())
                self.assertIn("Stopped;", (directory / "server.log").read_text())
                stop(state_dir)  # Idempotent.
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait()

    def test_occupied_port_fails_without_stopping_existing_server(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with MockServer(directory) as server:
                with patch("mock_rest_catalog.lifecycle.load_config", return_value=({}, server.uri)):
                    with self.assertRaises(RuntimeError):
                        start(directory / "managed")
                self.assertFalse((directory / "managed/server.json").exists())
                self.assertTrue(server.thread.is_alive())

    def test_static_config_is_directly_executable(self):
        config, uri = load_config()
        self.assertIn(f"URI '{uri}'", config["on_init"])
        self.assertNotIn("${", config["on_init"])


if __name__ == "__main__":
    unittest.main()
