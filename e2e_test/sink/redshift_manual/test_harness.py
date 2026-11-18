# Copyright 2026 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Offline regression checks; these never connect to AWS or RisingWave."""

import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import helper
import regressions
import run


class ManifestTests(unittest.TestCase):
    def test_manifest_component_in_configured_prefix(self):
        # Include the target name in the configured prefix too, so append data
        # must not be mistaken for upsert data by the staging-ID inspection.
        state = {
            "prefix": "qa/manifest/upsert/run-123",
            "target": "upsert",
            "append": "append",
        }
        test = helper.LiveSink.__new__(helper.LiveSink)
        test.state, test.cfg, test.s3 = state, {"S3_BUCKET": "example"}, Mock()
        data_key = state["prefix"] + "/upsert/data.json"
        append_key = state["prefix"] + "/append/data.json"
        rows = [
            {"__row_id": f"100_{i + 1}_{i % 4}", "__op": 1 if i % 2 else 2}
            for i in range(256)
        ]
        test.s3.get_object.return_value = {
            "Body": io.BytesIO("\n".join(map(json.dumps, rows)).encode())
        }
        data = [{"Key": data_key}, {"Key": append_key}]

        def check_poll(read, expected, label):
            # Pending manifests in either table must be noticed; retained data
            # and a similarly named sibling table must not block completion.
            for table in ("upsert", "append"):
                with patch.object(
                    helper,
                    "s3_objects",
                    return_value=data
                    + [{"Key": state["prefix"] + f"/{table}/manifest/pending.json"}],
                ):
                    self.assertTrue(read())
            with patch.object(
                helper,
                "s3_objects",
                return_value=data
                + [{"Key": state["prefix"] + "/upsert_other/manifest/pending.json"}],
            ):
                self.assertEqual(read(), expected)

        with patch.object(helper, "wait_for", side_effect=check_poll), patch.object(
            helper, "s3_objects", return_value=data
        ), contextlib.redirect_stdout(io.StringIO()):
            test.check_s3()
        test.s3.get_object.assert_called_once_with(Bucket="example", Key=data_key)


class OptimizationTests(unittest.TestCase):
    def test_optimized_execution_rejected_before_reading_credentials(self):
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("REDSHIFT_", "AWS_", "S3_", "PYTHONOPTIMIZE"))
        }
        for flags, extra in [
            (["-O"], {}),
            (["-OO"], {}),
            ([], {"PYTHONOPTIMIZE": "1"}),
        ]:
            with self.subTest(flags=flags, extra=extra):
                result = subprocess.run(
                    [
                        sys.executable,
                        *flags,
                        str(run.HERE / "run.py"),
                        "--run-live",
                        "--env-file",
                        "/nonexistent-redshift-test.env",
                    ],
                    env={**env, **extra},
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("require Python assertions", result.stderr)
                self.assertNotIn("No such file", result.stderr)


class CleanupTests(unittest.TestCase):
    def invoke(self, test_error, cleanup_error, fail_regressions=False):
        sink = Mock()
        sink.cleanup.side_effect = cleanup_error
        stderr = io.StringIO()
        with patch.dict(os.environ, {}, clear=True), patch.object(
            sys, "argv", ["run.py", "--run-live", "--mode", "jdbc"]
        ), patch.object(run, "config", return_value={}), patch.object(
            helper, "LiveSink", return_value=sink
        ), patch.object(
            regressions, "run", side_effect=test_error if fail_regressions else None
        ), patch.object(
            run, "run_slt", side_effect=None if fail_regressions else test_error
        ), contextlib.redirect_stdout(
            io.StringIO()
        ), contextlib.redirect_stderr(
            stderr
        ):
            with self.assertRaises(BaseException) as caught:
                run.main()
        sink.cleanup.assert_called_once()
        return caught.exception, stderr.getvalue()

    def test_test_and_cleanup_failures_are_both_reported(self):
        for regressions_failed in (False, True):
            with self.subTest(regressions_failed=regressions_failed):
                original = RuntimeError("original test failure")
                caught, output = self.invoke(
                    original, RuntimeError("cleanup failure"), regressions_failed
                )
                self.assertIs(caught, original)
                self.assertIn("Cleanup also failed: cleanup failure", output)

    def test_cleanup_failure_alone_fails_run(self):
        original = RuntimeError("cleanup failure")
        caught, _ = self.invoke(None, original)
        self.assertIs(caught, original)

    def test_interruption_preserved_if_cleanup_fails(self):
        original = KeyboardInterrupt()
        caught, output = self.invoke(original, RuntimeError("cleanup failure"))
        self.assertIs(caught, original)
        self.assertIn("cleanup failure", output)


@unittest.skipUnless(os.name == "posix", "Process groups require POSIX")
class ProcessGroupTests(unittest.TestCase):
    def test_interrupted_slt_stops_its_own_process_group(self):
        def interrupted_output():
            raise KeyboardInterrupt()
            yield  # Make interruption occur while consuming the output iterator.

        process = Mock(stdout=interrupted_output())
        state = {
            "schema": "test_schema",
            "rw": {"host": "localhost", "port": 4566, "dbname": "dev", "user": "root"},
        }
        with tempfile.TemporaryDirectory() as temp, patch.object(
            run.subprocess, "Popen", return_value=process
        ) as launch, patch.object(run, "stop_process_group") as stop:
            with self.assertRaises(KeyboardInterrupt):
                run.run_slt(state, Path(temp))
        self.assertTrue(launch.call_args.kwargs["start_new_session"])
        stop.assert_called_once_with(process)

    def test_descendants_stop_even_if_wrapper_exits_or_child_ignores_term(self):
        child = """
import os, signal, sys, time
from pathlib import Path
if sys.argv[2] == 'ignore':
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(60)
"""
        parent = """
import subprocess, sys, time
subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2], sys.argv[3]])
if sys.argv[4] == 'stay':
    time.sleep(60)
"""
        for behavior, wrapper in [
            ("default", "stay"),
            ("ignore", "stay"),
            ("ignore", "exit"),
        ]:
            with self.subTest(
                behavior=behavior, wrapper=wrapper
            ), tempfile.TemporaryDirectory() as temp:
                ready = Path(temp) / "child-pid"
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-c",
                        parent,
                        child,
                        str(ready),
                        behavior,
                        wrapper,
                    ],
                    start_new_session=True,
                )
                try:
                    deadline = time.monotonic() + 5
                    while not ready.exists() and time.monotonic() < deadline:
                        time.sleep(0.05)
                    self.assertTrue(ready.exists(), "Child failed to start")
                    child_pid = int(ready.read_text())
                    if wrapper == "exit":
                        process.wait(timeout=5)
                    run.stop_process_group(process, timeout=0.3)
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        result = subprocess.run(
                            ["ps", "-o", "stat=", "-p", str(child_pid)],
                            capture_output=True,
                            text=True,
                        )
                        status = result.stdout.strip()
                        if not status or status.startswith("Z"):
                            break
                        time.sleep(0.05)
                    else:
                        self.fail(
                            "Descendant still running after process-group cleanup"
                        )
                    self.assertIsNotNone(process.poll())
                finally:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        pass
                    process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
