#!/usr/bin/env python3
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

"""python3 -m unittest discover -s scripts/bench -p 'test_*.py' (no boto3 needed)."""

import io
import threading
import unittest
import sys
from unittest import mock
from types import SimpleNamespace

import object_store_benchmark as bench


class Client:
    def __init__(self, data=b"0123456789", truncate=False):
        self.data, self.truncate = data, truncate
        self.ranges, self.bodies, self.objects = [], [], {}

    def get_object(self, **kwargs):
        start, end = map(int, kwargs["Range"][6:].split("-"))
        self.ranges.append((start, end))
        data = self.data[start:end + 1]
        body = io.BytesIO(data[:5] if self.truncate else data)
        self.bodies.append(body)
        return {"Body": body, "ContentLength": end - start + 1, "ResponseMetadata": {"HTTPStatusCode": 206}}

    def put_object(self, **kwargs):
        self.objects[kwargs["Key"]] = kwargs["Body"]

    def head_object(self, **kwargs):
        return {"ContentLength": len(self.objects[kwargs["Key"]])}

    def delete_objects(self, **kwargs):
        for item in kwargs["Delete"]["Objects"]:
            self.objects.pop(item["Key"], None)
        return {}


def benchmark(client, consume=3):
    args = SimpleNamespace(objects=3, metadata_objects=5, bucket="bucket", short_get_size=4,
                           seed=42, consume_size=consume, pause_ms=1, jitter_ms=0,
                           long_pause_every=0, long_pause_ms=1000)
    return bench.Benchmark(client, args, threading.Event())


class Stop:
    def __init__(self, client, cancel=False):
        self.client, self.cancel = client, cancel
        self.positions = []

    def check(self):
        pass

    def wait(self, _):
        self.positions.append(None if self.client.bodies[-1].closed else self.client.bodies[-1].tell())
        if self.cancel:
            raise bench.Interrupted()


class BenchmarkTest(unittest.TestCase):
    def test_one_response_with_pauses_and_separate_short_gets(self):
        for mode in ["continuous", "intermittent", "chunked"]:
            client, sample = Client(), bench.Sample()
            stop = Stop(client)
            benchmark(client).read("object", 10, mode, sample, stop)
            self.assertEqual(sample.bytes, 10)
            self.assertEqual(sample.items, 1)
            self.assertTrue(all(body.closed for body in client.bodies))
            self.assertEqual(client.ranges, [(0, 3), (4, 7), (8, 9)] if mode == "chunked" else [(0, 9)])
            if mode == "intermittent":
                self.assertEqual(stop.positions, [3, 6, 9])
                self.assertEqual(sample.resume.count, 3)
            elif mode == "continuous":
                self.assertEqual(stop.positions, [])

    def test_consumption_units_can_cross_short_get_boundaries(self):
        client, sample = Client(), bench.Sample()
        stop = Stop(client)
        benchmark(client, consume=5).read("object", 10, "chunked", sample, stop)
        self.assertEqual(sample.bytes, 10)
        self.assertEqual(len(stop.positions), 1)
        self.assertEqual(len(client.ranges), 3)

    def test_truncation_and_pause_cancellation_preserve_progress_and_close_body(self):
        client, sample = Client(truncate=True), bench.Sample()
        with self.assertRaisesRegex(ValueError, "truncated"):
            benchmark(client).read("object", 10, "continuous", sample, Stop(client))
        self.assertEqual(sample.bytes, 5)
        self.assertTrue(client.bodies[0].closed)
        client, sample = Client(), bench.Sample()
        with self.assertRaises(bench.Interrupted):
            benchmark(client).read("object", 10, "intermittent", sample, Stop(client, cancel=True))
        self.assertEqual(sample.bytes, 3)
        self.assertTrue(client.bodies[0].closed)

    def test_bad_range_content_fails_preflight_and_deletes_only_probe(self):
        client = Client(data=b"wrong data" * 500)
        client.objects["production/key"] = b"keep"
        b = benchmark(client)
        with self.assertRaisesRegex(ValueError, "incorrect content"):
            b.preflight(0, bench.Sample(), Stop(client))
        self.assertEqual(client.objects, {"production/key": b"keep"})

    def test_preflight_preserves_primary_error_when_cleanup_also_fails(self):
        class FailingCleanup(Client):
            def delete_objects(self, **kwargs):
                raise RuntimeError("cleanup unavailable")
        client = FailingCleanup(data=b"wrong data" * 500)
        with self.assertRaises(RuntimeError) as raised:
            benchmark(client).preflight(0, bench.Sample(), Stop(client))
        self.assertIn("incorrect content", str(raised.exception))
        self.assertIn("cleanup unavailable", str(raised.exception))

    def test_multipart_preserves_upload_and_abort_errors(self):
        class FailingMultipart(Client):
            def create_multipart_upload(self, **kwargs):
                return {"UploadId": "test-upload"}

            def upload_part(self, **kwargs):
                raise ValueError("primary upload failure")

            def abort_multipart_upload(self, **kwargs):
                raise RuntimeError("abort unavailable")
        b = benchmark(FailingMultipart())
        b.args.part_size = 5
        with self.assertRaises(RuntimeError) as raised:
            b.put("object", b"payload", bench.Sample(), multipart=True)
        self.assertIn("primary upload failure", str(raised.exception))
        self.assertIn("abort unavailable", str(raised.exception))

    def test_partial_report_does_not_refresh_credentials_after_io(self):
        state = {"started": False}
        secret = 'secret"quote'
        class Credentials:
            def get_frozen_credentials(self):
                if state["started"]:
                    raise RuntimeError("credential refresh unavailable")
                return SimpleNamespace(access_key="test-key", secret_key=secret, token=None)
        class Session:
            def client(self, *args, **kwargs):
                return SimpleNamespace(close=lambda: None)

            def get_credentials(self):
                return Credentials()
        def fail_run(_):
            state["started"] = True
            raise RuntimeError(f"primary read failed: {secret}")
        args = benchmark(Client()).args
        for name, value in dict(endpoint=None, region="us-east-1", addressing_style="path", ca_cert=None,
                                concurrency=2, fast_readers=1, timeout_secs=1, max_runtime_secs=60,
                                output=None, keep_data=False, delete_batch_size=5).items():
            setattr(args, name, value)
        modules = {"boto3": SimpleNamespace(Session=Session, __version__="test"),
                   "botocore": SimpleNamespace(__version__="test"),
                   "botocore.config": SimpleNamespace(Config=lambda **kwargs: None)}
        output = io.StringIO()
        with mock.patch.dict(sys.modules, modules), mock.patch.object(bench, "arguments", return_value=args), \
                mock.patch.object(bench.Benchmark, "run", fail_run), mock.patch.object(bench, "summary"), \
                mock.patch.object(bench.signal, "signal"), mock.patch.object(sys, "stdout", output):
            self.assertEqual(bench.main(), 1)
        import json
        report = json.loads(output.getvalue())
        self.assertEqual(report["benchmark"]["fatal_errors"], ["primary read failed: [redacted]"])
        self.assertEqual(report["benchmark"]["cleanup_status"], "completed")

    def test_histograms_pool_samples_and_separate_failures(self):
        a, b = bench.Stats(), bench.Stats()
        for _ in range(99):
            a.record(bench.Sample(), .001)
        b.record(bench.Sample(), .1)
        b.record(bench.Sample(), 1, ValueError("I/O"))
        b.record(bench.Sample(), 2, bench.Interrupted())
        a.merge(b)
        report = a.report("get", 1, "ranged_get")
        self.assertEqual(report["latency"]["samples"], 100)
        self.assertLess(report["latency"]["p99_ms"], 2)
        self.assertEqual(report["failed_latency"]["samples"], 1)
        self.assertEqual(report["interrupted"], 1)
        self.assertEqual(report["final_error_rate"], 1 / 101)


if __name__ == "__main__":
    unittest.main()
