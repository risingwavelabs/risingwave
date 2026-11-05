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

"""Standalone S3 diagnostic. Requires Python 3.10+ and boto3; never invokes risectl.

Install dependencies (or use wheels supplied on an offline machine):
  python3 -m pip install -r object_store_benchmark.requirements.txt
  python3 -m pip install --no-index --find-links ./wheels -r object_store_benchmark.requirements.txt

Credentials use boto3's provider chain, e.g. AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY and optional AWS_SESSION_TOKEN. No bucket is created.
  python3 object_store_benchmark.py --endpoint https://minio.example:9000 \
    --bucket benchmark-bucket --ca-cert ca.pem --profile all --output result.json

A range stays on one StreamingBody. No managed download API or background body
reader is used. Kernel, socket, TLS, HTTP and server buffers still exist. This is
an independent boto3 diagnostic, not the RW/OpenDAL client or a WARP-equivalent
baseline. No automatic performance threshold or backpressure claim is made.
"""

import argparse
import concurrent.futures
import contextlib
import io
import json
import math
import os
import random
import signal
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit


class Interrupted(Exception):
    pass


class Timings:
    """Bounded logarithmic histogram: 32 bins/octave, approximately 2.2% resolution."""
    def __init__(self):
        self.bins = {}
        self.count = 0
        self.total_ms = 0.0
        self.max_ms = 0.0

    def add(self, seconds):
        ms = seconds * 1000
        bucket = math.floor(math.log2(max(ms * 1000, 1)) * 32)
        self.bins[bucket] = self.bins.get(bucket, 0) + 1
        self.count += 1
        self.total_ms += ms
        self.max_ms = max(self.max_ms, ms)

    def merge(self, other):
        for key, count in other.bins.items():
            self.bins[key] = self.bins.get(key, 0) + count
        self.count += other.count
        self.total_ms += other.total_ms
        self.max_ms = max(self.max_ms, other.max_ms)

    def summary(self):
        if not self.count:
            return None
        def quantile(q):
            target, cumulative = math.ceil(self.count * q), 0
            for bucket, count in sorted(self.bins.items()):
                cumulative += count
                if cumulative >= target:
                    return min(self.max_ms, 2 ** ((bucket + 1) / 32) / 1000)
        return dict(samples=self.count, mean_ms=self.total_ms / self.count,
                    p50_ms=quantile(.5), p95_ms=quantile(.95), p99_ms=quantile(.99), max_ms=self.max_ms)


class Sample:
    def __init__(self):
        self.bytes = self.items = 0
        self.pause = 0.0
        self.first_data = Timings()
        self.consume = Timings()
        self.resume = Timings()
        self.measured_start = None


class Stats:
    def __init__(self):
        self.attempted = self.succeeded = self.failed = self.interrupted = 0
        self.bytes = self.items = 0
        self.pause = 0.0
        self.timings = {name: Timings() for name in ("latency", "failed_latency", "first_data", "consume", "resume_first_data")}
        self.errors = []

    def record(self, sample, elapsed, error=None):
        self.attempted += 1
        self.bytes += sample.bytes
        self.items += sample.items
        self.pause += sample.pause
        for name, timing in (("first_data", sample.first_data), ("consume", sample.consume), ("resume_first_data", sample.resume)):
            self.timings[name].merge(timing)
        if isinstance(error, Interrupted):
            self.interrupted += 1
        elif error:
            self.failed += 1
            self.timings["failed_latency"].add(elapsed)
            if len(self.errors) < 8:
                self.errors.append(str(error))
        else:
            self.succeeded += 1
            self.timings["latency"].add(elapsed)

    def merge(self, other):
        for name in ("attempted", "succeeded", "failed", "interrupted", "bytes", "items", "pause"):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        for name, timing in self.timings.items():
            timing.merge(other.timings[name])
        self.errors.extend(other.errors[:max(0, 8 - len(self.errors))])

    def report(self, name, elapsed, unit, stopped=False):
        completed = self.succeeded + self.failed
        return dict(name=name, operation_unit=unit,
                    item_unit={"functional_check": "functional_checks", "complete_prefix_scan": "enumerated_objects",
                               "delete_batch": "deleted_objects", "complete_object_upload": "uploaded_objects",
                               "small_object_upload": "uploaded_objects"}.get(unit, "completed_object_reads"),
                    status="cancelled" if stopped else "failed" if self.failed else "completed",
                    elapsed_ms=elapsed * 1000, wall_elapsed_ms=elapsed * 1000,
                    attempted=self.attempted, succeeded=self.succeeded, failed=self.failed, interrupted=self.interrupted,
                    bytes=self.bytes, items=self.items, active_pause_ms=self.pause * 1000,
                    mib_per_sec=self.bytes / 2**20 / elapsed if elapsed else 0,
                    logical_ops_per_sec=self.succeeded / elapsed if elapsed else 0,
                    items_per_sec=self.items / elapsed if elapsed else 0,
                    final_error_rate=self.failed / completed if completed else None,
                    errors=self.errors, **{k: v.summary() for k, v in self.timings.items()})


class Stop:
    def __init__(self, shutdown, end=None):
        self.shutdown = shutdown
        self.end = end or threading.Event()

    def check(self):
        if self.shutdown.is_set() or self.end.is_set():
            raise Interrupted()

    def wait(self, seconds):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.check()
            self.end.wait(min(.05, max(0, deadline - time.monotonic())))
        self.check()


class Reader:
    """One live GET body, or one fully downloaded short GET at a time."""
    def __init__(self, bench, key, length, chunked=False):
        self.bench, self.key, self.length = bench, key, length
        self.chunked, self.offset, self.body = chunked, 0, None
        self.opened = contextlib.ExitStack()

    def read(self, count):
        if self.body is None:
            self.bench.open_get()
            self.opened.callback(self.bench.close_get)
            end = min(self.length, self.offset + self.bench.args.short_get_size) if self.chunked else self.length
            response = self.bench.client.get_object(Bucket=self.bench.args.bucket, Key=self.key,
                                                   Range=f"bytes={self.offset}-{end - 1}")
            body = response["Body"]
            try:
                if response["ContentLength"] != end - self.offset or response["ResponseMetadata"]["HTTPStatusCode"] != 206:
                    raise ValueError("server did not honor the requested Range")
                if self.chunked:
                    data = body.read()
                    if len(data) != end - self.offset:
                        raise ValueError("truncated short GET")
                    body.close()
                    self.opened.close()
                    self.body = io.BytesIO(data)
                else:
                    self.body = body
            except Exception:
                body.close()
                raise
            self.offset = end
        data = self.body.read(count)
        if not data and self.chunked and self.offset < self.length:
            self.body.close()
            self.body = None
            return self.read(count)
        return data

    def close(self):
        try:
            if self.body is not None:
                self.body.close()
        finally:
            self.opened.close()


class Benchmark:
    def __init__(self, client, args, shutdown):
        self.client, self.args, self.shutdown = client, args, shutdown
        self.prefix = f"rw-object-store-bench/{uuid.uuid4()}/"
        self.keys = [f"{self.prefix}data/{i:08d}" for i in range(args.objects)]
        self.metadata = [f"{self.prefix}metadata/{i:08d}" for i in range(args.metadata_objects)]
        self.owned = set()
        self.owned_lock = threading.Lock()
        self.phases = []
        self.active_gets = self.peak_open_gets = 0

    def open_get(self):
        with self.owned_lock:
            self.active_gets += 1
            self.peak_open_gets = max(self.active_gets, self.peak_open_gets)

    def close_get(self):
        with self.owned_lock:
            self.active_gets -= 1

    def own(self, key):
        with self.owned_lock:
            self.owned.add(key)

    def put(self, key, data, sample, multipart=False):
        self.own(key)  # Register before I/O, including ambiguous failed writes.
        if not multipart:
            self.client.put_object(Bucket=self.args.bucket, Key=key, Body=data)
        else:
            upload = self.client.create_multipart_upload(Bucket=self.args.bucket, Key=key)["UploadId"]
            try:
                parts = []
                for start in range(0, len(data), self.args.part_size):
                    result = self.client.upload_part(Bucket=self.args.bucket, Key=key, UploadId=upload,
                        PartNumber=len(parts) + 1, Body=data[start:start + self.args.part_size])
                    parts.append(dict(PartNumber=len(parts) + 1, ETag=result["ETag"]))
                self.client.complete_multipart_upload(Bucket=self.args.bucket, Key=key, UploadId=upload,
                                                     MultipartUpload=dict(Parts=parts))
            except Exception as error:
                try:
                    self.client.abort_multipart_upload(Bucket=self.args.bucket, Key=key, UploadId=upload)
                except Exception as abort_error:
                    raise RuntimeError(f"multipart upload failed: {error}; abort failed: {abort_error}") from error
                raise
        sample.bytes += len(data)
        sample.items += 1

    def delete(self, keys):
        response = self.client.delete_objects(Bucket=self.args.bucket, Delete=dict(Objects=[dict(Key=k) for k in keys], Quiet=True))
        if response.get("Errors"):
            raise RuntimeError(f"DELETE errors: {response['Errors']}")

    def listed(self, prefix):
        result = []
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.args.bucket, Prefix=prefix):
            result.extend(obj["Key"] for obj in page.get("Contents", []))
        return result

    def preflight(self, _, sample, stop):
        key, data = f"{self.prefix}check/probe", bytes(range(251)) * 16
        failure = None
        try:
            self.put(key, data, Sample())
            if self.client.head_object(Bucket=self.args.bucket, Key=key)["ContentLength"] != len(data):
                raise ValueError("HEAD returned incorrect length")
            for begin, end in [(0, len(data)), (0, 1), (17, 83), (len(data) - 1, len(data))]:
                stop.check()
                response = self.client.get_object(Bucket=self.args.bucket, Key=key, Range=f"bytes={begin}-{end - 1}")
                with contextlib.closing(response["Body"]) as body:
                    if body.read() != data[begin:end]:
                        raise ValueError("Range GET returned incorrect content")
            if self.listed(f"{self.prefix}check/") != [key]:
                raise ValueError("LIST returned incorrect keys")
            self.delete([key])
            if self.listed(f"{self.prefix}check/"):
                raise ValueError("object is still listed after DELETE")
            sample.items = 1
        except Exception as error:
            failure = error
            raise
        finally:
            # Cleanup must not hide the operation that originally failed.
            try:
                self.delete([key])  # The probe is never retained, even with --keep-data.
            except Exception as cleanup_error:
                if failure is not None:
                    raise RuntimeError(f"preflight failed: {failure}; probe cleanup failed: {cleanup_error}") from failure
                raise

    def read(self, key, length, mode, sample, stop, reader_id=0, ready=None):
        started = time.monotonic()
        reader = Reader(self, key, length, mode == "chunked")
        try:
            # Initialization errors still join readiness to release the other cohort.
            try:
                stop.check()
                first = reader.read(1)
                if not first:
                    raise ValueError("empty GET body")
                stop.check()
                sample.bytes = 1
                sample.first_data.add(time.monotonic() - started)
            finally:
                if ready:
                    barrier, release, clock = ready
                    barrier.wait()
                    release.wait()
                    sample.measured_start = clock[0]
                    sample.bytes = 0
                    sample.first_data = Timings()
            remaining, unit, first_unit, resumed = length - 1, 0, True, False
            rng = random.Random(self.args.seed + reader_id)
            while remaining:
                stop.check()
                size = min(remaining, self.args.consume_size - int(first_unit))
                first_unit = False
                before = time.monotonic()
                needed = size
                if needed:
                    data = reader.read(1)
                    stop.check()
                    if not data:
                        raise ValueError("truncated GET")
                    sample.bytes += 1
                    remaining -= 1
                    needed -= 1
                    if resumed:
                        sample.resume.add(time.monotonic() - before)
                while needed:
                    data = reader.read(needed)
                    stop.check()
                    if not data:
                        raise ValueError("truncated GET")
                    needed -= len(data)
                    remaining -= len(data)
                    sample.bytes += len(data)
                if size:
                    sample.consume.add(time.monotonic() - before)
                unit += 1
                if remaining and mode != "continuous":
                    pause = self.args.pause_ms + rng.uniform(0, self.args.jitter_ms)
                    if self.args.long_pause_every and unit % self.args.long_pause_every == 0:
                        pause += self.args.long_pause_ms
                    before = time.monotonic()
                    try:
                        stop.wait(pause / 1000)
                    finally:
                        sample.pause += time.monotonic() - before
                    resumed = pause > 0
            sample.items = 1
        finally:
            reader.close()

    def execute(self, count, workers, function, deadline=None):
        stop = Stop(self.shutdown)
        def worker(worker_id):
            stats = Stats()
            for index in range(worker_id, count, workers):
                if self.shutdown.is_set() or (deadline is not None and time.monotonic() >= deadline):
                    break
                sample, error, started = Sample(), None, time.monotonic()
                try:
                    function(index, sample, stop)
                except Exception as exception:
                    error = exception
                stats.record(sample, time.monotonic() - (sample.measured_start or started), error)
            return stats
        started, stats = time.monotonic(), Stats()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            for result in pool.map(worker, range(min(workers, count))):
                stats.merge(result)
        return stats, time.monotonic() - started

    def phase(self, name, count, function, unit, basic=False, workers=None):
        print(f"benchmark: {name}", file=sys.stderr, flush=True)
        duration = self.args.basic_duration_ms / 1000 if basic else 0
        limit = self.args.basic_max_operations if duration else min(count, self.args.basic_max_operations) if basic else count
        started_unix_ms = int(time.time() * 1000)
        self.peak_open_gets = 0
        stats, elapsed = self.execute(limit, workers or self.args.concurrency, function,
                                      time.monotonic() + duration if duration else None)
        report = stats.report(name, elapsed, unit, self.shutdown.is_set())
        report["started_unix_ms"] = started_unix_ms
        report["peak_open_streaming_gets"] = self.peak_open_gets
        report["stop_reason"] = "cancelled" if self.shutdown.is_set() else "operation_limit" if stats.attempted >= limit else "duration"
        if basic:
            report["budget"] = dict(duration_ms=self.args.basic_duration_ms, duration_basis="phase_wall_time", max_operations=limit)
        if stats.attempted == 0 and not self.shutdown.is_set():
            report["status"] = "failed"
            report["errors"].append("measurement ended without operations")
        self.phases.append(report)
        return report["status"] == "completed"

    def basic(self, payload):
        workers = min(self.args.concurrency, len(self.keys))
        def upload(index, sample, stop):
            worker = index % workers
            owned = (len(self.keys) - 1 - worker) // workers + 1
            key = self.keys[worker + (index // workers % owned) * workers]
            self.put(key, payload, sample, multipart=True)
        self.phase("multipart_put", len(self.keys), upload, "complete_object_upload", basic=True, workers=workers)
        for name, size in [("range_get", self.args.range_size), ("block_get", self.args.block_size)]:
            def read(index, sample, stop, size=size):
                offset = random.Random(self.args.seed + index).randint(0, self.args.object_size - size)
                response = self.client.get_object(Bucket=self.args.bucket, Key=self.keys[index % len(self.keys)],
                                                  Range=f"bytes={offset}-{offset + size - 1}")
                with contextlib.closing(response["Body"]) as body:
                    data = body.read(size)
                    if len(data) != size:
                        raise ValueError("truncated GET")
                sample.bytes, sample.items = size, 1
            self.phase(name, self.args.objects * 16, read, name, basic=True)

    def metadata_phases(self):
        def put(index, sample, stop):
            self.put(self.metadata[index], bytes([index % 251]) * 4096, sample)
        if not self.phase("prepare_metadata", len(self.metadata), put, "small_object_upload"):
            return
        def listed(_, sample, stop):
            keys = self.listed(f"{self.prefix}metadata/")
            if set(keys) != set(self.metadata) or len(keys) != len(self.metadata):
                raise ValueError("LIST returned incorrect keys")
            sample.items = len(keys)
        self.phase("list", self.args.concurrency, listed, "complete_prefix_scan", basic=True)
        batch, total = self.args.delete_batch_size, Stats()
        batches = math.ceil(len(self.metadata) / batch)
        duration = self.args.basic_duration_ms / 1000
        limit = self.args.basic_max_operations if duration else min(batches, self.args.basic_max_operations)
        started_unix_ms = int(time.time() * 1000)
        wall, measured, setup_seconds, setup_operations = time.monotonic(), 0.0, 0.0, 0
        setup_errors, previous = [], 0
        deadline = wall + duration if duration else None
        print("benchmark: batch_delete", file=sys.stderr, flush=True)
        while total.attempted < limit and not self.shutdown.is_set() and (deadline is None or time.monotonic() < deadline):
            if previous:
                setup, cost = self.execute(previous, self.args.concurrency, put, deadline)
                setup_seconds += cost
                setup_operations += setup.attempted
                if setup.failed:
                    setup_errors = setup.errors
                    break
                if setup.attempted < previous or (deadline and time.monotonic() >= deadline):
                    break
            count = min(batches, limit - total.attempted)
            def delete(index, sample, stop):
                keys = self.metadata[index * batch:(index + 1) * batch]
                self.delete(keys)
                sample.items = len(keys)
            stats, cost = self.execute(count, self.args.concurrency, delete, deadline)
            total.merge(stats)
            measured += cost
            if stats.failed or stats.attempted < count or not duration:
                break
            previous = min(count * batch, len(self.metadata))
        report = total.report("batch_delete", measured, "delete_batch", self.shutdown.is_set())
        report.update(started_unix_ms=started_unix_ms, wall_elapsed_ms=(time.monotonic() - wall) * 1000, setup_elapsed_ms=setup_seconds * 1000,
                      setup_operations=setup_operations, budget=dict(duration_ms=self.args.basic_duration_ms,
                      duration_basis="phase_wall_time", max_operations=limit),
                      stop_reason="cancelled" if self.shutdown.is_set() else "setup_failed" if setup_errors else "operation_limit" if total.attempted >= limit else "duration")
        if total.attempted == 0 and not self.shutdown.is_set():
            report["status"] = "failed"
            report["errors"].append("measurement ended without operations")
        if setup_errors:
            report["status"] = "failed"
            report["errors"].extend(setup_errors)
        self.phases.append(report)

    def interference(self):
        for name, competitors, mode in [("fast_only", 0, "continuous"),
                ("continuous_competitors", self.args.concurrency, "continuous"),
                ("slow_competitors", self.args.concurrency, "intermittent")]:
            if self.shutdown.is_set():
                break
            self.peak_open_gets = 0
            count = self.args.fast_readers + competitors
            barrier, release, clock = threading.Barrier(count + 1), threading.Event(), [None]
            end, fast, slow = threading.Event(), Stats(), Stats()
            stop = Stop(self.shutdown, end)
            def worker(index):
                is_fast, stats = index < self.args.fast_readers, Stats()
                if is_fast:
                    barrier.wait()
                    release.wait()
                initial = not is_fast
                while not self.shutdown.is_set() and not end.is_set():
                    sample, error, started = Sample(), None, time.monotonic()
                    try:
                        self.read(self.keys[index], self.args.range_size if is_fast else self.args.object_size,
                                  "continuous" if is_fast else mode, sample, stop, index,
                                  (barrier, release, clock) if initial else None)
                    except Exception as exception:
                        error = exception
                    initial = False
                    stats.record(sample, time.monotonic() - (sample.measured_start or started), error)
                    if error:
                        break
                # A shutdown before initialization must still release readiness.
                if initial:
                    barrier.wait()
                    release.wait()
                return is_fast, stats
            print(f"benchmark: interference_{name}", file=sys.stderr, flush=True)
            with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
                futures = [pool.submit(worker, index) for index in range(count)]
                barrier.wait()
                clock[0] = time.monotonic()
                started_unix_ms = int(time.time() * 1000)
                release.set()
                self.shutdown.wait(self.args.interference_ms / 1000)
                elapsed = time.monotonic() - clock[0]
                end.set()
                for future in futures:
                    is_fast, stats = future.result()
                    (fast if is_fast else slow).merge(stats)
            wall = time.monotonic() - clock[0]
            for cohort, stats in [("fast", fast)] + ([("competitors", slow)] if competitors else []):
                report = stats.report(f"interference_{name}/{cohort}", elapsed, "object_read", self.shutdown.is_set())
                report.update(peak_open_streaming_gets_total=self.peak_open_gets, started_unix_ms=started_unix_ms, wall_elapsed_ms=wall * 1000, drain_ms=(wall - elapsed) * 1000,
                              stop_reason="cancelled" if self.shutdown.is_set() else "observation_window")
                self.phases.append(report)

    def run(self):
        if not self.phase("preflight", 1, self.preflight, "functional_check", workers=1):
            return
        payload = os.urandom(self.args.object_size)
        if not self.phase("prepare_data", len(self.keys), lambda i, s, _: self.put(self.keys[i], payload, s, True), "complete_object_upload"):
            return
        if self.args.profile in ("basic", "all") and not self.shutdown.is_set():
            self.basic(payload)
        del payload
        if self.args.profile in ("compactor", "all"):
            for mode in ("continuous", "intermittent", "chunked"):
                if self.shutdown.is_set():
                    break
                self.phase(f"compactor_{mode}", len(self.keys),
                           lambda i, s, stop, mode=mode: self.read(self.keys[i], self.args.object_size, mode, s, stop, i), "object_read")
            self.interference()
        if self.args.profile in ("basic", "all") and not self.shutdown.is_set():
            self.metadata_phases()

    def cleanup(self):
        if self.args.keep_data:
            return []
        errors, keys = [], sorted(self.owned)
        for start in range(0, len(keys), self.args.delete_batch_size):
            try:
                self.delete(keys[start:start + self.args.delete_batch_size])
            except Exception as error:
                errors.append(str(error))
        return errors


def size(value):
    for suffix, multiplier in (("GiB", 2**30), ("MiB", 2**20), ("KiB", 2**10), ("B", 1)):
        if value.endswith(suffix):
            value, multiplier = value[:-len(suffix)], multiplier
            break
    else:
        multiplier = 1
    try:
        result = int(value) * multiplier
        if result <= 0:
            raise ValueError()
        return result
    except ValueError:
        raise argparse.ArgumentTypeError("expected positive bytes or integer KiB/MiB/GiB")


def arguments():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--endpoint", default=os.environ.get("RW_S3_ENDPOINT"), help="S3 endpoint; otherwise AWS defaults")
    p.add_argument("--bucket", required=True, help="existing bucket")
    p.add_argument("--region", default=os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")))
    p.add_argument("--ca-cert", default=os.environ.get("AWS_CA_BUNDLE", os.environ.get("SSL_CERT_FILE")))
    p.add_argument("--addressing-style", choices=["path", "virtual"], default="path")
    p.add_argument("--profile", choices=["basic", "compactor", "all"], default="compactor")
    p.add_argument("--objects", type=int, help="default max(16, concurrency + fast readers)")
    p.add_argument("--object-size", type=size, default=64 * 2**20)
    p.add_argument("--part-size", type=size, default=8 * 2**20)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--fast-readers", type=int, default=2)
    p.add_argument("--range-size", type=size, default=2**20)
    p.add_argument("--block-size", type=size, default=64 * 2**10)
    p.add_argument("--consume-size", type=size, default=256 * 2**10)
    p.add_argument("--short-get-size", type=size, default=4 * 2**20)
    p.add_argument("--pause-ms", type=float, default=10)
    p.add_argument("--jitter-ms", type=float, default=0)
    p.add_argument("--long-pause-every", type=int, default=0)
    p.add_argument("--long-pause-ms", type=float, default=1000)
    p.add_argument("--metadata-objects", type=int, default=1000)
    p.add_argument("--delete-batch-size", type=int, default=100)
    p.add_argument("--basic-duration-ms", type=int, default=30_000)
    p.add_argument("--basic-max-operations", type=int, default=10_000)
    p.add_argument("--interference-ms", type=int, default=10_000)
    p.add_argument("--timeout-secs", type=float, default=15, help="connect/read timeout; a blocked read can delay shutdown")
    p.add_argument("--max-runtime-secs", type=float, default=1800)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--keep-data", action="store_true")
    p.add_argument("--output", type=Path)
    args = p.parse_args()
    if args.objects is None:
        args.objects = max(16, args.concurrency + args.fast_readers)
    positive = (args.objects, args.concurrency, args.fast_readers, args.metadata_objects,
                args.basic_max_operations, args.interference_ms, args.timeout_secs, args.max_runtime_secs)
    if not all(math.isfinite(value) for value in (*positive, args.pause_ms, args.jitter_ms, args.long_pause_ms)):
        p.error("times/counts must be finite")
    if min(positive) <= 0 or min(args.pause_ms, args.jitter_ms, args.long_pause_ms, args.long_pause_every, args.basic_duration_ms) < 0:
        p.error("counts/timeouts must be positive; pauses and basic duration must be nonnegative")
    if args.object_size > 2**30 or max(args.range_size, args.block_size, args.consume_size, args.short_get_size) > args.object_size:
        p.error("object size must be <=1 GiB and cover all read sizes")
    if not 1 <= args.delete_batch_size <= 1000 or args.part_size < 5 * 2**20:
        p.error("DELETE batch must be 1..1000; multipart part size must be >=5 MiB")
    if args.profile != "basic" and args.objects < args.concurrency + args.fast_readers:
        p.error("provide at least concurrency + fast-readers objects for distinct concurrent inputs")
    if args.endpoint:
        endpoint = urlsplit(args.endpoint)
        if endpoint.scheme not in ("http", "https") or not endpoint.netloc or endpoint.username or endpoint.query or endpoint.fragment:
            p.error("endpoint must be an HTTP(S) URL without credentials/query/fragment")
    return args


def summary(report):
    print("\nStandalone S3 benchmark (boto3; independent of risectl)", file=sys.stderr)
    print(f"{'Phase':46} {'Status':10} {'n':>7} {'MiB/s':>10} {'Ops/s':>10} {'p99 ms':>10}", file=sys.stderr)
    for phase in report["benchmark"]["phases"]:
        latency = phase["latency"]
        print(f"{phase['name']:46} {phase['status']:10} {latency['samples'] if latency else 0:7} "
              f"{phase['mib_per_sec']:10.2f} {phase['logical_ops_per_sec']:10.2f} "
              f"{latency['p99_ms'] if latency else 0:10.2f}", file=sys.stderr)
        print(f"  unit={phase['operation_unit']}; failed={phase['failed']}; interrupted={phase['interrupted']}; "
              f"active_pause_ms={phase['active_pause_ms']:.2f}; resume={phase['resume_first_data']}", file=sys.stderr)
    print(f"Cleanup: {report['benchmark']['cleanup_status']}; prefix: {report['benchmark']['prefix']}", file=sys.stderr)


def main():
    args = arguments()
    try:
        import boto3
        import botocore
        from botocore.config import Config
    except ImportError:
        raise SystemExit("Install boto3 using object_store_benchmark.requirements.txt (offline wheels are supported).")
    config = Config(signature_version="s3v4", s3=dict(addressing_style=args.addressing_style),
                    max_pool_connections=args.concurrency + args.fast_readers, connect_timeout=args.timeout_secs,
                    read_timeout=args.timeout_secs, retries=dict(mode="standard", total_max_attempts=1),
                    request_checksum_calculation="when_required", response_checksum_validation="when_required")
    session = boto3.Session()
    client = session.client("s3", endpoint_url=args.endpoint, region_name=args.region, config=config,
                          verify=args.ca_cert or True)
    # Snapshot resolved credentials before I/O. Report writing must not trigger
    # a provider refresh, which can itself fail after a partial run.
    credentials = session.get_credentials()
    secrets = [os.environ.get(k, "") for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")]
    if credentials:
        frozen = credentials.get_frozen_credentials()
        secrets += [frozen.access_key, frozen.secret_key, frozen.token]
    shutdown = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: shutdown.set())
    signal.signal(signal.SIGTERM, lambda *_: shutdown.set())
    timer = threading.Timer(args.max_runtime_secs, shutdown.set)
    timer.daemon = True
    timer.start()
    bench, fatal = Benchmark(client, args, shutdown), []
    try:
        bench.run()
    except Exception as error:
        fatal.append(str(error))
    finally:
        timer.cancel()
        cleanup_errors = bench.cleanup()
        client.close()
    parameters = {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()}
    report = dict(tool="standalone_s3_python", schema_version=1,
        client=dict(boto3=boto3.__version__, botocore=botocore.__version__, sdk_total_max_attempts=1,
                    histogram="32 logarithmic bins/octave; approximately 2.2% quantization; sub-microsecond values rounded"),
        parameters=parameters, benchmark=dict(prefix=bench.prefix, phases=bench.phases,
        cancelled=shutdown.is_set(), backpressure_verified=False, cache_state="uncontrolled",
        cleanup_status="kept" if args.keep_data else "failed" if cleanup_errors else "completed",
        cleanup_errors=cleanup_errors, fatal_errors=fatal),
        notes=["Independent boto3 diagnostic, not the RW client or a WARP-comparable baseline.",
               "Basic rates count successful logical operations; multipart latency is the complete object upload.",
               "DELETE replenishment consumes the wall-time budget but is excluded from measured rates.",
               "Fixed compactor modes read the same objects/bytes. Deliberate pauses and resume waits are separate.",
               "No background body consumer; OS/socket/TLS/HTTP buffers remain. Server backpressure is unverified.",
               "Interrupted body reads are closed after the current blocking read returns or times out; drain is separate."])
    def redact(value):
        if isinstance(value, str):
            for secret in secrets:
                if secret:
                    value = value.replace(secret, "[redacted]")
        elif isinstance(value, dict):
            return {key: redact(item) for key, item in value.items()}
        elif isinstance(value, list):
            return [redact(item) for item in value]
        return value
    report = redact(report)
    text = json.dumps(report, indent=2)
    summary(report)
    if args.output:
        args.output.write_text(text + "\n")
    print(text)
    success = not shutdown.is_set() and not fatal and not cleanup_errors and all(p["status"] == "completed" for p in bench.phases)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
