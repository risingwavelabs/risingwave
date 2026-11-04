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

"""Opt-in AI_DECIDE benchmark against a synthetic, asyncio Jev backend.

Requires Python 3.10+, psql and an isolated running RisingWave. No Python packages,
model credentials or GPU are needed. Example (Linux --pid enables RSS/CPU samples):
  python3 e2e_test/streaming/ai_decide_bench.py --port 14566 --pid 1234 \
      --output /tmp/ai-decide-bench.json --rounds 3

Results include SQL end-to-end latency (including psql startup/barriers), backend
request latency, concurrency, attempts, bytes and sampled RW RSS/CPU. The mock's
latency is measured from receipt of the request body to response drain, not RW's
per-row latency. Artificial sleeps do not model GPU inference or production load.
The suite creates and drops only uniquely named bench tables/views.
"""

import argparse
import asyncio
import collections
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid


def percentile(values, pct):
    return sorted(values)[max(0, math.ceil(len(values) * pct / 100) - 1)] if values else 0


class JevMock:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.connections = set()
        self.tasks = set()
        self.errors = []
        self.server = self.submit(asyncio.start_server(self.handle, '127.0.0.1', 0, backlog=4096))
        self.url = f'http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}/evaluate'

    def submit(self, coroutine):
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout=30)

    async def configure(self, case):
        assert not getattr(self, 'active', 0)
        self.case = case
        self.expected_questions = {'q': {'type': 'noul', 'instructions': 'x' * case['prompt_bytes']}}
        self.active = self.working = self.peak_active = self.peak_working = 0
        self.calls = collections.Counter()
        self.latencies = []
        self.request_bytes = self.response_bytes = self.truncated = 0
        self.first = self.last = None
        self.limit = asyncio.Semaphore(case.get('slots', 1000000))
        self.cpu_start = time.process_time()

    async def snapshot(self):
        assert self.active == 0
        assert not self.errors, self.errors
        elapsed = (self.last - self.first) if self.first is not None else 0
        return dict(attempts=sum(self.calls.values()), unique_rows=len(self.calls),
                    truncated=self.truncated, peak_active=self.peak_active,
                    peak_working=self.peak_working, request_bytes=self.request_bytes,
                    response_bytes=self.response_bytes, active_seconds=elapsed,
                    attempts_per_active_second=sum(self.calls.values()) / elapsed if elapsed else 0,
                    latency_p50_ms=percentile(self.latencies, 50) * 1000,
                    latency_p95_ms=percentile(self.latencies, 95) * 1000,
                    latency_p99_ms=percentile(self.latencies, 99) * 1000,
                    driver_and_mock_cpu_seconds=time.process_time() - self.cpu_start)

    async def handle(self, reader, writer):
        self.connections.add(writer)
        self.tasks.add(asyncio.current_task())
        try:
            while True:
                try:
                    headers = await reader.readuntil(b'\r\n\r\n')
                except asyncio.IncompleteReadError:
                    return
                lines = headers.decode('ascii').split('\r\n')
                fields = dict(line.lower().split(': ', 1) for line in lines[1:] if ': ' in line)
                assert lines[0] == 'POST /evaluate HTTP/1.1', lines[0]
                assert fields['authorization'] == 'bearer synthetic-bench-key', 'unexpected authorization'
                body = await reader.readexactly(int(fields['content-length']))
                started = time.perf_counter()
                if self.first is None:
                    self.first = started
                self.active += 1
                self.peak_active = max(self.peak_active, self.active)
                try:
                    self.request_bytes += len(body)
                    request = json.loads(body)
                    row = request['state']
                    assert isinstance(row, int), type(row)
                    assert request['model'] == 'synthetic-jev', request['model']
                    assert request['questions'] == self.expected_questions, repr(request['questions'])[:200]
                    self.calls[row] += 1
                    async with self.limit:
                        self.working += 1
                        self.peak_working = max(self.peak_working, self.working)
                        try:
                            delay = self.case.get('delay_ms', 0)
                            if self.case.get('tail_ms') and row % 32 == 0:
                                delay = self.case['tail_ms']
                            await asyncio.sleep(delay / 1000)
                            response = json.dumps({'answers': {'q': {'type': 'noul', 'noul': (row % 1000) / 1024}}, 'mock': True}).encode()
                            truncate = self.case.get('truncate_once') and row % 10 == 0 and self.calls[row] == 1
                            sent = response[:12] if truncate else response
                            writer.write(f'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(response)}\r\n\r\n'.encode() + sent)
                            await writer.drain()
                            self.response_bytes += len(sent)
                            self.truncated += bool(truncate)
                        finally:
                            self.working -= 1
                    self.last = time.perf_counter()
                    self.latencies.append(self.last - started)
                    if truncate:
                        return
                finally:
                    self.active -= 1
        except (ConnectionResetError, BrokenPipeError) as error:
            self.errors.append(repr(error))
        except Exception as error:
            self.errors.append(repr(error))
            print(f"mock failure: {error!r}", file=sys.stderr, flush=True)
        finally:
            self.connections.discard(writer)
            self.tasks.discard(asyncio.current_task())
            writer.close()

    def close(self):
        async def cleanup():
            self.server.close()
            await self.server.wait_closed()
            for writer in list(self.connections):
                writer.close()
            tasks = list(self.tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.submit(cleanup())
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)
        self.loop.close()


def process_usage(pid):
    if not pid:
        return 0, 0
    stat = Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()
    cpu = (int(stat[11]) + int(stat[12])) / os.sysconf('SC_CLK_TCK')
    rss = int(stat[21]) * os.sysconf('SC_PAGE_SIZE')
    return rss, cpu


class RwSample:
    def __init__(self, pid):
        self.pid = pid
        self.start_rss, self.start_cpu = process_usage(pid)
        self.peak = self.start_rss
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.sample, daemon=True)
        self.thread.start()

    def sample(self):
        while not self.stop.wait(0.005):
            rss, _ = process_usage(self.pid)
            self.peak = max(self.peak, rss)

    def finish(self):
        self.stop.set()
        self.thread.join()
        if not self.pid:
            return dict(rss_before_mib=None, rss_peak_mib=None, rss_growth_mib=None, rw_cpu_seconds=None)
        rss, cpu = process_usage(self.pid)
        self.peak = max(self.peak, rss)
        return dict(rss_before_mib=self.start_rss / 2**20, rss_peak_mib=self.peak / 2**20,
                    rss_growth_mib=(self.peak - self.start_rss) / 2**20,
                    rw_cpu_seconds=cpu - self.start_cpu)


def run(args):
    mock = JevMock()
    results = []
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    cases = [
        dict(name='null_64b', rows=16384, prompt_bytes=64, null=True),
        dict(name='null_64k', rows=16384, prompt_bytes=65536, null=True),
        dict(name='fast_64b', rows=1024, prompt_bytes=64),
        dict(name='fast_64k', rows=1024, prompt_bytes=65536),
        dict(name='latency_20ms', rows=1024, prompt_bytes=64, delay_ms=20),
        dict(name='long_tail', rows=1024, prompt_bytes=64, delay_ms=20, tail_ms=200),
        dict(name='backend_64_slots', rows=1024, prompt_bytes=64, delay_ms=20, slots=64),
        dict(name='mixed_null_64k', rows=1024, prompt_bytes=65536, mixed_null=True),
        dict(name='stream_project_1', rows=8192, prompt_bytes=64, delay_ms=20, stream=True, window=1),
        dict(name='stream_project_4', rows=8192, prompt_bytes=64, delay_ms=20, stream=True, window=4),
        dict(name='stream_retract', rows=8192, prompt_bytes=64, delay_ms=20, stream=True, window=4, retract=True),
    ]
    if args.faults:
        cases.append(dict(name='truncate_once', rows=256, prompt_bytes=64, truncate_once=True))
    if args.rows:
        for case in cases:
            case['rows'] = args.rows
    if args.only:
        cases = [c for c in cases if c['name'] in args.only.split(',')]
        assert cases, 'no matching cases'

    def sql(statement):
        result = subprocess.run(['psql', '-X', '-qAt', '-h', args.host, '-p', str(args.port),
                                 '-d', args.database, '-U', 'root', '-v', 'ON_ERROR_STOP=1'],
                                input=statement, capture_output=True, text=True, timeout=180)
        if result.returncode:
            raise RuntimeError(result.stderr + f'\nmock errors={mock.errors}, attempts={sum(mock.calls.values())}, active={mock.active}, responses={len(mock.latencies)}')
        return result.stdout.strip().splitlines()

    try:
        for case in cases:
            table = 'ai_bench_' + uuid.uuid4().hex[:12]
            view = table + '_mv'
            state = 'NULL::jsonb' if case.get('null') else 'to_jsonb(i)'
            if case.get('mixed_null'):
                state = 'CASE WHEN i % 4 = 0 THEN NULL::jsonb ELSE to_jsonb(i) END'
            expression = f"ai_decide({state}, '{mock.url}', 'synthetic-jev', ai_questions(ai_prob_question('q', '{'x' * case['prompt_bytes']}')), 'synthetic-bench-key')"
            eligible = [i for i in range(case['rows']) if not case.get('null') and not (case.get('mixed_null') and i % 4 == 0)]
            expected_sum = sum(i % 1000 for i in eligible) / 1024 if eligible else None
            try:
                if case.get('stream'):
                    suffix = '' if case.get('retract') else ' APPEND ONLY'
                    sql(f'CREATE TABLE {table} (i INT){suffix};')
                    sql(f"SET streaming_parallelism = 1; SET streaming_project_expr_concurrency = {case['window']}; "
                        f"SET streaming_project_expr_inflight_request_concurrency = 0; "
                        f'CREATE MATERIALIZED VIEW {view} AS SELECT i, {expression} AS answer FROM {table};')
                for repeat in range(args.rounds + 1):
                    # Keep closed query clients from exhausting small loopback port ranges.
                    time.sleep(1.1)
                    mock.submit(mock.configure(case))
                    sample = RwSample(args.pid)
                    started = time.perf_counter()
                    try:
                        if case.get('stream'):
                            # Repeat the same input distribution in each round.
                            sql(f'INSERT INTO {table} SELECT generate_series(0, {case["rows"] - 1}); FLUSH;')
                            rows = sql(f'SELECT count((answer).q), sum((answer).q) FROM {view};')
                            multiplier = 1 if case.get('retract') else repeat + 1
                        else:
                            rows = sql(f'SELECT count((answer).q), sum((answer).q) FROM '
                                       f'(SELECT {expression} AS answer FROM generate_series(0, {case["rows"] - 1}) AS t(i)) e;')
                            multiplier = 1
                        seconds = time.perf_counter() - started
                    finally:
                        usage = sample.finish()
                    count, total = rows[-1].split('|')
                    assert int(count) == len(eligible) * multiplier, (case, rows)
                    if expected_sum is None:
                        assert total == '', rows
                    else:
                        assert float(total) == expected_sum * multiplier, (case, rows, expected_sum)
                    stats = mock.submit(mock.snapshot())
                    expected_attempts = len(eligible) + (sum(i % 10 == 0 for i in eligible) if case.get('truncate_once') else 0)
                    assert stats['attempts'] == expected_attempts, (case, stats)
                    assert stats['unique_rows'] == len(eligible), (case, stats)
                    if case.get('slots'):
                        assert stats['peak_working'] <= case['slots'], stats
                    record = dict(case=case, repeat=repeat, warmup=repeat == 0, seconds=seconds,
                                  eligible_rows_per_second=len(eligible) / seconds,
                                  input_rows_per_second=case['rows'] / seconds, **usage, backend=stats)
                    if case.get('retract'):
                        deleted = time.perf_counter()
                        sql(f'DELETE FROM {table}; FLUSH;')
                        assert sql(f'SELECT count(*) FROM {view};') == ['0']
                        assert mock.submit(mock.snapshot())['attempts'] == stats['attempts'], 'DELETE called backend'
                        record['delete_seconds'] = time.perf_counter() - deleted
                    results.append(record)
                    output.write_text(json.dumps(dict(label=args.label, rounds=args.rounds,
                                                       sample_interval_ms=5, results=results), indent=2) + '\n')
                    print(json.dumps(dict(label=args.label, case=case['name'], repeat=repeat,
                                          seconds=round(seconds, 3), rps=round(record['eligible_rows_per_second']),
                                          rss_mib=round(usage['rss_peak_mib'], 1) if args.pid else None, peak=stats['peak_active'])), flush=True)
            finally:
                if case.get('stream'):
                    sql(f'DROP MATERIALIZED VIEW IF EXISTS {view}; DROP TABLE IF EXISTS {table};')
    finally:
        mock.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=4566)
    parser.add_argument('--database', default='dev')
    parser.add_argument('--pid', type=int)
    parser.add_argument('--output', required=True)
    parser.add_argument('--label', default='current')
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--rows', type=int, help='override input rows for a size sweep')
    parser.add_argument('--only', help='comma-separated case names')
    parser.add_argument('--faults', action='store_true', help='include truncated-body retry scenario')
    args = parser.parse_args()
    if args.rounds < 1 or (args.rows is not None and args.rows < 1):
        parser.error('rounds and rows must be positive')
    run(args)
