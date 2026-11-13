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

"""AI_DECIDE configuration regression. RECOVER requires an isolated serial cluster."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from ai_decide_http import sql


class Server(ThreadingHTTPServer):
    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.lock = threading.Lock()
        self.active = self.peak = self.calls = 0
        self.status, self.delay = 200, 0
        self.body, self.failures = None, None

    def reset(self, status=200, delay=0, body=None, failures=None):
        deadline = time.monotonic() + 10
        while True:
            with self.lock:
                if not self.active:
                    self.peak = self.calls = 0
                    self.status, self.delay = status, delay
                    self.body, self.failures = body, failures
                    return
            assert time.monotonic() < deadline, "mock requests did not drain"
            time.sleep(0.01)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert json.loads(self.headers["X-State"]) == body["state"]
        with self.server.lock:
            self.server.calls += 1
            self.server.active += 1
            self.server.peak = max(self.server.peak, self.server.active)
            if self.server.failures is None or self.server.calls <= self.server.failures:
                status, delay, body = self.server.status, self.server.delay, self.server.body
            else:
                status, delay, body = 200, 0, None
        try:
            time.sleep(delay)
            response = body if body is not None else b'{"answers":{"q":{"type":"noul","noul":0.5}}}'
            self.send_response(status)
            self.send_header("Content-Length", str(len(response)))
            self.send_header("Retry-After", "100")
            self.end_headers()
            self.wfile.write(response)
        except (BrokenPipeError, ConnectionResetError):
            pass  # A configured request timeout cancels the client, not remote work.
        finally:
            with self.server.lock:
                self.server.active -= 1

    def log_message(self, *_):
        pass


def expect_error(statement, message):
    try:
        sql(statement)
    except AssertionError as error:
        assert message in str(error), error
    else:
        raise AssertionError(f"expected {message}: {statement}")


def cleanup():
    sql("DROP MATERIALIZED VIEW IF EXISTS ai_config_result; DROP TABLE IF EXISTS ai_config_input")


def exercise(server):
    def call(state):
        return (f"ai_decide({state}, 'http://127.0.0.1:{server.server_port}', 'test-model', "
                f"ai_questions(ai_prob_question('q', 'Synthetic question?')), 'test-key', MAP {{'X-State': ({state})::varchar}})")

    empty_state = "'{}'::jsonb"
    query = f"SELECT sum(({call('to_jsonb(i)')}).q) FROM generate_series(1, 12) AS t(i)"
    for mode in ["local", "distributed"]:
        server.reset(delay=0.04)
        assert sql(f"SET batch_parallelism = 1; SET query_mode = {mode}; SET ai_decide_max_concurrency = 2; {query}") == ["6"]
        assert server.calls == 12 and server.peak == 2, (mode, server.calls, server.peak)
        prefix = (f"SET batch_parallelism = 1; SET query_mode = {mode}; "
                  "SET ai_decide_retry_delay_ms = 1; SET ai_decide_max_retry_delay_ms = 2; "
                  "SET ai_decide_max_retry_after_ms = 0; ")
        server.reset(status=401)
        expect_error(prefix + "SET statement_timeout = '1s'; SET ai_decide_max_retry_after_ms = 20; " +
                     f"SELECT coalesce(({call(empty_state)}).q, -1)", "Query cancelled")
        assert server.calls > 3, "persistent errors must keep retrying until cancellation"
        server.reset(status=401, failures=4)
        assert sql(prefix + f"SELECT ({call(empty_state)}).q") == ["0.5"]
        assert server.calls == 5, (mode, server.calls)
        server.reset(body=b" " * 129, failures=4)
        assert sql(prefix + "SET ai_decide_max_response_bytes = 128; " +
                   f"SELECT ({call(empty_state)}).q") == ["0.5"]
        assert server.calls == 5
        server.reset(delay=0.15, failures=4)
        assert sql(prefix + "SET ai_decide_request_timeout_ms = 25; " +
                   f"SELECT ({call(empty_state)}).q") == ["0.5"]
        assert server.calls == 5
    server.reset()
    for key in ["request_timeout_ms", "max_response_bytes"]:
        expect_error(f"SET ai_decide_{key} = 0", "must be greater than zero")
        expect_error(f"CREATE MATERIALIZED VIEW ai_config_result WITH (ai_decide_{key} = 0) AS SELECT 1 AS value", "must be greater than zero")

    expected = {"request_timeout_ms": 1000, "max_retry_delay_ms": 2, "retry_delay_ms": 1,
                "max_retry_after_ms": 2, "max_response_bytes": 4096, "max_concurrency": 1}
    options = ", ".join(f"ai_decide_{key} = {value}" for key, value in expected.items())
    for append_only in [False, True]:
        cleanup()
        sql("SET streaming_parallelism = 1; CREATE TABLE ai_config_input (id INT, payload JSONB)" +
            (" APPEND ONLY" if append_only else ""))
        statement = ("SET streaming_parallelism = 1; SET streaming_project_expr_concurrency = 4; "
                     "SET ai_decide_max_concurrency = 3; SET ai_decide_request_timeout_ms = 2000; "
                     f"CREATE MATERIALIZED VIEW ai_config_result WITH ({options}) AS "
                     f"SELECT id, {call('payload')} AS result FROM ai_config_input; "
                     "SHOW ai_decide_max_concurrency")
        assert sql(statement) == ["3"], "WITH must not mutate the session"
        assert sql("SELECT key, value FROM rw_streaming_job_config WHERE name = 'ai_config_result' "
                   "AND key LIKE 'streaming.ai_decide.%' ORDER BY key") == sorted(
                       f"streaming.ai_decide.{key}|{value}" for key, value in expected.items())
        server.reset(status=401, delay=0.04, failures=4)
        sql("INSERT INTO ai_config_input SELECT i, to_jsonb(i) FROM generate_series(1, 8) AS t(i); FLUSH")
        assert sql("SELECT count(*), sum((result).q) FROM ai_config_result") == ["8|4"]
        assert server.calls == 12 and server.peak == 1, (append_only, server.calls, server.peak)
        expect_error("ALTER MATERIALIZED VIEW ai_config_result SET CONFIG (streaming.ai_decide.request_timeout_ms = 0)",
                     "greater than zero")
        sql("ALTER MATERIALIZED VIEW ai_config_result SET CONFIG (streaming.ai_decide.max_concurrency = 2)")
        sql("RECOVER")
        deadline = time.monotonic() + 30
        while True:
            try:
                if sql("SELECT count(*) FROM ai_config_result") == ["8"]:
                    break
            except AssertionError:
                pass
            assert time.monotonic() < deadline, "recovery timed out"
            time.sleep(0.2)
        server.reset(delay=0.04)
        sql("INSERT INTO ai_config_input SELECT i, to_jsonb(i) FROM generate_series(9, 16) AS t(i); FLUSH")
        assert sql("SELECT count(*), sum((result).q) FROM ai_config_result") == ["16|8"]
        assert server.calls == 8 and server.peak == 2, (append_only, server.calls, server.peak)
        if not append_only:
            before = server.calls
            sql("DELETE FROM ai_config_input; FLUSH")
            assert sql("SELECT count(*) FROM ai_config_result") == ["0"]
            assert server.calls == before, "stored retractions must not call the backend"
        cleanup()


def exercise_row_errors(server):
    url = f"http://127.0.0.1:{server.server_port}"
    endpoint = f"CASE WHEN id = 2 THEN 'invalid' ELSE '{url}' END"
    api_key = "CASE WHEN id = 3 THEN E'bad\\nkey' ELSE 'test-key' END"

    def call(endpoint, api_key):
        return (f"ai_decide(to_jsonb(id), {endpoint}, 'test-model', "
                f"ai_questions(ai_prob_question('q', 'Synthetic question?')), {api_key}, MAP {{'X-State': id::varchar}})")

    expected = ["1|0.5", "2|-1", "3|-1", "4|0.5"]
    for mode in ["local", "distributed"]:
        prefix = f"SET batch_parallelism = 1; SET query_mode = {mode}; "
        for limit in [0, 1]:
            server.reset()
            assert sql(prefix + "SET batch_expr_strict_mode = false; " +
                       f"SET ai_decide_max_concurrency = {limit}; " +
                       f"SELECT id, coalesce(({call(endpoint, api_key)}).q, -1) " +
                       "FROM generate_series(1, 4) AS t(id) ORDER BY id") == expected
            assert server.calls == 2, (mode, limit, server.calls)
        for condition, message in [("id <> 3", "invalid model_url"),
                                   ("id <> 2", "could not build HTTP request")]:
            server.reset()
            expect_error(prefix + "SET batch_expr_strict_mode = true; " +
                         f"SELECT ({call(endpoint, api_key)}).q " +
                         f"FROM generate_series(1, 4) AS t(id) WHERE {condition}", message)

    for append_only in [False, True]:
        cleanup()
        server.reset()
        sql("SET streaming_parallelism = 1; "
            "CREATE TABLE ai_config_input (id INT, endpoint VARCHAR, api_key VARCHAR)" +
            (" APPEND ONLY" if append_only else ""))
        sql("SET streaming_parallelism = 1; "
            "CREATE MATERIALIZED VIEW ai_config_result WITH (ai_decide_max_concurrency = 1) AS " +
            f"SELECT id, {call('endpoint', 'api_key')} AS result FROM ai_config_input")
        sql(f"INSERT INTO ai_config_input SELECT id, {endpoint}, {api_key} "
            "FROM generate_series(1, 4) AS t(id); FLUSH")
        query = "SELECT id, coalesce((result).q, -1) FROM ai_config_result ORDER BY id"
        assert sql(query) == expected
        assert server.calls == 2, (append_only, server.calls)
        before = server.calls
        sql("RECOVER")
        deadline = time.monotonic() + 30
        while True:
            try:
                if sql(query) == expected:
                    break
            except AssertionError:
                pass
            assert time.monotonic() < deadline, "recovery did not preserve per-row results"
            time.sleep(0.2)
        assert server.calls == before, "recovery must reuse the checkpointed results"
        if not append_only:
            sql("DELETE FROM ai_config_input; FLUSH")
            assert sql("SELECT count(*) FROM ai_config_result") == ["0"]
            assert server.calls == before, "deletion must retract the stored good and NULL results"
        cleanup()


def main():
    server = Server()
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        cleanup()
        try:
            exercise(server)
            exercise_row_errors(server)
        finally:
            cleanup()
    finally:
        server.shutdown()
        server.server_close()
        worker.join()
    print("ai_decide configuration and recovery passed")


if __name__ == "__main__":
    main()
