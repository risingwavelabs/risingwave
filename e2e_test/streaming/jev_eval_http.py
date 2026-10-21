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

"""SQL-to-HTTP regression fixture; uses only synthetic data and a temporary server."""

import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class EvaluationServer(ThreadingHTTPServer):
    def __init__(self):
        super().__init__(("127.0.0.1", 0), EvaluationHandler)
        self.calls = []
        self.lock = threading.Lock()

    def count(self):
        with self.lock:
            return len(self.calls)


class EvaluationHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path != "/evaluate" or self.headers.get("Authorization") != "Bearer test-key":
            self.send_error(400)
            return
        with self.server.lock:
            self.server.calls.append(body)
            generation = len(self.server.calls)
        # A different result on every call exposes accidental reevaluation of retractions.
        response = json.dumps({"echo": body, "generation": generation, "unknown_metadata": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, *_):
        pass


def sql(statement):
    result = subprocess.run(
        ["psql", "-X", "-qAt", "-h", os.environ["SLT_HOST"],
         "-p", os.environ["SLT_PORT"], "-d", os.environ["SLT_DB"], "-U", "root",
         "-v", "ON_ERROR_STOP=1", "-c", statement],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode:
        raise AssertionError(f"SQL failed: {statement}\n{result.stderr}")
    return result.stdout.strip().splitlines()


def wait_rows(query, expected):
    deadline = time.monotonic() + 30
    while True:
        sql("FLUSH")
        actual = [json.loads(row) for row in sql(query)]
        if actual == expected:
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"{query}: expected {expected!r}, got {actual!r}")
        time.sleep(0.1)


def exercise(server):
    url = f"http://127.0.0.1:{server.server_port}/evaluate"

    def analyze(state, questions="'{}'::jsonb"):
        return f"""jev_eval({state}, '{url}', 'test-model', {questions}, 'test-key',
            '{{"max_concurrency":2}}'::jsonb)"""

    def states(view, where="", order="id"):
        return f"SELECT result->'echo'->'state' FROM {view} {where} ORDER BY {order}"

    p1, p2, p3 = {"message": "first"}, {"message": "second"}, {"message": "updated"}
    question = {"q": {"type": "noul", "instructions": "Resolved?"}}
    batch_state = "'" + json.dumps(p1) + "'::jsonb"
    response = json.loads(sql(f"SELECT {analyze(batch_state)}")[0])
    assert response["echo"] == {"state": p1, "model": "test-model", "questions": {}}
    assert response["unknown_metadata"] is True
    for function in ["jev_eval", "jev_analyze"]:
        for options in ["", ", '{\"max_concurrency\":2}'::jsonb"]:
            response = json.loads(sql(
                f"SELECT {function}({batch_state}, '{url}', 'test-model', '{{}}'::jsonb, 'test-key'{options})"
            )[0])
            assert response["echo"] == {"state": p1, "model": "test-model", "questions": {}}
            assert response["unknown_metadata"] is True

    sql("CREATE TABLE jev_input (id INT PRIMARY KEY, user_id INT, payload JSONB, questions JSONB)")
    sql(f"CREATE MATERIALIZED VIEW jev_direct AS SELECT id, {analyze('payload', 'questions')} AS result FROM jev_input")
    sql("INSERT INTO jev_input VALUES (1, 1, '{\"message\":\"first\"}', '{}'), (2, 1, '{\"message\":\"second\"}', '{}')")
    wait_rows(states("jev_direct"), [p1, p2])
    sql(f"UPDATE jev_input SET payload = '{json.dumps(p3)}', questions = '{json.dumps(question)}' WHERE id = 1")
    wait_rows(states("jev_direct"), [p3, p2])
    assert json.loads(sql("SELECT result->'echo'->'questions' FROM jev_direct WHERE id = 1")[0]) == question
    calls = server.count()
    sql("DELETE FROM jev_input")
    wait_rows(states("jev_direct"), [])
    assert server.count() == calls, "deletion reevaluated the remote function"
    sql("DROP MATERIALIZED VIEW jev_direct")

    sql(f"CREATE MATERIALIZED VIEW jev_grouped AS SELECT user_id, {analyze('jsonb_agg(payload ORDER BY id)')} AS result FROM jev_input GROUP BY user_id")
    sql("INSERT INTO jev_input VALUES (1, 1, '{\"message\":\"first\"}', '{}'), (2, 1, '{\"message\":\"second\"}', '{}')")
    wait_rows(states("jev_grouped", order="user_id"), [[p1, p2]])
    sql("DELETE FROM jev_input WHERE id = 1")
    wait_rows(states("jev_grouped", order="user_id"), [[p2]])
    calls = server.count()
    sql("DELETE FROM jev_input")
    wait_rows(states("jev_grouped", order="user_id"), [])
    assert server.count() == calls, "removing the last group reevaluated its old result"
    sql("DROP MATERIALIZED VIEW jev_grouped")

    frame = "to_jsonb(array_agg(payload) OVER (PARTITION BY user_id ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW))"
    sql(f"CREATE MATERIALIZED VIEW jev_over AS SELECT id, {analyze(frame)} AS result FROM jev_input")
    sql("INSERT INTO jev_input VALUES (1, 1, '{\"message\":\"first\"}', '{}'), (2, 1, '{\"message\":\"second\"}', '{}')")
    wait_rows(states("jev_over"), [[p1], [p1, p2]])
    sql("DELETE FROM jev_input WHERE id = 1")
    wait_rows(states("jev_over"), [[p2]])
    sql("DROP MATERIALIZED VIEW jev_over")
    sql("DROP TABLE jev_input")

    # One input actor gives this fixture a single advancing event-time watermark.
    sql("SET streaming_parallelism = 1; CREATE TABLE jev_events (id INT, user_id INT, ts TIMESTAMP, payload JSONB, WATERMARK FOR ts AS ts - INTERVAL '1 second') APPEND ONLY")
    for suffix, emit in [("immediate", ""), ("closed", "EMIT ON WINDOW CLOSE")]:
        sql(f"CREATE MATERIALIZED VIEW jev_window_{suffix} AS SELECT user_id, window_start, {analyze('jsonb_agg(payload ORDER BY ts)')} AS result FROM tumble(jev_events, ts, INTERVAL '5 minutes') GROUP BY user_id, window_start {emit}")
    frame = "to_jsonb(array_agg(payload) OVER (PARTITION BY user_id ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW))"
    sql(f"CREATE MATERIALIZED VIEW jev_over_closed AS SELECT id, user_id, {analyze(frame)} AS result FROM jev_events EMIT ON WINDOW CLOSE")
    sql("INSERT INTO jev_events VALUES (1, 1, '2026-01-01 00:00:01', '{\"message\":\"first\"}'), (2, 1, '2026-01-01 00:00:02', '{\"message\":\"second\"}')")
    wait_rows(states("jev_window_immediate", "WHERE user_id = 1", "window_start"), [[p1, p2]])
    assert sql("SELECT count(*) FROM jev_window_closed") == ["0"]
    sql("INSERT INTO jev_events VALUES (3, 99, '2026-01-01 00:20:00', '{}')")
    wait_rows(states("jev_window_closed", "WHERE user_id = 1", "window_start"), [[p1, p2]])
    wait_rows(states("jev_over_closed", "WHERE user_id = 1"), [[p1], [p1, p2]])


def cleanup():
    for view in ["jev_direct", "jev_grouped", "jev_over", "jev_window_immediate", "jev_window_closed", "jev_over_closed"]:
        sql(f"DROP MATERIALIZED VIEW IF EXISTS {view}")
    for table in ["jev_input", "jev_events"]:
        sql(f"DROP TABLE IF EXISTS {table}")


def main():
    server = EvaluationServer()
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        cleanup()
        try:
            exercise(server)
        finally:
            cleanup()
    finally:
        server.shutdown()
        server.server_close()
        worker.join()
    print("jev_eval HTTP integration passed")


if __name__ == "__main__":
    main()
