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
        state = body["state"]
        rows = state if isinstance(state, list) else [state]
        signal = round(sum({"first": 0.1, "second": 0.2, "updated": 0.3}.get(row.get("message"), 0) for row in rows if isinstance(row, dict)), 6)
        team = "billing" if signal < 0.25 else "technical"
        level = min(len(rows), 2)
        response = {"answers": {
            "signal": {"type": "noul", "noul": signal},
            # Exact binary fractions keep retraction checks independent of sum rounding.
            "generation": {"type": "noul", "noul": generation / 1024},
            "team": {"type": "choice", "choice": team, "confidence": 0.6,
                     "probabilities": {label: 0.8 if label == team else 0.2 for label in ["billing", "technical"]}},
            "size": {"type": "score", "score": float(level), "confidence": 1.0,
                     "probabilities": {str(i): float(i == level) for i in range(3)}}
        }, "unknown_metadata": True}
        if isinstance(state, dict) and state.get("malformed"):
            response["answers"]["team"]["choice"] = "undeclared"
        response = json.dumps(response).encode()
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

    questions = """ai_questions(
        ai_prob_question('signal', 'Synthetic signal?'),
        ai_prob_question('generation', 'Synthetic generation?'),
        ai_choice_question('team', 'Which team?', ARRAY[
            ai_option('billing', 'Payments and refunds'), ai_option('technical', 'Software issues')]),
        ai_score_question('size', 'How many events?', ARRAY['No events', 'One event', 'At least two events'])
    )"""

    def analyze(state):
        return f"ai_decide({state}, '{url}', 'test-model', {questions}, 'test-key')"

    def states(view, where="", order="id"):
        return f"SELECT (result).signal FROM {view} {where} ORDER BY {order}"

    p1, p2, p3 = {"message": "first"}, {"message": "second"}, {"message": "updated"}
    batch_state = "'" + json.dumps(p1) + "'::jsonb"
    before = server.count()
    response = json.loads(sql(
        f"SELECT to_jsonb(result) FROM (SELECT {analyze(batch_state)} AS result) e"
    )[0])
    assert response["signal"] == 0.1
    assert response["team"] == {"choice": "billing", "confidence": 0.6, "probabilities": {"billing": 0.8, "technical": 0.2}}
    assert response["size"] == {"score": 1.0, "confidence": 1.0, "probabilities": [0.0, 1.0, 0.0]}
    assert server.count() == before + 1
    assert server.calls[-1]["questions"]["team"]["criteria"] == {"billing": "Payments and refunds", "technical": "Software issues"}
    assert server.calls[-1]["state"] == p1

    # Referencing several fields of one subquery result must not duplicate inference.
    before = server.count()
    assert sql(f"SELECT (result).signal, (result).team.choice, (result).team.confidence, (result).size.score FROM (SELECT {analyze(batch_state)} AS result) e") == ["0.1|billing|0.6|1"]
    assert server.count() == before + 1, "batch field projection duplicated inference"

    # A successful HTTP status with an invalid typed answer is still a fatal error.
    before = server.count()
    malformed_state = "'{\"malformed\":true}'::jsonb"
    try:
        sql(f"SELECT coalesce(({analyze(malformed_state)}).signal, 0)")
    except AssertionError as error:
        assert "choice must be a declared option" in str(error), error
    else:
        raise AssertionError("malformed answer was swallowed")
    assert server.count() == before + 1, "response contract failure must not retry"

    for append_only in [False, True]:
        sql("CREATE TABLE ai_fields_input (id INT, payload JSONB, enabled BOOLEAN)" + (" APPEND ONLY" if append_only else ""))
        # Keep the settings and DDL in one session so the job captures both overrides.
        # A two-row input can exceed the row budget without preventing progress.
        sql(f"SET streaming_project_expr_concurrency = 4; "
            f"SET streaming_project_expr_inflight_request_concurrency = 1; "
            f"CREATE MATERIALIZED VIEW ai_fields AS "
            f"SELECT id, id + 1 AS next_id, (result).signal AS signal, "
            f"(result).team.choice AS team, (result).size.score AS size "
            f"FROM (SELECT id, CASE WHEN enabled THEN {analyze('payload')} END AS result "
            f"FROM ai_fields_input) e")
        assert sql("SELECT key, value FROM rw_streaming_job_config WHERE name = 'ai_fields' "
                   "AND key LIKE 'streaming.developer.project_expr_%' ORDER BY key") == [
            "streaming.developer.project_expr_concurrency|4",
            "streaming.developer.project_expr_inflight_request_concurrency|1",
        ]
        before = server.count()
        sql("INSERT INTO ai_fields_input VALUES "
            "(1, '{\"message\":\"first\"}', true), (2, '{\"malformed\":true}', false)")
        wait_rows("SELECT jsonb_build_array(next_id, signal, team, size) FROM ai_fields ORDER BY id",
                  [[2, 0.1, "billing", 1], [3, None, None, None]])
        assert server.count() == before + 1, "projection duplicated inference or evaluated a false branch"
        if not append_only:
            sql("DELETE FROM ai_fields_input")
            wait_rows("SELECT signal FROM ai_fields", [])
            assert server.count() == before + 1, "retraction reevaluated inference"
        sql("DROP MATERIALIZED VIEW ai_fields; DROP TABLE ai_fields_input")

    sql("CREATE TABLE jev_input (id INT PRIMARY KEY, user_id INT, payload JSONB)")
    sql(f"CREATE MATERIALIZED VIEW jev_direct AS SELECT id, {analyze('payload')} AS result FROM jev_input")
    sql("CREATE MATERIALIZED VIEW jev_totals AS SELECT count(*) AS n, sum((result).generation) AS generation_sum FROM jev_direct")
    sql("INSERT INTO jev_input VALUES (1, 1, '{\"message\":\"first\"}'), (2, 1, '{\"message\":\"second\"}')")
    wait_rows(states("jev_direct"), [0.1, 0.2])
    before = server.count()
    for _ in range(2):
        assert len(sql("SELECT (result).signal, (result).team.choice, (result).size.score FROM jev_direct")) == 2
    assert server.count() == before, "reading stored fields reevaluated the model"
    sql(f"UPDATE jev_input SET payload = '{json.dumps(p3)}' WHERE id = 1")
    wait_rows(states("jev_direct"), [0.3, 0.2])
    # A downstream aggregate checks that updates and deletes retract the saved value.
    actual = sql("SELECT count(*), sum((result).generation) FROM jev_direct")
    assert sql("SELECT n, generation_sum FROM jev_totals") == actual
    calls = server.count()
    sql("DELETE FROM jev_input")
    wait_rows(states("jev_direct"), [])
    assert server.count() == calls, "deletion reevaluated the remote function"
    # An empty SUM is NULL after all saved results have been retracted.
    actual = sql("SELECT n, generation_sum IS NULL FROM jev_totals")
    assert actual == ["0|t"], actual
    sql("DROP MATERIALIZED VIEW jev_totals")
    sql("DROP MATERIALIZED VIEW jev_direct")

    sql(f"CREATE MATERIALIZED VIEW jev_grouped AS SELECT user_id, {analyze('jsonb_agg(payload ORDER BY id)')} AS result FROM jev_input GROUP BY user_id")
    sql("INSERT INTO jev_input VALUES (1, 1, '{\"message\":\"first\"}'), (2, 1, '{\"message\":\"second\"}')")
    wait_rows(states("jev_grouped", order="user_id"), [0.3])
    sql("DELETE FROM jev_input WHERE id = 1")
    wait_rows(states("jev_grouped", order="user_id"), [0.2])
    calls = server.count()
    sql("DELETE FROM jev_input")
    wait_rows(states("jev_grouped", order="user_id"), [])
    assert server.count() == calls, "removing the last group reevaluated its old result"
    sql("DROP MATERIALIZED VIEW jev_grouped")

    frame = "to_jsonb(array_agg(payload) OVER (PARTITION BY user_id ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW))"
    sql(f"CREATE MATERIALIZED VIEW jev_over AS SELECT id, {analyze(frame)} AS result FROM jev_input")
    sql("INSERT INTO jev_input VALUES (1, 1, '{\"message\":\"first\"}'), (2, 1, '{\"message\":\"second\"}')")
    wait_rows(states("jev_over"), [0.1, 0.3])
    sql("DELETE FROM jev_input WHERE id = 1")
    wait_rows(states("jev_over"), [0.2])
    sql("DROP MATERIALIZED VIEW jev_over")
    sql("DROP TABLE jev_input")

    # One input actor gives this fixture a single advancing event-time watermark.
    sql("SET streaming_parallelism = 1; CREATE TABLE jev_events (id INT, user_id INT, ts TIMESTAMP, payload JSONB, WATERMARK FOR ts AS ts - INTERVAL '1 second') APPEND ONLY")
    for suffix, emit in [("immediate", ""), ("closed", "EMIT ON WINDOW CLOSE")]:
        sql(f"CREATE MATERIALIZED VIEW jev_window_{suffix} AS SELECT user_id, window_start, {analyze('jsonb_agg(payload ORDER BY ts)')} AS result FROM tumble(jev_events, ts, INTERVAL '5 minutes') GROUP BY user_id, window_start {emit}")
    frame = "to_jsonb(array_agg(payload) OVER (PARTITION BY user_id ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW))"
    sql(f"CREATE MATERIALIZED VIEW jev_over_closed AS SELECT id, user_id, {analyze(frame)} AS result FROM jev_events EMIT ON WINDOW CLOSE")
    sql("INSERT INTO jev_events VALUES (1, 1, '2026-01-01 00:00:01', '{\"message\":\"first\"}'), (2, 1, '2026-01-01 00:00:02', '{\"message\":\"second\"}')")
    wait_rows(states("jev_window_immediate", "WHERE user_id = 1", "window_start"), [0.3])
    assert sql("SELECT count(*) FROM jev_window_closed") == ["0"]
    sql("INSERT INTO jev_events VALUES (3, 99, '2026-01-01 00:20:00', '{}')")
    wait_rows(states("jev_window_closed", "WHERE user_id = 1", "window_start"), [0.3])
    wait_rows(states("jev_over_closed", "WHERE user_id = 1"), [0.1, 0.3])


def cleanup():
    for view in ["ai_fields", "jev_totals", "jev_direct", "jev_grouped", "jev_over", "jev_window_immediate", "jev_window_closed", "jev_over_closed"]:
        sql(f"DROP MATERIALIZED VIEW IF EXISTS {view}")
    for table in ["ai_fields_input", "jev_input", "jev_events"]:
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
    print("ai_decide HTTP integration passed")


if __name__ == "__main__":
    main()
