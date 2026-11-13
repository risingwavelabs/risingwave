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

"""SQL-to-HTTP extraction contract and saved-result regression fixture."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from ai_decide_http import sql, wait_rows

SPEC = {"task": "entities", "labels": ["person", "company"], "word_splitter": "char"}
RECORD = {"task": "records", "record_name": "sale", "anchor": "product",
          "fields": [{"name": "product"}, {"name": "price"}, {"name": "tags", "dtype": "list"}],
          "word_splitter": "char"}


class Server(ThreadingHTTPServer):
    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.calls, self.headers = [], []
        self.lock = threading.Lock()
        self.invalid_remaining = 0

    def count(self):
        with self.lock:
            return len(self.calls)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert self.path == "/extract"
        assert self.headers.get("Authorization") == "Bearer test-key"
        assert body["model"] == "test-model"
        assert body["word_splitter"] == "char"
        assert "state" not in body and "questions" not in body
        with self.server.lock:
            self.server.calls.append(body)
            self.server.headers.append({k.lower(): v for k, v in self.headers.items()})
            generation = len(self.server.calls)
            invalid = self.server.invalid_remaining > 0
            self.server.invalid_remaining -= int(invalid)
        content = body["text"]
        span = {"text": content, "start": 0, "end": len(content), "confidence": generation / 1024}
        if invalid:
            span["start"] = 1  # Valid JSON, but the substring no longer matches.
        if body["task"] == "entities":
            assert body["labels"] == ["person", "company"]
            result = {"entities": {"person": [span], "company": []}}
        else:
            assert body["task"] == "records" and body["anchor"] == "product"
            result = {"sale": [{"product": span, "price": None, "tags": [], "confidence": 1.0}]}
        response = json.dumps({"task": body["task"], "word_splitter": "char",
                               "offset_unit": "unicode_code_point", "end_exclusive": True,
                               "result": result, "extra_metadata": "ignored"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, *_):
        pass


def cleanup():
    sql("DROP MATERIALIZED VIEW IF EXISTS extract_totals; "
        "DROP MATERIALIZED VIEW IF EXISTS extract_result; "
        "DROP TABLE IF EXISTS extract_input")


def exercise(server):
    url = f"http://127.0.0.1:{server.server_port}/extract"

    def call(content, spec=SPEC, headers=None):
        extra = f", {headers}" if headers is not None else ""
        return f"ai_extract({content}, '{url}', 'test-model', '{json.dumps(spec)}'::jsonb, 'test-key'{extra})"

    content = "'🙂张三'"
    for spec in [SPEC, RECORD]:
        before = server.count()
        result = json.loads(sql(f"SELECT to_jsonb({call(content, spec)})")[0])
        assert server.count() == before + 1
        span = result["person"][0] if spec is SPEC else result["sale"][0]["product"]
        assert (span["text"], span["start"], span["end"]) == ("🙂张三", 0, 3)
        if spec is SPEC:
            assert result["company"] == []
        else:
            assert result["sale"][0]["price"] is None and result["sale"][0]["tags"] == []

    before = server.count()
    assert sql(f"SELECT cardinality((r).person), cardinality((r).company) FROM (SELECT {call(content)} AS r) t") == ["1|0"]
    assert server.count() == before + 1, "field projection duplicated inference"
    server.invalid_remaining = 3
    before = server.count()
    assert sql("SET ai_extract_retry_delay_ms = 1; SET ai_extract_max_retry_delay_ms = 2; "
               f"SELECT coalesce(cardinality(({call(content)}).person), -1)") == ["1"]
    assert server.count() == before + 4, "malformed spans must stay inside the retry loop"

    # Dynamic headers and partial local errors in local and distributed batch plans.
    sql("CREATE TABLE extract_input (id INT PRIMARY KEY, body VARCHAR, headers MAP(VARCHAR,VARCHAR))")
    sql("INSERT INTO extract_input VALUES (1,'first',MAP {'X-Tenant':'one'}), "
        "(2,'second',MAP {'X-Tenant':'two'}), (3,'skip',NULL), (4,'bad',MAP {'Authorization':'conflict'}); FLUSH")
    for mode in ["local", "distributed"]:
        before = server.count()
        actual = sql(f"SET query_mode = {mode}; SET batch_expr_strict_mode = false; "
                     f"SELECT id, cardinality((r).person) FROM (SELECT id,{call('body', headers='headers')} AS r FROM extract_input) t ORDER BY id")
        assert actual == ["1|1", "2|1", "3|", "4|"], actual
        assert server.count() == before + 2
        assert sorted(h["x-tenant"] for h in server.headers[before:]) == ["one", "two"]
    cleanup()

    # Each mode persists one result; changes retract the saved value, not a fresh inference.
    for append_only in [False, True]:
        sql("CREATE TABLE extract_input (id INT, body VARCHAR, enabled BOOLEAN)" + (" APPEND ONLY" if append_only else ""))
        before = server.count()
        sql("SET streaming_parallelism = 1; SET streaming_project_expr_concurrency = 4; "
            f"CREATE MATERIALIZED VIEW extract_result AS SELECT id, CASE WHEN enabled THEN {call('body')} END AS r FROM extract_input")
        sql("CREATE MATERIALIZED VIEW extract_totals AS SELECT count(*) AS n, sum(((r).person[1]).confidence) AS score FROM extract_result")
        sql("INSERT INTO extract_input VALUES (1,'first',true), (2,'second',true), (3,'skip',false)")
        wait_rows("SELECT jsonb_build_array(id, cardinality((r).person), cardinality((r).company)) FROM extract_result ORDER BY id",
                  [[1, 1, 0], [2, 1, 0], [3, None, None]])
        assert server.count() == before + 2
        saved = sql("SELECT id,to_jsonb(r) FROM extract_result ORDER BY id")
        assert sql("SELECT id,to_jsonb(r) FROM extract_result ORDER BY id") == saved
        assert server.count() == before + 2, "reading a materialized result must not call HTTP"
        if not append_only:
            sql("UPDATE extract_input SET body='updated' WHERE id=1; FLUSH")
            assert server.count() == before + 3, "update should infer only the new content"
            assert sql("SELECT n,score FROM extract_totals") == sql("SELECT count(*),sum(((r).person[1]).confidence) FROM extract_result")
            before = server.count()
            sql("DELETE FROM extract_input; FLUSH")
            assert sql("SELECT n,score IS NULL FROM extract_totals") == ["0|t"]
            assert server.count() == before, "delete must retract the saved value"
        cleanup()

    # Record arrays use the same streaming machinery, including nullable and list fields.
    sql("CREATE TABLE extract_input (id INT PRIMARY KEY, body VARCHAR)")
    sql(f"CREATE MATERIALIZED VIEW extract_result AS SELECT id,{call('body', RECORD)} AS r FROM extract_input")
    sql("INSERT INTO extract_input VALUES (1,'product'); FLUSH")
    result = json.loads(sql("SELECT to_jsonb(r) FROM extract_result")[0])
    assert result["sale"][0]["product"]["text"] == "product"
    assert result["sale"][0]["price"] is None and result["sale"][0]["tags"] == []
    sql("UPDATE extract_input SET body='changed'; FLUSH")
    assert json.loads(sql("SELECT to_jsonb(r) FROM extract_result")[0])["sale"][0]["product"]["text"] == "changed"
    before = server.count()
    sql("DELETE FROM extract_input; FLUSH")
    assert sql("SELECT count(*) FROM extract_result") == ["0"] and server.count() == before


if __name__ == "__main__":
    server = Server()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        cleanup()
        exercise(server)
    finally:
        cleanup()
        server.shutdown()
        server.server_close()
        thread.join()
    print("ai_extract HTTP integration passed")
