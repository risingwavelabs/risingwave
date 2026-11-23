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

"""Checkpoint/recovery regression for saved AI_DECIDE answers and HTTP calls."""

import json
import threading

from ai_decide_http import EvaluationServer, sql, wait_rows


def cleanup():
    sql("DROP MATERIALIZED VIEW IF EXISTS ai_recovery_totals")
    sql("DROP MATERIALIZED VIEW IF EXISTS ai_recovery_answers")
    sql("DROP TABLE IF EXISTS ai_recovery_input")


def exercise(server):
    url = f"http://127.0.0.1:{server.server_port}/evaluate"
    sql("CREATE TABLE ai_recovery_input (id INT, payload JSONB)")
    sql(f"CREATE MATERIALIZED VIEW ai_recovery_answers AS SELECT id, "
        f"ai_decide(payload, '{url}', 'test-model', "
        "ai_questions(ai_prob_question('signal', 'Synthetic signal?'), "
        "ai_prob_question('generation', 'Synthetic generation?')), "
        "'test-key') AS result FROM ai_recovery_input")
    sql("CREATE MATERIALIZED VIEW ai_recovery_totals AS "
        "SELECT count(*) AS n, sum((result).generation) AS generation_sum "
        "FROM ai_recovery_answers")
    sql("INSERT INTO ai_recovery_input VALUES (1, '{\"message\":\"first\"}')")
    wait_rows("SELECT (result).signal FROM ai_recovery_answers", [0.1])
    saved = sql("SELECT to_jsonb(result) FROM ai_recovery_answers")
    assert json.loads(saved[0])["generation"] == 1 / 1024
    calls = server.count()
    assert calls == 1
    wait_rows("SELECT jsonb_build_array(n, generation_sum) FROM ai_recovery_totals",
              [[1, 1 / 1024]])

    # FLUSH commits a checkpoint before restarting actors, so no input needs replay.
    # Keep this HTTP server alive across recovery to detect any repeated inference.
    sql("FLUSH; RECOVER; WAIT;")
    assert sql("SELECT to_jsonb(result) FROM ai_recovery_answers") == saved
    assert server.count() == calls, "recovery reevaluated a checkpointed answer"

    # The first retraction after recovery must read persisted expression state.
    sql("DELETE FROM ai_recovery_input")
    wait_rows("SELECT (result).signal FROM ai_recovery_answers", [])
    # Normalize the pre-#27429 empty-SUM representation while checking the residual.
    wait_rows("SELECT jsonb_build_array(n, coalesce(generation_sum, 0)) FROM ai_recovery_totals", [[0, 0]])
    assert server.count() == calls, "deletion after recovery reevaluated inference"


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
    print("ai_decide recovery integration passed")


if __name__ == "__main__":
    main()
