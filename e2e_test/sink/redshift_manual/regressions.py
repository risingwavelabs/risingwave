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

"""Run production merge SQL against live Redshift with controlled CDC fixtures."""

import json
from pathlib import Path
import subprocess

from common import ident, redshift, s3_client
from fixtures import FIXTURES

ROOT = Path(__file__).resolve().parents[3]


class MergeSql:
    def __init__(self, directory):
        source = (
            ROOT / "src/connector/src/sink/snowflake_redshift/redshift.rs"
        ).read_text()

        def section(start, end):
            return source[source.index(start) : source.index(end, source.index(start))]

        # Compile the actual pure formatters from this checkout instead of
        # maintaining a second copy of the merge SQL in the test. No Cargo build
        # or external Rust dependencies are needed for these two functions.
        code = 'const __ROW_ID: &str = "__row_id"; const __OP: &str = "__op";\n'
        code += section(
            "pub fn build_full_table_name(", "\nfn build_alter_add_column_sql("
        )
        code += section(
            "fn build_create_merge_into_task_sql(", "\nfn build_copy_into_sql("
        )
        code += """
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let schema = if args[1].is_empty() { None } else { Some(args[1].as_str()) };
    let keys = args[4].split(',').map(str::to_owned).collect();
    let columns = args[5].split(',').map(str::to_owned).collect();
    for sql in build_create_merge_into_task_sql(schema, schema, &args[2], &args[3], &keys, &columns) {
        println!("@@EXEC\\n{}", sql);
    }
}
"""
        path = directory / "merge.rs"
        path.write_text(code)
        self.binary = directory / "merge"
        subprocess.run(
            ["rustc", "--edition", "2024", str(path), "-o", str(self.binary)],
            check=True,
            capture_output=True,
            text=True,
        )

    def statements(
        self, keys="id", columns="id,payload", schema="", cdc="cdc", target="target"
    ):
        result = subprocess.run(
            [str(self.binary), schema, cdc, target, keys, columns],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.split("@@EXEC\n")[1:]


def run(cfg, state, directory):
    formatter = MergeSql(directory)
    s3 = s3_client(cfg) if state["mode"] == "s3" else None
    conn = redshift(cfg)
    upload_index = 0
    passed = 0

    def load(cursor, rows, table="cdc", columns=("id", "payload", "__row_id", "__op")):
        nonlocal upload_index
        if not rows:
            return
        if s3 and table == "cdc":
            base = state["prefix"] + "/regressions/" + str(upload_index)
            upload_index += 1
            data = "".join(json.dumps(dict(zip(columns, row))) + "\n" for row in rows)
            s3.put_object(
                Bucket=cfg["S3_BUCKET"], Key=base + ".json", Body=data.encode()
            )
            manifest = json.dumps(
                {
                    "entries": [
                        {
                            "url": f"s3://{cfg['S3_BUCKET']}/{base}.json",
                            "mandatory": True,
                        }
                    ]
                }
            )
            s3.put_object(
                Bucket=cfg["S3_BUCKET"], Key=base + ".manifest", Body=manifest.encode()
            )
            # Match the connector's JSON/manifest COPY options. Credentials are
            # bound in memory, never written to generated SQL or command lines.
            credentials = f"aws_access_key_id={cfg['AWS_ACCESS_KEY_ID']};aws_secret_access_key={cfg['AWS_SECRET_ACCESS_KEY']}"
            cursor.execute(
                f"COPY {table} FROM %s CREDENTIALS %s FORMAT AS JSON 'auto' "
                "DATEFORMAT 'auto' TIMEFORMAT 'auto' MANIFEST",
                (f"s3://{cfg['S3_BUCKET']}/{base}.manifest", credentials),
            )
        else:
            cursor.executemany(
                f"INSERT INTO {table} VALUES ({','.join(['%s'] * len(rows[0]))})", rows
            )

    def setup(cursor, rows=(), initial=(), key="id", value="payload"):
        cursor.execute(
            f"CREATE TEMP TABLE cdc ({key} INT, {value} VARCHAR(256), __row_id VARCHAR(256), __op INT)"
        )
        cursor.execute(f"CREATE TEMP TABLE target ({key} INT, {value} VARCHAR(256))")
        load(cursor, rows, columns=(key, value, "__row_id", "__op"))
        load(cursor, initial, "target")

    def check(
        cursor,
        expected,
        remaining=0,
        key="id",
        value="payload",
        cdc="cdc",
        target="target",
    ):
        cursor.execute(f"SELECT {key},{value} FROM {target} ORDER BY {key}")
        assert cursor.fetchall() == list(expected), "Target rows differ"
        cursor.execute(f"SELECT count(*) FROM {cdc}")
        assert cursor.fetchone() == (remaining,), "Unexpected remaining CDC rows"

    def execute(cursor, statements):
        for statement in statements:
            cursor.execute(statement)

    def case(name, test):
        nonlocal passed
        try:
            with conn.cursor() as cursor:
                cursor.execute("SET statement_timeout TO 120000")
                test(cursor)
            passed += 1
            print(f"PASS {state['mode']} SQL/{name}", flush=True)
        except Exception as exc:
            raise RuntimeError(f"{state['mode']} SQL/{name}: {exc}") from None
        finally:
            conn.rollback()

    def simple(cursor, rows, initial, expected, key="id", value="payload"):
        setup(cursor, rows, initial, key, value)
        statements = formatter.statements(key, key + "," + value)
        for _ in range(2):
            execute(cursor, statements)
            check(cursor, expected, key=key, value=value)

    try:
        for name, rows, initial, expected in FIXTURES:
            case(name, lambda cursor: simple(cursor, rows, initial, expected))

        def watermark(cursor):
            rows = [
                (1, "epoch", "99_999_4294967295", 1),
                (2, "counter", "100_9_4294967295", 1),
                (3, "actor", "100_10_9", 1),
            ]
            late = [
                (4, "later actor", "100_10_10", 1),
                (5, "later counter", "100_11_0", 1),
                (6, "later epoch", "101_0_0", 1),
            ]
            setup(cursor, rows)
            statements = formatter.statements()
            cursor.execute(statements[0])
            cursor.execute(
                "SELECT max_epoch,max_row_count,max_actor_id FROM max_id_table"
            )
            assert cursor.fetchone() == (100, 10, 9)
            load(cursor, late)
            execute(cursor, statements[1:])
            check(cursor, [row[:2] for row in rows], remaining=3)
            for _ in range(2):
                execute(cursor, statements)
                check(cursor, [row[:2] for row in rows + late])

        case("watermark-and-late-rows", watermark)
        if not s3:
            for column in ("max_epoch", "max_row_count", "max_actor_id"):
                for role in ("key", "value"):
                    key, value = (
                        (column, "payload") if role == "key" else ("id", column)
                    )
                    for operation, op in (("insert", 1), ("update", 3), ("delete", 2)):
                        rows = [(1, "old", "100_9_1", 1), (1, "new", "100_10_1", op)]
                        initial = [] if operation == "insert" else [(1, "old")]
                        expected = [] if operation == "delete" else [(1, "new")]
                        case(
                            f"collision-{column}-{role}-{operation}",
                            lambda cursor: simple(
                                cursor, rows, initial, expected, key, value
                            ),
                        )

            def composite(cursor):
                columns = "max_epoch,max_row_count,max_actor_id,payload"
                keys = "max_epoch,max_row_count,max_actor_id"
                cursor.execute(
                    "CREATE TEMP TABLE cdc (max_epoch INT, max_row_count INT, max_actor_id INT, payload VARCHAR(256), __row_id VARCHAR(256), __op INT)"
                )
                cursor.execute(
                    "CREATE TEMP TABLE target (max_epoch INT, max_row_count INT, max_actor_id INT, payload VARCHAR(256))"
                )
                load(
                    cursor,
                    [
                        (1, 2, 3, "old", "100_9_1", 1),
                        (1, 2, 3, "new", "100_10_1", 3),
                        (1, 2, 4, "other", "100_11_1", 1),
                    ],
                )
                execute(cursor, formatter.statements(keys, columns))
                cursor.execute(
                    "SELECT " + columns + " FROM target ORDER BY max_actor_id"
                )
                assert cursor.fetchall() == [(1, 2, 3, "new"), (1, 2, 4, "other")]
                cursor.execute("SELECT count(*) FROM cdc")
                assert cursor.fetchone() == (0,)

            case("composite-watermark-column-keys", composite)

            def qualified(cursor):
                schema = cfg["REDSHIFT_SCHEMA"]
                cdc, target = state["cdc"] + "_sql", state["target"] + "_sql"
                fc, ft = ident(schema) + "." + ident(cdc), ident(schema) + "." + ident(
                    target
                )
                cursor.execute(
                    f"CREATE TABLE {fc} (id INT, payload VARCHAR(256), __row_id VARCHAR(256), __op INT)"
                )
                cursor.execute(f"CREATE TABLE {ft} (id INT, payload VARCHAR(256))")
                load(cursor, [(1, "old", "100_9_1", 1), (1, "new", "100_10_1", 3)], fc)
                execute(
                    cursor, formatter.statements(schema=schema, cdc=cdc, target=target)
                )
                check(cursor, [(1, "new")], cdc=fc, target=ft)

            case("schema-qualified-tables", qualified)
    finally:
        conn.close()
    print(f"PASS {state['mode']} SQL regressions: {passed}", flush=True)
