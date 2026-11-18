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

"""SLT actions. Credentials stay in the process environment and SQL secrets."""

import json
import os
from pathlib import Path
import sys
from urllib.parse import urlencode

from common import (
    clean_s3,
    config,
    ident,
    literal,
    redact,
    redshift,
    require_opt_in,
    s3_client,
    s3_objects,
    wait_for,
)


class LiveSink:
    def __init__(self, state):
        self.state = state
        self.cfg = config(state["mode"])
        self.s3 = s3_client(self.cfg) if state["mode"] == "s3" else None

    def rw(self, sql):
        import psycopg2

        # These are the same connection arguments passed to risedev slt.
        conn = psycopg2.connect(**self.state["rw"], connect_timeout=15)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("SET search_path TO " + ident(self.state["schema"]))
                cursor.execute(sql)
        finally:
            conn.close()

    def rs(self, sql):
        conn = redshift(self.cfg)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("SET statement_timeout TO 120000")
                cursor.execute(sql)
                return cursor.fetchall() if cursor.description else None
        finally:
            conn.close()

    def target(self, kind):
        return ident(self.cfg["REDSHIFT_SCHEMA"]) + "." + ident(self.state[kind])

    def setup(self):
        self.rw("CREATE SCHEMA " + ident(self.state["schema"]))
        # Encode credentials in one URL secret, including special characters.
        # Separate user/password options currently interpolate them unescaped.
        url = (
            self.cfg["REDSHIFT_JDBC_URL"]
            + "?"
            + urlencode(
                {
                    "user": self.cfg["REDSHIFT_USER"],
                    "password": self.cfg["REDSHIFT_PASSWORD"],
                }
            )
        )
        self.rw("CREATE SECRET jdbc_url WITH (backend='meta') AS " + literal(url))
        if self.s3:
            for name, key in [
                ("s3_access", "AWS_ACCESS_KEY_ID"),
                ("s3_secret", "AWS_SECRET_ACCESS_KEY"),
            ]:
                self.rw(
                    f"CREATE SECRET {name} WITH (backend='meta') AS {literal(self.cfg[key])}"
                )

    def create_sink(self, append):
        kind = "append" if append else "target"
        name = "append_sink" if append else "upsert_sink"
        source = "append_source" if append else "upsert_source"
        extras = (
            "type='append-only', force_append_only='true'"
            if append
            else (
                "type='upsert', primary_key='id', intermediate.table.name="
                + literal(self.state["cdc"])
            )
        )
        options = ""
        if self.s3:
            options = f"""
                s3.bucket_name={literal(self.cfg['S3_BUCKET'])},
                s3.region_name={literal(self.cfg['AWS_REGION'])},
                s3.path={literal(self.state['prefix'])},
                s3.credentials.access=SECRET s3_access,
                s3.credentials.secret=SECRET s3_secret,
            """
        self.rw(f"""
            SET streaming_parallelism=4;
            CREATE SINK {name} FROM {source} WITH (
                connector='redshift', jdbc.url=SECRET jdbc_url,
                schema={literal(self.cfg['REDSHIFT_SCHEMA'])}, table.name={literal(self.state[kind])},
                create_table_if_not_exists='true', with_s3='{str(bool(self.s3)).lower()}',
                write.target.interval.seconds='2', write.intermediate.interval.seconds='1',
                batch.insert.rows='32', {options} {extras});
        """)

    def check_rows(self, phase):
        if phase == "initial":
            expected = [(i, "initial") for i in range(1, 257)]
        elif phase == "changed":
            expected = [
                (i, "revived" if i == 3 else "updated")
                for i in range(1, 257)
                if i % 3 != 0 or i == 3
            ]
        else:
            expected = [(i, "append") for i in range(1, 129)]
        table = self.target("append" if phase == "append" else "target")
        wait_for(
            lambda: self.rs(f"SELECT id, payload FROM {table} ORDER BY id"),
            expected,
            phase,
        )
        if phase != "append":
            wait_for(
                lambda: self.rs(f"SELECT count(*) FROM {self.target('cdc')}"),
                [(0,)],
                phase + " CDC cleanup",
            )

    def check_s3(self):
        if not self.s3:
            return
        wait_for(
            lambda: any(
                "/manifest/" in obj["Key"]
                for obj in s3_objects(self.s3, self.cfg, self.state["prefix"])
            ),
            False,
            "S3 manifest consumption",
        )
        rows = []
        for obj in s3_objects(self.s3, self.cfg, self.state["prefix"]):
            if "/" + self.state["target"] + "/" in obj["Key"]:
                response = self.s3.get_object(
                    Bucket=self.cfg["S3_BUCKET"], Key=obj["Key"]
                )
                with response["Body"] as body:
                    rows.extend(json.loads(line) for line in body.read().splitlines())
        assert len(rows) >= 256, "Missing staged CDC events"
        ids = [row["__row_id"].split("_") for row in rows]
        assert all(len(parts) == 3 for parts in ids), "Missing actor component"
        assert len({tuple(parts) for parts in ids}) == len(ids), "Duplicate CDC row IDs"
        assert len({parts[2] for parts in ids}) == 4, "Expected four sink writers"
        assert (
            max(int(parts[1]) for parts in ids) >= 10
        ), "Counter boundary not exercised"
        # Sink input may normalize updates into delete/insert pairs. The target
        # row assertions above verify the resulting update semantics.
        assert {1, 2} <= {row["__op"] for row in rows} <= {1, 2, 3, 4}
        print(
            f"PASS S3 staging: {len(rows)} unique events from four writers", flush=True
        )

    def cleanup(self):
        # Stop writers before removing their external tables and objects.
        self.rw("DROP SCHEMA IF EXISTS " + ident(self.state["schema"]) + " CASCADE")
        errors = []
        for kind in ("cdc", "target", "append"):
            try:
                self.rs("DROP TABLE IF EXISTS " + self.target(kind))
            except Exception as exc:
                errors.append(str(exc))
        if self.s3:
            try:
                clean_s3(self.s3, self.cfg, self.state["prefix"])
            except Exception as exc:
                errors.append(str(exc))
        if errors:
            raise RuntimeError("Cleanup failed: " + "; ".join(errors))
        print("PASS test resource cleanup", flush=True)


def main():
    require_opt_in()
    state = json.loads(Path(os.environ["REDSHIFT_E2E_STATE"]).read_text())
    test = LiveSink(state)
    action = sys.argv[1]
    if action == "setup":
        test.setup()
    elif action in ("create-upsert", "create-append"):
        test.create_sink(action == "create-append")
    elif action.startswith("check-") and action != "check-s3":
        test.check_rows(action.removeprefix("check-"))
    elif action == "check-s3":
        test.check_s3()
    elif action == "cleanup":
        test.cleanup()
    else:
        raise ValueError("Unknown test action")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(redact(str(exc)), file=sys.stderr)
        sys.exit(1)
