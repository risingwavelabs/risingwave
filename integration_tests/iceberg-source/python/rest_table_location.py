import json
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import psycopg2
from psycopg2 import sql


def test_rest_table_location(rw_config, source_config, rest_uri):
    """Exercise namespace location fallback against tabulario/iceberg-rest."""
    namespace = "rw_location_" + uuid.uuid4().hex[:12]
    warehouse = source_config["warehouse.path"].rstrip("/")
    namespace_location = f"{warehouse}/custom/{namespace}"
    namespaces = {
        namespace: {"location": namespace_location + "///"},
        namespace + "_missing": {},
        namespace + "_empty": {"location": ""},
    }
    tables = []

    def request(method, path, body=None, ignore_missing=False):
        data = json.dumps(body).encode() if body is not None else None
        req = Request(
            rest_uri.rstrip("/") + "/v1/" + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(req, timeout=30) as response:
                payload = response.read()
                return json.loads(payload) if payload else None
        except HTTPError as error:
            if ignore_missing and error.code == 404:
                return None
            raise

    def options_sql(options):
        return sql.SQL(", ").join(
            sql.SQL("{} = {}").format(sql.SQL(key), sql.Literal(value))
            for key, value in options.items()
        )

    with psycopg2.connect(
        database=rw_config["db"],
        user=rw_config["user"],
        host=rw_config["host"],
        port=rw_config["port"],
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(namespace)))
            try:
                cursor.execute(
                    sql.SQL("SET search_path TO {}").format(sql.Identifier(namespace))
                )
                cursor.execute("SET sink_decouple = false")
                cursor.execute("SET streaming_parallelism = 1")
                cursor.execute("CREATE TABLE input (id INT) APPEND ONLY")
                cursor.execute("INSERT INTO input VALUES (42)")
                for ns, properties in namespaces.items():
                    request(
                        "POST", "namespaces", {"namespace": [ns], "properties": properties}
                    )

                for runtime, catalog_type, vended in [
                    ("java", "rest", "false"),
                    ("rust", "rest_rust", "false"),
                    ("vended", "rest", "true"),
                ]:
                    for case, ns, enabled, explicit_warehouse in [
                        ("default", namespace, False, None),
                        ("derived", namespace, True, None),
                        ("explicit", namespace, True, f"{warehouse}/explicit"),
                        ("missing", namespace + "_missing", True, None),
                        ("empty", namespace + "_empty", True, None),
                    ]:
                        table = f"{runtime}_{case}"
                        common = {
                            key: value
                            for key, value in source_config.items()
                            if key not in ("warehouse.path", "database.name", "table.name")
                        }
                        common.update(
                            {
                                "catalog.type": catalog_type,
                                "vended_credentials": vended,
                                "s3.path.style.access": "true",
                                # Exercise the namespace-qualified table-name path.
                                "table.name": f"{ns}.{table}",
                            }
                        )
                        sink_options = {
                            **common,
                            "type": "append-only",
                            "create_table_if_not_exists": "true",
                            "commit_checkpoint_interval": "1",
                            "enable_snapshot_expiration": "false",
                        }
                        if enabled:
                            sink_options["default_table_location_from_namespace"] = "true"
                        if explicit_warehouse:
                            sink_options["warehouse.path"] = explicit_warehouse

                        tables.append((ns, table))
                        cursor.execute(
                            sql.SQL("CREATE SINK output FROM input WITH ({})").format(
                                options_sql(sink_options)
                            )
                        )
                        metadata = request("GET", f"namespaces/{ns}/tables/{table}")[
                            "metadata"
                        ]
                        if case == "derived":
                            expected_location = f"{namespace_location}/{table}"
                        else:
                            expected_location = (
                                f"{explicit_warehouse or warehouse}/{ns}/{table}"
                            )
                        assert metadata["location"] == expected_location, (
                            f"{runtime}/{case}: expected {expected_location}, "
                            f"got {metadata['location']}"
                        )

                        cursor.execute(
                            sql.SQL("CREATE SOURCE result WITH ({})").format(
                                options_sql(common)
                            )
                        )
                        cursor.execute("FLUSH")
                        deadline = time.monotonic() + 60
                        while True:
                            cursor.execute("SELECT id FROM result ORDER BY id")
                            rows = cursor.fetchall()
                            if rows == [(42,)]:
                                break
                            assert time.monotonic() < deadline, (
                                f"{runtime}/{case}: sink data did not become readable: {rows}"
                            )
                            time.sleep(1)
                        cursor.execute("DROP SOURCE result")
                        cursor.execute("DROP SINK output")
                        print(f"REST namespace location: {runtime}/{case} passed")
            finally:
                # Only this test's unique schema, namespaces, and tables are removed.
                cursor.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace))
                )
                for ns, table in tables:
                    request(
                        "DELETE",
                        f"namespaces/{ns}/tables/{table}?purgeRequested=true",
                        ignore_missing=True,
                    )
                for ns in namespaces:
                    request("DELETE", f"namespaces/{ns}", ignore_missing=True)
