# Copyright 2026 RisingWave Labs
# Licensed under the Apache License, Version 2.0.

"""Force the dedicated table-source test user to reauthenticate after rotation."""

import re
import subprocess


def main():
    # The SLT mysql wrapper supplies the admin connection settings. Only terminate
    # sessions belonging to this test, including any surviving binlog connection.
    result = subprocess.run(
        ["mysql", "-N", "-B", "-e",
         "SELECT ID FROM INFORMATION_SCHEMA.PROCESSLIST "
         "WHERE USER = 'mysql-orders-cdc'"],
        check=True, capture_output=True, text=True,
    )
    for value in result.stdout.splitlines():
        connection_id = int(value)
        killed = subprocess.run(
            ["mysql", "-e", f"KILL CONNECTION {connection_id}"],
            capture_output=True, text=True,
        )
        # ALTER SOURCE or reconnect can close a listed session before KILL runs.
        # Other errors (permissions, connectivity, etc.) must fail the test.
        if killed.returncode and not re.search(r"ERROR 1094 \(HY000\)", killed.stderr):
            raise RuntimeError(killed.stderr)


if __name__ == "__main__":
    main()
