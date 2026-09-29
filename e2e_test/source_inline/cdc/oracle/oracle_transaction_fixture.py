#!/usr/bin/env python3

"""Hold the recovery test's Oracle transaction across CREATE SOURCE in the SLT."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

from oracle_test_utils import connect_as_sys, identifier


def paths(name: str) -> Path:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
        raise RuntimeError("invalid transaction fixture name")
    return Path(tempfile.gettempdir()) / f"rw-oracle-transaction-{name}"


def publish(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value))
    temporary.replace(path)


def wait(path: Path, timeout: int = 30) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return json.loads(path.read_text())
        time.sleep(0.1)
    raise RuntimeError(f"transaction fixture timed out waiting for {path}")


def finish(name: str, action: str) -> None:
    directory = paths(name)
    publish(directory / "request.json", {"action": action})
    result = wait(directory / "done.json")
    if not result.get("ok"):
        raise RuntimeError(result)


def hold(name: str) -> None:
    directory = paths(name)
    try:
        with connect_as_sys(identifier("ORACLE_PDB")) as connection:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE APP.RW_LIVE_PAGE SET VALUE = 'recovered-long' WHERE ID = 1")
            publish(directory / "ready.json", {"ready": True})
            # Roll back automatically if the SLT fails to finish the transaction.
            request = wait(directory / "request.json", timeout=300)
            if request["action"] == "commit":
                connection.commit()
            else:
                connection.rollback()
        publish(directory / "done.json", {"ok": True})
    except Exception as error:
        publish(directory / "done.json", {"error": str(error)})
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["begin", "commit", "rollback", "hold"])
    parser.add_argument("--name", required=True)
    args = parser.parse_args()
    directory = paths(args.name)
    if args.command == "hold":
        hold(args.name)
    elif args.command == "begin":
        if directory.exists():
            if not (directory / "done.json").exists():
                raise RuntimeError(f"unfinished transaction fixture: {directory}")
            for path in directory.iterdir():
                path.unlink()
        else:
            directory.mkdir()
        with (directory / "worker.log").open("w") as log:
            subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "hold", "--name", args.name],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
            )
        wait(directory / "ready.json")
    else:
        finish(args.name, args.command)


if __name__ == "__main__":
    main()
