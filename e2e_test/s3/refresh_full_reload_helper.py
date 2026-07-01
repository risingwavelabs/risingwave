#!/usr/bin/env python3

import os
import sys
import tempfile

from minio import Minio


BUCKET = "hummock001"
PREFIX = "refresh_full_reload_issue26012"


def client() -> Minio:
    return Minio(
        endpoint="127.0.0.1:9301",
        access_key="hummockadmin",
        secret_key="hummockadmin",
        secure=False,
    )


def cleanup(minio_client: Minio) -> None:
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        return

    for obj in minio_client.list_objects(BUCKET, prefix=f"{PREFIX}/", recursive=True):
        minio_client.remove_object(BUCKET, obj.object_name)


def upload_csv(minio_client: Minio, object_name: str, content: str) -> None:
    fd, path = tempfile.mkstemp(prefix="rw_s3_refresh_", suffix=".csv")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        minio_client.fput_object(BUCKET, object_name, path)
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def setup_initial(minio_client: Minio) -> None:
    cleanup(minio_client)
    upload_csv(minio_client, f"{PREFIX}/part-1.csv", "1,alice\n2,bob\n")


def setup_updated(minio_client: Minio) -> None:
    cleanup(minio_client)
    upload_csv(minio_client, f"{PREFIX}/part-1.csv", "1,alice_updated\n")
    upload_csv(minio_client, f"{PREFIX}/part-2.csv", "3,carol\n")


def main() -> None:
    if len(sys.argv) != 2:
        raise ValueError("expected one command: cleanup, setup-initial, or setup-updated")

    minio_client = client()
    command = sys.argv[1]
    if command == "cleanup":
        cleanup(minio_client)
    elif command == "setup-initial":
        setup_initial(minio_client)
    elif command == "setup-updated":
        setup_updated(minio_client)
    else:
        raise ValueError(f"unknown command: {command}")


if __name__ == "__main__":
    main()
