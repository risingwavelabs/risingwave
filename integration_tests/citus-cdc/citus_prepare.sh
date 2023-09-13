#!/bin/bash

set -e

until test $(psql postgresql://myuser:123456@citus-master:5432/mydb -tc 'select count(*) from master_get_active_worker_nodes() limit 1;' | sed 's/[^0-9]*//g') -eq 2 ; do
  >&2 echo "Waiting for both citus workers registered at citus coordinator - sleeping"
  sleep 1
done

until test $(psql postgresql://myuser:123456@citus-master:5432/mydb -tc "select count(*) from information_schema.tables where table_name = 'orders'" | sed 's/[^0-9]*//g') -ge 1 ; do
  >&2 echo "Waiting for the table 'orders' to be created - sleeping"
  sleep 1
done

>&2 echo "Both workers are up, and the table 'orders' has been created - running citus prepare sql statements"

psql postgresql://myuser:123456@citus-master:5432/mydb < citus_prepare.sql
