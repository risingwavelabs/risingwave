#!/usr/bin/env bash
set -euo pipefail

execute_queries() {
  QUERIES=$1
  # Delimit by newlines ONLY using bash's IFS (internal field separator)
  IFS_RESET=$IFS
  IFS=$'\n'

  for QUERY in $QUERIES
  do
    echo "[EXECUTING]: $QUERY"
    echo -n "[RESULT]: "; psql -h localhost -p 4566 -d dev -U root -c "$QUERY"
    echo ""
  done

  IFS=$IFS_RESET
}

# TODO(kwannoel): Make this an actual test script.
echo "--- Clean data"
./risedev clean-data >/dev/null 2>&1
echo "--- Start cluster"
./risedev d ci-3cn-2fe-3meta-with-recovery
QUERIES="CREATE TABLE t(v1 int primary key);
INSERT INTO t values (1), (2), (3), (4);
FLUSH;
CREATE MATERIALIZED VIEW m as select * from t;"

execute_queries "$QUERIES"

echo "--- KILL ALL NODES EXCEPT META"

echo "--- WAIT FOR RECOVERY"

echo "--- KILL"
./risedev k >/dev/null 2>&1
