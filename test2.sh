#!/usr/bin/env bash

set -euo pipefail

for i in $(seq 1 1000)
   do
     ./risedev psql -c "DELETE FROM tomb; FLUSH;"
     sleep 1
   done
