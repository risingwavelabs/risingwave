#!/bin/bash

set -euo pipefail

SCRIPT_PATH="./await-tree-analyzer"

if ps -ef | grep 'compute-node-5688' | grep -v 'grep' > /dev/null
then
    echo "RW ready"
else
    ./risedev k; ./risedev clean-data; ./risedev d
    sleep 20
    echo "RW built"
fi

psql -v ON_ERROR_STOP=1 -h localhost -p 4566 -d dev -U root -f ${SCRIPT_PATH}/clean.sql

sleep 10

psql -v ON_ERROR_STOP=1 -h localhost -p 4566 -d dev -U root -f ${SCRIPT_PATH}/prepare.sql
