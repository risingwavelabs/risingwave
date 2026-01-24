#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

download_and_prepare_rw "$profile" source

echo "--- starting risingwave cluster"
cargo make ci-start ci-sink-test
sleep 1

# install the mongo shell
wget --no-verbose http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2_amd64.deb
wget --no-verbose https://repo.mongodb.org/apt/ubuntu/dists/focal/mongodb-org/4.4/multiverse/binary-amd64/mongodb-org-shell_4.4.28_amd64.deb
dpkg -i libssl1.1_1.1.1f-1ubuntu2_amd64.deb
dpkg -i mongodb-org-shell_4.4.28_amd64.deb

echo '> ping mongodb'
echo 'db.runCommand({ping: 1})' | mongo mongodb://mongodb:27017
echo '> rs config'
echo 'rs.conf()' | mongo mongodb://mongodb:27017
echo '> run mongodb sink test..'

sqllogictest -p 4566 -d dev './e2e_test/sink/mongodb_sink.slt'
sleep 1

append_only_result=$(mongo mongodb://mongodb:27017 --eval 'db.getSiblingDB("demo").t1.countDocuments({})' | tail -n 1)
if [ "$append_only_result" != "1" ]; then
    echo "The append-only output is not as expected."
    exit 1
fi

upsert_and_dynamic_coll_result1=$(mongo mongodb://mongodb:27017 --eval 'db.getSiblingDB("demo").t2.countDocuments({})' | tail -n 1)
if [ "$upsert_and_dynamic_coll_result1" != "1" ]; then
    echo "The upsert output is not as expected."
    exit 1
fi

upsert_and_dynamic_coll_result2=$(mongo mongodb://mongodb:27017 --eval 'db.getSiblingDB("shard_2024_01").tenant_1.countDocuments({})' | tail -n 1)
if [ "$upsert_and_dynamic_coll_result2" != "1" ]; then
    echo "The upsert output is not as expected."
    exit 1
fi

compound_pk_result=$(mongo mongodb://mongodb:27017 --eval 'db.getSiblingDB("demo").t3.countDocuments({})' | tail -n 1)
if [ "$compound_pk_result" != "1" ]; then
    echo "The upsert output is not as expected."
    exit 1
fi

update_id1_result=$(mongo mongodb://mongodb:27017 --eval 'db.getSiblingDB("demo").t4.find({ "_id": 1 }).toArray()' | tail -n 1)
if [ "$update_id1_result" != "[ { \"_id\" : 1, \"v1\" : 1, \"v2\" : 10, \"v3\" : 1, \"v4\" : 1 } ]" ]; then
    echo "The upsert output is not as expected."
    echo $update_id1_result
    exit 1
fi

update_id2_result=$(mongo mongodb://mongodb:27017 --eval 'db.getSiblingDB("demo").t4.find({ "_id": 2 }).toArray()' | tail -n 1)
if [ "$update_id2_result" != "[ { \"_id\" : 2, \"v1\" : 2, \"v2\" : 200, \"v3\" : 2, \"v4\" : 2 } ]" ]; then
    echo "The upsert output is not as expected."
    echo $update_id2_result
    exit 1
fi

echo "Mongodb sink check passed"

echo "--- Kill cluster"
risedev ci-kill