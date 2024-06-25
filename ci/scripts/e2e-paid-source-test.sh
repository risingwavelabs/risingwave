#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# prepare environment
export CONNECTOR_LIBS_PATH="./connector-node/libs"

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

echo "--- Download connector node package"
buildkite-agent artifact download risingwave-connector.tar.gz ./
mkdir ./connector-node
tar xf ./risingwave-connector.tar.gz -C ./connector-node

echo "--- Install sql server client"
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list
apt-get update -y
ACCEPT_EULA=Y DEBIAN_FRONTEND=noninteractive apt-get install -y mssql-tools unixodbc-dev
export PATH="/opt/mssql-tools/bin/:$PATH"
sleep 2

echo "--- Create an independent db in sql server"
export MSSQL_HOST=sqlserver-server MSSQL_PORT=1433 MSSQL_USER=SA MSSQL_PASSWORD=SomeTestOnly@SA
sqlcmd -S $MSSQL_HOST -U $MSSQL_USER -P $MSSQL_PASSWORD -d master -Q 'create database mydb;' -b

echo "--- Import data to sql server"
export MSSQL_HOST=sqlserver-server MSSQL_PORT=1433 MSSQL_USER=SA MSSQL_PASSWORD=SomeTestOnly@SA MSSQL_DATABASE=mydb
sqlcmd -S $MSSQL_HOST -U $MSSQL_USER -P $MSSQL_PASSWORD -d $MSSQL_DATABASE -i ./e2e_test/source/cdc_paid/sql_server_cdc.sql -b

echo "--- Starting risingwave cluster"
RUST_LOG="debug,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-1cn-1fe-with-recovery

echo "--- Sql Server cdc validate test"
risedev slt './e2e_test/source/cdc_paid/cdc.validate.sql_server.slt'

echo "--- Cdc share source test"
# cdc share stream test cases
export MSSQL_HOST=sqlserver-server MSSQL_PORT=1433 MSSQL_USER=SA MSSQL_PASSWORD=SomeTestOnly@SA MSSQL_DATABASE=mydb
risedev slt './e2e_test/source/cdc_paid/cdc.share_stream.slt'

echo "--- Sql Server load and check"
risedev slt './e2e_test/source/cdc_paid/cdc.load.slt'
# wait for cdc loading
sleep 10
risedev slt './e2e_test/source/cdc_paid/cdc.check.slt'

# kill cluster
risedev kill
echo "> cluster killed "


# insert new rows
export MSSQL_HOST=sqlserver-server MSSQL_PORT=1433 MSSQL_USER=SA MSSQL_PASSWORD=SomeTestOnly@SA MSSQL_DATABASE=mydb
sqlcmd -S $MSSQL_HOST -U $MSSQL_USER -P $MSSQL_PASSWORD -d $MSSQL_DATABASE -i ./e2e_test/source/cdc_paid/sql_server_cdc_insert.sql -b
echo "> inserted new rows into sql server"

# start cluster w/o clean-data
unset RISINGWAVE_CI
export RUST_LOG="events::stream::message::chunk=trace,risingwave_stream=debug,risingwave_batch=info,risingwave_storage=info" \

risedev dev ci-1cn-1fe-with-recovery
echo "> wait for cluster recovery finish"
sleep 20
echo "> check mviews after cluster recovery"
# check results
risedev slt './e2e_test/source/cdc_paid/cdc.check_new_rows.slt'

# drop relations
risedev slt './e2e_test/source/cdc_paid/cdc_share_stream_drop.slt'

echo "--- Kill cluster"
risedev ci-kill
export RISINGWAVE_CI=true
