#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

VERSION=11

while getopts 'p:v:' opt; do
    case ${opt} in
        v ):
            echo "The java version is $OPTARG"
            VERSION=$OPTARG
            ;;
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

RISINGWAVE_ROOT=${PWD}

echo "--- install java"
sudo apt-get update -y
if [ "$VERSION" = "11" ]; then
  echo "The test imgae default java version is 11, no need to install"
else
  echo "The test imgae default java version is 11, need to install java 17"
  sudo apt install openjdk-17-jdk openjdk-17-jre -y
fi
java_version=$(java --version 2>&1)
echo "$java_version"

# echo "--- build connector node"
# cd ${RISINGWAVE_ROOT}/java
# mvn --batch-mode --update-snapshots clean package -DskipTests

echo "--- install postgresql server"
DEBIAN_FRONTEND=noninteractive TZ=America/New_York apt-get -y install tzdata
sudo apt install postgresql postgresql-contrib libpq-dev -y
sudo service postgresql start || sudo pg_ctlcluster 14 main start
# disable password encryption
sudo -u postgres psql -c "CREATE ROLE test LOGIN SUPERUSER PASSWORD 'connector';"
sudo -u postgres createdb test
sudo -u postgres psql -d test -c "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR (50) NOT NULL);"

echo "--- starting minio"
echo "setting up minio"
wget --no-verbose https://dl.minio.io/server/minio/release/linux-amd64/minio > /dev/null
chmod +x minio
sudo ./minio server /tmp/minio &
# wait for minio to start
sleep 3
wget --no-verbose https://dl.minio.io/client/mc/release/linux-amd64/mc > /dev/null
chmod +x mc
MC_PATH=${PWD}/mc
${MC_PATH} alias set minio http://127.0.0.1:9000 minioadmin minioadmin

echo "--- starting connector-node service"
mkdir -p "${RISINGWAVE_ROOT}"/java/connector-node/assembly/target/
cd "${RISINGWAVE_ROOT}"/java/connector-node/assembly/target/
# tar xvf risingwave-connector-1.0.0.tar.gz > /dev/null
buildkite-agent artifact download risingwave-connector.tar.gz ./
tar xvf risingwave-connector.tar.gz > /dev/null
sh ./start-service.sh &
sleep 3

# generate data
echo "--- starting generate streamchunk data"
cd "${RISINGWAVE_ROOT}"/java/connector-node/python-client
buildkite-agent artifact download java-binding-integration-test.tar.zst ./
tar xf java-binding-integration-test.tar.zst bin
./bin/data-chunk-payload-convert-generator data/sink_input.json > ./data/sink_input
./bin/data-chunk-payload-convert-generator data/upsert_sink_input.json > ./data/upsert_sink_input
./bin/data-chunk-payload-generator 30 > ./data/stream_chunk_data

echo "--- prepare integration tests"
cd "${RISINGWAVE_ROOT}"/java/connector-node
pip3 install --break-system-packages grpcio grpcio-tools psycopg2 psycopg2-binary pyspark==3.3 black
cd python-client && bash gen-stub.sh && bash format-python.sh --check
export PYTHONPATH=proto

echo "--- running streamchunk data format integration tests"
cd "${RISINGWAVE_ROOT}"/java/connector-node/python-client
if python3 integration_tests.py --stream_chunk_format_test --input_binary_file="./data/stream_chunk_data"; then
  echo "StreamChunk data format test passed"
else
  echo "StreamChunk data format test failed"
  exit 1
fi

sink_input_feature=("--input_binary_file=./data/sink_input")
upsert_sink_input_feature=("--input_binary_file=./data/upsert_sink_input")
type=("StreamChunk format")

${MC_PATH} mb minio/bucket
for ((i=0; i<${#type[@]}; i++)); do
    echo "--- running file ${type[i]} integration tests"
    cd "${RISINGWAVE_ROOT}"/java/connector-node/python-client
    if python3 integration_tests.py --file_sink "${sink_input_feature[i]}"; then
      echo "File sink ${type[i]} test passed"
    else
      echo "File sink ${type[i]} test failed"
      exit 1
    fi
done


${MC_PATH} rm -r -force minio/bucket
${MC_PATH} rb minio/bucket

echo "all deltalake tests passed"
