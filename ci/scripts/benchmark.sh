#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

#while getopts 's:' opt; do
#    case ${opt} in
#        s )
#            SKU=$OPTARG
#            ;;
#        \? )
#            echo "Invalid Option: -$OPTARG" 1>&2
#            exit 1
#            ;;
#        : )
#            echo "Invalid option: $OPTARG requires an argument" 1>&2
#            ;;
#    esac
#done
#shift $((OPTIND -1))

function polling() {
    set +e
    try_times=30
    while :; do
        if [ $try_times == 0 ]; then
            echo "❌ ERROR: Polling Timeout"
            exit 1
        fi
        psql "$@" -c '\q'
        if [ $? == 0 ]; then
            echo "✅ Instance Ready"
            break
        fi
        sleep 10
        try_times=$((try_times - 1))
    done
    set -euo pipefail
}

function cleanup {
  echo "--- Delete tenant"
  rwc tenant delete -name ${TENANT_NAME}
}

trap cleanup EXIT

if [[ -z "${RISINGWAVE_IMAGE_TAG+x}" ]]; then
  IMAGE_TAG="latest"
else
  IMAGE_TAG="${RISINGWAVE_IMAGE_TAG}"
fi

if [ -z "${BENCH_SKU+x}" ] || [ "${BENCH_SKU}" == "MultiNodeBench" ]; then
  SKU="multinode"
  BENCH_SKU="MultiNodeBench"
elif [ "${BENCH_SKU}" == "SingleNodeBench" ]; then
  SKU="singlenode"
else
  exit 1
fi

date=$(date '+%Y%m%d-%H%M%S')
TENANT_NAME="${SKU}-${date}"

echo "--- Echo Info"
echo "BENCH-SKU: ${BENCH_SKU}"
echo "Tenant-Name: ${TENANT_NAME}"
echo "Host-Ip: ${HOST_IP}"
echo "IMAGE-TAG: ${IMAGE_TAG}"

echo "--- Download Necessary Tools"
apt-get -y install golang-go librdkafka-dev python3-pip
curl -L -o ./rwc https://rwc-cli-internal-release.s3.ap-southeast-1.amazonaws.com/bench-tmp/rwc && chmod 755 ./rwc && mv rwc /usr/local/bin

echo "--- RWC Config and Login"
rwc config -region bench-ap-southeast-1
rwc config ls
rwc login -account benchmark -password "$BENCH_TOKEN"

echo "--- RWC Create a Risingwave Instance"
rwc tenant create -name ${TENANT_NAME} -sku ${BENCH_SKU} -imagetag ${IMAGE_TAG}

sleep 2

echo "--- Wait Risingwave Instance Ready "
endpoint=$(rwc tenant endpoint -name ${TENANT_NAME})
polling ${endpoint}

echo "--- Generate Tpch-Bench Args"
mkdir ~/risingwave-deploy
echo "--frontend-url ${endpoint}" > ~/risingwave-deploy/tpch-bench-args-frontend
echo "--kafka-addr ${HOST_IP}:29092" >  ~/risingwave-deploy/tpch-bench-args-kafka
cat ~/risingwave-deploy/tpch-bench-args-frontend
cat ~/risingwave-deploy/tpch-bench-args-kafka

echo "--- Clone Tpch-Bench Repo"
git clone https://"$GITHUB_TOKEN"@github.com/risingwavelabs/tpch-bench.git

echo "--- Run Tpch-Bench"
cd tpch-bench/
./scripts/build.sh
./scripts/launch_risedev_bench.sh

echo "--- sleep 10"
sleep 10

echo "--- Clone Risingwave-test Repo"
git clone https://"$GITHUB_TOKEN"@github.com/risingwavelabs/risingwave-test.git

echo "--- Send Matrices to S3 for Slack Bot"
cd risingwave-test/rwbot
pip3 install -r requirement.txt
cd commands/utilities/
python3 promql.py ${SKU}
