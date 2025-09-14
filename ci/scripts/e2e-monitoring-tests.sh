#!/usr/bin/env bash

set -euo pipefail

while getopts 'p:m:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        m )
            mode=$OPTARG
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

source ci/scripts/common.sh

download_and_prepare_rw "$profile" monitoring

# Test script for internal_get_channel_delta_stats table function with monitoring
# This script starts a RisingWave cluster with Prometheus monitoring and runs the SLT test

echo "Starting RisingWave cluster with monitoring..."

# Start the cluster with monitoring
./risedev d ci-3cn-1fe-with-monitoring

echo "Waiting for cluster to be ready..."
sleep 10

echo "Running internal_get_channel_delta_stats SLT test..."

# Run the SLT test
./risedev slt './e2e_test/table_function/internal_get_channel_delta_stats.slt'

echo "Test completed successfully!"

# Optional: Keep cluster running for debugging
# Uncomment the following lines if you want to keep the cluster running
# echo "Cluster is still running. Use './risedev k' to stop it."
# echo "Prometheus is available at: http://127.0.0.1:9500"
# echo "Grafana is available at: http://127.0.0.1:3001"
# echo "Press Ctrl+C to stop the cluster and exit."
#
# # Wait for user interrupt
# trap './risedev k' INT
# while true; do
#   sleep 1
# done

# Stop the cluster
echo "Stopping cluster..."
./risedev k
