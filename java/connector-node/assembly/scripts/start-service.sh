#!/bin/bash
# This script will be packaged to the release tar file
usage() { echo "Usage: $0 [-p <port>]" 1>&2; exit 0; }

while getopts ":h:p:" o; do
    case "${o}" in
        p)
            port=${OPTARG}
            ;;
        h | *)
            usage
            exit 0
            ;;
    esac
done
shift $((OPTIND-1))


DIR="$( cd "$( dirname "$0" )" && pwd )"
MAIN='com.risingwave.connector.ConnectorService'
PORT=50051

if [ -z "${port}" ]; then
    echo "Use default port: ${PORT}"
    port=$PORT
fi

java -ea -classpath "${DIR}/libs/*" $MAIN --port ${port}
