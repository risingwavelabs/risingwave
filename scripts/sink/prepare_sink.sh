#!/bin/bash

# Exits as soon as any line fails.
set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/.." || exit 1

wait_server() {
    # https://stackoverflow.com/a/44484835/5242660
    # Licensed by https://creativecommons.org/licenses/by-sa/3.0/
    {
        failed_times=0
        while ! echo -n >/dev/tcp/localhost/"$1"; do
            sleep 0.5
            failed_times=$((failed_times + 1))
            if [ $failed_times -gt 30 ]; then
                echo "ERROR: failed to start server $1 [timeout=15s]"
                exit 1
            fi
        done
    } 2>/dev/null
}

if command -v docker-compose; then
    echo "Starting single node mysql"
    docker-compose -f "$SCRIPT_PATH"/docker-compose.yml up -d

    cd "$SCRIPT_PATH" 
    mkdir -p ../logs 
    docker-compose logs -f > ../logs/docker-compose.log & 
    cd -
else
    echo "This script requires docker-compose, please follow docker install instructions (https://docs.docker.com/compose/install/)."
    exit 1
fi

echo "Waiting for mysql sink"
wait_server 23306

echo "Waiting for cluster"
sleep 10

echo "end of prepare sink so far"