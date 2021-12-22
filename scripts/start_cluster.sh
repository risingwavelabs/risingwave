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
        while ! echo -n > /dev/tcp/localhost/"$1"; do
            sleep 0.5
            failed_times=$((failed_times+1))
            if [ $failed_times -gt 60 ]; then
                echo "ERROR: failed to start server $1 [timeout=30s]"
                exit 1
            fi
        done
    } 2>/dev/null
}

start_compute_node() {
    log_dir="../log/compute-node-$1.out"
    echo "Starting compute-node 0.0.0.0:$1 ... logging to $log_dir"
    nohup ./target/debug/compute-node --log4rs-config config/log4rs.yaml --host "0.0.0.0:$1" > "$log_dir" &
    wait_server "$1"
}

start_frontend() {
    log_dir="../log/frontend.out"
    pgserver_build_dir="${SCRIPT_PATH}/../java/pgserver/build/"
    conf_file=$1
    echo "Starting frontend with config file $conf_file ... logging to $log_dir"
    run_cmd="nohup java -cp ${pgserver_build_dir}libs/risingwave-fe-runnable.jar \
           -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:5005 \
           -Dlogback.configurationFile=${pgserver_build_dir}resources/main/logback.xml \
           com.risingwave.pgserver.FrontendServer -c ${conf_file} > ${log_dir} &"
    eval "${run_cmd}"
    wait_server 4567
}

start_meta_node() {
    log_dir="../log/meta.out"
    echo "Starting meta service ... logging to $log_dir"
    nohup ./target/debug/meta-node --log4rs-config config/log4rs.yaml > "$log_dir" &
    wait_server 5690
}

start_n_nodes_cluster() {
    mkdir -p ./log/

    echo ""
    echo "Checking if there's any zombie compute-node"
    list_nodes_in_cluster
    echo ""

    cd rust/ || exit 1
    addresses=()
    for ((i=0; i<$1; i++)); do
        port=$((5687+i))
        start_compute_node "$port"
        addresses+=("127.0.0.1:$port")
    done

    echo ""
    echo "All compute-nodes:"
    list_nodes_in_cluster
    echo ""

    start_meta_node
    echo ""
    echo "meta-node:"
    list_meta_node_in_cluster
    echo ""

    FRONTEND_CFG_FILE=pgserver/src/main/resources/server.properties
    FRONTEND_CFG_FILE_BASENAME=$(basename $FRONTEND_CFG_FILE)

    cd ../java || exit 1

    TEMP_DIR=$(mktemp -d "risingwave.XXXXXX" -p /tmp)

    cp $FRONTEND_CFG_FILE "$TEMP_DIR"

    CONF_FILE="$TEMP_DIR/$FRONTEND_CFG_FILE_BASENAME"

    echo "Using temp conf $CONF_FILE"

    addr_str=$(IFS=, ; echo "${addresses[*]}")
    sed -i -e "s/.*computenodes.*/risingwave.leader.computenodes=$addr_str/" "$CONF_FILE"

    echo "Rewritten $CONF_FILE:"
    echo ""
    cat "$CONF_FILE"
    echo ""

    start_frontend "$CONF_FILE"

    rm -rf "$TEMP_DIR"
}

list_nodes_in_cluster() {
    pgrep -fl compute-node || true
}

list_meta_node_in_cluster() {
    pgrep -fl meta-node || true
}

if [ $# -ne 1 ]; then
    echo "ERROR: Must specify the number of compute-nodes"
    echo "Help: "
    echo "  ./start_cluster <NUMBER_COMPUTE_NODES>" # show help
    exit 1
fi

./scripts/kill_cluster.sh

start_n_nodes_cluster "$1"
