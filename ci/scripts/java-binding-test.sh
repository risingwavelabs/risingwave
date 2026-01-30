#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

dump_java_binding_diagnostics() {
    # Don't let the diagnose routine fail the script further.
    set +e

    echo "+++ java-binding diagnostics: environment"
    echo "pwd: $(pwd)"
    echo "uname: $(uname -a 2>/dev/null)"
    if command -v cat >/dev/null 2>&1 && [[ -f /etc/os-release ]]; then
        echo "--- /etc/os-release"
        cat /etc/os-release
    fi
    echo "--- java -version"
    java -version 2>&1 || true
    echo "--- ldd --version"
    ldd --version 2>&1 | head -n 5 || true

    # Try to find the extracted JNI library path from the failing run. JarJniLoader extracts to /tmp.
    so_path="$(ls -1 /tmp/librisingwave_java_binding*.so 2>/dev/null | head -n 1)"
    if [[ -n "${so_path}" && -f "${so_path}" ]]; then
        echo "+++ java-binding diagnostics: static TLS scan"
        ci/scripts/diagnose-static-tls.sh "${so_path}" || true
    else
        echo "+++ java-binding diagnostics: no /tmp/librisingwave_java_binding*.so found, trying packaged JNI"
        RISINGWAVE_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
        if [[ -n "${RISINGWAVE_ROOT}" && -d "${RISINGWAVE_ROOT}/java" ]]; then
            packaged_so="$(
                find "${RISINGWAVE_ROOT}/java/java-binding-integration-test/target/classes" \
                    -type f -name "librisingwave_java_binding*.so" 2>/dev/null | head -n 1
            )"
            if [[ -n "${packaged_so}" && -f "${packaged_so}" ]]; then
                ci/scripts/diagnose-static-tls.sh "${packaged_so}" || true
            else
                echo "no packaged librisingwave_java_binding*.so found under target/classes"
            fi
        fi
    fi

    # Reproduce the loader failure with LD_DEBUG=libs (doesn't require the cluster; the JNI load fails first).
    RISINGWAVE_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
    if [[ -n "${RISINGWAVE_ROOT}" && -d "${RISINGWAVE_ROOT}/java" ]]; then
        echo "+++ java-binding diagnostics: reproduce with LD_DEBUG=libs"
        (
            cd "${RISINGWAVE_ROOT}/java" || exit 0
            rm -f /tmp/java-binding-ld-debug.log >/dev/null 2>&1
            LD_DEBUG=libs \
                java -cp "./java-binding-integration-test/target/dependency/*:./java-binding-integration-test/target/classes" \
                com.risingwave.java.binding.HummockReadDemo \
                2> /tmp/java-binding-ld-debug.log
            rc=$?
            echo "LD_DEBUG repro exit code: ${rc}"
            echo "--- tail /tmp/java-binding-ld-debug.log"
            tail -n 200 /tmp/java-binding-ld-debug.log || true
            exit 0
        ) || true
    fi
}

on_err() {
    status=$?
    # Avoid recursion if the diagnose routine runs any failing commands.
    trap - ERR
    dump_java_binding_diagnostics
    exit "${status}"
}
trap on_err ERR

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

echo "--- download java binding integration test"
buildkite-agent artifact download java-binding-integration-test.tar.zst ./
tar xf java-binding-integration-test.tar.zst

echo "--- starting risingwave cluster"
risedev ci-start java-binding-demo

echo "--- ingest data and run java binding"
risedev ingest-data-and-run-java-binding

echo "--- Kill cluster"
risedev ci-kill

echo "--- run stream chunk java binding"
RISINGWAVE_ROOT=$(git rev-parse --show-toplevel)

cd "${RISINGWAVE_ROOT}"/java

("${RISINGWAVE_ROOT}"/bin/data-chunk-payload-generator) | \
    java -cp "./java-binding-integration-test/target/dependency/*:./java-binding-integration-test/target/classes" \
    com.risingwave.java.binding.StreamChunkDemo
