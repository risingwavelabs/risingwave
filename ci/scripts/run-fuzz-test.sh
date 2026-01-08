#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

cleanup_reducer_rw_cmd_script() {
    if [[ -n "${REDUCER_RW_CMD_SCRIPT:-}" && -f "${REDUCER_RW_CMD_SCRIPT:-}" ]]; then
        rm -f "${REDUCER_RW_CMD_SCRIPT}"
    fi
}

export LOGDIR=.risingwave/log
export RUST_LOG=info

if [[ $RUN_SQLSMITH_FRONTEND -eq "1" ]]; then
    echo "--- Run sqlsmith frontend tests"
    NEXTEST_PROFILE=ci cargo nextest run --package risingwave_sqlsmith --features "enable_sqlsmith_unit_test"
fi

extract_error_sql() {
  cat "$1" \
   | grep -E "(\[EXECUTING|\[TEST)" \
   | sed 's/.*\[EXECUTING .*\]: //' \
   | sed 's/.*\[TEST.*\]: //' \
   | sed 's/$/;/' > $LOGDIR/error.sql.log
}

if [[ "$RUN_SQLSMITH" -eq "1" ]]; then
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

    download_and_prepare_rw "$profile" common

    echo "--- Download sqlsmith e2e bin"
    download-and-decompress-artifact sqlsmith-"$profile" target/debug/
    mv target/debug/sqlsmith-"$profile" target/debug/sqlsmith
    chmod +x ./target/debug/sqlsmith

    echo "--- Download sqlsmith reducer bin"
    download-and-decompress-artifact sqlsmith-reducer-"$profile" target/debug/
    mv target/debug/sqlsmith-reducer-"$profile" target/debug/sqlsmith-reducer
    chmod +x ./target/debug/sqlsmith-reducer

    echo "--- e2e, ci-3cn-1fe, build"
    risedev ci-start ci-3cn-1fe

    echo "--- e2e, ci-3cn-1fe, run fuzzing"
    ./target/debug/sqlsmith test \
      --count "$SQLSMITH_COUNT" \
      --testdata ./src/tests/sqlsmith/tests/testdata > $LOGDIR/fuzzing.log 2>&1 && rm $LOGDIR/*

    echo "LOGDIR ${LOGDIR}"
    ls ${LOGDIR}

    if [[ -e $LOGDIR/fuzzing.log ]]; then
        echo "Fuzzing failed, please look at the artifacts fuzzing.log and error.sql.log for more details"
        extract_error_sql $LOGDIR/fuzzing.log
        echo "--- Running reducer on failing queries"
        REDUCER_RW_CMD_SCRIPT=$(mktemp "${TMPDIR:-/tmp}/rw-reducer-run-cmd.XXXXXX")
        trap cleanup_reducer_rw_cmd_script EXIT
        cat <<'EOF' > "${REDUCER_RW_CMD_SCRIPT}"
#!/usr/bin/env bash
set -euo pipefail

backup_existing_logs() {
    local dest="$1"
    if [[ ! -d "${LOGDIR}" ]]; then
        return
    fi
    mkdir -p "${dest}"
    while IFS= read -r -d '' entry; do
        mv "${entry}" "${dest}/"
    done < <(find "${LOGDIR}" -mindepth 1 -maxdepth 1 -print0)
}

restore_preserved_logs() {
    local src="$1"
    local prefix="$2"
    if [[ ! -d "${src}" ]]; then
        return
    fi
    mkdir -p "${LOGDIR}"
    while IFS= read -r -d '' entry; do
        local name
        name=$(basename "${entry}")
        mv "${entry}" "${LOGDIR}/${prefix}-${name}"
    done < <(find "${src}" -mindepth 1 -maxdepth 1 -print0)
}

./risedev k
preserved_dir=$(mktemp -d "${TMPDIR:-/tmp}/rw-preserved.XXXXXX")
trap 'rm -rf "${preserved_dir}"' EXIT

backup_existing_logs "${preserved_dir}"
./risedev clean-data
restore_preserved_logs "${preserved_dir}" "reducer-prev-$(date +%s)-${RANDOM}"
./risedev ci-start ci-3cn-1fe
EOF
        chmod +x "${REDUCER_RW_CMD_SCRIPT}"
        ./target/debug/sqlsmith-reducer \
            --input-file $LOGDIR/error.sql.log \
            --output-file $LOGDIR/error.sql.shrunk.log \
            --run-rw-cmd "${REDUCER_RW_CMD_SCRIPT}" \
            |& tee "$LOGDIR/reducer.log"
        echo "--- Reducer finished (log: $LOGDIR/reducer.log)"
        echo "Reduced queries saved at $LOGDIR/error.sql.shrunk.log"
        exit 1
    fi

    # Sqlsmith does not write to stdout, so we need this to ensure buildkite
    # shows the right timing.
    echo "Fuzzing complete"

    # Using `kill` instead of `ci-kill` avoids storing excess logs.
    # If there's errors, the failing query will be printed to stderr.
    # Use that to reproduce logs on local machine.
    echo "--- Kill cluster"
    risedev kill
fi
