#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

RUN_ALL=0

while getopts 'a:' opt; do
    case ${opt} in
        a )
            RUN_ALL=1
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

REPO_ROOT=${PWD}

echo "+++ Run python UDF SDK unit tests"
cd ${REPO_ROOT}/src/udf/python
python3 -m pytest
cd ${REPO_ROOT}

# NOTE(kwannoel): The slow test section is updated manually, via feedback from CI's slow test output.
SLOW_TEST_PATTERN="\
test(executor::join::chunked_data::tests::test_try_from_zero_chunk_size_should_fail) or \
test(executor::join::chunked_data::tests::test_zero_chunk_size_should_fail) or \
test(error::tests::test_display_internal_error) or \
test(util::panic::tests::test_sync_not_work) or \
test(delete_range_runner::tests::test_small_data) or \
test(parser::debezium::simd_json_parser::tests::test2_mysql::test2_debezium_json_parser_overflow_f64) or \
test(backup_restore::restore::tests::test_sanity_check_monotonicity_requirement) or \
test(backup_restore::restore::tests::test_sanity_check_superset_requirement) or \
test(stream::stream_manager::tests::test_failpoints_drop_mv_recovery) or \
test(dml_manager::tests::test_bad_schema) or \
test(hummock::conflict_detector::test::test_write_below_epoch_watermark) or \
test(hummock::conflict_detector::test::test_write_conflict_in_multi_batch) or \
test(hummock::conflict_detector::test::test_write_conflict_in_one_batch) or \
test(hummock::conflict_detector::test::test_write_to_archived_epoch) or \
test(hummock::shared_buffer::shared_buffer_batch::tests::test_invalid_table_id) or \
test(row_serde::value_serde::tests::test_row_hard3)"
# NOTE(kwannoel): Limitation of https://nexte.st/book/filter-expressions.html.
# not($SLOW_TEST_PATTERN) is not supported, so we have to use all() - ($SLOW_TEST_PATTERN).
FAST_TEST_PATTERN="all() - (${SLOW_TEST_PATTERN})"

echo "+++ Run fast unit tests"
NEXTEST_PROFILE=ci cargo llvm-cov nextest --lcov --output-path lcov.info --features failpoints,sync_point --workspace --exclude risingwave_simulation \
-E "$FAST_TEST_PATTERN"

if [[ $RUN_ALL -eq "1" ]]; then
  echo "+++ Run slow unit tests"
  NEXTEST_PROFILE=ci cargo llvm-cov nextest --lcov --output-path lcov.info --features failpoints,sync_point --workspace --exclude risingwave_simulation \
  -E "$SLOW_TEST_PATTERN"
else
  echo "--- Skip slow unit tests"
fi

echo "--- Codecov upload coverage reports"
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
./codecov -t "$CODECOV_TOKEN" -s . -F rust
