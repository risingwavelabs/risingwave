#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

REPO_ROOT=${PWD}

echo "+++ Run python UDF SDK unit tests"
cd ${REPO_ROOT}/src/udf/python
python3 -m pytest
cd ${REPO_ROOT}

echo "+++ Run unit tests with coverage"
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo llvm-cov nextest --lcov --output-path lcov.info --features failpoints,sync_point --workspace --exclude risingwave_simulation

NEXTEST_PROFILE=ci cargo llvm-cov nextest --lcov --output-path lcov.info --features failpoints,sync_point --workspace --exclude risingwave_simulation \
-E "not test(executor::join::chunked_data::tests::test_try_from_zero_chunk_size_should_fail) & \
not test(executor::join::chunked_data::tests::test_zero_chunk_size_should_fail) & \
not test(error::tests::test_display_internal_error) & \
not test(util::panic::tests::test_sync_not_work) & \
not test(delete_range_runner::tests::test_small_data) & \
not test(parser::debezium::simd_json_parser::tests::test2_mysql::test2_debezium_json_parser_overflow_f64) & \
not test(backup_restore::restore::tests::test_sanity_check_monotonicity_requirement) & \
not test(backup_restore::restore::tests::test_sanity_check_superset_requirement) & \
not test(stream::stream_manager::tests::test_failpoints_drop_mv_recovery) & \
not test(dml_manager::tests::test_bad_schema) & \
not test(hummock::conflict_detector::test::test_write_below_epoch_watermark) & \
not test(hummock::conflict_detector::test::test_write_conflict_in_multi_batch) & \
not test(hummock::conflict_detector::test::test_write_conflict_in_one_batch) & \
not test(hummock::conflict_detector::test::test_write_to_archived_epoch) & \
not test(hummock::shared_buffer::shared_buffer_batch::tests::test_invalid_table_id) & \
not test(row_serde::value_serde::tests::test_row_hard3)"

echo "--- Codecov upload coverage reports"
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
./codecov -t "$CODECOV_TOKEN" -s . -F rust
