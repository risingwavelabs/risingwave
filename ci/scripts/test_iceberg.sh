set -ex

cargo make sarchive-it-test
#NEXTEST_PROFILE=ci-sim ./risedev sarchive-it-test --cargo-profile ci-sim
mv simulation-it-test.tar.zst .risingwave/tmp
cd .risingwave/tmp

rm -rf target && tar xf simulation-it-test.tar.zst && mkdir target/sim && mv target/debug target/sim/ci-sim


RUST_LOG=debug,risingwave_simulation=debug,integration_tests=debug ENABLE_TELEMETRY=false NEXTEST_PROFILE=ci-sim MADSIM_TEST_SEED=1 cargo nextest run \
 --no-fail-fast \
 --cargo-metadata target/nextest/cargo-metadata.json \
 --no-capture \
 --binaries-metadata target/nextest/binaries-metadata.json sink::exactly_once_iceberg::test_exactly_once_sink_basic