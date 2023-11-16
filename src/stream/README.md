# RisingWave Stream Engine

Read [Stream Engine][stream-engine] for more details about architecture.

[stream-engine]: https://github.com/risingwavelabs/risingwave/blob/main/docs/streaming-overview.md

## Writing executor tests

It's recommended to use [expect_test](https://github.com/rust-analyzer/expect-test) to write new tests, which can automatically update the expected output for you. See `check_until_pending` 's doc and its usage for more details.

It's recommended to write new tests as *integration tests* (i.e. in `tests/` directory) instead of *unit tests* (i.e. in `src/` directory). See [#9878](https://github.com/risingwavelabs/risingwave/issues/9878) for more details.
