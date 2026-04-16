# Kafka Poison Message Check

## STEP_ID
- P1-poison-message

## Expected demo behavior

- The system surfaces source-error telemetry/logging for the poison message.
- The bad payload is skipped.
- Subsequent valid messages continue to be processed.

## Rehearsal result

- [x] Poison message injected at offset `8` and follow-up valid record injected at offset `9`.
- [x] RisingWave log shows parser/source error. Evidence: `artifacts/poison-kafka/risingwave.log`.
- [x] `user_source_error_cnt` increased from `0.0` to `1.0`. Evidence: `artifacts/poison-kafka/metric_before.prom`, `artifacts/poison-kafka/metric_after.prom`.
- [x] The follow-up valid record was produced after the poison offset. Evidence: `artifacts/poison-kafka/produce-result.json`.
- [x] The follow-up valid record materialized downstream, showing continued processing after the poison payload. Evidence: `artifacts/poison-kafka/latest_state_after.json`.
- [x] Observed behavior matches the expected demo behavior statement.
