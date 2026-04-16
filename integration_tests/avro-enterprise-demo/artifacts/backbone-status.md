# Backbone Status

## STEP_IDS
- B1-source-create
- B2-mv-create
- B3-updates-tombstones
- B4-join-subscribe
- B5-json-output

- [x] Kafka Avro source auto-decodes schema registry fields
- [x] Latest-state materialized view preserves only active non-deleted keys
- [x] Tombstone handling removes deleted keys from latest-state view
- [x] Streaming join and tumble window materialized view produces joined rows
- [x] UDF output and complex-field projections are visible in the latest-state view
- [x] Complex Avro fields remain queryable after decoding
- [x] Native SQL SUBSCRIBE receives incremental rows
- [x] Processed output is visible in downstream JSON format and reflects latest state plus deletes

## Scope boundaries

- Scope boundary: remove optional field is not part of this demo scope.
- Scope boundary: AD group-to-role mapping is not part of this demo scope.
- Scope boundary: real higher-environment promotion is not part of this demo scope.
- Scope boundary: CSV/XML/fixed-width outputs are not part of this demo scope; fixed-width remains deferred with a remark only.
