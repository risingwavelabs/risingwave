# Backwards Compatibility Tests

The backwards compatibility tests run in the following manner:
1. Prepare old-cluster artifacts
2. Configure the old-cluster.
3. Start the old-cluster.
4. Run DDL / DML / DQL.
5. Stop the old-cluster.
6. Prepare new-cluster artifacts.
7. Configure the new-cluster.
8. Start the new-cluster.
9. Verify results of step 4.

We currently cover the following:
1. Basic mv
2. Hash join with watermark / EOWC
3. Nexmark (on rw table not nexmark source)
4. EOWC over window numbering functions
5. TPC-H
6. Kafka Source
7. AsOf join
8. Hummock stale SST table ids after dropping one table from a mixed-table SST (old version >= 2.8.0)
