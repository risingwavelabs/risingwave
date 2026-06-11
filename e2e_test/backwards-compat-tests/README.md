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
2. Nexmark (on rw table not nexmark source)
3. TPC-H
4. Kafka Source