#!/usr/bin/env bash
# TODO(kwannoel): Make this an actual test script.
./risedev clean-data
./risedev d ci-3cn-2fe-3meta-with-recovery
psql -h localhost -p 4566 -d dev -U root -c "
CREATE TABLE t(v1 int);
CREATE MATERIALIZED VIEW m2(v1 int);
"
./risedev k
