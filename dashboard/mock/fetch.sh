#!/usr/bin/env bash

set -e

curl http://localhost:5691/api/actors > actors.json
curl http://localhost:5691/api/clusters/0 > cluster_0.json
curl http://localhost:5691/api/clusters/1 > cluster_1.json
curl http://localhost:5691/api/clusters/2 > cluster_2.json
curl http://localhost:5691/api/fragments > fragments.json
curl http://localhost:5691/api/fragments2 > fragments2.json
curl http://localhost:5691/api/materialized_views > materialized_views.json
curl http://localhost:5691/api/tables > tables.json
curl http://localhost:5691/api/indexes > indexes.json
curl http://localhost:5691/api/internal_tables > internal_tables.json
curl http://localhost:5691/api/sinks > sinks.json
curl http://localhost:5691/api/sources > sources.json
curl http://localhost:5691/api/metrics/cluster > metrics_cluster.json
curl http://localhost:5691/api/monitor/await_tree/1 > await_tree_1.json
