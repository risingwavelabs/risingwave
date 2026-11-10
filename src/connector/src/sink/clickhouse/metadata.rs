// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use thiserror_ext::AsReport;

use super::{
    ClickHouseClient, ClickHouseConfig, ClickHouseEngine, ClickhouseQueryEngine, SystemColumn,
};
use crate::sink::{Result, SinkError};

const QUERY_ENGINE: &str = "SELECT ?fields FROM system.tables WHERE database = ? AND name = ?";
const QUERY_COLUMN: &str =
    "SELECT ?fields FROM system.columns WHERE database = ? AND table = ? ORDER BY position";
const QUERY_CLUSTER_REPLICAS: &str = "SELECT _shard_num FROM clusterAllReplicas(?, system.one)";
const QUERY_CLUSTER_ENGINE: &str = "SELECT ?fields, _shard_num FROM clusterAllReplicas(?, system.tables) WHERE database = ? AND name = ?";
const QUERY_CLUSTER_COLUMN: &str = "SELECT ?fields, _shard_num FROM clusterAllReplicas(?, system.columns) WHERE database = ? AND table = ?";

/// Reads metadata for a table on the connected node.
pub(super) async fn query_table_metadata(
    client: &ClickHouseClient,
    database: &str,
    table: &str,
) -> Result<(Vec<SystemColumn>, ClickhouseQueryEngine)> {
    let engine = client
        .query(QUERY_ENGINE)
        .bind(database)
        .bind(table)
        .fetch_optional::<ClickhouseQueryEngine>()
        .await
        .map_err(|err| {
            SinkError::ClickHouse(format!(
                "Cannot read engine metadata for ClickHouse table {database:?}.{table:?}: {}",
                err.to_report_string()
            ))
        })?
        .ok_or_else(|| {
            SinkError::ClickHouse(format!(
                "ClickHouse table {database:?}.{table:?}: table not found on the connected node"
            ))
        })?;
    let columns = client
        .query(QUERY_COLUMN)
        .bind(database)
        .bind(table)
        .fetch_all::<SystemColumn>()
        .await
        .map_err(|err| {
            SinkError::ClickHouse(format!(
                "Cannot read column metadata for ClickHouse table {database:?}.{table:?}: {}",
                err.to_report_string()
            ))
        })?;
    if columns.is_empty() {
        return Err(SinkError::ClickHouse(format!(
            "ClickHouse table {database:?}.{table:?}: no column metadata found"
        )));
    }
    Ok((columns, engine))
}

/// Reads target metadata from all replicas, requiring reachability and consistency.
pub(super) async fn query_cluster_metadata(
    client: &ClickHouseClient,
    cluster: &str,
    database: &str,
    table: &str,
    config: &ClickHouseConfig,
) -> Result<(Vec<SystemColumn>, ClickHouseEngine)> {
    let client = client.clone().with_setting("skip_unavailable_shards", "0");
    let replicas = client
        .query(QUERY_CLUSTER_REPLICAS)
        .bind(cluster)
        .fetch_all::<u32>()
        .await
        .map_err(|err| {
            SinkError::ClickHouse(format!(
                "Cannot read replica list from cluster {cluster:?} for ClickHouse table {database:?}.{table:?}: {}",
                err.to_report_string()
            ))
        })?;
    let engines = client
        .query(QUERY_CLUSTER_ENGINE)
        .bind(cluster)
        .bind(database)
        .bind(table)
        .fetch_all::<(ClickhouseQueryEngine, u32)>()
        .await
        .map_err(|err| {
            SinkError::ClickHouse(format!(
                "Cannot read engine metadata from cluster {cluster:?} for ClickHouse table {database:?}.{table:?}: {}",
                err.to_report_string()
            ))
        })?;
    let columns = client
        .query(QUERY_CLUSTER_COLUMN)
        .bind(cluster)
        .bind(database)
        .bind(table)
        .fetch_all::<(SystemColumn, u32)>()
        .await
        .map_err(|err| {
            SinkError::ClickHouse(format!(
                "Cannot read column metadata from cluster {cluster:?} for ClickHouse table {database:?}.{table:?}: {}",
                err.to_report_string()
            ))
        })?;
    merge_replica_metadata(replicas, engines, columns, config).map_err(|err| {
        SinkError::ClickHouse(format!(
            "Cannot validate replica metadata from cluster {cluster:?} for ClickHouse table {database:?}.{table:?}: {}",
            err.to_report_string()
        ))
    })
}

/// Validates replica metadata, ignoring replication paths and replica names.
fn merge_replica_metadata(
    replicas: Vec<u32>,
    engines: Vec<(ClickhouseQueryEngine, u32)>,
    columns: Vec<(SystemColumn, u32)>,
    config: &ClickHouseConfig,
) -> Result<(Vec<SystemColumn>, ClickHouseEngine)> {
    let mut engines: BTreeMap<_, _> = engines
        .into_iter()
        .map(|(engine, replica)| (replica, engine))
        .collect();
    let mut replica_columns: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for (column, replica) in columns {
        replica_columns.entry(replica).or_default().push(column);
    }
    let mut metadata = None;
    for replica in replicas {
        let engine = engines.remove(&replica).ok_or_else(|| {
            SinkError::ClickHouse(format!(
                "Target table not found on cluster replica {replica}"
            ))
        })?;
        if engine.engine == "Distributed" {
            return Err(SinkError::ClickHouse(format!(
                "Nested Distributed tables are not supported (cluster replica {replica})"
            )));
        }
        let engine = ClickHouseEngine::from_query_engine(&engine, config)?;
        let mut columns = replica_columns.remove(&replica).ok_or_else(|| {
            SinkError::ClickHouse(format!(
                "No column metadata found on cluster replica {replica}"
            ))
        })?;
        columns.sort_by(|left, right| left.name.cmp(&right.name));
        if let Some((expected_columns, expected_engine)) = &metadata {
            if &engine != expected_engine || &columns != expected_columns {
                return Err(SinkError::ClickHouse(format!(
                    "Inconsistent target engine or column metadata on cluster replica {replica}"
                )));
            }
        } else {
            metadata = Some((columns, engine));
        }
    }
    metadata.ok_or_else(|| SinkError::ClickHouse("No replicas found in target cluster".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ClickHouseConfig {
        ClickHouseConfig::from_btreemap(
            [
                ("clickhouse.url", "http://router:8123"),
                ("clickhouse.user", "default"),
                ("clickhouse.password", ""),
                ("clickhouse.database", "default"),
                ("clickhouse.table", "events"),
                ("type", "append-only"),
            ]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect(),
        )
        .unwrap()
    }

    fn replica_engine(engine: &str, args: &str, replica: u32) -> (ClickhouseQueryEngine, u32) {
        (
            ClickhouseQueryEngine {
                name: "events_local".to_owned(),
                engine: engine.to_owned(),
                engine_full: format!("{engine}({args})"),
                create_table_query: format!(
                    "CREATE TABLE events_local (id Int32) ENGINE = {engine}({args}) ORDER BY id"
                ),
            },
            replica,
        )
    }

    fn replica_column(replica: u32) -> (SystemColumn, u32) {
        (
            SystemColumn {
                name: "id".to_owned(),
                r#type: "Int32".to_owned(),
                is_in_primary_key: 1,
            },
            replica,
        )
    }

    #[test]
    fn test_replica_metadata_allows_different_replication_paths() {
        let (columns, engine) = merge_replica_metadata(
            vec![1, 2],
            vec![
                replica_engine("ReplicatedMergeTree", "'/shard1', 'replica1'", 1),
                replica_engine("ReplicatedMergeTree", "'/shard2', 'replica2'", 2),
            ],
            vec![replica_column(2), replica_column(1)],
            &config(),
        )
        .unwrap();
        assert_eq!(columns, vec![replica_column(1).0]);
        assert_eq!(engine, ClickHouseEngine::ReplicatedMergeTree);
    }

    #[test]
    fn test_replica_metadata_rejects_missing_targets() {
        // A system.tables query returns no row for a missing table, even if the replica
        // is reachable. Do not accept metadata returned by just the remaining replica.
        let err = merge_replica_metadata(
            vec![1, 2],
            vec![replica_engine("MergeTree", "", 1)],
            vec![replica_column(1)],
            &config(),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Target table not found on cluster replica 2")
        );

        let err = merge_replica_metadata(
            vec![1],
            vec![replica_engine("MergeTree", "", 1)],
            vec![],
            &config(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("No column metadata"));
        assert!(merge_replica_metadata(vec![], vec![], vec![], &config()).is_err());
    }

    #[test]
    fn test_replica_metadata_rejects_incompatible_targets() {
        for second_engine in [
            replica_engine("Distributed", "logs, default, events", 2),
            replica_engine("CollapsingMergeTree", "sign", 2),
        ] {
            assert!(
                merge_replica_metadata(
                    vec![1, 2],
                    vec![replica_engine("MergeTree", "", 1), second_engine],
                    vec![replica_column(1), replica_column(2)],
                    &config(),
                )
                .is_err()
            );
        }
        for column in [
            SystemColumn {
                r#type: "String".to_owned(),
                ..replica_column(2).0
            },
            SystemColumn {
                is_in_primary_key: 0,
                ..replica_column(2).0
            },
        ] {
            let err = merge_replica_metadata(
                vec![1, 2],
                vec![
                    replica_engine("MergeTree", "", 1),
                    replica_engine("MergeTree", "", 2),
                ],
                vec![replica_column(1), (column, 2)],
                &config(),
            )
            .unwrap_err();
            assert!(
                err.to_string()
                    .contains("Inconsistent target engine or column metadata")
            );
        }
    }
}
