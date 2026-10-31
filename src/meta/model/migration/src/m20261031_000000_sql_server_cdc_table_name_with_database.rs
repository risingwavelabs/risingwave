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

//! SQL Server CDC tables route change events by the Debezium topic suffix
//! `database.schema.table`. Tables created since #23739 persisted `schema.table` instead, and
//! some older tables persisted the bare `TABLE '...'` clause. This migration rewrites, for every
//! stream job that scans a SQL Server CDC table, the persisted names to the ones the frontend
//! builds now from the table's own connect properties:
//! - the `_rw_table_name` literal of the `CdcFilter` node: `database.schema.table`
//! - `ExternalTableDesc.table_name` of the `StreamCdcScan` node: `[database].schema.table`
//! - `table.cdc_table_id`: `<source id>.database.schema.table`

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use prost::Message;
use risingwave_pb::expr::ExprNode;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::plan_common::ExternalTableDesc;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use sea_orm::{FromQueryResult, Statement};
use sea_orm_migration::prelude::*;
use thiserror_ext::AsReport;

const CONNECTOR_KEY: &str = "connector";
const SQL_SERVER_CDC_CONNECTOR: &str = "sqlserver-cdc";
const DATABASE_NAME_KEY: &str = "database.name";
const SCHEMA_NAME_KEY: &str = "schema.name";
const TABLE_NAME_KEY: &str = "table.name";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        let database_backend = connection.get_database_backend();
        let (sql, values) = Query::select()
            .columns([Fragment::FragmentId, Fragment::JobId, Fragment::StreamNode])
            .from(Fragment::Table)
            .build_any(&*database_backend.get_query_builder());

        let mut fragments = vec![];
        for row in connection
            .query_all(Statement::from_sql_and_values(
                database_backend,
                sql,
                values,
            ))
            .await?
        {
            let fragment = FragmentEntity::from_query_result(&row, "")?;
            let stream_node =
                StreamNode::decode(fragment.stream_node.as_slice()).map_err(|err| {
                    DbErr::Custom(format!(
                        "failed to decode stream node for fragment {}: {}",
                        fragment.fragment_id,
                        err.as_report()
                    ))
                })?;
            fragments.push((fragment.fragment_id, fragment.job_id, stream_node));
        }

        let mut tables_by_job: HashMap<i32, Vec<SqlServerTable>> = HashMap::new();
        for (_, job_id, stream_node) in &fragments {
            collect_sql_server_tables(stream_node, tables_by_job.entry(*job_id).or_default());
        }
        tables_by_job.retain(|_, tables| !tables.is_empty());

        for (fragment_id, job_id, mut stream_node) in fragments {
            let Some(tables) = tables_by_job.get(&job_id) else {
                continue;
            };
            if rewrite_stream_node(&mut stream_node, &filter_literal_rewrites(tables)) {
                manager
                    .exec_stmt(
                        Query::update()
                            .table(Fragment::Table)
                            .value(Fragment::StreamNode, stream_node.encode_to_vec())
                            .and_where(Expr::col(Fragment::FragmentId).eq(fragment_id))
                            .to_owned(),
                    )
                    .await?;
            }
        }

        for (job_id, tables) in tables_by_job {
            let [table] = tables.as_slice() else {
                continue;
            };
            manager
                .exec_stmt(
                    Query::update()
                        .table(Table::Table)
                        .value(
                            Table::CdcTableId,
                            format!("{}.{}", table.source_id, table.canonical_name()),
                        )
                        .and_where(Expr::col(Table::TableId).eq(job_id))
                        .and_where(Expr::col(Table::CdcTableId).is_not_null())
                        .and_where(Expr::col(Table::CdcTableId).ne(""))
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "cannot rollback SQL Server CDC table name migration".to_owned(),
        ))
    }
}

/// A SQL Server table scanned by a `StreamCdcScan` node, named by its connect properties.
#[derive(Debug, Clone, PartialEq)]
struct SqlServerTable {
    source_id: String,
    database_name: String,
    schema_name: String,
    table_name: String,
}

impl SqlServerTable {
    fn from_external_table_desc(desc: &ExternalTableDesc) -> Option<Self> {
        let properties = &desc.connect_properties;
        if properties.get(CONNECTOR_KEY).map(String::as_str) != Some(SQL_SERVER_CDC_CONNECTOR) {
            return None;
        }
        Some(Self {
            source_id: desc.source_id.to_string(),
            database_name: properties.get(DATABASE_NAME_KEY)?.clone(),
            schema_name: properties.get(SCHEMA_NAME_KEY)?.clone(),
            table_name: properties.get(TABLE_NAME_KEY)?.clone(),
        })
    }

    /// Same as `SqlServer::canonical_name` in `risingwave_connector`.
    fn canonical_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.database_name, self.schema_name, self.table_name
        )
    }

    /// Same as the `external_table_name` that the frontend builds from a `schema.table` clause.
    fn external_table_name(&self) -> String {
        format!(
            "[{}].{}.{}",
            self.database_name.replace(']', "]]"),
            self.schema_name,
            self.table_name
        )
    }
}

fn collect_sql_server_tables(node: &StreamNode, tables: &mut Vec<SqlServerTable>) {
    if let Some(NodeBody::StreamCdcScan(scan)) = &node.node_body
        && let Some(desc) = &scan.cdc_table_desc
        && let Some(table) = SqlServerTable::from_external_table_desc(desc)
    {
        tables.push(table);
    }
    for input in &node.input {
        collect_sql_server_tables(input, tables);
    }
}

/// Maps every name that a persisted `CdcFilter` literal of the job's SQL Server tables may hold
/// to its canonical name: `table` and `schema.table` from clauses without a database, and the
/// canonical name itself. A name shared by two tables of the job maps to `None` and is kept.
fn filter_literal_rewrites(tables: &[SqlServerTable]) -> HashMap<String, Option<String>> {
    let mut rewrites = HashMap::new();
    for table in tables {
        let canonical_name = table.canonical_name();
        for literal in [
            table.table_name.clone(),
            format!("{}.{}", table.schema_name, table.table_name),
            canonical_name.clone(),
        ] {
            match rewrites.entry(literal) {
                Entry::Vacant(entry) => {
                    entry.insert(Some(canonical_name.clone()));
                }
                Entry::Occupied(mut entry) => {
                    if entry.get().as_ref() != Some(&canonical_name) {
                        entry.insert(None);
                    }
                }
            }
        }
    }
    rewrites
}

/// Returns whether `node` or any of its inputs changed.
fn rewrite_stream_node(
    node: &mut StreamNode,
    filter_literal_rewrites: &HashMap<String, Option<String>>,
) -> bool {
    let mut changed = false;
    match &mut node.node_body {
        Some(NodeBody::StreamCdcScan(scan)) => {
            if let Some(desc) = &mut scan.cdc_table_desc
                && let Some(table) = SqlServerTable::from_external_table_desc(desc)
            {
                let external_table_name = table.external_table_name();
                if desc.table_name != external_table_name {
                    desc.table_name = external_table_name;
                    changed = true;
                }
            }
        }
        Some(NodeBody::CdcFilter(filter)) => {
            if let Some(search_condition) = &mut filter.search_condition {
                changed |= rewrite_filter_literals(search_condition, filter_literal_rewrites);
            }
        }
        _ => {}
    }
    for input in &mut node.input {
        changed |= rewrite_stream_node(input, filter_literal_rewrites);
    }
    changed
}

fn rewrite_filter_literals(
    expr: &mut ExprNode,
    filter_literal_rewrites: &HashMap<String, Option<String>>,
) -> bool {
    match &mut expr.rex_node {
        Some(RexNode::Constant(constant)) => {
            if let Some(literal) = decode_varchar_literal(&constant.body)
                && let Some(Some(canonical_name)) = filter_literal_rewrites.get(literal)
                && literal != canonical_name
            {
                constant.body = encode_varchar_literal(canonical_name);
                true
            } else {
                false
            }
        }
        Some(RexNode::FuncCall(func_call)) => {
            let mut changed = false;
            for child in &mut func_call.children {
                changed |= rewrite_filter_literals(child, filter_literal_rewrites);
            }
            changed
        }
        _ => false,
    }
}

/// Decodes a non-null varchar datum in `risingwave_common::util::value_encoding`: a `1` tag byte,
/// the length as `u32` in little endian, then the UTF-8 bytes.
fn decode_varchar_literal(body: &[u8]) -> Option<&str> {
    let Some((&1, rest)) = body.split_first() else {
        return None;
    };
    let (len, bytes) = rest.split_first_chunk::<4>()?;
    if bytes.len() != u32::from_le_bytes(*len) as usize {
        return None;
    }
    std::str::from_utf8(bytes).ok()
}

/// Inverse of [`decode_varchar_literal`].
fn encode_varchar_literal(literal: &str) -> Vec<u8> {
    let mut body = vec![1];
    body.extend_from_slice(&(literal.len() as u32).to_le_bytes());
    body.extend_from_slice(literal.as_bytes());
    body
}

#[derive(Debug, FromQueryResult)]
#[sea_orm(entity = "Fragment")]
struct FragmentEntity {
    fragment_id: i32,
    job_id: i32,
    stream_node: Vec<u8>,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    FragmentId,
    JobId,
    StreamNode,
}

#[derive(DeriveIden)]
enum Table {
    Table,
    TableId,
    CdcTableId,
}

#[cfg(test)]
mod tests {
    use risingwave_pb::data::Datum;
    use risingwave_pb::expr::FunctionCall;
    use risingwave_pb::expr::expr_node::Type as ExprType;
    use risingwave_pb::stream_plan::{CdcFilterNode, StreamCdcScanNode};

    use super::*;

    fn sql_server_scan_node(database_name: &str, table_name: &str) -> StreamNode {
        StreamNode {
            node_body: Some(NodeBody::StreamCdcScan(Box::new(StreamCdcScanNode {
                cdc_table_desc: Some(ExternalTableDesc {
                    source_id: 7.into(),
                    table_name: table_name.to_owned(),
                    connect_properties: [
                        (CONNECTOR_KEY, SQL_SERVER_CDC_CONNECTOR),
                        (DATABASE_NAME_KEY, database_name),
                        (SCHEMA_NAME_KEY, "dbo"),
                        (TABLE_NAME_KEY, "orders"),
                    ]
                    .into_iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect(),
                    ..Default::default()
                }),
                ..Default::default()
            }))),
            ..Default::default()
        }
    }

    fn cdc_filter_node(literal: &str) -> StreamNode {
        let search_condition = ExprNode {
            function_type: ExprType::Equal as i32,
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![
                    ExprNode {
                        rex_node: Some(RexNode::InputRef(2)),
                        ..Default::default()
                    },
                    ExprNode {
                        rex_node: Some(RexNode::Constant(Datum {
                            body: encode_varchar_literal(literal),
                        })),
                        ..Default::default()
                    },
                ],
            })),
            ..Default::default()
        };
        StreamNode {
            node_body: Some(NodeBody::CdcFilter(Box::new(CdcFilterNode {
                search_condition: Some(search_condition),
                ..Default::default()
            }))),
            ..Default::default()
        }
    }

    fn filter_literal(node: &StreamNode) -> String {
        let Some(NodeBody::CdcFilter(filter)) = &node.node_body else {
            panic!("expect a CdcFilter node");
        };
        let Some(RexNode::FuncCall(func_call)) =
            &filter.search_condition.as_ref().unwrap().rex_node
        else {
            panic!("expect a function call");
        };
        let Some(RexNode::Constant(constant)) = &func_call.children[1].rex_node else {
            panic!("expect a constant");
        };
        decode_varchar_literal(&constant.body).unwrap().to_owned()
    }

    fn scan_table_name(node: &StreamNode) -> String {
        let Some(NodeBody::StreamCdcScan(scan)) = &node.node_body else {
            panic!("expect a StreamCdcScan node");
        };
        scan.cdc_table_desc.as_ref().unwrap().table_name.clone()
    }

    #[test]
    fn test_varchar_literal_encoding() {
        let body = b"\x01\x0c\x00\x00\x00public.\"a.b\"";
        assert_eq!(decode_varchar_literal(body), Some("public.\"a.b\""));
        assert_eq!(encode_varchar_literal("public.\"a.b\""), body);
        assert_eq!(decode_varchar_literal(b"\x00"), None);
        assert_eq!(decode_varchar_literal(b"\x01\x05\x00\x00\x00abc"), None);
    }

    #[test]
    fn test_rewrite_sql_server_names() {
        for (literal, table_name) in [
            ("dbo.orders", "dbo.orders"),
            ("repro.dbo.orders", "repro.dbo.orders"),
            ("orders", "orders"),
        ] {
            let mut scan = sql_server_scan_node("repro", table_name);
            let mut filter = cdc_filter_node(literal);
            let mut tables = vec![];
            collect_sql_server_tables(&scan, &mut tables);
            let rewrites = filter_literal_rewrites(&tables);

            assert!(rewrite_stream_node(&mut scan, &rewrites));
            assert_eq!(scan_table_name(&scan), "[repro].dbo.orders");
            assert_eq!(
                rewrite_stream_node(&mut filter, &rewrites),
                literal != "repro.dbo.orders"
            );
            assert_eq!(filter_literal(&filter), "repro.dbo.orders");
            assert_eq!(tables[0].canonical_name(), "repro.dbo.orders");
            assert_eq!(tables[0].source_id, "7");
        }
    }

    #[test]
    fn test_rewrite_is_idempotent() {
        let mut scan = sql_server_scan_node("x]y", "dbo.orders");
        let mut filter = cdc_filter_node("dbo.orders");
        let mut tables = vec![];
        collect_sql_server_tables(&scan, &mut tables);
        let rewrites = filter_literal_rewrites(&tables);

        assert!(rewrite_stream_node(&mut scan, &rewrites));
        assert!(rewrite_stream_node(&mut filter, &rewrites));
        assert_eq!(scan_table_name(&scan), "[x]]y].dbo.orders");
        assert_eq!(filter_literal(&filter), "x]y.dbo.orders");
        assert!(!rewrite_stream_node(&mut scan, &rewrites));
        assert!(!rewrite_stream_node(&mut filter, &rewrites));
    }

    #[test]
    fn test_skip_other_connectors() {
        let mut scan = sql_server_scan_node("repro", "public.orders");
        let Some(NodeBody::StreamCdcScan(scan_node)) = &mut scan.node_body else {
            unreachable!()
        };
        scan_node
            .cdc_table_desc
            .as_mut()
            .unwrap()
            .connect_properties
            .insert(CONNECTOR_KEY.to_owned(), "postgres-cdc".to_owned());

        let mut tables = vec![];
        collect_sql_server_tables(&scan, &mut tables);
        assert!(tables.is_empty());
        assert!(!rewrite_stream_node(&mut scan, &HashMap::new()));
        assert_eq!(scan_table_name(&scan), "public.orders");
    }

    #[test]
    fn test_ambiguous_literal_is_kept() {
        let mut tables = vec![];
        collect_sql_server_tables(&sql_server_scan_node("db1", "dbo.orders"), &mut tables);
        collect_sql_server_tables(&sql_server_scan_node("db2", "dbo.orders"), &mut tables);
        let rewrites = filter_literal_rewrites(&tables);

        let mut filter = cdc_filter_node("dbo.orders");
        assert!(!rewrite_stream_node(&mut filter, &rewrites));
        assert_eq!(filter_literal(&filter), "dbo.orders");

        let mut filter = cdc_filter_node("db1.dbo.orders");
        assert!(!rewrite_stream_node(&mut filter, &rewrites));
        assert_eq!(filter_literal(&filter), "db1.dbo.orders");
    }
}
