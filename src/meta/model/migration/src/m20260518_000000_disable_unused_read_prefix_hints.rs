use std::collections::BTreeSet;

use prost::Message;
use risingwave_pb::catalog::Table as PbTable;
use risingwave_pb::id::{FragmentId, TableId};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{SinkLogStoreType, StreamNode};
use sea_orm::{FromQueryResult, Statement};
use sea_orm_migration::prelude::*;
use thiserror_ext::AsReport;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        let database_backend = connection.get_database_backend();

        let (sql, values) = Query::select()
            .columns([Fragment::FragmentId, Fragment::StreamNode])
            .from(Fragment::Table)
            .build_any(&*database_backend.get_query_builder());

        let fragments = connection
            .query_all(Statement::from_sql_and_values(
                database_backend,
                sql,
                values,
            ))
            .await?;

        let mut table_ids = BTreeSet::new();

        for row in fragments {
            let fragment = FragmentEntity::from_query_result(&row, "")?;
            let mut stream_node =
                StreamNode::decode(fragment.stream_node.as_slice()).map_err(|err| {
                    DbErr::Custom(format!("failed to decode stream node: {}", err.as_report()))
                })?;

            if disable_unused_read_prefix_hints(&mut stream_node, &mut table_ids) {
                manager
                    .exec_stmt(
                        Query::update()
                            .table(Fragment::Table)
                            .value(Fragment::StreamNode, stream_node.encode_to_vec())
                            .and_where(Expr::col(Fragment::FragmentId).eq(fragment.fragment_id))
                            .to_owned(),
                    )
                    .await?;
            }
        }

        if !table_ids.is_empty() {
            let table_ids = table_ids.into_iter().collect::<Vec<_>>();
            manager
                .exec_stmt(
                    Query::update()
                        .table(Table::Table)
                        .value(Table::ReadPrefixLenHint, 0)
                        .and_where(Expr::col(Table::TableId).is_in(table_ids))
                        .and_where(Expr::col(Table::ReadPrefixLenHint).ne(0))
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "cannot rollback unused read prefix hint migration".to_owned(),
        ))
    }
}

fn disable_unused_read_prefix_hints(
    stream_node: &mut StreamNode,
    table_ids: &mut BTreeSet<TableId>,
) -> bool {
    let mut changed = false;

    if let Some(node_body) = stream_node.node_body.as_mut() {
        changed |= match node_body {
            NodeBody::DynamicFilter(node) => {
                disable_table_hint(node.left_table.as_mut(), table_ids)
            }
            NodeBody::Materialize(node) => {
                disable_table_hint(node.staging_table.as_mut(), table_ids)
            }
            NodeBody::VectorIndexWrite(node) => disable_table_hint(node.table.as_mut(), table_ids),
            NodeBody::Sink(node) if node.log_store_type == SinkLogStoreType::KvLogStore as i32 => {
                disable_table_hint(node.table.as_mut(), table_ids)
            }
            NodeBody::SyncLogStore(node) => {
                disable_table_hint(node.log_store_table.as_mut(), table_ids)
            }
            _ => false,
        };
    }

    for input in &mut stream_node.input {
        changed |= disable_unused_read_prefix_hints(input, table_ids);
    }

    changed
}

fn disable_table_hint(table: Option<&mut PbTable>, table_ids: &mut BTreeSet<TableId>) -> bool {
    let Some(table) = table else {
        return false;
    };

    table_ids.insert(table.id);
    if table.read_prefix_len_hint == 0 {
        return false;
    }

    table.read_prefix_len_hint = 0;
    true
}

#[derive(Debug, FromQueryResult)]
#[sea_orm(entity = "Fragment")]
struct FragmentEntity {
    fragment_id: FragmentId,
    stream_node: Vec<u8>,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    FragmentId,
    StreamNode,
}

#[derive(DeriveIden)]
enum Table {
    Table,
    TableId,
    ReadPrefixLenHint,
}

#[cfg(test)]
mod tests {
    use risingwave_pb::stream_plan::{
        DynamicFilterNode, MaterializeNode, SinkNode, SyncLogStoreNode, VectorIndexWriteNode,
    };

    use super::*;

    fn table(id: u32, read_prefix_len_hint: u32) -> PbTable {
        PbTable {
            id: TableId::new(id),
            read_prefix_len_hint,
            ..Default::default()
        }
    }

    #[test]
    fn disable_only_targeted_table_hints() {
        let mut stream_node = StreamNode {
            node_body: Some(NodeBody::DynamicFilter(Box::new(DynamicFilterNode {
                left_table: Some(table(1, 3)),
                right_table: Some(table(2, 1)),
                ..Default::default()
            }))),
            input: vec![
                StreamNode {
                    node_body: Some(NodeBody::Materialize(Box::new(MaterializeNode {
                        staging_table: Some(table(3, 2)),
                        refresh_progress_table: Some(table(4, 1)),
                        ..Default::default()
                    }))),
                    ..Default::default()
                },
                StreamNode {
                    node_body: Some(NodeBody::Sink(Box::new(SinkNode {
                        table: Some(table(5, 3)),
                        log_store_type: SinkLogStoreType::KvLogStore as i32,
                        ..Default::default()
                    }))),
                    ..Default::default()
                },
                StreamNode {
                    node_body: Some(NodeBody::Sink(Box::new(SinkNode {
                        table: Some(table(6, 3)),
                        log_store_type: SinkLogStoreType::InMemoryLogStore as i32,
                        ..Default::default()
                    }))),
                    ..Default::default()
                },
                StreamNode {
                    node_body: Some(NodeBody::SyncLogStore(Box::new(SyncLogStoreNode {
                        log_store_table: Some(table(7, 3)),
                        ..Default::default()
                    }))),
                    ..Default::default()
                },
                StreamNode {
                    node_body: Some(NodeBody::VectorIndexWrite(Box::new(VectorIndexWriteNode {
                        table: Some(table(8, 3)),
                    }))),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let mut table_ids = BTreeSet::new();
        assert!(disable_unused_read_prefix_hints(
            &mut stream_node,
            &mut table_ids
        ));
        assert_eq!(
            table_ids,
            BTreeSet::from([
                TableId::new(1),
                TableId::new(3),
                TableId::new(5),
                TableId::new(7),
                TableId::new(8),
            ])
        );

        let Some(NodeBody::DynamicFilter(dynamic_filter)) = &stream_node.node_body else {
            unreachable!()
        };
        assert_eq!(
            dynamic_filter
                .left_table
                .as_ref()
                .unwrap()
                .read_prefix_len_hint,
            0
        );
        assert_eq!(
            dynamic_filter
                .right_table
                .as_ref()
                .unwrap()
                .read_prefix_len_hint,
            1
        );

        let Some(NodeBody::Materialize(materialize)) = &stream_node.input[0].node_body else {
            unreachable!()
        };
        assert_eq!(
            materialize
                .staging_table
                .as_ref()
                .unwrap()
                .read_prefix_len_hint,
            0
        );
        assert_eq!(
            materialize
                .refresh_progress_table
                .as_ref()
                .unwrap()
                .read_prefix_len_hint,
            1
        );

        let Some(NodeBody::Sink(in_memory_sink)) = &stream_node.input[2].node_body else {
            unreachable!()
        };
        assert_eq!(
            in_memory_sink.table.as_ref().unwrap().read_prefix_len_hint,
            3
        );
    }

    #[test]
    fn collect_zero_hint_table_without_rewriting_fragment() {
        let mut stream_node = StreamNode {
            node_body: Some(NodeBody::SyncLogStore(Box::new(SyncLogStoreNode {
                log_store_table: Some(table(42, 0)),
                ..Default::default()
            }))),
            ..Default::default()
        };

        let mut table_ids = BTreeSet::new();
        assert!(!disable_unused_read_prefix_hints(
            &mut stream_node,
            &mut table_ids
        ));
        assert_eq!(table_ids, BTreeSet::from([TableId::new(42)]));
    }
}
