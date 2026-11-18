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

use prost::Message;
use risingwave_pb::id::FragmentId;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use sea_orm::{FromQueryResult, Statement};
use sea_orm_migration::prelude::*;
use thiserror_ext::AsReport;

// Keep this synchronized with `FragmentTypeFlag::IcebergWithPkIndexWriter`.
const ICEBERG_WITH_PK_INDEX_WRITER_FLAG: i32 = 1 << 18;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        let database_backend = connection.get_database_backend();
        let (sql, values) = Query::select()
            .columns([
                Fragment::FragmentId,
                Fragment::FragmentTypeMask,
                Fragment::StreamNode,
            ])
            .from(Fragment::Table)
            .build_any(&*database_backend.get_query_builder());

        for row in connection
            .query_all(Statement::from_sql_and_values(
                database_backend,
                sql,
                values,
            ))
            .await?
        {
            let fragment = FragmentEntity::from_query_result(&row, "")?;
            if fragment.fragment_type_mask & ICEBERG_WITH_PK_INDEX_WRITER_FLAG != 0 {
                continue;
            }
            let stream_node =
                StreamNode::decode(fragment.stream_node.as_slice()).map_err(|err| {
                    DbErr::Custom(format!(
                        "failed to decode stream node for fragment {}: {}",
                        fragment.fragment_id,
                        err.as_report()
                    ))
                })?;
            if contains_iceberg_writer(&stream_node) {
                manager
                    .exec_stmt(
                        Query::update()
                            .table(Fragment::Table)
                            .value(
                                Fragment::FragmentTypeMask,
                                fragment.fragment_type_mask | ICEBERG_WITH_PK_INDEX_WRITER_FLAG,
                            )
                            .and_where(Expr::col(Fragment::FragmentId).eq(fragment.fragment_id))
                            .to_owned(),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "cannot rollback Iceberg writer fragment flag migration".to_owned(),
        ))
    }
}

fn contains_iceberg_writer(node: &StreamNode) -> bool {
    matches!(node.node_body, Some(NodeBody::IcebergWithPkIndexWriter(_)))
        || node.input.iter().any(contains_iceberg_writer)
}

#[derive(Debug, FromQueryResult)]
#[sea_orm(entity = "Fragment")]
struct FragmentEntity {
    fragment_id: FragmentId,
    fragment_type_mask: i32,
    stream_node: Vec<u8>,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    FragmentId,
    FragmentTypeMask,
    StreamNode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_nested_iceberg_writer() {
        let node = StreamNode {
            input: vec![StreamNode {
                node_body: Some(NodeBody::IcebergWithPkIndexWriter(Default::default())),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(contains_iceberg_writer(&node));
        assert!(!contains_iceberg_writer(&StreamNode::default()));
    }
}
