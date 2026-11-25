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

//! A sink into a table persists the ids of the target table columns it writes, so that dropping a
//! column of the target table can be rejected exactly when some sink writes it. This migration adds
//! the column and backfills the sinks that have no list yet with the rule the frontend applies at
//! creation: the column list of `CREATE SINK ... INTO t(cols)` when there is one, otherwise the
//! sink's visible columns matched by position to the DML columns of the target table as recorded
//! when the sink was created (`original_target_columns`, falling back to the table's current
//! columns). Columns the table has lost since then are left out, so that a table whose
//! sink-written column was already dropped by an auto schema change keeps accepting changes.
//!
//! A sink whose definition cannot be parsed, or that writes none of the table's remaining columns,
//! keeps a NULL list. Until such a sink is recreated, every schema change of its target table is
//! rejected.

use std::collections::HashSet;

use prost::Message;
use risingwave_common::catalog::{
    ColumnCatalog, ColumnId, target_columns_to_visible_positions, written_target_column_ids,
};
use risingwave_pb::plan_common::PbColumnCatalog;
use risingwave_sqlparser::ast::Statement as SqlStatement;
use risingwave_sqlparser::parser::Parser;
use sea_orm::{FromQueryResult, Statement};
use sea_orm_migration::prelude::{Table as MigrationTable, *};
use thiserror_ext::AsReport;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager.has_column("sink", "target_column_ids").await? {
            manager
                .alter_table(
                    MigrationTable::alter()
                        .table(Sink::Table)
                        .add_column(ColumnDef::new(Sink::TargetColumnIds).json_binary())
                        .to_owned(),
                )
                .await?;
        }

        let connection = manager.get_connection();
        let database_backend = connection.get_database_backend();
        let (sql, values) = Query::select()
            .columns([
                (Sink::Table, Sink::SinkId),
                (Sink::Table, Sink::Definition),
                (Sink::Table, Sink::Columns),
                (Sink::Table, Sink::OriginalTargetColumns),
            ])
            .expr_as(
                Expr::col((Table::Table, Table::Columns)),
                Alias::new("table_columns"),
            )
            .from(Sink::Table)
            .left_join(
                Table::Table,
                Expr::col((Table::Table, Table::TableId)).equals((Sink::Table, Sink::TargetTable)),
            )
            .and_where(Expr::col((Sink::Table, Sink::TargetTable)).is_not_null())
            .and_where(Expr::col((Sink::Table, Sink::TargetColumnIds)).is_null())
            .build_any(&*database_backend.get_query_builder());

        let rows = connection
            .query_all(Statement::from_sql_and_values(
                database_backend,
                sql,
                values,
            ))
            .await?;
        for row in rows {
            let sink = SinkRow::from_query_result(&row, "")?;
            let Some(target_column_ids) = sink.target_column_ids()? else {
                continue;
            };
            manager
                .exec_stmt(
                    Query::update()
                        .table(Sink::Table)
                        .value(
                            Sink::TargetColumnIds,
                            serde_json::to_value(target_column_ids).unwrap(),
                        )
                        .and_where(Expr::col(Sink::SinkId).eq(sink.sink_id))
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Sink::Table)
                    .drop_column(Sink::TargetColumnIds)
                    .to_owned(),
            )
            .await
    }
}

#[derive(Clone, PartialEq, Message)]
struct ColumnCatalogArray {
    #[prost(message, repeated, tag = "1")]
    inner: Vec<PbColumnCatalog>,
}

fn decode_columns(sink_id: i32, bytes: &[u8]) -> Result<Vec<ColumnCatalog>, DbErr> {
    let columns = ColumnCatalogArray::decode(bytes).map_err(|err| {
        DbErr::Custom(format!(
            "failed to decode columns for sink {}: {}",
            sink_id,
            err.as_report()
        ))
    })?;
    Ok(columns.inner.into_iter().map(ColumnCatalog::from).collect())
}

#[derive(FromQueryResult)]
struct SinkRow {
    sink_id: i32,
    definition: String,
    columns: Vec<u8>,
    original_target_columns: Option<Vec<u8>>,
    table_columns: Option<Vec<u8>>,
}

impl SinkRow {
    fn target_column_ids(&self) -> Result<Option<Vec<i32>>, DbErr> {
        let Ok(SqlStatement::CreateSink { stmt }) = Parser::parse_exactly_one(&self.definition)
        else {
            return Ok(None);
        };
        let column_list = (!stmt.columns.is_empty()).then(|| {
            stmt.columns
                .iter()
                .map(|c| c.real_value())
                .collect::<Vec<_>>()
        });

        let Some(table_columns) = &self.table_columns else {
            return Ok(None);
        };
        let table_columns = decode_columns(self.sink_id, table_columns)?;
        let mut target_columns = match &self.original_target_columns {
            Some(bytes) => decode_columns(self.sink_id, bytes)?,
            None => vec![],
        };
        if target_columns.is_empty() {
            target_columns = table_columns.clone();
        }
        target_columns.retain(|c| !c.is_rw_timestamp_column());

        let visible_column_count = decode_columns(self.sink_id, &self.columns)?
            .iter()
            .filter(|c| !c.is_hidden())
            .count();

        let positions = target_columns_to_visible_positions(
            &target_columns,
            column_list.as_deref(),
            visible_column_count,
        );
        let table_column_ids: HashSet<ColumnId> =
            table_columns.iter().map(|c| c.column_id()).collect();
        let target_column_ids: Vec<i32> = written_target_column_ids(&target_columns, &positions)
            .into_iter()
            .filter(|id| table_column_ids.contains(id))
            .map(|id| id.get_id())
            .collect();
        Ok((!target_column_ids.is_empty()).then_some(target_column_ids))
    }
}

#[derive(DeriveIden)]
enum Sink {
    Table,
    SinkId,
    Definition,
    Columns,
    TargetTable,
    OriginalTargetColumns,
    TargetColumnIds,
}

#[derive(DeriveIden)]
enum Table {
    Table,
    TableId,
    Columns,
}
