// Copyright 2024 RisingWave Labs
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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{Row, RowExt};
use serde_derive::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use simd_json::prelude::ArrayTrait;
use thiserror_ext::AsReport;
use tokio_postgres::Statement;

use super::{
    SinkError, SinkWriterMetrics, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::connector_common::{create_pg_client, PostgresExternalTable, SslMode};
use crate::parser::scalar_adapter::{validate_pg_type_to_rw_type, ScalarAdapter};
use crate::sink::writer::{LogSinkerOf, SinkWriter, SinkWriterExt};
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkParam, SinkWriterParam};

pub const POSTGRES_SINK: &str = "postgres";

macro_rules! rw_row_to_pg_values {
    ($row:expr, $statement:expr) => {
        $row.iter().enumerate().map(|(i, d)| {
            d.and_then(|d| {
                let ty = &$statement.params()[i];
                match ScalarAdapter::from_scalar(d, ty) {
                    Ok(scalar) => Some(scalar),
                    Err(e) => {
                        tracing::error!(error=%e.as_report(), scalar=?d, "Failed to convert scalar to pg value");
                        None
                    }
                }
            })
        })
    };
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    #[serde_as(as = "DisplayFromStr")]
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
    #[serde(default = "default_schema")]
    pub schema: String,
    #[serde(default = "Default::default")]
    pub ssl_mode: SslMode,
    #[serde(rename = "ssl.root.cert")]
    pub ssl_root_cert: Option<String>,
    #[serde(default = "default_max_batch_rows")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_rows: usize,
    pub r#type: String, // accept "append-only" or "upsert"
}

fn default_max_batch_rows() -> usize {
    1024
}

fn default_schema() -> String {
    "public".to_string()
}

impl PostgresConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<PostgresConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

#[derive(Debug)]
pub struct PostgresSink {
    pub config: PostgresConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl PostgresSink {
    pub fn new(
        config: PostgresConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl TryFrom<SinkParam> for PostgresSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = PostgresConfig::from_btreemap(param.properties)?;
        PostgresSink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}

impl Sink for PostgresSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<PostgresSinkWriter>;

    const SINK_NAME: &'static str = POSTGRES_SINK;

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert Postgres sink (please define in `primary_key` field)")));
        }

        // Verify our sink schema is compatible with Postgres
        {
            let pg_table = PostgresExternalTable::connect(
                &self.config.user,
                &self.config.password,
                &self.config.host,
                self.config.port,
                &self.config.database,
                &self.config.schema,
                &self.config.table,
                &self.config.ssl_mode,
                &self.config.ssl_root_cert,
                self.is_append_only,
            )
            .await?;

            // Check that names and types match, order of columns doesn't matter.
            {
                let pg_columns = pg_table.column_descs();
                let sink_columns = self.schema.fields();
                if pg_columns.len() != sink_columns.len() {
                    return Err(SinkError::Config(anyhow!(
                    "Column count mismatch: Postgres table has {} columns, but sink schema has {} columns",
                    pg_columns.len(),
                    sink_columns.len()
                )));
                }

                let pg_columns_lookup = pg_columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect::<BTreeMap<_, _>>();
                for sink_column in sink_columns {
                    let pg_column = pg_columns_lookup.get(&sink_column.name);
                    match pg_column {
                        None => {
                            return Err(SinkError::Config(anyhow!(
                                "Column `{}` not found in Postgres table `{}`",
                                sink_column.name,
                                self.config.table
                            )))
                        }
                        Some(pg_column) => {
                            if !validate_pg_type_to_rw_type(pg_column, &sink_column.data_type()) {
                                return Err(SinkError::Config(anyhow!(
                                "Column `{}` in Postgres table `{}` has type `{}`, but sink schema defines it as type `{}`",
                                sink_column.name,
                                self.config.table,
                                pg_column,
                                sink_column.data_type()
                            )));
                            }
                        }
                    }
                }
            }

            // check that pk matches
            {
                let pg_pk_names = pg_table.pk_names();
                let sink_pk_names = self
                    .pk_indices
                    .iter()
                    .map(|i| &self.schema.fields()[*i].name)
                    .collect::<HashSet<_>>();
                if pg_pk_names.len() != sink_pk_names.len() {
                    return Err(SinkError::Config(anyhow!(
                    "Primary key mismatch: Postgres table has primary key on columns {:?}, but sink schema defines primary key on columns {:?}",
                    pg_pk_names,
                    sink_pk_names
                )));
                }
                for name in pg_pk_names {
                    if !sink_pk_names.contains(name) {
                        return Err(SinkError::Config(anyhow!(
                        "Primary key mismatch: Postgres table has primary key on column `{}`, but sink schema does not define it as a primary key",
                        name
                    )));
                    }
                }
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(PostgresSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?
        .into_log_sinker(SinkWriterMetrics::new(&writer_param)))
    }
}

struct Buffer {
    buffer: Vec<StreamChunk>,
    size: usize,
}

impl Buffer {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            size: 0,
        }
    }

    fn push(&mut self, chunk: StreamChunk) -> usize {
        self.size += chunk.cardinality();
        self.buffer.push(chunk);
        self.size
    }

    fn drain(&mut self) -> Vec<StreamChunk> {
        self.size = 0;
        std::mem::take(&mut self.buffer)
    }
}

pub struct PostgresSinkWriter {
    config: PostgresConfig,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    client: tokio_postgres::Client,
    buffer: Buffer,
    insert_statement: Statement,
    delete_statement: Option<Statement>,
    upsert_statement: Option<Statement>,
}

impl PostgresSinkWriter {
    async fn new(
        config: PostgresConfig,
        mut schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let client = create_pg_client(
            &config.user,
            &config.password,
            &config.host,
            &config.port.to_string(),
            &config.database,
            &config.ssl_mode,
            &config.ssl_root_cert,
        )
        .await?;

        // Rewrite schema types for serialization
        let schema_types = {
            let pg_table = PostgresExternalTable::connect(
                &config.user,
                &config.password,
                &config.host,
                config.port,
                &config.database,
                &config.schema,
                &config.table,
                &config.ssl_mode,
                &config.ssl_root_cert,
                is_append_only,
            )
            .await?;
            let name_to_type = pg_table.column_name_to_pg_type();
            let mut schema_types = Vec::with_capacity(schema.fields.len());
            for field in &mut schema.fields[..] {
                let field_name = &field.name;
                let actual_data_type = name_to_type.get(field_name).map(|t| (*t).clone());
                let actual_data_type = actual_data_type
                    .ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "Column `{}` not found in sink schema",
                            field_name
                        ))
                    })?
                    .clone();
                schema_types.push(actual_data_type);
            }
            schema_types
        };

        let insert_statement = {
            let insert_sql = create_insert_sql(&schema, &config.table);
            client
                .prepare_typed(&insert_sql, &schema_types)
                .await
                .context("Failed to prepare insert statement")?
        };

        let delete_statement = if is_append_only {
            None
        } else {
            let delete_types = pk_indices
                .iter()
                .map(|i| schema_types[*i].clone())
                .collect_vec();
            let delete_sql = create_delete_sql(&schema, &config.table, &pk_indices);
            Some(
                client
                    .prepare_typed(&delete_sql, &delete_types)
                    .await
                    .context("Failed to prepare delete statement")?,
            )
        };

        let upsert_statement = if is_append_only {
            None
        } else {
            let upsert_sql = create_upsert_sql(&schema, &config.table, &pk_indices);
            Some(
                client
                    .prepare_typed(&upsert_sql, &schema_types)
                    .await
                    .context("Failed to prepare upsert statement")?,
            )
        };

        let writer = Self {
            config,
            pk_indices,
            is_append_only,
            client,
            buffer: Buffer::new(),
            insert_statement,
            delete_statement,
            upsert_statement,
        };
        Ok(writer)
    }

    async fn flush(&mut self) -> Result<()> {
        if self.is_append_only {
            for chunk in self.buffer.drain() {
                for (op, row) in chunk.rows() {
                    match op {
                        Op::Insert => {
                            self.client
                                .execute_raw(
                                    &self.insert_statement,
                                    rw_row_to_pg_values!(row, self.insert_statement),
                                )
                                .await?;
                        }
                        Op::UpdateInsert | Op::Delete | Op::UpdateDelete => {
                            debug_assert!(!self.is_append_only);
                        }
                    }
                }
            }
        } else {
            let mut unmatched_update_insert = 0;
            for chunk in self.buffer.drain() {
                for (op, row) in chunk.rows() {
                    match op {
                        Op::Insert => {
                            self.client
                                .execute_raw(
                                    &self.insert_statement,
                                    rw_row_to_pg_values!(row, self.insert_statement),
                                )
                                .await?;
                        }
                        Op::UpdateInsert => {
                            unmatched_update_insert += 1;
                            self.client
                                .execute_raw(
                                    self.upsert_statement.as_ref().unwrap(),
                                    rw_row_to_pg_values!(
                                        row,
                                        self.upsert_statement.as_ref().unwrap()
                                    ),
                                )
                                .await?;
                        }
                        Op::Delete => {
                            self.client
                                .execute_raw(
                                    self.delete_statement.as_ref().unwrap(),
                                    rw_row_to_pg_values!(
                                        row.project(&self.pk_indices),
                                        self.delete_statement.as_ref().unwrap()
                                    ),
                                )
                                .await?;
                        }
                        Op::UpdateDelete => {
                            unmatched_update_insert -= 1;
                        }
                    }
                }
            }
            assert_eq!(unmatched_update_insert, 0);
        }

        Ok(())
    }
}

#[async_trait]
impl SinkWriter for PostgresSinkWriter {
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let cardinality = self.buffer.push(chunk);
        if cardinality >= self.config.max_batch_rows {
            self.flush().await?;
        }
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        if is_checkpoint {
            self.flush().await?;
        }
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

fn create_insert_sql(schema: &Schema, table_name: &str) -> String {
    let columns: String = schema
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect_vec()
        .join(", ");
    let parameters: String = (0..schema.fields().len())
        .map(|i| format!("${}", i + 1))
        .collect_vec()
        .join(", ");
    format!("INSERT INTO {table_name} ({columns}) VALUES ({parameters})")
}

fn create_upsert_sql(schema: &Schema, table_name: &str, pk_indices: &[usize]) -> String {
    let columns: String = schema
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect_vec()
        .join(", ");
    let parameters: String = (0..schema.fields().len())
        .map(|i| format!("${}", i + 1))
        .collect_vec()
        .join(", ");
    let pk_columns = pk_indices
        .iter()
        .map(|i| schema.fields()[*i].name.clone())
        .collect_vec()
        .join(", ");
    let update_parameters: String = (0..schema.fields().len())
        .filter(|i| !pk_indices.contains(i))
        .map(|i| {
            let column = schema.fields()[i].name.clone();
            let param = format!("${}", i + 1);
            format!("{column} = {param}")
        })
        .collect_vec()
        .join(", ");
    format!("INSERT INTO {table_name} ({columns}) VALUES ({parameters}) on conflict ({pk_columns}) do update set {update_parameters}")
}

fn create_delete_sql(schema: &Schema, table_name: &str, pk_indices: &[usize]) -> String {
    let parameters: String = pk_indices
        .iter()
        .map(|i| format!("{} = ${}", schema.fields()[*i].name, i + 1))
        .collect_vec()
        .join(" AND ");
    format!("DELETE FROM {table_name} WHERE {parameters}")
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use expect_test::{expect, Expect};
    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;

    fn check(actual: impl Display, expect: Expect) {
        let actual = actual.to_string();
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_create_insert_sql() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_string(),
                sub_fields: vec![],
                type_name: "".to_string(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_string(),
                sub_fields: vec![],
                type_name: "".to_string(),
            },
        ]);
        let table_name = "test_table";
        let sql = create_insert_sql(&schema, table_name);
        check(
            sql,
            expect!["INSERT INTO test_table (a, b) VALUES ($1, $2)"],
        );
    }

    #[test]
    fn test_create_delete_sql() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_string(),
                sub_fields: vec![],
                type_name: "".to_string(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_string(),
                sub_fields: vec![],
                type_name: "".to_string(),
            },
        ]);
        let table_name = "test_table";
        let sql = create_delete_sql(&schema, table_name, &[1]);
        check(sql, expect!["DELETE FROM test_table WHERE b = $2"]);
    }

    #[test]
    fn test_create_upsert_sql() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_string(),
                sub_fields: vec![],
                type_name: "".to_string(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_string(),
                sub_fields: vec![],
                type_name: "".to_string(),
            },
        ]);
        let table_name = "test_table";
        let sql = create_upsert_sql(&schema, table_name, &[1]);
        check(
            sql,
            expect!["INSERT INTO test_table (a, b) VALUES ($1, $2) on conflict (b) do update set a = $1"],
        );
    }
}
