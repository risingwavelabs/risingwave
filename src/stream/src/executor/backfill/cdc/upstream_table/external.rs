// Copyright 2025 RisingWave Labs
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

use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::error::ConnectorResult;
use risingwave_connector::source::cdc::external::{
    CdcOffset, CdcTableType, ExternalTableConfig, ExternalTableReader, ExternalTableReaderImpl,
    SchemaTableName,
};

/// This struct represents an external table to be read during backfill
#[derive(Debug, Clone)]
pub struct ExternalStorageTable {
    /// Id for this table.
    table_id: TableId,

    /// The normalized name of the table, e.g. `dbname.schema_name.table_name`.
    table_name: String,

    schema_name: String,

    database_name: String,

    config: ExternalTableConfig,

    table_type: CdcTableType,

    /// The schema of the output columns, i.e., this table VIEWED BY some executor like
    /// `RowSeqScanExecutor`.
    /// todo: the schema of the external table defined in the CREATE TABLE DDL
    schema: Schema,

    pk_order_types: Vec<OrderType>,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table.
    pk_indices: Vec<usize>,
}

impl ExternalStorageTable {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: TableId,
        SchemaTableName {
            table_name,
            schema_name,
        }: SchemaTableName,
        database_name: String,
        config: ExternalTableConfig,
        table_type: CdcTableType,
        schema: Schema,
        pk_order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
    ) -> Self {
        Self {
            table_id,
            table_name,
            schema_name,
            database_name,
            config,
            table_type,
            schema,
            pk_order_types,
            pk_indices,
        }
    }

    #[cfg(test)]
    pub fn for_test_undefined() -> Self {
        Self {
            table_id: 1.into(),
            table_name: "for_test_table_name".into(),
            schema_name: "for_test_schema_name".into(),
            database_name: "for_test_database_name".into(),
            config: ExternalTableConfig::default(),
            table_type: CdcTableType::Undefined,
            schema: Schema::empty().to_owned(),
            pk_order_types: vec![],
            pk_indices: vec![],
        }
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn pk_order_types(&self) -> &[OrderType] {
        &self.pk_order_types
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    pub fn schema_table_name(&self) -> SchemaTableName {
        SchemaTableName {
            schema_name: self.schema_name.clone(),
            table_name: self.table_name.clone(),
        }
    }

    pub async fn create_table_reader(&self) -> ConnectorResult<ExternalTableReaderImpl> {
        self.table_type
            .create_table_reader(
                self.config.clone(),
                self.schema.clone(),
                self.pk_indices.clone(),
            )
            .await
    }

    pub fn qualified_table_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }

    pub fn database_name(&self) -> &str {
        self.database_name.as_str()
    }

    pub async fn current_cdc_offset(
        &self,
        table_reader: &ExternalTableReaderImpl,
    ) -> ConnectorResult<Option<CdcOffset>> {
        let binlog = table_reader.current_cdc_offset().await?;
        Ok(Some(binlog))
    }
}
