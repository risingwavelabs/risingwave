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

use std::collections::BTreeMap;

use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::types::{DataType, JsonbRef};
use risingwave_pb::ddl_service::table_schema_change::TableChangeType as PbTableChangeType;
use risingwave_pb::ddl_service::{
    SchemaChangeEnvelope as PbSchemaChangeEnvelope, TableSchemaChange as PbTableSchemaChange,
};

use crate::parser::unified::AccessError;

#[derive(Debug)]
pub struct SchemaChangeEnvelope {
    pub table_changes: Vec<TableSchemaChange>,
}

/// Input of `CdcDebeziumPayloadInterpreter::columns_from_table_change`: one `tableChanges[]`
/// entry of a Debezium schema-change event, handed from the shared envelope loop in
/// `parse_schema_change` to the connector that maps its columns.
#[derive(Debug, Clone, Copy)]
pub struct TableChangeContext<'a> {
    /// WITH options of the source, for connectors that consult the upstream catalog.
    pub source_properties: &'a BTreeMap<String, String>,
    /// The `table.columns` array of this entry.
    pub columns: JsonbRef<'a>,
    pub source_name: &'a str,
    /// `tableChanges[].id` as Debezium reports it, e.g. `"public"."orders"`.
    pub debezium_table_id: &'a str,
    /// `debezium_table_id` without the quotes.
    pub table_name: &'a str,
}

impl TableChangeContext<'_> {
    pub fn unsupported_type(&self, ty: String) -> AccessError {
        AccessError::CdcAutoSchemaChangeError {
            ty,
            table_name: format!("{}.{}", self.source_name, self.table_name),
        }
    }
}

/// Output of `CdcDebeziumPayloadInterpreter::columns_from_table_change`: one column of a
/// [`TableChangeContext`] with its upstream type mapped to a RisingWave type. The shared loop in
/// `parse_schema_change` turns it into a `ColumnDesc` by parsing `default_value_text`.
#[derive(Debug)]
pub struct TableChangeColumn {
    pub name: String,
    pub data_type: DataType,
    /// The upstream default expression as Debezium reports it, kept for diagnostics.
    pub default_value_expression: Option<String>,
    /// The default value in the text form that `ScalarImpl::from_text` parses for `data_type`.
    pub default_value_text: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum TableChangeType {
    Unspecified,
    Alter,
    Create,
    Drop,
}

impl TableChangeType {
    #[expect(dead_code)]
    pub fn from_proto(value: PbTableChangeType) -> Self {
        match value {
            PbTableChangeType::Alter => TableChangeType::Alter,
            PbTableChangeType::Create => TableChangeType::Create,
            PbTableChangeType::Drop => TableChangeType::Drop,
            PbTableChangeType::Unspecified => TableChangeType::Unspecified,
        }
    }

    pub fn to_proto(self) -> PbTableChangeType {
        match self {
            TableChangeType::Alter => PbTableChangeType::Alter,
            TableChangeType::Create => PbTableChangeType::Create,
            TableChangeType::Drop => PbTableChangeType::Drop,
            TableChangeType::Unspecified => PbTableChangeType::Unspecified,
        }
    }
}

impl From<&str> for TableChangeType {
    fn from(value: &str) -> Self {
        match value {
            "ALTER" => TableChangeType::Alter,
            "CREATE" => TableChangeType::Create,
            "DROP" => TableChangeType::Drop,
            _ => TableChangeType::Unspecified,
        }
    }
}

#[derive(Debug)]
pub struct TableSchemaChange {
    pub cdc_table_id: String,
    pub(crate) columns: Vec<ColumnCatalog>,
    pub(crate) change_type: TableChangeType,
    pub upstream_ddl: String,
}

impl SchemaChangeEnvelope {
    pub fn is_empty(&self) -> bool {
        self.table_changes.is_empty()
    }

    pub fn to_protobuf(&self) -> PbSchemaChangeEnvelope {
        let table_changes = self
            .table_changes
            .iter()
            .map(|table_change| {
                let columns = table_change
                    .columns
                    .iter()
                    .map(|column| column.to_protobuf())
                    .collect();
                PbTableSchemaChange {
                    change_type: table_change.change_type.to_proto() as _,
                    cdc_table_id: table_change.cdc_table_id.clone(),
                    columns,
                    upstream_ddl: table_change.upstream_ddl.clone(),
                }
            })
            .collect();

        PbSchemaChangeEnvelope { table_changes }
    }

    pub fn table_ids(&self) -> Vec<String> {
        self.table_changes
            .iter()
            .map(|table_change| table_change.cdc_table_id.clone())
            .collect()
    }
}
