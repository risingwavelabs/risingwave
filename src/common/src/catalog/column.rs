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

use std::borrow::Cow;

use itertools::Itertools;
use risingwave_common::types::Datum;
use risingwave_pb::expr::ExprNode;
use risingwave_pb::expr::expr_node::{RexNode, Type as ExprType};
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::{
    AdditionalColumn, ColumnDescVersion, DefaultColumnDesc, PbColumnCatalog, PbColumnDesc,
};

use super::schema::FieldLike;
use super::{
    CDC_OFFSET_COLUMN_NAME, CDC_TABLE_NAME_COLUMN_NAME, ICEBERG_FILE_PATH_COLUMN_NAME,
    ICEBERG_FILE_POS_COLUMN_NAME, ICEBERG_SEQUENCE_NUM_COLUMN_NAME, ROW_ID_COLUMN_NAME,
    RW_TIMESTAMP_COLUMN_ID, RW_TIMESTAMP_COLUMN_NAME, USER_COLUMN_ID_OFFSET,
};
use crate::catalog::{Field, ROW_ID_COLUMN_ID};
use crate::types::DataType;
use crate::util::value_encoding::DatumToProtoExt;

/// Column ID is the unique identifier of a column in a table. Different from table ID, column ID is
/// not globally unique.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnId(i32);

impl std::fmt::Debug for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

impl ColumnId {
    pub const fn new(column_id: i32) -> Self {
        Self(column_id)
    }

    /// Sometimes the id field is filled later, we use this value for better debugging.
    pub const fn placeholder() -> Self {
        Self(i32::MAX - 1)
    }

    pub const fn first_user_column() -> Self {
        Self(USER_COLUMN_ID_OFFSET)
    }
}

impl ColumnId {
    pub const fn get_id(&self) -> i32 {
        self.0
    }

    /// Returns the subsequent column id.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn apply_delta_if_not_row_id(&mut self, delta: i32) {
        if self.0 != ROW_ID_COLUMN_ID.get_id() {
            self.0 += delta;
        }
    }
}

impl From<i32> for ColumnId {
    fn from(column_id: i32) -> Self {
        Self::new(column_id)
    }
}
impl From<&i32> for ColumnId {
    fn from(column_id: &i32) -> Self {
        Self::new(*column_id)
    }
}

impl From<ColumnId> for i32 {
    fn from(id: ColumnId) -> i32 {
        id.0
    }
}

impl From<&ColumnId> for i32 {
    fn from(id: &ColumnId) -> i32 {
        id.0
    }
}

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SystemColumn {
    RwTimestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnDesc {
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub name: String,
    pub generated_or_default_column: Option<GeneratedOrDefaultColumn>,
    pub description: Option<String>,
    pub additional_column: AdditionalColumn,
    pub version: ColumnDescVersion,
    /// Currently the system column is used for `_rw_timestamp` only and is generated at runtime,
    /// so this field is not persisted.
    pub system_column: Option<SystemColumn>,
    /// Whether the column is nullable.
    /// If a column is not nullable, BatchInsert/BatchUpdate operations will throw an error when NULL is inserted/updated into.
    /// The row contains NULL for this column will be ignored when streaming data into the table.
    pub nullable: bool,
}

impl ColumnDesc {
    pub fn unnamed(column_id: ColumnId, data_type: DataType) -> ColumnDesc {
        Self::named("", column_id, data_type)
    }

    pub fn named(name: impl Into<String>, column_id: ColumnId, data_type: DataType) -> ColumnDesc {
        ColumnDesc {
            data_type,
            column_id,
            name: name.into(),
            generated_or_default_column: None,
            description: None,
            additional_column: AdditionalColumn { column_type: None },
            version: ColumnDescVersion::LATEST,
            system_column: None,
            nullable: true,
        }
    }

    pub fn named_with_default_value(
        name: impl Into<String>,
        column_id: ColumnId,
        data_type: DataType,
        snapshot_value: Datum,
    ) -> ColumnDesc {
        let default_col = DefaultColumnDesc {
            expr: Some(ExprNode {
                // equivalent to `Literal::to_expr_proto`
                function_type: ExprType::Unspecified as i32,
                return_type: Some(data_type.to_protobuf()),
                rex_node: Some(RexNode::Constant(snapshot_value.to_protobuf())),
            }),
            snapshot_value: Some(snapshot_value.to_protobuf()),
        };
        ColumnDesc {
            generated_or_default_column: Some(GeneratedOrDefaultColumn::DefaultColumn(default_col)),
            ..Self::named(name, column_id, data_type)
        }
    }

    pub fn named_with_additional_column(
        name: impl Into<String>,
        column_id: ColumnId,
        data_type: DataType,
        additional_column_type: AdditionalColumn,
    ) -> ColumnDesc {
        ColumnDesc {
            additional_column: additional_column_type,
            ..Self::named(name, column_id, data_type)
        }
    }

    pub fn named_with_system_column(
        name: impl Into<String>,
        column_id: ColumnId,
        data_type: DataType,
        system_column: SystemColumn,
    ) -> ColumnDesc {
        ColumnDesc {
            system_column: Some(system_column),
            ..Self::named(name, column_id, data_type)
        }
    }

    /// Convert to proto
    pub fn to_protobuf(&self) -> PbColumnDesc {
        PbColumnDesc {
            column_type: Some(self.data_type.to_protobuf()),
            column_id: self.column_id.get_id(),
            name: self.name.clone(),
            generated_or_default_column: self.generated_or_default_column.clone(),
            description: self.description.clone(),
            additional_column_type: 0, // deprecated
            additional_column: Some(self.additional_column.clone()),
            version: self.version as i32,
            nullable: self.nullable,
        }
    }

    pub fn from_field_with_column_id(field: &Field, id: i32) -> Self {
        Self::named(&field.name, ColumnId::new(id), field.data_type.clone())
    }

    pub fn from_field_without_column_id(field: &Field) -> Self {
        Self::from_field_with_column_id(field, ColumnId::placeholder().into())
    }

    pub fn is_generated(&self) -> bool {
        matches!(
            self.generated_or_default_column,
            Some(GeneratedOrDefaultColumn::GeneratedColumn(_))
        )
    }
}

impl From<PbColumnDesc> for ColumnDesc {
    fn from(prost: PbColumnDesc) -> Self {
        let additional_column = prost
            .get_additional_column()
            .unwrap_or(&AdditionalColumn { column_type: None })
            .clone();
        let version = prost.version();

        Self {
            data_type: DataType::from(prost.column_type.as_ref().unwrap()),
            column_id: ColumnId::new(prost.column_id),
            name: prost.name,
            generated_or_default_column: prost.generated_or_default_column,
            description: prost.description.clone(),
            additional_column,
            version,
            system_column: None,
            nullable: prost.nullable,
        }
    }
}

impl From<&PbColumnDesc> for ColumnDesc {
    fn from(prost: &PbColumnDesc) -> Self {
        prost.clone().into()
    }
}

impl From<&ColumnDesc> for PbColumnDesc {
    fn from(c: &ColumnDesc) -> Self {
        c.to_protobuf()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnCatalog {
    pub column_desc: ColumnDesc,
    pub is_hidden: bool,
}

impl ColumnCatalog {
    pub fn visible(column_desc: ColumnDesc) -> Self {
        Self {
            column_desc,
            is_hidden: false,
        }
    }

    pub fn hidden(column_desc: ColumnDesc) -> Self {
        Self {
            column_desc,
            is_hidden: true,
        }
    }

    /// Get the column catalog's is hidden.
    pub fn is_hidden(&self) -> bool {
        self.is_hidden
    }

    /// If the column is a generated column
    pub fn is_generated(&self) -> bool {
        self.column_desc.is_generated()
    }

    pub fn can_dml(&self) -> bool {
        !self.is_generated() && !self.is_rw_timestamp_column()
    }

    /// Returns whether the column is defined by user within the column definition clause
    /// in the `CREATE TABLE` statement.
    pub fn is_user_defined(&self) -> bool {
        !self.is_hidden() && !self.is_rw_sys_column() && !self.is_connector_additional_column()
    }

    /// If the column is a generated column
    pub fn generated_expr(&self) -> Option<&ExprNode> {
        if let Some(GeneratedOrDefaultColumn::GeneratedColumn(desc)) =
            &self.column_desc.generated_or_default_column
        {
            Some(desc.expr.as_ref().unwrap())
        } else {
            None
        }
    }

    /// If the columns is an `INCLUDE ... AS ...` connector column.
    pub fn is_connector_additional_column(&self) -> bool {
        self.column_desc.additional_column.column_type.is_some()
    }

    /// Get a reference to the column desc's data type.
    pub fn data_type(&self) -> &DataType {
        &self.column_desc.data_type
    }

    /// Get nullable info of the column.
    pub fn nullable(&self) -> bool {
        self.column_desc.nullable
    }

    /// Get the column desc's column id.
    pub fn column_id(&self) -> ColumnId {
        self.column_desc.column_id
    }

    /// Get a reference to the column desc's name.
    pub fn name(&self) -> &str {
        self.column_desc.name.as_ref()
    }

    /// Convert column catalog to proto
    pub fn to_protobuf(&self) -> PbColumnCatalog {
        PbColumnCatalog {
            column_desc: Some(self.column_desc.to_protobuf()),
            is_hidden: self.is_hidden,
        }
    }

    /// Creates a row ID column (for implicit primary key).
    /// It'll always have the ID `0`.
    pub fn row_id_column() -> Self {
        Self::hidden(ColumnDesc::named(
            ROW_ID_COLUMN_NAME,
            ROW_ID_COLUMN_ID,
            DataType::Serial,
        ))
    }

    pub fn is_rw_sys_column(&self) -> bool {
        self.column_desc.system_column.is_some()
    }

    pub fn rw_timestamp_column() -> Self {
        Self::hidden(ColumnDesc::named_with_system_column(
            RW_TIMESTAMP_COLUMN_NAME,
            RW_TIMESTAMP_COLUMN_ID,
            DataType::Timestamptz,
            SystemColumn::RwTimestamp,
        ))
    }

    pub fn is_rw_timestamp_column(&self) -> bool {
        matches!(
            self.column_desc.system_column,
            Some(SystemColumn::RwTimestamp)
        )
    }

    // XXX: should we use INCLUDE columns or SYSTEM columns instead of normal hidden columns?

    pub fn iceberg_hidden_cols() -> [Self; 3] {
        [
            Self::hidden(ColumnDesc::named(
                ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
                ColumnId::placeholder(),
                DataType::Int64,
            )),
            Self::hidden(ColumnDesc::named(
                ICEBERG_FILE_PATH_COLUMN_NAME,
                ColumnId::placeholder(),
                DataType::Varchar,
            )),
            Self::hidden(ColumnDesc::named(
                ICEBERG_FILE_POS_COLUMN_NAME,
                ColumnId::placeholder(),
                DataType::Int64,
            )),
        ]
    }

    /// Note: these columns are added in `SourceStreamChunkRowWriter::do_action`.
    /// May also look for the usage of `SourceColumnType`.
    pub fn debezium_cdc_source_cols() -> [Self; 3] {
        [
            Self::visible(ColumnDesc::named(
                "payload",
                ColumnId::placeholder(),
                DataType::Jsonb,
            )),
            // upstream offset
            Self::hidden(ColumnDesc::named(
                CDC_OFFSET_COLUMN_NAME,
                ColumnId::placeholder(),
                DataType::Varchar,
            )),
            // upstream table name of the cdc table
            Self::hidden(ColumnDesc::named(
                CDC_TABLE_NAME_COLUMN_NAME,
                ColumnId::placeholder(),
                DataType::Varchar,
            )),
        ]
    }

    pub fn is_row_id_column(&self) -> bool {
        self.column_desc.column_id == ROW_ID_COLUMN_ID
    }
}

impl From<PbColumnCatalog> for ColumnCatalog {
    fn from(prost: PbColumnCatalog) -> Self {
        Self {
            column_desc: prost.column_desc.unwrap().into(),
            is_hidden: prost.is_hidden,
        }
    }
}

impl ColumnCatalog {
    pub fn name_with_hidden(&self) -> Cow<'_, str> {
        if self.is_hidden {
            Cow::Owned(format!("{}(hidden)", self.column_desc.name))
        } else {
            Cow::Borrowed(&self.column_desc.name)
        }
    }
}

impl FieldLike for ColumnDesc {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl FieldLike for ColumnCatalog {
    fn data_type(&self) -> &DataType {
        &self.column_desc.data_type
    }

    fn name(&self) -> &str {
        &self.column_desc.name
    }
}

pub fn columns_extend(preserved_columns: &mut Vec<ColumnCatalog>, columns: Vec<ColumnCatalog>) {
    debug_assert_eq!(ROW_ID_COLUMN_ID.get_id(), 0);
    let mut max_incoming_column_id = ROW_ID_COLUMN_ID.get_id();
    columns.iter().for_each(|column| {
        let column_id = column.column_id().get_id();
        if column_id > max_incoming_column_id {
            max_incoming_column_id = column_id;
        }
    });
    preserved_columns.iter_mut().for_each(|column| {
        column
            .column_desc
            .column_id
            .apply_delta_if_not_row_id(max_incoming_column_id)
    });

    preserved_columns.extend(columns);
}

pub fn debug_assert_column_ids_distinct(columns: &[ColumnCatalog]) {
    debug_assert!(
        columns
            .iter()
            .map(|c| c.column_id())
            .duplicates()
            .next()
            .is_none(),
        "duplicate ColumnId found in source catalog. Columns: {columns:#?}"
    );
}

/// FIXME: Perhaps we should use sth like `ColumnIdGenerator::new_alter`,
/// However, the `SourceVersion` is problematic: It doesn't contain `next_col_id`.
/// (But for now this isn't a large problem, since drop column is not allowed for source yet..)
///
/// Besides, the logic of column id handling is a mess.
/// In some places, we use `ColumnId::placeholder()`, and use `col_id_gen` to fill it at the end;
/// In other places, we create column id ad-hoc.
pub fn max_column_id(columns: &[ColumnCatalog]) -> ColumnId {
    // XXX: should we check the column IDs of struct fields here?
    columns
        .iter()
        .fold(ColumnId::first_user_column(), |a, b| a.max(b.column_id()))
}

#[cfg(test)]
pub mod tests {
    use risingwave_pb::plan_common::PbColumnDesc;

    use crate::catalog::ColumnDesc;
    use crate::test_prelude::*;
    use crate::types::{DataType, StructType};

    pub fn build_prost_desc() -> PbColumnDesc {
        let city = DataType::from(StructType::new([
            ("country.city.address", DataType::Varchar),
            ("country.city.zipcode", DataType::Varchar),
        ]));
        let country = DataType::from(StructType::new([
            ("country.address", DataType::Varchar),
            ("country.city", city),
        ]));
        PbColumnDesc::new(country.to_protobuf(), "country", 5)
    }

    pub fn build_desc() -> ColumnDesc {
        let city = StructType::new([
            ("country.city.address", DataType::Varchar),
            ("country.city.zipcode", DataType::Varchar),
        ]);
        let country = StructType::new([
            ("country.address", DataType::Varchar),
            ("country.city", city.into()),
        ]);
        ColumnDesc::named("country", 5.into(), country.into())
    }

    #[test]
    fn test_into_column_catalog() {
        let desc: ColumnDesc = build_prost_desc().into();
        assert_eq!(desc, build_desc());
    }
}
