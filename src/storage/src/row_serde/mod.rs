// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, VirtualNode};

use self::cell_based_row_deserializer::CellBasedRowDeserializer;
use self::cell_based_row_serializer::CellBasedRowSerializer;
use self::row_based_deserializer::RowBasedDeserializer;
use self::row_based_serializer::RowBasedSerializer;

pub mod cell_based_encoding_util;
pub mod cell_based_row_deserializer;
pub mod cell_based_row_serializer;
pub mod dedup_pk_cell_based_row_deserializer;
pub mod dedup_pk_cell_based_row_serializer;
pub mod row_based_deserializer;
pub mod row_based_serializer;

#[derive(Clone)]
pub struct CellBasedRowSerde;

impl RowSerde for CellBasedRowSerde {
    type Deserializer = CellBasedRowDeserializer;
    type Serializer = CellBasedRowSerializer;
}

#[derive(Clone)]
pub struct RowBasedSerde;

impl RowSerde for RowBasedSerde {
    type Deserializer = RowBasedDeserializer;
    type Serializer = RowBasedSerializer;
}

pub type KeyBytes = Vec<u8>;
pub type ValueBytes = Vec<u8>;

/// `Encoding` defines an interface for encoding a key row into kv storage.
pub trait RowSerialize: Clone {
    /// Constructs a new serializer.
    fn create_row_serializer(
        pk_indices: &[usize],
        column_descs: &[ColumnDesc],
        column_ids: &[ColumnId],
    ) -> Self;

    /// Serialize key and value.
    fn serialize(
        &mut self,
        vnode: VirtualNode,
        pk: &[u8],
        row: Row,
    ) -> Result<Vec<(KeyBytes, ValueBytes)>>;

    /// Serialize key and value. Each column id will occupy a position in Vec. For `column_ids` that
    /// doesn't correspond to a cell, the position will be None. Aparts from user-specified
    /// `column_ids`, there will also be a `SENTINEL_CELL_ID` at the end.
    fn serialize_for_update(
        &mut self,
        vnode: VirtualNode,
        pk: &[u8],
        row: Row,
    ) -> Result<Vec<Option<(KeyBytes, ValueBytes)>>>;
    fn column_ids(&self) -> &[ColumnId];
}

/// Record mapping from [`ColumnDesc`], [`ColumnId`], and output index of columns in a table.
#[derive(Clone)]
pub struct ColumnDescMapping {
    pub output_columns: Vec<ColumnDesc>,

    pub id_to_column_index: HashMap<ColumnId, usize>,
}

/// `Decoding` defines an interface for decoding a key row from kv storage.
pub trait RowDeserialize {
    /// Constructs a new serializer.
    fn create_row_deserializer(
        column_mapping: Arc<ColumnDescMapping>,
        data_types: Vec<DataType>,
    ) -> Self;

    /// When we encounter a new key, we can be sure that the previous row has been fully
    /// deserialized. Then we return the key and the value of the previous row.
    fn deserialize(
        &mut self,
        raw_key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<Option<(VirtualNode, Vec<u8>, Row)>>;

    /// Take the remaining data out of the deserializer.
    fn take(&mut self) -> Option<(VirtualNode, Vec<u8>, Row)>;
}

/// `RowSerde` provides the ability to convert between Row and KV entry.
pub trait RowSerde: Send + Sync + Clone {
    type Serializer: RowSerialize + Send;
    type Deserializer: RowDeserialize + Send;

    /// `create_serializer` will create a row serializer to convert row into KV pairs.
    fn create_serializer(
        pk_indices: &[usize],
        column_descs: &[ColumnDesc],
        column_ids: &[ColumnId],
    ) -> Self::Serializer {
        RowSerialize::create_row_serializer(pk_indices, column_descs, column_ids)
    }

    /// `create_deserializer` will create a row deserializer to convert KV pairs into row.
    fn create_deserializer(
        column_mapping: Arc<ColumnDescMapping>,
        data_types: Vec<DataType>,
    ) -> Self::Deserializer {
        RowDeserialize::create_row_deserializer(column_mapping, data_types)
    }
}
