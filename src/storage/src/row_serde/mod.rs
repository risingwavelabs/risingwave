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
use risingwave_common::util::ordered::OrderedRowSerializer;

use self::row_based_deserializer::RowBasedDeserializer;
use self::row_based_serializer::RowBasedSerializer;

pub mod row_based_deserializer;
pub mod row_based_serializer;
pub mod row_serde_util;

#[derive(Clone)]
pub struct RowBasedSerde;

impl RowSerde for RowBasedSerde {
    type Deserializer = RowBasedDeserializer;
    type Serializer = RowBasedSerializer;
}

pub type KeyBytes = Vec<u8>;
pub type ValueBytes = Vec<u8>;

/// `RowSerialize` defines an interface for encoding a key row into kv storage.
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
    ) -> Result<(KeyBytes, ValueBytes)>;
}

/// `ColumnDescMapping` is the record mapping from [`ColumnDesc`], [`ColumnId`], and is used in both
/// cell-based encoding and row-based encoding.
#[derive(Clone)]
pub struct ColumnDescMapping {
    /// output_columns are some of the columns that to be partially scan.
    pub output_columns: Vec<ColumnDesc>,

    /// The output column's column index in output row, which is used in cell-based deserialize.
    pub output_id_to_index: HashMap<ColumnId, usize>,

    /// The full row data types, which is used in row-based deserialize.
    pub all_data_types: Vec<DataType>,

    /// The output column's column index in full row, which is used in row-based deserialize.
    pub output_index: Vec<usize>,
}

/// `RowDeserialize` defines an interface for decoding a key row from kv storage.
pub trait RowDeserialize {
    /// Constructs a new deserializer.
    fn create_row_deserializer(column_mapping: Arc<ColumnDescMapping>) -> Self;

    /// When we encounter a new key, we can be sure that the previous row has been fully
    /// deserialized. Then we return the key and the value of the previous row.
    fn deserialize(
        &mut self,
        raw_key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(VirtualNode, Vec<u8>, Row)>;
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
    fn create_deserializer(column_mapping: Arc<ColumnDescMapping>) -> Self::Deserializer {
        RowDeserialize::create_row_deserializer(column_mapping)
    }
}

pub fn serialize_pk(pk: &Row, serializer: &OrderedRowSerializer) -> Vec<u8> {
    let mut result = vec![];
    serializer.serialize(pk, &mut result);
    result
}
