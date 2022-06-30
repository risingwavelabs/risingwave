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
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{Datum, VirtualNode, VIRTUAL_NODE_SIZE};
use risingwave_common::util::ordered::deserialize_column_id;
use risingwave_common::util::value_encoding::deserialize_cell;

use crate::table::cell_based_table::DEFAULT_VNODE;
use crate::cell_based_row_deserializer::{CellBasedRowDeserializer, ColumnDescMapping};

#[derive(Clone)]
pub struct DedupPkCellBasedRowDeserializer<Desc: Deref<Target = ColumnDescMapping>> {
    inner: CellBasedRowDeserializer<Desc>,
}

impl<Desc: Deref<Target = ColumnDescMapping>> DedupPkCellBasedRowDeserializer<Desc> {
    pub fn new(column_mapping: Desc) -> Self {
        let inner = CellBasedRowDeserializer::new(column_mapping);
        Self { inner }
    }

    /// When we encounter a new key, we can be sure that the previous row has been fully
    /// deserialized. Then we return the key and the value of the previous row.
    pub fn deserialize(
        &mut self,
        raw_key: impl AsRef<[u8]>,
        cell: impl AsRef<[u8]>,
    ) -> Result<Option<(VirtualNode, Vec<u8>, Row)>> {
        self.inner.deserialize(raw_key, cell)
    }

    // TODO: remove this once we refactored lookup in delta join with cell-based table
    pub fn deserialize_without_vnode(
        &mut self,
        raw_key: impl AsRef<[u8]>,
        cell: impl AsRef<[u8]>,
    ) -> Result<Option<(VirtualNode, Vec<u8>, Row)>> {
        self.inner.deserialize_without_vnode(raw_key, cell)
    }

    /// Take the remaining data out of the deserializer.
    pub fn take(&mut self) -> Option<(VirtualNode, Vec<u8>, Row)> {
        self.inner.take()
    }

    /// Since [`CellBasedRowDeserializer`] can be repetitively used with different inputs,
    /// it needs to be reset so that pk and data are both cleared for the next use.
    pub fn reset(&mut self) {
        self.inner.reset()
    }
}
