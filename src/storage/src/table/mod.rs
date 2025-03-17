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

pub mod batch_table;
pub mod merge_sort;

use std::ops::Deref;

use futures::{Stream, StreamExt};
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VirtualNode;
pub use risingwave_common::hash::table_distribution::*;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_hummock_sdk::key::TableKey;

use crate::StateStoreIter;
use crate::error::StorageResult;
use crate::row_serde::value_serde::ValueRowSerde;
use crate::store::{ChangeLogValue, StateStoreIterExt, StateStoreReadLogItem};

pub trait TableIter: Send {
    async fn next_row(&mut self) -> StorageResult<Option<OwnedRow>>;
}

pub async fn collect_data_chunk<E, S, R>(
    stream: &mut S,
    schema: &Schema,
    chunk_size: Option<usize>,
) -> Result<Option<DataChunk>, E>
where
    S: Stream<Item = Result<R, E>> + Unpin,
    R: Row,
{
    let mut builders = schema.create_array_builders(chunk_size.unwrap_or(0));
    let mut row_count = 0;
    for _ in 0..chunk_size.unwrap_or(usize::MAX) {
        match stream.next().await.transpose()? {
            Some(row) => {
                for (datum, builder) in row.iter().zip_eq_debug(builders.iter_mut()) {
                    builder.append(datum);
                }
            }
            None => break,
        }

        row_count += 1;
    }

    let chunk = {
        let columns: Vec<_> = builders
            .into_iter()
            .map(|builder| builder.finish().into())
            .collect();
        DataChunk::new(columns, row_count)
    };

    if chunk.cardinality() == 0 {
        Ok(None)
    } else {
        Ok(Some(chunk))
    }
}

/// Collects data chunks from stream of rows.
pub async fn collect_data_chunk_with_builder<E, S, R>(
    stream: &mut S,
    builder: &mut DataChunkBuilder,
) -> Result<Option<DataChunk>, E>
where
    R: Row,
    S: Stream<Item = Result<R, E>> + Unpin,
{
    // TODO(kwannoel): If necessary, we can optimize it in the future.
    // This can be done by moving the check if builder is full from `append_one_row` to here,
    while let Some(row) = stream.next().await.transpose()? {
        let result = builder.append_one_row(row);
        if let Some(chunk) = result {
            return Ok(Some(chunk));
        }
    }

    let chunk = builder.consume_all();
    Ok(chunk)
}

pub fn get_second<T, U, E>(arg: Result<(T, U), E>) -> Result<U, E> {
    arg.map(|x| x.1)
}

#[derive(Debug)]
pub struct KeyedRow<T: AsRef<[u8]>, R = OwnedRow> {
    vnode_prefixed_key: TableKey<T>,
    row: R,
}

impl<T: AsRef<[u8]>, R> KeyedRow<T, R> {
    pub fn new(table_key: TableKey<T>, row: R) -> Self {
        Self {
            vnode_prefixed_key: table_key,
            row,
        }
    }

    pub fn into_owned_row(self) -> R {
        self.row
    }

    pub fn into_owned_row_key(self) -> (TableKey<T>, R) {
        (self.vnode_prefixed_key, self.row)
    }

    pub fn vnode(&self) -> VirtualNode {
        self.vnode_prefixed_key.vnode_part()
    }

    pub fn key(&self) -> &[u8] {
        self.vnode_prefixed_key.key_part()
    }

    pub fn row(&self) -> &R {
        &self.row
    }

    pub fn into_parts(self) -> (TableKey<T>, R) {
        (self.vnode_prefixed_key, self.row)
    }
}

impl<T: AsRef<[u8]>> Deref for KeyedRow<T> {
    type Target = OwnedRow;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

pub type KeyedChangeLogRow<T> = KeyedRow<T, ChangeLogRow>;

pub type ChangeLogRow = ChangeLogValue<OwnedRow>;

pub fn deserialize_log_stream<'a>(
    iter: impl StateStoreIter<StateStoreReadLogItem> + 'a,
    deserializer: &'a impl ValueRowSerde,
) -> impl Stream<Item = StorageResult<ChangeLogRow>> + 'a {
    iter.into_stream(|(_key, log_value)| {
        log_value.try_map(|slice| Ok(OwnedRow::new(deserializer.deserialize(slice)?)))
    })
}
