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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Bound;

use futures::pin_mut;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::OwnedRow;
use risingwave_common::util::sort_util::{OrderType, cmp_rows};
use risingwave_common::util::value_encoding::ValueRowSerializer;
use risingwave_storage::StateStore;
use risingwave_storage::mem_table::KeyOp;
use risingwave_storage::row_serde::value_serde::{ValueRowSerde, ValueRowSerdeNew};
use risingwave_storage::store::PrefetchOptions;

use crate::common::table::state_table::StateTableInner;
use crate::executor::prelude::*;

/// Change buffer for refresh diff operations
pub struct RefreshChangeBuffer {
    buffer: HashMap<Vec<u8>, KeyOp>,
}

impl RefreshChangeBuffer {
    pub fn new() -> Self {
        Self {
            buffer: HashMap::new(),
        }
    }

    pub fn insert(&mut self, pk: Vec<u8>, row: bytes::Bytes) {
        self.buffer.insert(pk, KeyOp::Insert(row));
    }

    pub fn update(&mut self, pk: Vec<u8>, old_row: bytes::Bytes, new_row: bytes::Bytes) {
        self.buffer.insert(pk, KeyOp::Update((old_row, new_row)));
    }

    pub fn delete(&mut self, pk: Vec<u8>, row: bytes::Bytes) {
        self.buffer.insert(pk, KeyOp::Delete(row));
    }

    pub fn into_parts(self) -> HashMap<Vec<u8>, KeyOp> {
        self.buffer
    }
}

/// Diff calculator for refreshable tables using sort-merge join algorithm
pub struct RefreshDiffCalculator<S: StateStore, SD: ValueRowSerde> {
    main_table: StateTableInner<S, SD>,
    staging_table: StateTableInner<S, SD>,
}

impl<S: StateStore, SD: ValueRowSerde> RefreshDiffCalculator<S, SD> {
    pub fn new(main_table: StateTableInner<S, SD>, staging_table: StateTableInner<S, SD>) -> Self {
        Self {
            main_table,
            staging_table,
        }
    }

    /// Calculate the difference between main table and staging table using sort-merge join
    pub async fn calculate_diff(&self) -> StreamExecutorResult<RefreshChangeBuffer> {
        use std::ops::Bound::Unbounded;

        let mut change_buffer = RefreshChangeBuffer::new();

        // Get all vnodes for both tables
        let main_vnodes: Vec<_> = self.main_table.vnodes().iter_vnodes().collect();
        let staging_vnodes: Vec<_> = self.staging_table.vnodes().iter_vnodes().collect();

        // For simplicity in MVP, assume both tables have the same vnode distribution
        // In production, this would need more sophisticated handling

        for vnode in main_vnodes {
            if !staging_vnodes.contains(&vnode) {
                continue; // Skip vnodes not in staging table
            }

            let pk_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Unbounded, Unbounded);

            // Get sorted iterators for both tables for this vnode
            let main_stream = self
                .main_table
                .iter_keyed_row_with_vnode(vnode, pk_range, PrefetchOptions::default())
                .await?;
            pin_mut!(main_stream);

            let staging_stream = self
                .staging_table
                .iter_keyed_row_with_vnode(vnode, pk_range, PrefetchOptions::default())
                .await?;
            pin_mut!(staging_stream);

            // Collect all rows from both streams for sort-merge join
            let mut main_rows = Vec::new();
            #[for_await]
            for entry in main_stream {
                let keyed_row = entry?;
                let (table_key, row) = keyed_row.into_owned_row_key();
                let pk = table_key.key_part();
                let pk_row = self.main_table.pk_serde().deserialize(pk)?;
                main_rows.push((pk_row, row));
            }

            let mut staging_rows = Vec::new();
            #[for_await]
            for entry in staging_stream {
                let keyed_row = entry?;
                let (table_key, row) = keyed_row.into_owned_row_key();
                let pk = table_key.key_part();
                let pk_row = self.staging_table.pk_serde().deserialize(pk)?;
                staging_rows.push((pk_row, row));
            }

            // Sort both collections by primary key (ascending order)
            let pk_order_types: Vec<OrderType> = (0..self.main_table.pk_indices().len())
                .map(|_| OrderType::ascending())
                .collect();

            main_rows.sort_by(|(pk1, _), (pk2, _)| cmp_rows(pk1, pk2, &pk_order_types));
            staging_rows.sort_by(|(pk1, _), (pk2, _)| cmp_rows(pk1, pk2, &pk_order_types));

            // Perform sort-merge join
            self.sort_merge_join(&mut change_buffer, main_rows, staging_rows)
                .await?;
        }

        Ok(change_buffer)
    }

    /// Perform sort-merge join to calculate differences
    async fn sort_merge_join(
        &self,
        change_buffer: &mut RefreshChangeBuffer,
        main_rows: Vec<(OwnedRow, OwnedRow)>,
        staging_rows: Vec<(OwnedRow, OwnedRow)>,
    ) -> StreamExecutorResult<()> {
        let mut main_iter = main_rows.into_iter().peekable();
        let mut staging_iter = staging_rows.into_iter().peekable();

        loop {
            match (main_iter.peek(), staging_iter.peek()) {
                (Some((main_pk, _)), Some((staging_pk, _))) => {
                    let pk_order_types: Vec<OrderType> =
                        (0..main_pk.len()).map(|_| OrderType::ascending()).collect();
                    match cmp_rows(main_pk, staging_pk, &pk_order_types) {
                        Ordering::Less => {
                            // PK exists in main but not in staging - DELETE
                            let (pk, row) = main_iter.next().unwrap();
                            let pk_bytes = self.serialize_pk(&pk)?;
                            let row_bytes = self.serialize_row(&row)?;
                            change_buffer.delete(pk_bytes, row_bytes);
                        }
                        Ordering::Equal => {
                            // PK exists in both - potential UPDATE
                            let (main_pk, main_row) = main_iter.next().unwrap();
                            let (_, staging_row) = staging_iter.next().unwrap();

                            let pk_bytes = self.serialize_pk(&main_pk)?;
                            let main_row_bytes = self.serialize_row(&main_row)?;
                            let staging_row_bytes = self.serialize_row(&staging_row)?;

                            // Check if rows are different
                            if main_row_bytes != staging_row_bytes {
                                change_buffer.update(pk_bytes, main_row_bytes, staging_row_bytes);
                            }
                            // If rows are the same, no change needed
                        }
                        Ordering::Greater => {
                            // PK exists in staging but not in main - INSERT
                            let (pk, row) = staging_iter.next().unwrap();
                            let pk_bytes = self.serialize_pk(&pk)?;
                            let row_bytes = self.serialize_row(&row)?;
                            change_buffer.insert(pk_bytes, row_bytes);
                        }
                    }
                }
                (Some(_), None) => {
                    // Remaining rows in main table should be deleted
                    let (pk, row) = main_iter.next().unwrap();
                    let pk_bytes = self.serialize_pk(&pk)?;
                    let row_bytes = self.serialize_row(&row)?;
                    change_buffer.delete(pk_bytes, row_bytes);
                }
                (None, Some(_)) => {
                    // Remaining rows in staging table should be inserted
                    let (pk, row) = staging_iter.next().unwrap();
                    let pk_bytes = self.serialize_pk(&pk)?;
                    let row_bytes = self.serialize_row(&row)?;
                    change_buffer.insert(pk_bytes, row_bytes);
                }
                (None, None) => {
                    // Both iterators exhausted
                    break;
                }
            }
        }

        Ok(())
    }

    /// Serialize primary key to bytes for consistent comparison
    fn serialize_pk(&self, pk: &OwnedRow) -> StreamExecutorResult<Vec<u8>> {
        let mut pk_bytes = Vec::new();
        self.main_table.pk_serde().serialize(pk, &mut pk_bytes);
        Ok(pk_bytes)
    }

    /// Serialize row to bytes for storage
    fn serialize_row(&self, row: &OwnedRow) -> StreamExecutorResult<bytes::Bytes> {
        // Use basic serialization for now to avoid the value_serde access issue
        use risingwave_common::catalog::{ColumnDesc, ColumnId};
        use risingwave_common::types::DataType;
        use risingwave_common::util::value_encoding::BasicSerde;

        let column_descs: Vec<ColumnDesc> = (0..row.len())
            .map(|i| ColumnDesc::unnamed(ColumnId::new(i as i32), DataType::Varchar))
            .collect();
        let serde = BasicSerde::new(
            std::sync::Arc::from_iter(0..row.len()),
            std::sync::Arc::from(column_descs.into_boxed_slice()),
        );
        Ok(bytes::Bytes::from(serde.serializer.serialize(row)))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;

    use super::*;

    #[tokio::test]
    async fn test_refresh_change_buffer() {
        let mut buffer = RefreshChangeBuffer::new();

        let pk = vec![1, 2, 3];
        let row = bytes::Bytes::from("test_row");

        buffer.insert(pk.clone(), row.clone());

        let parts = buffer.into_parts();
        assert_eq!(parts.len(), 1);
        assert!(matches!(parts.get(&pk), Some(KeyOp::Insert(_))));
    }
}
