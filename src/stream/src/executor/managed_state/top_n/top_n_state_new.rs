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

use std::borrow::Cow;
use std::ops::Index;

use futures::pin_mut;
use futures::stream::StreamExt;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::ordered::*;
use risingwave_storage::error::StorageResult;
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::StateStore;

use crate::executor::error::StreamExecutorResult;
use crate::executor::PkIndices;

#[allow(dead_code)]
pub struct ManagedTopNStateNew<S: StateStore> {
    /// Relational table.
    state_table: StateTable<S>,
    /// The number of elements in both cache and storage.
    total_count: usize,
    /// Number of entries to retain in memory after each flush.
    top_n_count: Option<usize>,
    /// For deserializing `OrderedRow`.
    ordered_row_deserializer: OrderedRowDeserializer,
}

#[derive(Clone, PartialEq, Debug)]
pub struct TopNStateRow {
    pub ordered_key: Option<OrderedRow>,
    pub row: Row,
}

impl TopNStateRow {
    pub fn new(ordered_key: OrderedRow, row: Row) -> Self {
        Self {
            ordered_key: Some(ordered_key),
            row,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.ordered_key.is_some()
    }
}

impl<S: StateStore> ManagedTopNStateNew<S> {
    pub fn new(
        top_n_count: Option<usize>,
        total_count: usize,
        store: S,
        table_id: TableId,
        data_types: Vec<DataType>,
        ordered_row_deserializer: OrderedRowDeserializer,
        pk_indices: PkIndices,
    ) -> Self {
        let order_types = ordered_row_deserializer.get_order_types().to_vec();

        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| {
                ColumnDesc::unnamed(ColumnId::from(id as i32), data_type.clone())
            })
            .collect::<Vec<_>>();
        let state_table =
            StateTable::new(store, table_id, column_descs, order_types, None, pk_indices);
        Self {
            state_table,
            total_count,
            top_n_count,
            ordered_row_deserializer,
        }
    }

    pub async fn insert(
        &mut self,
        _key: OrderedRow,
        value: Row,
        _epoch: u64,
    ) -> StreamExecutorResult<()> {
        self.state_table.insert(value)?;
        self.total_count += 1;
        Ok(())
    }

    pub async fn delete(
        &mut self,
        _key: &OrderedRow,
        value: Row,
        _epoch: u64,
    ) -> StreamExecutorResult<()> {
        self.state_table.delete(value)?;
        self.total_count -= 1;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn total_count(&self) -> usize {
        self.total_count
    }

    fn get_topn_row(&self, iter_res: StorageResult<Cow<Row>>) -> TopNStateRow {
        let row = iter_res.unwrap().into_owned();
        let mut datums = vec![];
        for pk_index in self.state_table.pk_indices() {
            datums.push(row.index(*pk_index).clone());
        }
        let pk = Row::new(datums);
        let pk_ordered = OrderedRow::new(pk, self.ordered_row_deserializer.get_order_types());
        TopNStateRow::new(pk_ordered, row)
    }

    /// This function will return the rows at the position of `offset` and (`offset` + `limit` - 1),
    /// which forms the range `[start_row, end_row]` of the top-N range .
    /// When `offset` is 0 the `start_row` will be the first row in state table;
    /// When `limit` is 0 the `end_row` will be an invalid row.
    pub async fn find_range(
        &self,
        offset: usize,
        limit: usize,
        epoch: u64,
    ) -> StreamExecutorResult<(TopNStateRow, TopNStateRow)> {
        let state_table_iter = self.state_table.iter(epoch).await?;
        pin_mut!(state_table_iter);

        let invalid_row = TopNStateRow {
            ordered_key: None,
            row: Row::default(),
        };
        let mut first_row = invalid_row.clone();
        if let Some(next_res) = state_table_iter.next().await {
            first_row = self.get_topn_row(next_res);
        }

        // fetch start row
        let mut start_row = first_row;
        for i in 0..offset {
            match state_table_iter.next().await {
                Some(next_res) => {
                    if i == offset - 1 {
                        start_row = self.get_topn_row(next_res);
                    }
                }
                None => {
                    start_row = invalid_row.clone();
                    break;
                }
            }
        }

        // fetch end row
        if limit == 0 {
            return Ok((start_row, invalid_row.clone()));
        }
        let mut end_row = start_row.clone();
        let num_limit = limit - 1;
        for i in 0..num_limit {
            match state_table_iter.next().await {
                Some(next_res) => {
                    if i == num_limit - 1 {
                        end_row = self.get_topn_row(next_res);
                    }
                }
                None => {
                    end_row = invalid_row.clone();
                    break;
                }
            }
        }

        Ok((start_row, end_row))
    }

    pub async fn flush(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.state_table.commit(epoch).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::row_nonnull;

    #[tokio::test]
    async fn test_managed_top_n_state() {
        let store = MemoryStateStore::new();
        let data_types = vec![DataType::Varchar, DataType::Int64];
        let order_types = vec![OrderType::Ascending, OrderType::Ascending];
        let mut managed_state = ManagedTopNStateNew::new(
            Some(10),
            0,
            store,
            TableId::from(0x11),
            data_types.clone(),
            OrderedRowDeserializer::new(data_types, order_types.clone()),
            vec![0, 1],
        );

        let row1 = row_nonnull!["abc".to_string(), 2i64];
        let row2 = row_nonnull!["abc".to_string(), 3i64];
        let row3 = row_nonnull!["abd".to_string(), 3i64];
        let row4 = row_nonnull!["ab".to_string(), 4i64];
        let rows = vec![row1, row2, row3, row4];
        let ordered_rows = rows
            .clone()
            .into_iter()
            .map(|row| OrderedRow::new(row, &order_types))
            .collect::<Vec<_>>();

        let epoch = 1;
        managed_state
            .insert(ordered_rows[3].clone(), rows[3].clone(), epoch)
            .await
            .unwrap();

        // now ("ab", 4)
        let range = managed_state.find_range(0, 1, epoch).await.unwrap();

        assert_eq!(
            range,
            (
                TopNStateRow {
                    ordered_key: Some(ordered_rows[3].clone()),
                    row: rows[3].clone()
                },
                TopNStateRow {
                    ordered_key: Some(ordered_rows[3].clone()),
                    row: rows[3].clone()
                },
            )
        );

        managed_state
            .insert(ordered_rows[2].clone(), rows[2].clone(), epoch)
            .await
            .unwrap();
        let range = managed_state.find_range(1, 1, epoch).await.unwrap();
        // now ("ab", 4) -> ("abd", 3)
        assert_eq!(
            range,
            (
                TopNStateRow {
                    ordered_key: Some(ordered_rows[2].clone()),
                    row: rows[2].clone()
                },
                TopNStateRow {
                    ordered_key: Some(ordered_rows[2].clone()),
                    row: rows[2].clone()
                },
            )
        );

        managed_state
            .insert(ordered_rows[1].clone(), rows[1].clone(), epoch)
            .await
            .unwrap();

        assert_eq!(3, managed_state.total_count());

        // managed_state.flush(epoch).await.unwrap();
        let range = managed_state.find_range(1, 2, epoch).await.unwrap();
        // now ("ab", 4) -> ("abc", 3) -> ("abd", 3)
        assert_eq!(
            range,
            (
                TopNStateRow {
                    ordered_key: Some(ordered_rows[1].clone()),
                    row: rows[1].clone()
                },
                TopNStateRow {
                    ordered_key: Some(ordered_rows[2].clone()),
                    row: rows[2].clone()
                }
            )
        );

        // delete ("abc", 3)
        managed_state
            .delete(&ordered_rows[1].clone(), rows[1].clone(), epoch)
            .await
            .unwrap();

        // insert ("abc", 2)
        managed_state
            .insert(ordered_rows[0].clone(), rows[0].clone(), epoch)
            .await
            .unwrap();

        let range = managed_state.find_range(1, 3, epoch).await.unwrap();
        // now ("ab", 4) -> ("abc", 2) -> ("abd", 3)
        assert_eq!(
            range,
            (
                TopNStateRow {
                    ordered_key: Some(ordered_rows[0].clone()),
                    row: rows[0].clone()
                },
                TopNStateRow {
                    ordered_key: None,
                    row: Row::default(),
                }
            )
        );
    }
}
