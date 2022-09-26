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

use std::ops::Deref;

use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::bail;
use risingwave_common::catalog::{DatabaseId, SchemaId};
use risingwave_common::types::ScalarImpl;
use risingwave_connector::source::{SplitId, SplitImpl, SplitMetaData};
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::plan_common::{ColumnCatalog, ColumnDesc, ColumnOrder};
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use crate::executor::error::StreamExecutorError;
use crate::executor::StreamExecutorResult;

#[derive(Clone)]
pub struct SourceStateTableHandler<S: StateStore> {
    pub state_store: StateTable<S>,
}

impl<S: StateStore> SourceStateTableHandler<S> {
    pub fn from_table_catalog(table_catalog: &ProstTable, store: S) -> Self {
        Self {
            state_store: StateTable::from_table_catalog(table_catalog, store, None),
        }
    }

    pub fn init_epoch(&mut self, epoch: u64) {
        self.state_store.init_epoch(epoch);
    }

    fn string_to_scalar(rhs: impl Into<String>) -> ScalarImpl {
        ScalarImpl::Utf8(rhs.into())
    }

    pub(crate) async fn get(&self, key: SplitId) -> StreamExecutorResult<Option<Row>> {
        self.state_store
            .get_row(&Row::new(vec![Some(Self::string_to_scalar(key.deref()))]))
            .await
            .map_err(StreamExecutorError::from)
    }

    async fn set(&mut self, key: SplitId, value: Bytes) -> StreamExecutorResult<()> {
        let row = Row::new(vec![
            Some(Self::string_to_scalar(key.deref())),
            Some(Self::string_to_scalar(
                String::from_utf8_lossy(&value).to_string(),
            )),
        ]);
        match self.get(key).await? {
            Some(prev_row) => {
                self.state_store.update(prev_row, row);
            }
            None => {
                self.state_store.insert(row);
            }
        }
        Ok(())
    }

    /// This function provides the ability to persist the source state
    /// and needs to be invoked by the ``SourceReader`` to call it,
    /// and will return the error when the dependent ``StateStore`` handles the error.
    /// The caller should ensure that the passed parameters are not empty.
    pub async fn take_snapshot<SS>(&mut self, states: Vec<SS>) -> StreamExecutorResult<()>
    where
        SS: SplitMetaData,
    {
        if states.is_empty() {
            // TODO should be a clear Error Code
            bail!("states require not null");
        } else {
            for split_impl in states {
                self.set(split_impl.id(), split_impl.encode_to_bytes())
                    .await?;
            }
        }
        Ok(())
    }

    ///
    pub async fn try_recover_from_state_store(
        &mut self,
        stream_source_split: &SplitImpl,
    ) -> StreamExecutorResult<Option<SplitImpl>> {
        Ok(match self.get(stream_source_split.id()).await? {
            None => None,
            Some(row) => match row.0.get(1).unwrap() {
                Some(ScalarImpl::Utf8(s)) => Some(SplitImpl::restore_from_bytes(s.as_bytes())?),
                _ => unreachable!(),
            },
        })
    }
}

// align with schema defined in `LogicalSource::infer_internal_table_catalog`. The function is used
// for test purpose and should not be used in production.
pub fn default_source_internal_table(id: u32) -> ProstTable {
    let make_column = |column_type: TypeName, column_id: i32| -> ColumnCatalog {
        ColumnCatalog {
            column_desc: Some(ColumnDesc {
                column_type: Some(DataType {
                    type_name: column_type as i32,
                    ..Default::default()
                }),
                column_id,
                ..Default::default()
            }),
            is_hidden: false,
        }
    };

    let columns = vec![
        make_column(TypeName::Varchar, 0),
        make_column(TypeName::Varchar, 1),
    ];
    ProstTable {
        id,
        schema_id: SchemaId::placeholder() as u32,
        database_id: DatabaseId::placeholder() as u32,
        name: String::new(),
        columns,
        is_index: false,
        value_indices: vec![0, 1],
        pk: vec![ColumnOrder {
            index: 0,
            order_type: 1,
        }],
        ..Default::default()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use risingwave_common::array::Row;
    use risingwave_common::types::{Datum, ScalarImpl};
    use risingwave_connector::source::kafka::KafkaSplit;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;

    #[tokio::test]
    async fn test_from_table_catalog() {
        let store = MemoryStateStore::new();
        let mut state_table =
            StateTable::from_table_catalog(&default_source_internal_table(0x2333), store, None);
        let a: Arc<str> = String::from("a").into();
        let a: Datum = Some(ScalarImpl::Utf8(a.as_ref().into()));
        let b: Arc<str> = String::from("b").into();
        let b: Datum = Some(ScalarImpl::Utf8(b.as_ref().into()));

        state_table.init_epoch(100100);
        state_table.insert(Row::new(vec![a.clone(), b.clone()]));
        state_table.commit(100100).await.unwrap();

        let a: Arc<str> = String::from("a").into();
        let a: Datum = Some(ScalarImpl::Utf8(a.as_ref().into()));
        let _resp = state_table.get_row(&Row::new(vec![a])).await.unwrap();
    }

    #[tokio::test]
    async fn test_set_and_get() -> StreamExecutorResult<()> {
        let store = MemoryStateStore::new();
        let mut state_table_handler = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            store,
        );
        let split_impl = SplitImpl::Kafka(KafkaSplit::new(0, Some(0), None, "test".into()));
        let serialized = split_impl.encode_to_bytes();

        state_table_handler.init_epoch(1);
        state_table_handler
            .take_snapshot(vec![split_impl.clone()])
            .await?;
        state_table_handler.state_store.commit(2).await?;

        state_table_handler.state_store.commit(3).await?;

        match state_table_handler
            .try_recover_from_state_store(&split_impl)
            .await?
        {
            Some(s) => {
                assert_eq!(s.encode_to_bytes(), serialized);
            }
            None => unreachable!(),
        }
        Ok(())
    }
}
