// Copyright 2023 RisingWave Labs
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

use std::collections::HashSet;
use std::ops::{Bound, Deref};

use futures::{pin_mut, StreamExt};
use risingwave_common::array::JsonbVal;
use risingwave_common::catalog::{DatabaseId, SchemaId};
use risingwave_common::constants::hummock::PROPERTIES_RETENTION_SECOND_KEY;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{ScalarImpl, ScalarRef, ScalarRefImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::{bail, row};
use risingwave_connector::source::{SplitId, SplitImpl, SplitMetaData};
use risingwave_hummock_sdk::key::next_key;
use risingwave_pb::catalog::table::TableType;
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::common::{PbColumnOrder, PbDirection, PbOrderType};
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::plan_common::{ColumnCatalog, ColumnDesc};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::error::StreamExecutorError;
use crate::executor::StreamExecutorResult;

const COMPLETE_SPLIT_PREFIX: &str = "SsGLdzRDqBuKzMf9bDap";

pub struct SourceStateTableHandler<S: StateStore> {
    pub state_store: StateTable<S>,
}

impl<S: StateStore> SourceStateTableHandler<S> {
    pub async fn from_table_catalog(table_catalog: &ProstTable, store: S) -> Self {
        // The state of source should not be cleaned up by retention_seconds
        assert!(!table_catalog
            .properties
            .contains_key(&String::from(PROPERTIES_RETENTION_SECOND_KEY)));

        Self {
            state_store: StateTable::from_table_catalog(table_catalog, store, None).await,
        }
    }

    pub fn init_epoch(&mut self, epoch: EpochPair) {
        self.state_store.init_epoch(epoch);
    }

    fn string_to_scalar(rhs: impl Into<String>) -> ScalarImpl {
        ScalarImpl::Utf8(rhs.into().into_boxed_str())
    }

    pub(crate) async fn get(&self, key: SplitId) -> StreamExecutorResult<Option<OwnedRow>> {
        self.state_store
            .get_row(row::once(Some(Self::string_to_scalar(key.deref()))))
            .await
            .map_err(StreamExecutorError::from)
    }

    // this method should only be used by `FsSourceExecutor
    pub(crate) async fn get_all_completed(&self) -> StreamExecutorResult<HashSet<SplitId>> {
        let start = Bound::Excluded(row::once(Some(Self::string_to_scalar(
            COMPLETE_SPLIT_PREFIX,
        ))));
        let next = next_key(COMPLETE_SPLIT_PREFIX.as_bytes());
        let end = Bound::Excluded(row::once(Some(Self::string_to_scalar(
            String::from_utf8(next).unwrap(),
        ))));

        // all source executor has vnode id zero
        let iter = self
            .state_store
            .iter_with_pk_range(
                &(start, end),
                VirtualNode::ZERO,
                PrefetchOptions::new_for_exhaust_iter(),
            )
            .await?;

        let mut set = HashSet::new();
        pin_mut!(iter);
        while let Some(row) = iter.next().await {
            let row = row?;
            if let Some(ScalarRefImpl::Jsonb(jsonb_ref)) = row.datum_at(1) {
                let split = SplitImpl::restore_from_json(jsonb_ref.to_owned_scalar())?;
                let fs = split
                    .as_fs()
                    .unwrap_or_else(|| panic!("split {:?} is not fs", split));
                if fs.offset == fs.size {
                    let split_id = split.id();
                    set.insert(split_id);
                }
            }
        }
        Ok(set)
    }

    async fn set_complete(&mut self, key: SplitId, value: JsonbVal) -> StreamExecutorResult<()> {
        let row = [
            Some(Self::string_to_scalar(format!(
                "{}{}",
                COMPLETE_SPLIT_PREFIX,
                key.deref()
            ))),
            Some(ScalarImpl::Jsonb(value)),
        ];
        if let Some(prev_row) = self.get(key).await? {
            self.state_store.delete(prev_row);
        }
        self.state_store.insert(row);
        Ok(())
    }

    /// set all complete
    /// can only used by `FsSourceExecutor`
    pub(crate) async fn set_all_complete<SS>(&mut self, states: Vec<SS>) -> StreamExecutorResult<()>
    where
        SS: SplitMetaData,
    {
        if states.is_empty() {
            // TODO should be a clear Error Code
            bail!("states require not null");
        } else {
            for split in states {
                self.set_complete(split.id(), split.encode_to_json())
                    .await?;
            }
        }
        Ok(())
    }

    async fn set(&mut self, key: SplitId, value: JsonbVal) -> StreamExecutorResult<()> {
        let row = [
            Some(Self::string_to_scalar(key.deref())),
            Some(ScalarImpl::Jsonb(value)),
        ];
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
                self.set(split_impl.id(), split_impl.encode_to_json())
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
            Some(row) => match row.datum_at(1) {
                Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                    Some(SplitImpl::restore_from_json(jsonb_ref.to_owned_scalar())?)
                }
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
        make_column(TypeName::Jsonb, 1),
    ];
    ProstTable {
        id,
        schema_id: SchemaId::placeholder().schema_id,
        database_id: DatabaseId::placeholder().database_id,
        name: String::new(),
        columns,
        table_type: TableType::Internal as i32,
        value_indices: vec![0, 1],
        pk: vec![PbColumnOrder {
            column_index: 0,
            order_type: Some(PbOrderType {
                direction: PbDirection::Ascending as _,
            }),
        }],
        ..Default::default()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{Datum, ScalarImpl};
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_connector::source::kafka::KafkaSplit;
    use risingwave_storage::memory::MemoryStateStore;
    use serde_json::Value;

    use super::*;

    #[tokio::test]
    async fn test_from_table_catalog() {
        let store = MemoryStateStore::new();
        let mut state_table =
            StateTable::from_table_catalog(&default_source_internal_table(0x2333), store, None)
                .await;
        let a: Arc<str> = String::from("a").into();
        let a: Datum = Some(ScalarImpl::Utf8(a.as_ref().into()));
        let b: JsonbVal = serde_json::from_str::<Value>("{\"k1\": \"v1\", \"k2\": 11}").unwrap().into();
        let b: Datum = Some(ScalarImpl::Jsonb(b));

        let init_epoch_num = 100100;
        let init_epoch = EpochPair::new_test_epoch(init_epoch_num);
        let next_epoch = EpochPair::new_test_epoch(init_epoch_num + 1);

        state_table.init_epoch(init_epoch);
        state_table.insert(OwnedRow::new(vec![a.clone(), b.clone()]));
        state_table.commit(next_epoch).await.unwrap();

        let a: Arc<str> = String::from("a").into();
        let a: Datum = Some(ScalarImpl::Utf8(a.as_ref().into()));
        let _resp = state_table.get_row(&OwnedRow::new(vec![a])).await.unwrap();
    }

    #[tokio::test]
    async fn test_set_and_get() -> StreamExecutorResult<()> {
        let store = MemoryStateStore::new();
        let mut state_table_handler = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            store,
        )
        .await;
        let split_impl = SplitImpl::Kafka(KafkaSplit::new(0, Some(0), None, "test".into()));
        let serialized = split_impl.encode_to_bytes();
        let serialized_json = split_impl.encode_to_json();

        let epoch_1 = EpochPair::new_test_epoch(1);
        let epoch_2 = EpochPair::new_test_epoch(2);
        let epoch_3 = EpochPair::new_test_epoch(3);

        state_table_handler.init_epoch(epoch_1);
        state_table_handler
            .take_snapshot(vec![split_impl.clone()])
            .await?;
        state_table_handler.state_store.commit(epoch_2).await?;

        state_table_handler.state_store.commit(epoch_3).await?;

        match state_table_handler
            .try_recover_from_state_store(&split_impl)
            .await?
        {
            Some(s) => {
                assert_eq!(s.encode_to_bytes(), serialized);
                assert_eq!(s.encode_to_json(), serialized_json);
            }
            None => unreachable!(),
        }
        Ok(())
    }
}
