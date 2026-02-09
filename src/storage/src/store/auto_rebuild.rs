// Copyright 2026 RisingWave Labs
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

use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_hummock_sdk::key::{FullKey, TableKey, TableKeyRange};

use super::{ReadOptions, StateStoreIter, StateStoreKeyedRowRef, StateStoreRead};
use crate::error::StorageResult;

pub struct AutoRebuildStateStoreReadIter<S: StateStoreRead, F> {
    state_store: Arc<S>,
    iter: S::Iter,
    // closure decides whether to rebuild the iterator; it should reset itself after returning true.
    should_rebuild: F,
    end_bound: Bound<TableKey<Bytes>>,
    options: ReadOptions,
}

impl<S: StateStoreRead, F: FnMut() -> bool> AutoRebuildStateStoreReadIter<S, F> {
    pub async fn new(
        state_store: Arc<S>,
        should_rebuild: F,
        range: TableKeyRange,
        options: ReadOptions,
    ) -> StorageResult<Self> {
        let (start_bound, end_bound) = range;
        let iter = state_store
            .iter((start_bound, end_bound.clone()), options.clone())
            .await?;
        Ok(Self {
            state_store,
            iter,
            should_rebuild,
            end_bound,
            options,
        })
    }
}

impl<S: StateStoreRead, F: FnMut() -> bool + Send> StateStoreIter
    for AutoRebuildStateStoreReadIter<S, F>
{
    async fn try_next(&mut self) -> StorageResult<Option<StateStoreKeyedRowRef<'_>>> {
        let should_rebuild = (self.should_rebuild)();
        if should_rebuild {
            let Some((key, _value)) = self.iter.try_next().await? else {
                return Ok(None);
            };
            let key: FullKey<&[u8]> = key;
            let range_start = Bytes::copy_from_slice(key.user_key.table_key.as_ref());
            let new_iter = self
                .state_store
                .iter(
                    (
                        Bound::Included(TableKey(range_start.clone())),
                        self.end_bound.clone(),
                    ),
                    self.options.clone(),
                )
                .await?;
            self.iter = new_iter;
            let item: Option<StateStoreKeyedRowRef<'_>> = self.iter.try_next().await?;
            if let Some((key, value)) = item {
                assert_eq!(
                    key.user_key.table_key.0,
                    range_start.as_ref(),
                    "the first key should be the previous key"
                );
                Ok(Some((key, value)))
            } else {
                unreachable!(
                    "the first key should be the previous key {:?}, but get None",
                    range_start
                )
            }
        } else {
            self.iter.try_next().await
        }
    }
}

pub mod timeout_auto_rebuild {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::TableKeyRange;
    use tracing::info;

    use super::{AutoRebuildStateStoreReadIter, ReadOptions, StateStoreRead};
    use crate::error::StorageResult;

    pub type TimeoutAutoRebuildIter<S: StateStoreRead> =
        AutoRebuildStateStoreReadIter<S, impl FnMut() -> bool + Send>;

    #[define_opaque(TimeoutAutoRebuildIter)]
    pub async fn iter_with_timeout_rebuild<S: StateStoreRead>(
        state_store: Arc<S>,
        range: TableKeyRange,
        table_id: TableId,
        options: ReadOptions,
        timeout: Duration,
    ) -> StorageResult<TimeoutAutoRebuildIter<S>> {
        const CHECK_TIMEOUT_PERIOD: usize = 100;
        // use a struct here to avoid accidental copy instead of move on primitive usize
        struct Count(usize);
        let mut check_count = Count(0);
        let mut total_count = Count(0);
        let mut curr_iter_item_count = Count(0);
        let mut start_time = Instant::now();
        let initial_start_time = start_time;
        AutoRebuildStateStoreReadIter::new(
            state_store,
            move || {
                check_count.0 += 1;
                curr_iter_item_count.0 += 1;
                total_count.0 += 1;
                if check_count.0 == CHECK_TIMEOUT_PERIOD {
                    check_count.0 = 0;
                    if start_time.elapsed() > timeout {
                        let prev_iter_item_count = curr_iter_item_count.0;
                        curr_iter_item_count.0 = 0;
                        start_time = Instant::now();
                        info!(
                            %table_id,
                            iter_exist_time_secs = initial_start_time.elapsed().as_secs(),
                            prev_iter_item_count,
                            total_iter_item_count = total_count.0,
                            "kv log store iter is rebuilt"
                        );
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            },
            range,
            options,
        )
        .await
    }
}
