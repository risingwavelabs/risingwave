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

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::{EpochExt, test_epoch};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_hummock_sdk::key::{KeyPayloadType, TableKey, prefixed_range_with_vnode};
use risingwave_storage::hummock::iterator::test_utils::{
    iterator_test_table_key_of, iterator_test_value_of,
};
use risingwave_storage::store::{
    AutoRebuildStateStoreReadIter, LocalStateStore, NewLocalOptions, NewReadSnapshotOptions,
    ReadOptions, SealCurrentEpochOptions, StateStoreRead, StateStoreWriteEpochControl,
};
use risingwave_storage::{StateStore, StateStoreIter};

use crate::local_state_store_test_utils::LocalStateStoreTestExt;
use crate::test_utils::prepare_hummock_test_env;

const TEST_TABLE_ID: TableId = TableId::new(233);

#[tokio::test]
async fn test_auto_rebuild_iter() {
    {
        let test_env = prepare_hummock_test_env().await;
        test_env.register_table_id(TEST_TABLE_ID).await;
        let mut state_store = test_env
            .storage
            .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
            .await;
        let epoch = test_epoch(1);
        test_env
            .storage
            .start_epoch(epoch, HashSet::from_iter([TEST_TABLE_ID]));
        state_store.init_for_test(epoch).await.unwrap();
        let key_count = 100;
        let pairs = (0..key_count)
            .map(|i| {
                let key = iterator_test_table_key_of(i);
                let value = iterator_test_value_of(i);
                (TableKey(Bytes::from(key)), Bytes::from(value))
            })
            .collect_vec();
        for (key, value) in &pairs {
            state_store
                .insert(key.clone(), value.clone(), None)
                .unwrap();
        }
        state_store.flush().await.unwrap();
        state_store.seal_current_epoch(epoch.next_epoch(), SealCurrentEpochOptions::for_test());
        test_env.commit_epoch(epoch).await;
        let state_store = Arc::new(test_env.storage.clone());

        async fn validate(
            mut kv_iter: impl Iterator<Item = (TableKey<Bytes>, Bytes)>,
            mut iter: impl StateStoreIter,
        ) {
            while let Some((key, value)) = iter.try_next().await.unwrap() {
                let (k, v) = kv_iter.next().unwrap();
                assert_eq!(key.user_key.table_key, k.to_ref());
                assert_eq!(v.as_ref(), value);
            }
            assert!(kv_iter.next().is_none());
        }

        let read_options = ReadOptions {
            ..Default::default()
        };
        let key_range = prefixed_range_with_vnode(
            (
                Bound::<KeyPayloadType>::Unbounded,
                Bound::<KeyPayloadType>::Unbounded,
            ),
            VirtualNode::ZERO,
        );

        let kv_iter = pairs.clone().into_iter();
        let snapshot = state_store
            .new_read_snapshot(
                HummockReadEpoch::NoWait(epoch),
                NewReadSnapshotOptions {
                    table_id: TEST_TABLE_ID,
                    table_option: TableOption::default(),
                },
            )
            .await
            .unwrap();
        let snapshot = Arc::new(snapshot);
        let iter = snapshot
            .iter(key_range.clone(), read_options.clone())
            .await
            .unwrap();
        validate(kv_iter, iter).await;

        let kv_iter = pairs.clone().into_iter();
        let mut count = 0;
        let count_mut_ref = &mut count;
        let rebuild_period = 8;
        let mut rebuild_count = 0;
        let rebuild_count_mut_ref = &mut rebuild_count;
        let iter = AutoRebuildStateStoreReadIter::new(
            snapshot,
            move || {
                *count_mut_ref += 1;
                if *count_mut_ref % rebuild_period == 0 {
                    *rebuild_count_mut_ref += 1;
                    true
                } else {
                    false
                }
            },
            key_range.clone(),
            read_options,
        )
        .await
        .unwrap();
        validate(kv_iter, iter).await;
        assert_eq!(count, key_count + 1); // with an extra call on the last None
        assert_eq!(rebuild_count, key_count / rebuild_period);
    }
}
