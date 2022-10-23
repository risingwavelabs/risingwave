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

use std::sync::Arc;

use risingwave_hummock_sdk::key::key_with_epoch;

use crate::assert_bytes_eq;
use crate::hummock::iterator::test_utils::mock_sstable_store;
use crate::hummock::iterator::HummockIterator;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::test_utils::{
    default_builder_opt_for_test, default_writer_opt_for_test, gen_test_sstable,
    gen_test_sstable_data, put_sst, test_key_of, test_value_of, TEST_KEYS_COUNT,
};
use crate::hummock::value::HummockValue;
use crate::hummock::{SstableIterator, SstableIteratorType};
use crate::monitor::StoreLocalStatistic;

#[tokio::test]
#[cfg(feature = "failpoints")]
async fn test_failpoints_table_read() {
    let mem_read_err_fp = "mem_read_err";
    // build remote table
    let sstable_store = mock_sstable_store();

    // We should close buffer, so that table iterator must read in object_stores
    let kv_iter =
        (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i))));
    let info = gen_test_sstable(
        default_builder_opt_for_test(),
        0,
        kv_iter,
        sstable_store.clone(),
    )
    .await;

    let mut stats = StoreLocalStatistic::default();
    let mut sstable_iter = SstableIterator::create(
        sstable_store
            .sstable(&info.get_sstable_info(), &mut stats)
            .await
            .unwrap(),
        sstable_store,
        Arc::new(SstableIteratorReadOptions::default()),
    );
    sstable_iter.rewind().await.unwrap();

    sstable_iter.seek(&test_key_of(500)).await.unwrap();
    assert_eq!(sstable_iter.key(), test_key_of(500));
    // Injection failure to read object_store
    fail::cfg(mem_read_err_fp, "return").unwrap();

    let seek_key = test_key_of(600 * 2 - 1);
    let result = sstable_iter.seek(&seek_key).await;
    assert!(result.is_err());

    assert_eq!(sstable_iter.key(), test_key_of(500));
    fail::remove(mem_read_err_fp);
    sstable_iter.seek(&seek_key).await.unwrap();
    assert_eq!(sstable_iter.key(), test_key_of(600));
}

#[tokio::test]
#[cfg(feature = "failpoints")]
async fn test_failpoints_vacuum_and_metadata() {
    let data_upload_err = "data_upload_err";
    let mem_upload_err = "mem_upload_err";
    let mem_delete_err = "mem_delete_err";
    let sstable_store = mock_sstable_store();
    // when upload data is successful, but upload meta is fail and delete is fail

    fail::cfg_callback(data_upload_err, move || {
        fail::cfg(mem_upload_err, "return").unwrap();
        fail::cfg(mem_delete_err, "return").unwrap();
        fail::remove(data_upload_err);
    })
    .unwrap();

    let table_id = 0;
    let kv_iter =
        (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i))));
    let (data, meta) = gen_test_sstable_data(default_builder_opt_for_test(), kv_iter).await;
    let result = put_sst(
        table_id,
        data.clone(),
        meta.clone(),
        sstable_store.clone(),
        default_writer_opt_for_test(),
    )
    .await;
    assert!(result.is_err());

    fail::remove(data_upload_err);
    fail::remove(mem_delete_err);
    fail::remove(mem_upload_err);

    let info = put_sst(
        table_id,
        data,
        meta,
        sstable_store.clone(),
        default_writer_opt_for_test(),
    )
    .await
    .unwrap();

    let mut stats = StoreLocalStatistic::default();

    let mut sstable_iter = SstableIterator::create(
        sstable_store.sstable(&info, &mut stats).await.unwrap(),
        sstable_store,
        Arc::new(SstableIteratorReadOptions::default()),
    );
    let mut cnt = 0;
    sstable_iter.rewind().await.unwrap();
    while sstable_iter.is_valid() {
        let key = sstable_iter.key();
        let value = sstable_iter.value();
        assert_bytes_eq!(key, test_key_of(cnt));
        assert_bytes_eq!(value.into_user_value().unwrap(), test_value_of(cnt));
        cnt += 1;
        sstable_iter.next().await.unwrap();
    }
    assert_eq!(cnt, TEST_KEYS_COUNT);
}
