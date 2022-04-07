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

use std::future::Future;
use std::ops::Bound::Excluded;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::error::{Result, ToRwResult};
use tikv_client::{BoundRange, KvPair, TransactionClient};
use tokio::sync::OnceCell;

use super::StateStore;
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStoreIter};

const SCAN_LIMIT: usize = 100;
#[derive(Clone)]
pub struct TikvStateStore {
    client: Arc<OnceCell<tikv_client::transaction::Client>>,
    pd: Vec<String>,
}

impl TikvStateStore {
    pub fn new(pd_endpoints: Vec<String>) -> Self {
        println!("\n use tikv stateStore \n");
        Self {
            client: Arc::new(OnceCell::new()),
            pd: pd_endpoints,
        }
    }
    pub async fn client(&self) -> &tikv_client::transaction::Client {
        self.client
            .get_or_init(|| async {
                let client = TransactionClient::new(self.pd.clone(), None).await.unwrap();
                client
            })
            .await
    }
}

impl StateStore for TikvStateStore {
    type Iter<'a> = TikvStateStoreIter;
    define_state_store_associated_type!();

    fn get<'a>(&'a self, key: &'a [u8], _epoch: u64) -> Self::GetFuture<'_> {
        async move {
            let mut txn = self.client().await.begin_optimistic().await.unwrap();
            let res = txn.get(key.to_owned()).await.expect("key not found");
            txn.commit().await.unwrap();
            Ok(res.map(Bytes::from))
        }
    }

    fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        _epoch: u64,
    ) -> Self::ScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            let mut data = vec![];

            if limit == Some(0) {
                return Ok(vec![]);
            }
            let scan_limit = match limit {
                Some(x) => x as u32,
                None => u32::MAX,
            };

            let range = (
                key_range.start_bound().map(|b| b.as_ref().to_owned()),
                key_range.end_bound().map(|b| b.as_ref().to_owned()),
            );

            let mut txn = self.client().await.begin_optimistic().await.unwrap();
            let res: Vec<KvPair> = txn
                .scan(BoundRange::from(range), scan_limit)
                .await
                .unwrap()
                .collect();
            txn.commit().await.unwrap();

            for tikv_client::KvPair(key, value) in res {
                let key = Bytes::copy_from_slice(key.as_ref().into());
                let value = Bytes::from(value);
                data.push((key.clone(), value.clone()));
                if let Some(limit) = limit {
                    if data.len() >= limit {
                        break;
                    }
                }
            }

            Ok(data)
        }
    }

    fn reverse_scan<R, B>(
        &self,
        _key_range: R,
        _limit: Option<usize>,
        _epoch: u64,
    ) -> Self::ReverseScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        _epoch: u64,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            let mut txn = self.client().await.begin_optimistic().await.unwrap();
            for (key, value) in kv_pairs {
                let value = value.user_value();
                match value {
                    Some(value) => {
                        txn.put(tikv_client::Key::from(key.to_vec()), value.to_vec())
                            .await
                            .map_err(anyhow::Error::new)
                            .to_rw_result()?;
                    }
                    None => {
                        txn.delete(tikv_client::Key::from(key.to_vec()))
                            .await
                            .map_err(anyhow::Error::new)
                            .to_rw_result()?;
                    }
                }
            }
            txn.commit().await.unwrap();
            Ok(())
        }
    }

    fn replicate_batch(
        &self,
        _kv_pairs: Vec<(Bytes, StorageValue)>,
        _epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move { unimplemented!() }
    }

    fn iter<R, B>(&self, key_range: R, _epoch: u64) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            let range = (
                key_range.start_bound().map(|b| b.as_ref().to_owned()),
                key_range.end_bound().map(|b| b.as_ref().to_owned()),
            );
            Ok(TikvStateStoreIter::new(self.clone(), range).await)
        }
    }

    fn reverse_iter<R, B>(&self, _key_range: R, _epoch: u64) -> Self::ReverseIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn wait_epoch(&self, _epoch: u64) -> Self::WaitEpochFuture<'_> {
        async move { unimplemented!() }
    }

    fn sync(&self, _epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move { unimplemented!() }
    }
}

pub struct TikvStateStoreIter {
    store: TikvStateStore,
    key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    index: usize,
    kv_pair_buffer: Vec<tikv_client::KvPair>,
}

impl TikvStateStoreIter {
    pub async fn new(store: TikvStateStore, key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Self {
        Self {
            store,
            key_range,
            index: 0,
            kv_pair_buffer: Vec::with_capacity(SCAN_LIMIT),
        }
    }
}

impl StateStoreIter for TikvStateStoreIter {
    type Item = (Bytes, Bytes);
    type NextFuture<'a> = impl Future<Output = Result<Option<Self::Item>>>;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            let mut txn = self.store.client().await.begin_optimistic().await.unwrap();
            if self.index == self.kv_pair_buffer.len() {
                let range = if self.kv_pair_buffer.is_empty() {
                    self.key_range.clone()
                } else {
                    (
                        Excluded(
                            Bytes::copy_from_slice(
                                self.kv_pair_buffer.last().unwrap().0.as_ref().into(),
                            )
                            .to_vec(),
                        ),
                        self.key_range.1.clone(),
                    )
                };
                self.kv_pair_buffer = txn.scan(range, SCAN_LIMIT as u32).await.unwrap().collect();

                self.index = 0;
            }
            if self.kv_pair_buffer.is_empty() {
                return Ok(None);
            }
            let key = self.kv_pair_buffer[self.index].0.clone();
            let value = self.kv_pair_buffer[self.index].1.clone();
            let key = &Bytes::copy_from_slice(key.as_ref().into());
            let value = &Bytes::from(value);
            txn.commit().await.unwrap();
            self.index += 1;

            Ok(Some((key.clone(), value.clone())))
        }
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use risingwave_hummock_sdk::key::next_key;

    use super::{TikvStateStore, *};
    use crate::StateStore;

    #[tokio::test]
    #[ignore]
    async fn test_basic() {
        let tikv_storage = TikvStateStore::new(vec!["127.0.0.1:2379".to_string()]);

        let anchor = Bytes::from("aa");
        let anchor_vec = anchor.to_vec();
        let range = anchor_vec.clone()..next_key(anchor_vec.as_slice());

        // First batch inserts the anchor and others.
        let batch1 = vec![
            (anchor.clone(), StorageValue::new_default_put("000")),
            (Bytes::from("aa1"), StorageValue::new_default_put("111")),
            (Bytes::from("aa2"), StorageValue::new_default_put("222")),
        ];

        // Second batch modifies the anchor.
        let mut batch2 = vec![
            (Bytes::from("cc"), StorageValue::new_default_put("333")),
            (anchor.clone(), StorageValue::new_default_put("111111")),
        ];

        // Make sure the batch is sorted.
        batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        // Third batch deletes the anchor
        let mut batch3 = vec![
            (Bytes::from("dd"), StorageValue::new_default_put("444")),
            (Bytes::from("ee"), StorageValue::new_default_put("555")),
            (anchor.clone(), StorageValue::new_default_put("")),
        ];

        // Make sure the batch is sorted.
        let epoch1 = 1;
        batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        let res = tikv_storage.ingest_batch(batch1, epoch1).await;
        assert!(res.is_ok());

        // get "aa"
        let value = tikv_storage.get(&anchor, epoch1).await.unwrap().unwrap();
        assert_eq!(value, Bytes::from("000"));

        // scan aa1, aa2
        let scan_res = tikv_storage.scan(range, Some(2), epoch1).await.unwrap();
        assert_eq!(scan_res.len(), 2);
        assert_eq!(scan_res[1].1, Bytes::from("111"));

        // scan non-existent prefix
        let scan_res = tikv_storage
            .scan(
                (Bytes::from("non-existent prefix").to_vec())..,
                Some(2),
                epoch1,
            )
            .await
            .unwrap();
        assert_eq!(scan_res.len(), 0);

        // get non-existent prefix
        let value = tikv_storage.get(&Bytes::from("ab"), epoch1).await.unwrap();
        assert_eq!(value, None);

        let epoch2 = epoch1 + 1;
        let res = tikv_storage.ingest_batch(batch2, epoch2).await;
        assert!(res.is_ok());
        let value = tikv_storage.get(&anchor, epoch2).await.unwrap().unwrap();
        assert_eq!(value, Bytes::from("111111"));

        let epoch3 = epoch2 + 1;
        let res = tikv_storage.ingest_batch(batch3, epoch3).await;
        assert!(res.is_ok());
        let value = tikv_storage.get(&anchor, epoch3).await.unwrap();
        assert_eq!(value, None);
    }
}
