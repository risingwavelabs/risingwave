use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::{Result, ToRwResult};
use tikv_client::{KvPair, TransactionClient};
use tokio::sync::OnceCell;

use super::StateStore;
use crate::StateStoreIter;
const SCAN_LIMIT: usize = 100;
#[derive(Clone)]
pub struct TikvStateStore {
    // client: Arc<AsyncOnce<tikv_client::transaction::Client>>,
    // client: Option<Arc<Mutex<tikv_client::transaction::Client>>>,
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
                let client = TransactionClient::new(self.pd.clone()).await.unwrap();
                client
            })
            .await
    }
}
#[async_trait]
impl StateStore for TikvStateStore {
    type Iter = TikvStateStoreIter;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let mut txn = self.client().await.begin_optimistic().await.unwrap();
        let res = txn
            .get(key.to_owned())
            .await
            .map(|x| x.map(Bytes::from))
            .map_err(anyhow::Error::new)
            .to_rw_result();
        txn.commit().await.unwrap();
        res
    }

    async fn scan(&self, prefix: &[u8], limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        let mut data = vec![];

        if limit == Some(0) {
            return Ok(vec![]);
        }
        let scan_limit;
        match limit {
            Some(x) => scan_limit = x as u32,
            None => return Ok(vec![]),
        }

        let mut txn = self.client().await.begin_optimistic().await.unwrap();
        let res: Vec<KvPair> = txn
            .scan(tikv_client::Key::from(prefix.to_vec()).., scan_limit)
            .await
            .unwrap()
            .collect();
        txn.commit().await.unwrap();

        for tikv_client::KvPair(key, value) in res {
            let key = Bytes::copy_from_slice(key.as_ref().into());
            let value = Bytes::from(value);
            if key.starts_with(prefix) {
                data.push((key.clone(), value.clone()));
                if let Some(limit) = limit {
                    if data.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(data)
    }

    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>, _epoch: u64) -> Result<()> {
        let mut txn = self.client().await.begin_optimistic().await.unwrap();
        for (key, value) in kv_pairs {
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

    async fn iter(&self, prefix: &[u8]) -> Result<Self::Iter> {
        Ok(TikvStateStoreIter::new(self.clone(), prefix.to_owned()).await)
    }
}

pub struct TikvStateStoreIter {
    store: TikvStateStore,
    prefix: Vec<u8>,
    index: usize,

    kv_pair_buffer: Vec<tikv_client::KvPair>,
    iter: Option<<Vec<(Bytes, Bytes)> as IntoIterator>::IntoIter>,
}

impl TikvStateStoreIter {
    pub async fn new(store: TikvStateStore, prefix: Vec<u8>) -> Self {
        Self {
            store,
            prefix,
            index: 0,
            kv_pair_buffer: Vec::with_capacity(SCAN_LIMIT),
            iter: None,
        }
    }
}

#[async_trait]
impl StateStoreIter for TikvStateStoreIter {
    type Item = (Bytes, Bytes);

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        // self.store.get_client().await;

        let mut txn = self.store.client().await.begin_optimistic().await.unwrap();
        if self.index == self.kv_pair_buffer.len() {
            let start_key;
            if self.kv_pair_buffer.is_empty() {
                start_key = tikv_client::Key::from(self.prefix.to_vec())
            } else {
                start_key = tikv_client::Key::from(
                    Bytes::copy_from_slice(self.kv_pair_buffer.last().unwrap().0.as_ref().into())
                        .to_vec(),
                )
            }
            self.kv_pair_buffer = txn
                .scan(start_key.., SCAN_LIMIT as u32)
                .await
                .unwrap()
                .collect();

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
        if key.starts_with(&self.prefix) {
            self.index += 1;

            return Ok(Some((key.clone(), value.clone())));
        } else {
            return Ok(None);
        }
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    // use rdkafka::message::ToBytes;
    use tikv_client::Result;

    use super::TikvStateStore;
    use crate::StateStore;

    #[tokio::test]
    async fn test_basic() -> Result<()> {
        let tikv_storage = TikvStateStore::new(vec![
            "ec2-68-79-50-236.cn-northwest-1.compute.amazonaws.com.cn:2379".to_string(),
        ]);

        let anchor = Bytes::from("aa");

        // First batch inserts the anchor and others.
        let batch1 = vec![
            (anchor.clone(), Some(Bytes::from("000"))),
            (Bytes::from("aa1"), Some(Bytes::from("111"))),
            (Bytes::from("aa2"), Some(Bytes::from("222"))),
        ];

        // Second batch modifies the anchor.
        let mut batch2 = vec![
            (Bytes::from("cc"), Some(Bytes::from("333"))),
            (anchor.clone(), Some(Bytes::from("111111"))),
        ];

        // Make sure the batch is sorted.
        batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        // Third batch deletes the anchor
        let mut batch3 = vec![
            (Bytes::from("dd"), Some(Bytes::from("444"))),
            (Bytes::from("ee"), Some(Bytes::from("555"))),
            (anchor.clone(), None),
        ];

        // Make sure the batch is sorted.

        batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        let res = tikv_storage.ingest_batch(batch1, 0).await;
        assert!(res.is_ok());

        // let gc_res = tikv_storage.gc().await;
        // assert!(gc_res.is_ok());
        // get "aa"
        let value = tikv_storage.get(&anchor).await.unwrap().unwrap();
        assert_eq!(value, Bytes::from("000"));

        // scan aa1, aa2
        let scan_res = tikv_storage.scan(&anchor, Some(2)).await.unwrap();
        assert_eq!(scan_res.len(), 2);
        assert_eq!(scan_res[1].1, Bytes::from("111"));

        // scan non-existent prefix
        let scan_res = tikv_storage
            .scan(&Bytes::from("non-existent prefix"), Some(2))
            .await
            .unwrap();
        assert_eq!(scan_res.len(), 0);

        // get non-existent prefix
        let value = tikv_storage.get(&Bytes::from("ab")).await.unwrap();
        assert_eq!(value, None);

        let res = tikv_storage.ingest_batch(batch2, 0).await;
        assert!(res.is_ok());
        let value = tikv_storage.get(&anchor).await.unwrap().unwrap();
        assert_eq!(value, Bytes::from("111111"));

        let res = tikv_storage.ingest_batch(batch3, 0).await;
        assert!(res.is_ok());
        let value = tikv_storage.get(&anchor).await.unwrap();
        assert_eq!(value, None);

        Ok(())
    }
}
