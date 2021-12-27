use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use hyper::service::{make_service_fn, service_fn};
use hyper::Server;
use log::info;
use risingwave_common::error::Result;

use super::iterator::UserKeyIterator;
use super::HummockStorage;
use crate::hummock::key::next_key;
use crate::metrics::StorageMetricsManager;
use crate::{StateStore, StateStoreIter};

/// A wrapper over [`HummockStorage`] as a state store.
///
/// TODO: this wrapper introduces extra overhead of async trait, may be turned into an enum if
/// possible.
#[derive(Clone)]
pub struct HummockStateStore {
    pub storage: HummockStorage,
}

impl HummockStateStore {
    pub fn new(storage: HummockStorage) -> Self {
        Self { storage }
    }

    pub fn boot_metrics_listener(&self, listen_addr: String) {
        if !self.storage.get_options().stats_enabled {
            info!("Failed to start because Hummock is not configured to enabled metrics.");
            return;
        }

        tokio::spawn(async move {
            println!(
                "Prometheus listener for Prometheus is set up on http://{}",
                listen_addr
            );

            let listen_socket_addr: SocketAddr = listen_addr.parse().unwrap();
            let serve_future =
                Server::bind(&listen_socket_addr).serve(make_service_fn(|_| async {
                    Ok::<_, hyper::Error>(service_fn(
                        StorageMetricsManager::hummock_metrics_service,
                    ))
                }));

            if let Err(err) = serve_future.await {
                eprintln!("server error: {}", err);
            }
        });
    }
}

// Note(eric): How about removing HummockStateStore and just impl StateStore for HummockStorage?
#[async_trait]
impl StateStore for HummockStateStore {
    type Iter = HummockStateStoreIter;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let value = self.storage.get(key).await?;
        let value = value.map(Bytes::from);
        Ok(value)
    }

    async fn ingest_batch(&self, mut kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        // TODO: reduce the redundant vec clone
        kv_pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        self.storage
            .write_batch(
                kv_pairs
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            )
            .await?;

        Ok(())
    }

    async fn iter(&self, prefix: &[u8]) -> Result<Self::Iter> {
        let range = prefix.to_owned()..next_key(prefix);
        let mut inner = self.storage.range_scan(range).await?;
        inner.rewind().await?;
        Ok(HummockStateStoreIter(inner))
    }
}

pub struct HummockStateStoreIter(UserKeyIterator);

#[async_trait]
impl StateStoreIter for HummockStateStoreIter {
    // TODO: directly return `&[u8]` to user instead of `Bytes`.
    type Item = (Bytes, Bytes);

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        let iter = &mut self.0;

        if iter.is_valid() {
            let kv = (
                Bytes::copy_from_slice(iter.key()),
                Bytes::copy_from_slice(iter.value()),
            );
            iter.next().await?;
            Ok(Some(kv))
        } else {
            Ok(None)
        }
    }
}
