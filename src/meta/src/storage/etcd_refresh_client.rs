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

use etcd_client::{
    ConnectOptions, DeleteOptions, DeleteResponse, GetOptions, GetResponse, PutOptions,
    PutResponse, Txn, TxnResponse,
};
use tokio::sync::RwLock;

struct ClientWithVersion {
    client: etcd_client::Client,
    // to avoid duplicate update.
    version: i32,
}

impl ClientWithVersion {
    fn new(client: etcd_client::Client, version: i32) -> Self {
        Self { client, version }
    }
}

/// Asynchronous `etcd` client using v3 API, wrapped with a refresh logic. Since the `etcd` client
/// doesn't provide any token refresh apis like golang library, we have to implement it ourselves:
/// https://github.com/etcdv3/etcd-client/issues/45.
///
/// The client will re-connect to etcd when An `Unauthenticated` error is encountered with message
/// "invalid auth token". The token used for authentication will be refreshed.
#[derive(Clone)]
pub struct EtcdRefreshClient {
    // (client, version)
    inner: Arc<RwLock<ClientWithVersion>>,

    endpoints: Vec<String>,
    options: Option<ConnectOptions>,
}

pub type Result<T> = std::result::Result<T, etcd_client::Error>;

impl EtcdRefreshClient {
    pub async fn connect(endpoints: Vec<String>, options: Option<ConnectOptions>) -> Result<Self> {
        let client = etcd_client::Client::connect(&endpoints, options.clone()).await?;

        Ok(Self {
            inner: Arc::new(RwLock::new(ClientWithVersion::new(client, 0))),
            endpoints,
            options,
        })
    }

    pub async fn try_refresh_conn(&self, old_version: i32) -> Result<()> {
        let mut guard = self.inner.write().await;
        if guard.version == old_version {
            let client =
                etcd_client::Client::connect(&self.endpoints, self.options.clone()).await?;
            *guard = ClientWithVersion::new(client, old_version + 1);
        }
        Ok(())
    }

    fn should_refresh(err: &etcd_client::Error) -> bool {
        match err {
            etcd_client::Error::GRpcStatus(status) => {
                status.code() == tonic::Code::Unauthenticated
                    && status.message().contains("invalid auth token")
            }
            _ => false,
        }
    }

    #[inline]
    pub async fn get(
        &self,
        key: impl Into<Vec<u8>> + Clone,
        options: Option<GetOptions>,
    ) -> Result<GetResponse> {
        let (resp, version) = {
            let inner = self.inner.read().await;
            (
                inner
                    .client
                    .to_owned()
                    .get(key.clone(), options.clone())
                    .await,
                inner.version,
            )
        };
        if let Err(err) = &resp && Self::should_refresh(err) {
            self.try_refresh_conn(version).await?;
            let inner = self.inner.read().await;
            inner.client.to_owned().get(key, options).await
        } else {
            resp
        }
    }

    #[inline]
    pub async fn put(
        &self,
        key: impl Into<Vec<u8>> + Clone,
        value: impl Into<Vec<u8>> + Clone,
        options: Option<PutOptions>,
    ) -> Result<PutResponse> {
        let (resp, version) = {
            let inner = self.inner.read().await;
            (
                inner
                    .client
                    .to_owned()
                    .put(key.clone(), value.clone(), options.clone())
                    .await,
                inner.version,
            )
        };
        if let Err(err) = &resp && Self::should_refresh(err) {
            self.try_refresh_conn(version).await?;
            let inner = self.inner.read().await;
            inner.client.to_owned().put(key, value, options).await
        } else {
            resp
        }
    }

    #[inline]
    pub async fn delete(
        &self,
        key: impl Into<Vec<u8>> + Clone,
        options: Option<DeleteOptions>,
    ) -> Result<DeleteResponse> {
        let (resp, version) = {
            let inner = self.inner.read().await;
            (
                inner
                    .client
                    .to_owned()
                    .delete(key.clone(), options.clone())
                    .await,
                inner.version,
            )
        };
        if let Err(err) = &resp && Self::should_refresh(err) {
            self.try_refresh_conn(version).await?;
            let inner = self.inner.read().await;
            inner.client.to_owned().delete(key, options).await
        } else {
            resp
        }
    }

    #[inline]
    pub async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
        let (resp, version) = {
            let inner = self.inner.read().await;
            (
                inner.client.to_owned().txn(txn.clone()).await,
                inner.version,
            )
        };
        if let Err(err) = &resp && Self::should_refresh(err) {
            self.try_refresh_conn(version).await?;
            let inner = self.inner.read().await;
            inner.client.to_owned().txn(txn).await
        } else {
            resp
        }
    }
}
