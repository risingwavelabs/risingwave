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

use std::sync::Arc;

use etcd_client::{
    CampaignResponse, ConnectOptions, DeleteOptions, DeleteResponse, GetOptions, GetResponse,
    LeaderResponse, LeaseGrantOptions, LeaseGrantResponse, LeaseKeepAliveStream, LeaseKeeper,
    ObserveStream, PutOptions, PutResponse, ResignOptions, ResignResponse, Txn, TxnResponse,
};
use tokio::sync::RwLock;

/// `WrappedEtcdClient` is a wrapper around `etcd_client::Client` that used by meta store.
#[derive(Clone)]
pub enum WrappedEtcdClient {
    Raw(etcd_client::Client),
    EnableRefresh(EtcdRefreshClient),
}

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
/// [support token refresh for long-live clients](https://github.com/etcdv3/etcd-client/issues/45).
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
            tracing::debug!("connection to etcd refreshed");
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
}

macro_rules! impl_etcd_client_command_proxy {
    ($func:ident, $client:ident, ($($arg:ident : $sig:ty),+), $result:ty) => {
        impl WrappedEtcdClient {
            pub async fn $func(
                &self,
                $($arg:$sig),+
            ) -> Result<$result> {
                match self {
                    Self::Raw(client) => client.$client().$func($($arg),+).await,
                    Self::EnableRefresh(client) => client.$func($($arg),+).await,
                }
            }
        }


        impl EtcdRefreshClient {
            #[inline]
            pub async fn $func(
                &self,
                $($arg:$sig),+
            ) -> Result<$result> {
                let (resp, version) = {
                    let inner = self.inner.read().await;
                    (
                        inner.client.$client().$func($($arg),+).await,
                        inner.version,
                    )
                };

                match resp {
                    Err(err) if Self::should_refresh(&err) => {
                        self.try_refresh_conn(version).await?;
                        Err(err)
                    }
                    _ => resp,
                }
            }
        }
    }
}

impl_etcd_client_command_proxy!(get, kv_client, (key: impl Into<Vec<u8>> + Clone, opts: Option<GetOptions>), GetResponse);
impl_etcd_client_command_proxy!(put, kv_client, (key: impl Into<Vec<u8>> + Clone, value: impl Into<Vec<u8>> + Clone, opts: Option<PutOptions>), PutResponse);
impl_etcd_client_command_proxy!(delete, kv_client, (key: impl Into<Vec<u8>> + Clone, opts: Option<DeleteOptions>), DeleteResponse);
impl_etcd_client_command_proxy!(txn, kv_client, (txn: Txn), TxnResponse);
impl_etcd_client_command_proxy!(
    grant,
    lease_client,
    (ttl: i64, options: Option<LeaseGrantOptions>),
    LeaseGrantResponse
);
impl_etcd_client_command_proxy!(
    keep_alive,
    lease_client,
    (id: i64),
    (LeaseKeeper, LeaseKeepAliveStream)
);
impl_etcd_client_command_proxy!(leader, election_client, (name: impl Into<Vec<u8>> + Clone), LeaderResponse);
impl_etcd_client_command_proxy!(
    campaign,
    election_client,
    (
        name: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        lease: i64
    ),
    CampaignResponse
);
impl_etcd_client_command_proxy!(
    observe,
    election_client,
    (name: impl Into<Vec<u8>>),
    ObserveStream
);

impl_etcd_client_command_proxy!(
    resign,
    election_client,
    (option: Option<ResignOptions>),
    ResignResponse
);

impl WrappedEtcdClient {
    pub async fn connect(
        endpoints: Vec<String>,
        options: Option<ConnectOptions>,
        auth_enabled: bool,
    ) -> Result<Self> {
        let client = etcd_client::Client::connect(&endpoints, options.clone()).await?;
        if auth_enabled {
            Ok(Self::EnableRefresh(EtcdRefreshClient {
                inner: Arc::new(RwLock::new(ClientWithVersion::new(client, 0))),
                endpoints,
                options,
            }))
        } else {
            Ok(Self::Raw(client))
        }
    }
}
