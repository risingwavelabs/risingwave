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

use std::iter::{Map, Take};
use std::time::Duration;

use etcd_client::{
    DeleteOptions, DeleteResponse, Error, GetOptions, GetResponse, KvClient, PutOptions,
    PutResponse, Txn, TxnResponse,
};
use tokio_retry::strategy::{jitter, FixedInterval};

type Result<T> = std::result::Result<T, etcd_client::Error>;

const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_RETRY_MAX_ATTEMPTS: usize = 20;

#[derive(Clone)]
pub struct EtcdRetryClient {
    client: KvClient,
}

impl EtcdRetryClient {
    pub fn new(client: KvClient) -> Self {
        Self { client }
    }

    #[inline(always)]
    fn get_retry_strategy() -> Map<Take<FixedInterval>, fn(Duration) -> Duration> {
        FixedInterval::new(DEFAULT_RETRY_INTERVAL)
            .take(DEFAULT_RETRY_MAX_ATTEMPTS)
            .map(jitter)
    }

    #[inline(always)]
    fn should_retry(err: &Error) -> bool {
        match err {
            Error::GRpcStatus(status) => status.code() == tonic::Code::Unavailable,
            _ => false,
        }
    }
}

/// Wrap etcd client with retry logic.
impl EtcdRetryClient {
    #[inline]
    pub async fn get(
        &mut self,
        key: impl Into<Vec<u8>> + Clone,
        options: Option<GetOptions>,
    ) -> Result<GetResponse> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let mut client = self.client.clone();
                client.get(key.clone(), options.clone()).await
            },
            Self::should_retry,
        )
        .await
    }

    #[inline]
    pub async fn put(
        &self,
        key: impl Into<Vec<u8>> + Clone,
        value: impl Into<Vec<u8>> + Clone,
        options: Option<PutOptions>,
    ) -> Result<PutResponse> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let mut client = self.client.clone();
                client
                    .put(key.clone(), value.clone(), options.clone())
                    .await
            },
            Self::should_retry,
        )
        .await
    }

    #[inline]
    pub async fn delete(
        &self,
        key: impl Into<Vec<u8>> + Clone,
        options: Option<DeleteOptions>,
    ) -> Result<DeleteResponse> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let mut client = self.client.clone();
                client.delete(key.clone(), options.clone()).await
            },
            Self::should_retry,
        )
        .await
    }

    #[inline]
    pub async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let mut client = self.client.clone();
                client.txn(txn.clone()).await
            },
            Self::should_retry,
        )
        .await
    }
}
