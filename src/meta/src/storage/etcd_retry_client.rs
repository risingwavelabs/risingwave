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

use std::time::Duration;

use etcd_client::{
    DeleteOptions, DeleteResponse, Error, GetOptions, GetResponse, PutOptions, PutResponse, Txn,
    TxnResponse,
};
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use crate::storage::WrappedEtcdClient;

type Result<T> = std::result::Result<T, Error>;

const DEFAULT_RETRY_INTERVAL: u64 = 20;
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_RETRY_MAX_ATTEMPTS: usize = 10;

#[derive(Clone)]
pub struct EtcdRetryClient {
    client: WrappedEtcdClient,
}

impl EtcdRetryClient {
    pub fn new(client: WrappedEtcdClient) -> Self {
        Self { client }
    }

    #[inline(always)]
    fn get_retry_strategy() -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(DEFAULT_RETRY_INTERVAL)
            .max_delay(DEFAULT_RETRY_MAX_DELAY)
            .take(DEFAULT_RETRY_MAX_ATTEMPTS)
            .map(jitter)
    }

    #[inline(always)]
    fn should_retry(err: &Error) -> bool {
        match err {
            Error::GRpcStatus(status) => {
                status.code() == tonic::Code::Unavailable
                    || status.code() == tonic::Code::Unknown
                    || (status.code() == tonic::Code::Unauthenticated
                        && status.message().contains("invalid auth token"))
            }
            _ => false,
        }
    }
}

/// Wrap etcd client with retry logic.
impl EtcdRetryClient {
    #[inline]
    pub async fn get(
        &self,
        key: impl Into<Vec<u8>> + Clone,
        options: Option<GetOptions>,
    ) -> Result<GetResponse> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let client = &self.client;
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
                let client = &self.client;
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
                let client = &self.client;
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
                let client = &self.client;
                client.txn(txn.clone()).await
            },
            Self::should_retry,
        )
        .await
    }
}
