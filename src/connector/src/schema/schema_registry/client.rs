// Copyright 2025 RisingWave Labs
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
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use futures::future::select_all;
use itertools::Itertools;
use reqwest::{Method, Url};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use thiserror_ext::AsReport as _;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use super::util::*;
use crate::connector_common::ConfluentSchemaRegistryConnection;
use crate::schema::{InvalidOptionError, invalid_option_error};
use crate::with_options::Get;

pub const SCHEMA_REGISTRY_USERNAME: &str = "schema.registry.username";
pub const SCHEMA_REGISTRY_PASSWORD: &str = "schema.registry.password";

pub const SCHEMA_REGISTRY_MAX_DELAY_KEY: &str = "schema.registry.max.delay.sec";
pub const SCHEMA_REGISTRY_BACKOFF_DURATION_KEY: &str = "schema.registry.backoff.duration.ms";
pub const SCHEMA_REGISTRY_BACKOFF_FACTOR_KEY: &str = "schema.registry.backoff.factor";
pub const SCHEMA_REGISTRY_RETRIES_MAX_KEY: &str = "schema.registry.retries.max";

const DEFAULT_MAX_DELAY_SEC: u32 = 3;
const DEFAULT_BACKOFF_DURATION_MS: u64 = 100;
const DEFAULT_BACKOFF_FACTOR: u64 = 2;
const DEFAULT_RETRIES_MAX: usize = 3;

#[derive(Debug, Clone)]
struct SchemaRegistryRetryConfig {
    pub max_delay_sec: u32,
    pub backoff_duration_ms: u64,
    pub backoff_factor: u64,
    pub retries_max: usize,
}

impl Default for SchemaRegistryRetryConfig {
    fn default() -> Self {
        Self {
            max_delay_sec: DEFAULT_MAX_DELAY_SEC,
            backoff_duration_ms: DEFAULT_BACKOFF_DURATION_MS,
            backoff_factor: DEFAULT_BACKOFF_FACTOR,
            retries_max: DEFAULT_RETRIES_MAX,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SchemaRegistryConfig {
    username: Option<String>,
    password: Option<String>,

    retry_config: SchemaRegistryRetryConfig,
}

impl<T: Get> From<&T> for SchemaRegistryConfig {
    fn from(props: &T) -> Self {
        SchemaRegistryConfig {
            username: props.get(SCHEMA_REGISTRY_USERNAME).cloned(),
            password: props.get(SCHEMA_REGISTRY_PASSWORD).cloned(),

            retry_config: SchemaRegistryRetryConfig {
                max_delay_sec: props
                    .get(SCHEMA_REGISTRY_MAX_DELAY_KEY)
                    .and_then(|v| v.parse::<u32>().ok())
                    .unwrap_or(DEFAULT_MAX_DELAY_SEC),
                backoff_duration_ms: props
                    .get(SCHEMA_REGISTRY_BACKOFF_DURATION_KEY)
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_BACKOFF_DURATION_MS),
                backoff_factor: props
                    .get(SCHEMA_REGISTRY_BACKOFF_FACTOR_KEY)
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_BACKOFF_FACTOR),
                retries_max: props
                    .get(SCHEMA_REGISTRY_RETRIES_MAX_KEY)
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(DEFAULT_RETRIES_MAX),
            },
        }
    }
}

/// An client for communication with schema registry
#[derive(Debug)]
pub struct Client {
    inner: reqwest::Client,
    url: Vec<Url>,
    username: Option<String>,
    password: Option<String>,

    retry_config: SchemaRegistryRetryConfig,
}

#[derive(Debug, thiserror::Error)]
#[error("all request confluent registry all timeout, {context}\n{}", errs.iter().map(|e| format!("\t{}", e.as_report())).join("\n"))]
pub struct ConcurrentRequestError {
    errs: Vec<itertools::Either<RequestError, tokio::task::JoinError>>,
    context: String,
}

type SrResult<T> = Result<T, ConcurrentRequestError>;

impl TryFrom<&ConfluentSchemaRegistryConnection> for Client {
    type Error = InvalidOptionError;

    fn try_from(value: &ConfluentSchemaRegistryConnection) -> Result<Self, Self::Error> {
        let urls = handle_sr_list(value.url.as_str())?;

        Client::new(
            urls,
            &SchemaRegistryConfig {
                username: value.username.clone(),
                password: value.password.clone(),
                ..Default::default()
            },
        )
    }
}

impl Client {
    pub(crate) fn new(
        url: Vec<Url>,
        client_config: &SchemaRegistryConfig,
    ) -> Result<Self, InvalidOptionError> {
        let valid_urls = url
            .iter()
            .map(|url| (url.cannot_be_a_base(), url))
            .filter(|(x, _)| !*x)
            .map(|(_, url)| url.clone())
            .collect_vec();
        if valid_urls.is_empty() {
            return Err(invalid_option_error!("non-base: {}", url.iter().join(" ")));
        } else {
            tracing::debug!(
                "schema registry client will use url {:?} to connect",
                valid_urls
            );
        }

        // `unwrap` as the builder is not affected by any input right now
        let inner = reqwest::Client::builder().build().unwrap();

        Ok(Client {
            inner,
            url: valid_urls,
            username: client_config.username.clone(),
            password: client_config.password.clone(),
            retry_config: client_config.retry_config.clone(),
        })
    }

    async fn concurrent_req<'a, T>(
        &'a self,
        method: Method,
        path: &'a [&'a (impl AsRef<str> + ?Sized + Debug + ToString)],
    ) -> SrResult<T>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let mut fut_req = Vec::with_capacity(self.url.len());
        let mut errs = Vec::with_capacity(self.url.len());
        let ctx = Arc::new(SchemaRegistryCtx {
            username: self.username.clone(),
            password: self.password.clone(),
            client: self.inner.clone(),
            path: path.iter().map(|p| p.to_string()).collect_vec(),
        });
        tracing::debug!("retry config: {:?}", self.retry_config);

        let retry_strategy = ExponentialBackoff::from_millis(self.retry_config.backoff_duration_ms)
            .factor(self.retry_config.backoff_factor)
            .max_delay(Duration::from_secs(self.retry_config.max_delay_sec as u64))
            .take(self.retry_config.retries_max)
            .map(jitter);

        for url in &self.url {
            let url_clone = url.clone();
            let ctx_clone = ctx.clone();
            let method_clone = method.clone();

            let retry_future = Retry::spawn(retry_strategy.clone(), move || {
                let ctx = ctx_clone.clone();
                let url = url_clone.clone();
                let method = method_clone.clone();
                async move { req_inner(ctx, url, method).await }
            });

            fut_req.push(tokio::spawn(retry_future));
        }

        while !fut_req.is_empty() {
            let (result, _index, remaining) = select_all(fut_req).await;
            match result {
                Ok(Ok(res)) => {
                    let _ = remaining.iter().map(|ele| ele.abort());
                    return Ok(res);
                }
                Ok(Err(e)) => errs.push(itertools::Either::Left(e)),
                Err(e) => errs.push(itertools::Either::Right(e)),
            }
            fut_req = remaining;
        }

        Err(ConcurrentRequestError {
            errs,
            context: format!("req path {:?}, urls {}", path, self.url.iter().join(" ")),
        })
    }

    /// get schema by id
    pub async fn get_schema_by_id(&self, id: i32) -> SrResult<ConfluentSchema> {
        let res: GetByIdResp = self
            .concurrent_req(Method::GET, &["schemas", "ids", &id.to_string()])
            .await?;
        Ok(ConfluentSchema {
            id,
            content: res.schema,
        })
    }

    /// get the latest schema of the subject
    pub async fn get_schema_by_subject(&self, subject: &str) -> SrResult<ConfluentSchema> {
        self.get_subject(subject).await.map(|s| s.schema)
    }

    // used for connection validate, just check if request is ok
    pub async fn validate_connection(&self) -> SrResult<()> {
        #[derive(Debug, Deserialize)]
        struct GetConfigResp {
            #[serde(rename = "compatibilityLevel")]
            _compatibility_level: String,
        }

        let _: GetConfigResp = self.concurrent_req(Method::GET, &["config"]).await?;
        Ok(())
    }

    /// get the latest version of the subject
    pub async fn get_subject(&self, subject: &str) -> SrResult<Subject> {
        let res: GetBySubjectResp = self
            .concurrent_req(Method::GET, &["subjects", subject, "versions", "latest"])
            .await?;
        tracing::debug!("update schema: {:?}", res);
        Ok(Subject {
            schema: ConfluentSchema {
                id: res.id,
                content: res.schema,
            },
            version: res.version,
            name: res.subject,
        })
    }

    /// get the latest version of the subject and all it's references(deps)
    pub async fn get_subject_and_references(
        &self,
        subject: &str,
    ) -> SrResult<(Subject, Vec<Subject>)> {
        let mut subjects = vec![];
        let mut visited = HashSet::new();
        let mut queue = vec![(subject.to_owned(), "latest".to_owned())];
        // use bfs to get all references
        while let Some((subject, version)) = queue.pop() {
            let res: GetBySubjectResp = self
                .concurrent_req(Method::GET, &["subjects", &subject, "versions", &version])
                .await?;
            let ref_subject = Subject {
                schema: ConfluentSchema {
                    id: res.id,
                    content: res.schema,
                },
                version: res.version,
                name: res.subject.clone(),
            };
            subjects.push(ref_subject);
            visited.insert(res.subject);
            queue.extend(
                res.references
                    .into_iter()
                    .filter(|r| !visited.contains(&r.subject))
                    .map(|r| (r.subject, r.version.to_string())),
            );
        }
        let origin_subject = subjects.remove(0);

        Ok((origin_subject, subjects))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_get_subject() {
        let url = Url::parse("http://localhost:8081").unwrap();
        let client = Client::new(
            vec![url],
            &SchemaRegistryConfig {
                username: None,
                password: None,
                retry_config: SchemaRegistryRetryConfig::default(),
            },
        )
        .unwrap();
        let subject = client
            .get_subject_and_references("proto_c_bin-value")
            .await
            .unwrap();
        println!("{:?}", subject);
    }
}
