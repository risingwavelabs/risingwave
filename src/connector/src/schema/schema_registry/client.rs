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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use futures::future::select_all;
use itertools::Itertools;
use reqwest::{Method, Url};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use serde::de::DeserializeOwned;

use super::util::*;

#[derive(Debug, Clone, Default)]
pub struct SchemaRegistryAuth {
    username: Option<String>,
    password: Option<String>,
}

impl From<&HashMap<String, String>> for SchemaRegistryAuth {
    fn from(props: &HashMap<String, String>) -> Self {
        const SCHEMA_REGISTRY_USERNAME: &str = "schema.registry.username";
        const SCHEMA_REGISTRY_PASSWORD: &str = "schema.registry.password";

        SchemaRegistryAuth {
            username: props.get(SCHEMA_REGISTRY_USERNAME).cloned(),
            password: props.get(SCHEMA_REGISTRY_PASSWORD).cloned(),
        }
    }
}

impl From<&BTreeMap<String, String>> for SchemaRegistryAuth {
    fn from(props: &BTreeMap<String, String>) -> Self {
        const SCHEMA_REGISTRY_USERNAME: &str = "schema.registry.username";
        const SCHEMA_REGISTRY_PASSWORD: &str = "schema.registry.password";

        SchemaRegistryAuth {
            username: props.get(SCHEMA_REGISTRY_USERNAME).cloned(),
            password: props.get(SCHEMA_REGISTRY_PASSWORD).cloned(),
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
}

impl Client {
    pub(crate) fn new(url: Vec<Url>, client_config: &SchemaRegistryAuth) -> Result<Self> {
        let valid_urls = url
            .iter()
            .map(|url| (url.cannot_be_a_base(), url))
            .filter(|(x, _)| !*x)
            .map(|(_, url)| url.clone())
            .collect_vec();
        if valid_urls.is_empty() {
            return Err(RwError::from(ProtocolError(format!(
                "no valid url provided, got {:?}",
                url
            ))));
        } else {
            tracing::debug!(
                "schema registry client will use url {:?} to connect",
                valid_urls
            );
        }

        let inner = reqwest::Client::builder().build().map_err(|e| {
            RwError::from(ProtocolError(format!("build reqwest client failed {}", e)))
        })?;

        Ok(Client {
            inner,
            url: valid_urls,
            username: client_config.username.clone(),
            password: client_config.password.clone(),
        })
    }

    async fn concurrent_req<'a, T>(
        &'a self,
        method: Method,
        path: &'a [&'a (impl AsRef<str> + ?Sized + Debug + ToString)],
    ) -> Result<T>
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
        for url in &self.url {
            fut_req.push(tokio::spawn(req_inner(
                ctx.clone(),
                url.clone(),
                method.clone(),
            )));
        }

        while !fut_req.is_empty() {
            let (result, _index, remaining) = select_all(fut_req).await;
            match result {
                Ok(Ok(res)) => {
                    let _ = remaining.iter().map(|ele| ele.abort());
                    return Ok(res);
                }
                Ok(Err(e)) => errs.push(e),
                Err(e) => errs.push(RwError::from(e)),
            }
            fut_req = remaining;
        }

        Err(RwError::from(ProtocolError(format!(
            "all request confluent registry all timeout, req path {:?}, urls {:?}, err: {:?}",
            path,
            self.url,
            errs.iter().map(|e| e.to_string()).collect_vec()
        ))))
    }

    /// get schema by id
    pub async fn get_schema_by_id(&self, id: i32) -> Result<ConfluentSchema> {
        let res: GetByIdResp = self
            .concurrent_req(Method::GET, &["schemas", "ids", &id.to_string()])
            .await?;
        Ok(ConfluentSchema {
            id,
            content: res.schema,
        })
    }

    /// get the latest schema of the subject
    pub async fn get_schema_by_subject(&self, subject: &str) -> Result<ConfluentSchema> {
        self.get_subject(subject).await.map(|s| s.schema)
    }

    /// get the latest version of the subject
    pub async fn get_subject(&self, subject: &str) -> Result<Subject> {
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
    ) -> Result<(Subject, Vec<Subject>)> {
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
            &SchemaRegistryAuth {
                username: None,
                password: None,
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
