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

use std::collections::{HashMap, HashSet};

use reqwest::{Method, Url};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// An client for communication with schema registry
#[derive(Debug)]
pub struct Client {
    inner: reqwest::Client,
    url: Url,
    username: Option<String>,
    password: Option<String>,
}

impl Client {
    pub(crate) fn new(url: Url, props: &HashMap<String, String>) -> Result<Self> {
        const SCHEMA_REGISTRY_USERNAME: &str = "schema.registry.username";
        const SCHEMA_REGISTRY_PASSWORD: &str = "schema.registry.password";

        if url.cannot_be_a_base() {
            return Err(RwError::from(ProtocolError(format!(
                "{} cannot be a base url",
                url
            ))));
        }

        let inner = reqwest::Client::builder().build().map_err(|e| {
            RwError::from(ProtocolError(format!("build reqwest client failed {}", e)))
        })?;

        let username = props.get(SCHEMA_REGISTRY_USERNAME).cloned();
        let password = props.get(SCHEMA_REGISTRY_PASSWORD).cloned();
        Ok(Client {
            inner,
            url,
            username,
            password,
        })
    }

    fn build_request<P>(&self, method: Method, path: P) -> reqwest::RequestBuilder
    where
        P: IntoIterator,
        P::Item: AsRef<str>,
    {
        let mut url = self.url.clone();
        url.path_segments_mut()
            .expect("constructor validated URL can be a base")
            .clear()
            .extend(path);

        let mut request = self.inner.request(method, url);

        if self.username.is_some() {
            request = request.basic_auth(self.username.clone().unwrap(), self.password.clone())
        }

        request
    }

    /// get schema by id
    pub async fn get_schema_by_id(&self, id: i32) -> Result<ConfluentSchema> {
        let req = self.build_request(Method::GET, &["schemas", "ids", &id.to_string()]);
        let res: GetByIdResp = request(req).await?;
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
        let req = self.build_request(Method::GET, &["subjects", subject, "versions", "latest"]);
        let res: GetBySubjectResp = request(req).await?;
        tracing::info!("res {:?}", res);
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
            let req =
                self.build_request(Method::GET, &["subjects", &subject, "versions", &version]);
            let res: GetBySubjectResp = request(req).await?;
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

async fn request<T>(req: reqwest::RequestBuilder) -> Result<T>
where
    T: DeserializeOwned,
{
    let res = req.send().await.map_err(|e| {
        RwError::from(ProtocolError(format!(
            "confluent registry send req error {}",
            e
        )))
    })?;
    let status = res.status();
    if status.is_success() {
        res.json().await.map_err(|e| {
            RwError::from(ProtocolError(format!(
                "confluent registry parse resp error {}",
                e
            )))
        })
    } else {
        let res = res.json::<ErrorResp>().await.map_err(|e| {
            RwError::from(ProtocolError(format!(
                "confluent registry resp error {}",
                e
            )))
        })?;
        Err(RwError::from(ProtocolError(format!(
            "confluent registry resp error, code: {}, msg {}",
            res.error_code, res.message
        ))))
    }
}

/// `Schema` format of confluent schema registry
#[derive(Debug, Eq, PartialEq)]
pub struct ConfluentSchema {
    /// The id of the schema
    pub id: i32,
    /// The raw text of the schema def
    pub content: String,
}

/// `Subject` stored in confluent schema registry
#[derive(Debug, Eq, PartialEq)]
pub struct Subject {
    /// The version of the current schema
    pub version: i32,
    /// The name of the schema
    pub name: String,
    /// The schema corresponding to that `version`
    pub schema: ConfluentSchema,
}

/// One schema can reference another schema
/// (e.g., import "other.proto" in protobuf)
#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaReference {
    /// The name of the reference.
    pub name: String,
    /// The subject that the referenced schema belongs to
    pub subject: String,
    /// The version of the referenced schema
    pub version: i32,
}

#[derive(Debug, Deserialize)]
struct GetByIdResp {
    schema: String,
}

#[derive(Debug, Deserialize)]
struct GetBySubjectResp {
    id: i32,
    schema: String,
    version: i32,
    subject: String,
    // default to empty/non-reference
    #[serde(default)]
    references: Vec<SchemaReference>,
}

#[derive(Debug, Deserialize)]
struct ErrorResp {
    error_code: i32,
    message: String,
}

#[derive(Debug)]
enum ReqResp<T> {
    Succeed(T),
    Failed(ErrorResp),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_get_subject() {
        let url = Url::parse("http://localhost:8081").unwrap();
        let client = Client::new(url, &HashMap::new()).unwrap();
        let subject = client
            .get_subject_and_references("proto_c_bin-value")
            .await
            .unwrap();
        println!("{:?}", subject);
    }
}
