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

use std::collections::HashMap;

use reqwest::{Method, Url};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use serde::de::DeserializeOwned;
use serde::Deserialize;

/// An client for communication with schema registry
#[derive(Debug)]
pub struct SchematizerClient {
    inner: reqwest::Client,
    url: Url,
}

impl SchematizerClient {
    pub(crate) fn new(url: Url, props: &HashMap<String, String>) -> Result<Self> {
        if url.cannot_be_a_base() {
            return Err(RwError::from(ProtocolError(format!(
                "{} cannot be a base url",
                url
            ))));
        }

        let inner = reqwest::Client::builder().build().map_err(|e| {
            RwError::from(ProtocolError(format!("build reqwest client failed {}", e)))
        })?;


        Ok(SchematizerClient {
            inner,
            url,
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
        request
    }

    /// get schema by id
    pub async fn get_schema_by_id(&self, id: i32) -> Result<SchematizerSchema> {
        // FIXME: make sure this is the right way to build request and the api is built correctly
        let req = self.build_request(Method::GET, &["v2","schemas", "ids", &id.to_string()]);
        dbg!("Calling schematizer registry to get schema by id request: {:?}", req.try_clone());
        let res: GetByIdResp = send_request(req).await?;
        Ok(SchematizerSchema {
            id,
            content: res.schema,
            namespace: res.namespace,
            src: res.src,
            alias: res.alias,
        })
    }

        /// get schema by id
        pub async fn get_schema_by_alias(&self, namespace: &str, source: &str, alias: &str) -> Result<SchematizerSchema> {
            todo!()
        }
}

async fn send_request<T>(req: reqwest::RequestBuilder) -> Result<T>
where
    T: DeserializeOwned,
{
    let res = req.send().await.map_err(|e| {
        RwError::from(ProtocolError(format!(
            "schematizer registry send req error {}",
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


#[derive(Debug, Eq, PartialEq)]
pub struct SchematizerSchema {
    /// The id of the schema
    pub id: i32,
    /// The raw text of the schema def
    pub content: String,
    pub namespace: String,
    pub src: String,
    pub alias: String,
}

#[derive(Debug, Deserialize)]
struct GetByIdResp {
    schema: String,
    namespace: String,
    src: String,
    alias: String,
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
        let client = SchematizerClient::new(url, &HashMap::new()).unwrap();
        let subject = client
            .get_schema_by_id(20)
            .await
            .unwrap();
        println!("{:?}", subject);
    }
}
