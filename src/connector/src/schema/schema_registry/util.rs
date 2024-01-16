// Copyright 2024 RisingWave Labs
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

use std::fmt::Debug;
use std::sync::Arc;

use reqwest::Method;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result as RwResult, RwError};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use url::{ParseError, Url};

#[derive(Debug, thiserror::Error)]
#[error("no valid url provided, got {errs:?}")]
pub struct SrUrlError {
    errs: Vec<ParseError>,
}

impl From<SrUrlError> for risingwave_common::error::RwError {
    fn from(value: SrUrlError) -> Self {
        anyhow::anyhow!(value).into()
    }
}

pub fn handle_sr_list(addr: &str) -> Result<Vec<Url>, SrUrlError> {
    let segment = addr.split(',').collect::<Vec<&str>>();
    let mut errs: Vec<ParseError> = Vec::with_capacity(segment.len());
    let mut urls = Vec::with_capacity(segment.len());
    for ele in segment {
        match ele.parse::<Url>() {
            Ok(url) => urls.push(url),
            Err(e) => errs.push(e),
        }
    }
    if urls.is_empty() {
        return Err(SrUrlError { errs });
    }
    tracing::debug!(
        "schema registry client will use url {:?} to connect, the rest failed because: {:?}",
        urls,
        errs
    );
    Ok(urls)
}

#[derive(Debug, thiserror::Error)]
pub enum WireFormatError {
    #[error("fail to match a magic byte of 0")]
    NoMagic,
    #[error("fail to read 4-byte schema ID")]
    NoSchemaId,
}

impl From<WireFormatError> for risingwave_common::error::RwError {
    fn from(value: WireFormatError) -> Self {
        anyhow::anyhow!(value).into()
    }
}

/// extract the magic number and `schema_id` at the front of payload
///
/// 0 -> magic number
/// 1-4 -> schema id
/// 5-... -> message payload
pub(crate) fn extract_schema_id(payload: &[u8]) -> Result<(i32, &[u8]), WireFormatError> {
    use byteorder::{BigEndian, ReadBytesExt as _};

    let mut cursor = payload;
    if !cursor.read_u8().is_ok_and(|magic| magic == 0) {
        return Err(WireFormatError::NoMagic);
    }

    let schema_id = cursor
        .read_i32::<BigEndian>()
        .map_err(|_| WireFormatError::NoSchemaId)?;

    Ok((schema_id, cursor))
}

pub(crate) struct SchemaRegistryCtx {
    pub username: Option<String>,
    pub password: Option<String>,
    pub client: reqwest::Client,
    pub path: Vec<String>,
}

pub(crate) async fn req_inner<T>(
    ctx: Arc<SchemaRegistryCtx>,
    mut url: Url,
    method: Method,
) -> RwResult<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    url.path_segments_mut()
        .expect("constructor validated URL can be a base")
        .clear()
        .extend(&ctx.path);
    tracing::debug!("request to url: {}, method {}", &url, &method);
    let mut request_builder = ctx.client.request(method, url);

    if let Some(ref username) = ctx.username {
        request_builder = request_builder.basic_auth(username, ctx.password.as_ref());
    }
    request(request_builder).await
}

async fn request<T>(req: reqwest::RequestBuilder) -> RwResult<T>
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
#[derive(Debug, Deserialize)]
pub struct SchemaReference {
    /// The name of the reference.
    pub name: String,
    /// The subject that the referenced schema belongs to
    pub subject: String,
    /// The version of the referenced schema
    pub version: i32,
}

#[derive(Debug, Deserialize)]
pub struct GetByIdResp {
    pub schema: String,
}

#[derive(Debug, Deserialize)]
pub struct GetBySubjectResp {
    pub id: i32,
    pub schema: String,
    pub version: i32,
    pub subject: String,
    // default to empty/non-reference
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

#[derive(Debug, Deserialize)]
struct ErrorResp {
    error_code: i32,
    message: String,
}

#[cfg(test)]
mod test {
    use super::super::handle_sr_list;

    #[test]
    fn test_handle_sr_list() {
        let addr1 = "http://localhost:8081".to_owned();
        assert_eq!(
            handle_sr_list(&addr1).unwrap(),
            vec!["http://localhost:8081".parse().unwrap()]
        );

        let addr2 = "http://localhost:8081,http://localhost:8082".to_owned();
        assert_eq!(
            handle_sr_list(&addr2).unwrap(),
            vec![
                "http://localhost:8081".parse().unwrap(),
                "http://localhost:8082".parse().unwrap()
            ]
        );

        let fail_addr = "http://localhost:8081,12345".to_owned();
        assert_eq!(
            handle_sr_list(&fail_addr).unwrap(),
            vec!["http://localhost:8081".parse().unwrap(),]
        );

        let all_fail_addr = "54321,12345".to_owned();
        assert!(handle_sr_list(&all_fail_addr).is_err());
    }
}
