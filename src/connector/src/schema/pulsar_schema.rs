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

use std::collections::HashMap;

use reqwest::{Method, Url};

use super::{invalid_option_error, InvalidOptionError};

/// An client for communication with pulsar schema registry
#[derive(Debug)]
pub struct Client {
    inner: reqwest::Client,
    base: Url,
    token: Option<String>,
}

#[derive(Debug)]
pub struct SchemaInfo {
    pub topic: String,
    pub version: usize,
    pub timestamp_ms: i64,
    pub schema: PulsarSchema,
    pub properties: HashMap<String, String>,
}

#[derive(Debug)]
pub enum PulsarSchema {
    ProtobufNative(prost_reflect::MessageDescriptor),
    Avro(apache_avro::Schema),
}

impl Client {
    pub fn new(base: Url, token: Option<String>) -> Self {
        // `unwrap` as the builder is not affected by any input right now
        let inner = reqwest::Client::builder().build().unwrap();

        Self { inner, base, token }
    }

    pub async fn get_schema(&self, topic: &str) -> SchemaInfo {
        #[derive(serde::Deserialize)]
        struct GetSchemaResponse {
            version: usize,
            r#type: String,
            timestamp: i64,
            data: String,
            properties: HashMap<String, String>,
        }
        #[derive(serde::Deserialize)]
        struct ProtobufNativeSchemaData {
            #[serde(rename = "fileDescriptorSet")]
            file_descriptor_set: String,
            #[serde(rename = "rootMessageTypeName")]
            root_message_type_name: String,
            #[serde(rename = "rootFileDescriptorName")]
            root_file_descriptor_name: String,
        }

        let url = self
            .base
            .join(&format!("/admin/v2/schemas/{topic}/schema"))
            .unwrap();
        let mut q = self.inner.request(Method::GET, url);
        if let Some(token) = &self.token {
            q = q.bearer_auth(token);
        }
        let res = q.send().await.unwrap();
        let res: GetSchemaResponse = res.json().await.unwrap();
        let schema = match res.r#type.as_str() {
            "PROTOBUF_NATIVE" => {
                use base64::prelude::*;
                use prost_reflect::DescriptorPool;

                let native_schema: ProtobufNativeSchemaData =
                    serde_json::from_str(&res.data).unwrap();
                let desc_bytes = BASE64_STANDARD
                    .decode(native_schema.file_descriptor_set)
                    .unwrap();
                let pool = DescriptorPool::decode(desc_bytes.as_ref()).unwrap();
                let message = pool
                    .get_message_by_name(&native_schema.root_message_type_name)
                    .unwrap();
                PulsarSchema::ProtobufNative(message)
            }
            "AVRO" => {
                let s = apache_avro::Schema::parse_str(&res.data).unwrap();
                PulsarSchema::Avro(s)
            }
            _ => unimplemented!(),
        };
        SchemaInfo {
            topic: topic.to_owned(),
            version: res.version,
            timestamp_ms: res.timestamp,
            schema,
            properties: res.properties,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_xxx() {
        let c = Client::new(Url::parse("http://0.0.0.0:8080").unwrap(), None);
        let z = c.get_schema("public/default/test-000").await;
        assert_eq!(format!("{z:#?}"), "");
    }
}
