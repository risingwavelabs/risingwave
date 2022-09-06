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

use anyhow::{anyhow, bail, Result};
use http::{Response, StatusCode};
use hyper::body::Buf;
use hyper::{Body, Client, Uri};
use serde_derive::{Deserialize, Serialize};

use crate::source::pulsar::topic::Topic;

#[derive(Debug, Default)]
pub struct PulsarAdminClient {
    pub(crate) base_path: String,
}

impl PulsarAdminClient {
    pub fn new(base_path: String) -> Self {
        Self {
            base_path: base_path.trim_end_matches('/').to_string(),
        }
    }
}

impl PulsarAdminClient {
    pub async fn get_last_message_id(&self, topic: &Topic) -> Result<LastMessageID> {
        self.get(topic, "lastMessageId").await
    }

    pub async fn get_topic_metadata(&self, topic: &Topic) -> Result<PartitionedTopicMetadata> {
        let res = self.http_get(topic, "partitions").await?;

        if res.status() == StatusCode::NOT_FOUND {
            bail!(
                "could not find metadata for pulsar topic {}",
                topic.to_string()
            );
        }

        let body = hyper::body::aggregate(res).await?;
        serde_json::from_reader(body.reader()).map_err(|e| anyhow!(e))
    }

    pub async fn http_get(&self, topic: &Topic, api: &str) -> Result<Response<Body>> {
        let client = Client::new();

        let url = format!(
            "{}/{}/{}/{}",
            self.base_path,
            "admin/v2",
            topic.rest_path(),
            api
        );

        let url: Uri = url.parse()?;
        client.get(url).await.map_err(|e| anyhow!(e))
    }

    pub async fn get<T>(&self, topic: &Topic, api: &str) -> Result<T>
    where
        T: for<'a> serde::Deserialize<'a>,
    {
        let res = self.http_get(topic, api).await?;
        let body = hyper::body::aggregate(res).await?;
        let result: T = serde_json::from_reader(body.reader())?;
        Ok(result)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastMessageID {
    pub ledger_id: i64,
    pub entry_id: i64,
    pub partition_index: i64,
    pub batch_index: Option<i64>,
    pub batch_size: Option<i64>,
    pub acker: Option<LastMessageIDAcker>,
    pub outstanding_acks_in_same_batch: Option<i64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastMessageIDAcker {
    pub batch_size: Option<i64>,
    pub prev_batch_cumulatively_acked: Option<bool>,
    pub outstanding_acks: Option<i64>,
    pub bit_set_size: Option<i64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionedTopicMetadata {
    pub partitions: i64,
}

#[cfg(test)]
mod test {
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::source::pulsar::admin::client::PulsarAdminClient;
    use crate::source::pulsar::topic::parse_topic;

    async fn mock_server(web_path: &str, body: &str) -> MockServer {
        let mock_server = MockServer::start().await;
        use wiremock::matchers::{method, path};

        let response = ResponseTemplate::new(200)
            .set_body_string(body)
            .append_header("content-type", "application/json");

        Mock::given(method("GET"))
            .and(path(web_path))
            .respond_with(response)
            .mount(&mock_server)
            .await;

        mock_server
    }

    #[tokio::test]
    #[cfg_attr(madsim, ignore)] // MockServer is not supported in simulation.
    async fn test_get_topic_metadata() {
        let server = mock_server(
            "/admin/v2/persistent/public/default/t2/partitions",
            "{\"partitions\":3}",
        )
        .await;

        let client = PulsarAdminClient::new(server.uri());

        let topic = parse_topic("public/default/t2").unwrap();

        let meta = client.get_topic_metadata(&topic).await.unwrap();

        assert_eq!(meta.partitions, 3);
    }
}
