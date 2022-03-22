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
//
use anyhow::Result;
use hyper::body::Buf;
use hyper::{Client, Uri};
use serde_derive::{Deserialize, Serialize};

use crate::pulsar::topic::ParsedTopic;

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
    pub async fn get_last_message_id(&self, topic: &ParsedTopic) -> Result<LastMessageID> {
        self.get(topic, "lastMessageId").await
    }

    pub async fn get_topic_metadata(
        &self,
        topic: &ParsedTopic,
    ) -> Result<PartitionedTopicMetadata> {
        self.get(topic, "partitions").await
    }

    pub async fn get<T>(&self, topic: &ParsedTopic, api: &str) -> Result<T>
    where
        T: for<'a> serde::Deserialize<'a>,
    {
        let client = Client::new();

        let url = format!(
            "{}/{}/{}/{}",
            self.base_path,
            "admin/v2",
            topic.rest_path(),
            api
        );

        let url: Uri = url.parse()?;
        let res = client.get(url).await?;
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
    use httpmock::Method::GET;
    use httpmock::MockServer;

    use crate::pulsar::admin::client::PulsarAdminClient;
    use crate::pulsar::topic::parse_topic;

    fn mock_server(path: &str, body: &str) -> MockServer {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path(path);
            then.status(200)
                .header("content-type", "application/json")
                .body(body);
        });

        server
    }

    #[tokio::test]
    async fn test_get_topic_metadata() {
        let server = mock_server(
            "/admin/v2/persistent/public/default/t2/partitions",
            "{\"partitions\":3}",
        );

        let client = PulsarAdminClient::new(server.base_url());

        let topic = parse_topic("public/default/t2").unwrap();

        let meta = client.get_topic_metadata(&topic).await.unwrap();

        assert_eq!(meta.partitions, 3);

        std::mem::drop(server);
    }
}
