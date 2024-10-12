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

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_kinesis::types::Shard;
use aws_sdk_kinesis::Client as kinesis_client;
use risingwave_common::bail;

use crate::error::ConnectorResult as Result;
use crate::source::kinesis::split::KinesisOffset;
use crate::source::kinesis::*;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

pub struct KinesisSplitEnumerator {
    stream_name: String,
    client: kinesis_client,
}

impl KinesisSplitEnumerator {}

#[async_trait]
impl SplitEnumerator for KinesisSplitEnumerator {
    type Properties = KinesisProperties;
    type Split = KinesisSplit;

    async fn new(
        properties: KinesisProperties,
        _context: SourceEnumeratorContextRef,
    ) -> Result<Self> {
        let client = properties.common.build_client().await?;
        let stream_name = properties.common.stream_name.clone();
        Ok(Self {
            stream_name,
            client,
        })
    }

    async fn list_splits(&mut self) -> Result<Vec<KinesisSplit>> {
        let mut next_token: Option<String> = None;
        let mut shard_collect: Vec<Shard> = Vec::new();

        loop {
            let mut req = self.client.list_shards();
            if let Some(token) = next_token.take() {
                req = req.next_token(token);
            } else {
                req = req.stream_name(&self.stream_name);
            }

            let list_shard_output = match req.send().await {
                Ok(output) => output,
                Err(e) => {
                    if let Some(e_inner) = e.as_service_error()
                        && e_inner.is_expired_next_token_exception()
                    {
                        tracing::info!("Kinesis ListShard token expired, retrying...");
                        next_token = None;
                        continue;
                    }
                    return Err(anyhow!(e).context("failed to list kinesis shards").into());
                }
            };
            match list_shard_output.shards {
                Some(shard) => shard_collect.extend(shard),
                None => bail!("no shards in stream {}", &self.stream_name),
            }

            match list_shard_output.next_token {
                Some(token) => next_token = Some(token),
                None => break,
            }
        }
        Ok(shard_collect
            .into_iter()
            .map(|x| KinesisSplit {
                shard_id: x.shard_id().to_string().into(),
                // handle start with position in reader part
                next_offset: KinesisOffset::None,
                end_offset: KinesisOffset::None,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_kinesis::config::Region;

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_kinesis_split_enumerator() -> Result<()> {
        let stream_name = "kinesis_debug".to_string();
        let config = aws_config::from_env()
            .region(Region::new("cn-northwest-1"))
            .load()
            .await;
        let client = aws_sdk_kinesis::Client::new(&config);
        let mut enumerator = KinesisSplitEnumerator {
            stream_name,
            client,
        };
        let list_splits_resp = enumerator.list_splits().await?;
        println!("{:#?}", list_splits_resp);
        assert_eq!(list_splits_resp.len(), 4);
        Ok(())
    }
}
