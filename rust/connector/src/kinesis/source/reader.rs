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
use std::str::from_utf8;
use std::{thread, time};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_kinesis::error::GetRecordsError;
use aws_sdk_kinesis::model::ShardIteratorType;
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::types::SdkError;
use aws_sdk_kinesis::Client as kinesis_client;
use aws_smithy_types::DateTime;
use http::uri::Uri;

use crate::base::{InnerMessage, SourceReader};
use crate::kinesis::config::AwsConfigInfo;
use crate::kinesis::source::message::KinesisMessage;
use crate::kinesis::source::state::KinesisSplitReaderState;
use crate::kinesis::split::{KinesisOffset, KinesisSplit};

pub struct KinesisSplitReader {
    client: kinesis_client,
    stream_name: String,
    shard_id: String,
    latest_sequence_num: String,
    shard_iter: Option<String>,
    assigned_split: Option<KinesisSplit>,
}

#[async_trait]
impl SourceReader for KinesisSplitReader {
    async fn next(&mut self) -> Result<Option<Vec<InnerMessage>>> {
        if self.assigned_split.is_none() {
            return Err(anyhow::Error::msg(
                "you should call `assign_split` before calling `next`".to_string(),
            ));
        }
        loop {
            let iter = match &self.shard_iter {
                Some(_iter) => _iter,
                None => {
                    return Err(anyhow::Error::msg(format!(
                        "invalid shard iter, shard_id {}",
                        self.shard_id
                    )));
                }
            };
            let get_record_output = match self.get_records(iter.clone()).await {
                Ok(record_resp) => record_resp,
                Err(SdkError::DispatchFailure(e)) => {
                    return Err(anyhow!(e));
                }
                Err(SdkError::ServiceError { err, .. }) if err.is_expired_iterator_exception() => {
                    match self.renew_shard_iter().await {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(e);
                        }
                    }
                    return self.next().await;
                }
                Err(SdkError::ServiceError { err, .. })
                    if err.is_provisioned_throughput_exceeded_exception() =>
                {
                    return Err(anyhow::Error::msg(err));
                }
                Err(e) => {
                    return Err(anyhow!("{}", e));
                }
            };
            println!("get_record_output {:#?}", get_record_output);
            self.shard_iter = get_record_output.next_shard_iterator.clone();

            let records = get_record_output.records.unwrap_or_default();
            if records.is_empty() {
                // if records is empty, retry after 200ms to avoid
                // ProvisionedThroughputExceededException
                thread::sleep(time::Duration::from_millis(200));
                continue;
            }

            let mut record_collection: Vec<InnerMessage> = Vec::new();
            for record in records {
                if !is_stopping(
                    record.sequence_number.as_ref().unwrap(),
                    self.assigned_split.as_ref().unwrap(),
                ) {
                    return Ok(Some(record_collection));
                }
                self.latest_sequence_num = record.sequence_number().unwrap().to_string();
                record_collection.push(InnerMessage::from(KinesisMessage::new(
                    self.shard_id.clone(),
                    record,
                )));
            }
            return Ok(Some(record_collection));
        }
    }

    async fn assign_split<'a>(&'a mut self, split: &'a [u8]) -> Result<()> {
        let split: KinesisSplit = serde_json::from_str(from_utf8(split)?)?;
        self.shard_id = split.shard_id.clone();
        match &split.start_position.clone() {
            KinesisOffset::Earliest | KinesisOffset::None => {
                self.get_kinesis_iterator(ShardIteratorType::TrimHorizon, None, None)
                    .await?;
            }
            KinesisOffset::Latest => {
                self.get_kinesis_iterator(ShardIteratorType::Latest, None, None)
                    .await?;
            }
            KinesisOffset::SequenceNumber(seq_num) => {
                self.get_kinesis_iterator(
                    ShardIteratorType::AtSequenceNumber,
                    None,
                    Some(seq_num.clone()),
                )
                .await?;
            }
            KinesisOffset::Timestamp(ts) => {
                self.get_kinesis_iterator(ShardIteratorType::AtTimestamp, Some(*ts), None)
                    .await?;
            }
        }
        self.assigned_split = Some(split);
        Ok(())
    }
}

impl KinesisSplitReader {
    /// This method is only used to initialize the [`KinesisSplitReader`], which is needed to
    /// allocate the [`KinesisSplit`] and then fetch the data.
    async fn new(config: AwsConfigInfo, endpoint: Option<String>) -> Self {
        let aws_config = config.load().await.unwrap();
        let mut builder = aws_sdk_kinesis::config::Builder::from(&aws_config);
        if let Some(endpoint) = &endpoint {
            let uri = endpoint.clone().parse::<Uri>().unwrap();
            builder =
                builder.endpoint_resolver(aws_smithy_http::endpoint::Endpoint::immutable(uri));
        }
        let client = kinesis_client::from_conf(builder.build());

        KinesisSplitReader {
            client,
            stream_name: config.stream_name.clone(),
            shard_id: "".to_string(),
            latest_sequence_num: "".to_string(),
            shard_iter: None,
            assigned_split: None,
        }
    }

    async fn get_records(
        &self,
        shard_iter: String,
    ) -> core::result::Result<GetRecordsOutput, SdkError<GetRecordsError>> {
        let resp = self
            .client
            .get_records()
            .shard_iterator(shard_iter.clone())
            .send()
            .await;
        resp
    }

    async fn renew_shard_iter(&mut self) -> Result<()> {
        let get_shard_iter_resp = self
            .client
            .get_shard_iterator()
            .stream_name(&self.stream_name)
            .shard_id(self.shard_id.clone())
            .shard_iterator_type(aws_sdk_kinesis::model::ShardIteratorType::AfterSequenceNumber)
            .starting_sequence_number(self.latest_sequence_num.clone())
            .send()
            .await;
        self.shard_iter = match get_shard_iter_resp {
            Ok(resp) => resp.shard_iterator().map(String::from),
            Err(e) => {
                return Err(anyhow!("{}", e));
            }
        };
        Ok(())
    }

    async fn get_kinesis_iterator(
        &mut self,
        shard_iterator_type: aws_sdk_kinesis::model::ShardIteratorType,
        timestamp: Option<i64>,
        seq_num: Option<String>,
    ) -> Result<()> {
        let mut get_shard_iter_req = self
            .client
            .get_shard_iterator()
            .stream_name(&self.stream_name)
            .shard_id(&self.shard_id)
            .shard_iterator_type(shard_iterator_type);

        if let Some(ts) = timestamp {
            get_shard_iter_req = get_shard_iter_req.set_timestamp(Some(DateTime::from_secs(ts)));
        }
        if let Some(seq) = seq_num {
            get_shard_iter_req = get_shard_iter_req.set_starting_sequence_number(Some(seq));
        }

        let get_shard_iter_resp = get_shard_iter_req.send().await;
        self.shard_iter = match get_shard_iter_resp {
            Ok(resp) => resp.shard_iterator().map(String::from),
            Err(e) => {
                return Err(anyhow!("{}", e));
            }
        };

        Ok(())
    }

    fn get_state(&self) -> KinesisSplitReaderState {
        KinesisSplitReaderState::new(
            self.stream_name.clone(),
            self.shard_id.clone(),
            self.latest_sequence_num.clone(),
        )
    }

    async fn restore_from_state(&mut self, state: KinesisSplitReaderState) -> Result<()> {
        self.stream_name = state.stream_name;
        self.shard_id = state.shard_id;
        self.latest_sequence_num = state.sequence_number;

        self.renew_shard_iter().await
    }
}

fn is_stopping(cur_seq_num: &str, split: &KinesisSplit) -> bool {
    match &split.end_position {
        KinesisOffset::SequenceNumber(stopping_seq_num) => {
            if cur_seq_num < stopping_seq_num.as_str() {
                return true;
            }
            false
        }
        _ => true,
    }
}
