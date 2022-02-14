use std::{thread, time};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_kinesis::error::GetRecordsError;
use aws_sdk_kinesis::model::ShardIteratorType;
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::{Client as kinesis_client, DateTime, SdkError};
use futures::executor::block_on;

use crate::base::SourceReader;
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
    assigned_split: KinesisSplit,
}

#[async_trait]
impl SourceReader for KinesisSplitReader {
    type Message = KinesisMessage;
    type Split = KinesisSplit;

    async fn next(&mut self) -> Result<Option<Vec<Self::Message>>> {
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
            let get_record_output = match block_on(self.get_records(iter)) {
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
            self.shard_iter = get_record_output.next_shard_iterator.clone();

            let records = get_record_output.records.unwrap_or_default();
            if records.is_empty() {
                // if records is empty, retry after 200ms to avoid
                // ProvisionedThroughputExceededException
                thread::sleep(time::Duration::from_millis(200));
                continue;
            }

            let mut record_collection: Vec<Self::Message> = Vec::new();
            for record in records {
                if !is_stopping(
                    record.sequence_number.as_ref().unwrap(),
                    &self.assigned_split,
                ) {
                    return Ok(Some(record_collection));
                }
                self.latest_sequence_num = record.sequence_number().unwrap().to_string();
                record_collection.push(Self::Message::new(self.shard_id.clone(), record));
            }
            return Ok(Some(record_collection));
        }
    }

    async fn assign_split(&mut self, split: Self::Split) -> Result<()> {
        self.assigned_split = split;
        match &self.assigned_split.start_position.clone() {
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
        Ok(())
    }
}

impl KinesisSplitReader {
    async fn create_consumer(&mut self, config: AwsConfigInfo) -> Result<()> {
        let config = config.load().await?;
        self.client = aws_sdk_kinesis::Client::new(&config);
        Ok(())
    }

    async fn get_records(
        &self,
        shard_iter: &str,
    ) -> core::result::Result<GetRecordsOutput, SdkError<GetRecordsError>> {
        self.client
            .get_records()
            .shard_iterator(shard_iter)
            .send()
            .await
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
