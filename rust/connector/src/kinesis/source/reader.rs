use std::{thread, time};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_kinesis::error::GetRecordsError;
use aws_sdk_kinesis::model::ShardIteratorType;
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::{Client as kinesis_client, DateTime, SdkError};
use futures::executor::block_on;
use http::uri::Uri;

use crate::base::SourceReader;
use crate::kinesis::config::AwsConfigInfo;
use crate::kinesis::source::message::KinesisMessage;
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
    type Message = KinesisMessage;
    type Split = KinesisSplit;

    async fn next(&mut self) -> Result<Option<Vec<Self::Message>>> {
        if let None = &self.assigned_split {
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
            let get_record_output = match block_on(self.get_records(iter.clone())) {
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

            let mut record_collection: Vec<Self::Message> = Vec::new();
            for record in records {
                if !is_stopping(
                    record.sequence_number.as_ref().unwrap(),
                    &self.assigned_split.as_ref().unwrap(),
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
    /// This method is only used to initialize the KinesisSplitReader, which is needed to allocate
    /// the KinesisSplit and then fetch the data.
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
        println!("get_records: send iter {:?}", shard_iter);
        let resp = self
            .client
            .get_records()
            .shard_iterator(shard_iter.clone())
            .send()
            .await;
        println!("get_records: resp {:#?}", resp);
        return resp;
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

#[cfg(test)]
mod tests {
    use aws_sdk_kinesis::Region;

    use super::*;

    const STREAM_NAME: &str = "kinesis_test_stream";

    async fn push_to_mq(client: &kinesis_client) -> Result<(String, String)> {
        let mut first_sequence_num: String = "".into();
        // let mut first_timestamp: String = "".into();
        let mut first_shard_id: String = "".into();
        for i in 0..99 {
            let blob = aws_sdk_kinesis::Blob::new(i.to_string());
            let resp = client
                .put_record()
                .data(blob)
                .partition_key(format!("{}", i))
                .stream_name(STREAM_NAME)
                .send()
                .await?;

            if i == 0 {
                first_sequence_num = resp.sequence_number.unwrap();
                first_shard_id = resp.shard_id.unwrap();
            }
        }

        Ok((first_shard_id, first_sequence_num))
    }

    async fn new_client() -> kinesis_client {
        let region = "cn-north-1".to_string();
        let config = aws_config::from_env()
            .region(Region::new(region.clone()))
            .load()
            .await;

        aws_sdk_kinesis::Client::new(&config)
    }

    #[tokio::test]
    #[ignore]
    async fn test_load_from_sequence_number() {
        let client = new_client().await;
        let (shard_id, sequence_num) = push_to_mq(&client).await.unwrap();
        println!(
            "first message: shard_id {:?}, seq num: {:?}",
            shard_id, sequence_num
        );

        let mock_split = KinesisSplit {
            shard_id: shard_id.clone(),
            start_position: KinesisOffset::SequenceNumber(sequence_num.clone()),
            end_position: KinesisOffset::None,
        };
        println!("mock split: {:#?}", mock_split);

        let demo_aws_config_info = AwsConfigInfo {
            stream_name: STREAM_NAME.to_string(),
            region: Some("cn-north-1".to_string()),
            credentials: None,
        };
        let mut split_reader = KinesisSplitReader::new(demo_aws_config_info, None).await;
        split_reader.assign_split(mock_split).await.unwrap();

        let resp = &split_reader.next().await.unwrap().unwrap()[0];
        println!("first message: {:#?}", resp);
        assert_eq!(sequence_num, resp.sequence_number);

        // let resp = split_reader
        //     .client
        //     .get_records()
        //     .shard_iterator(split_reader.shard_iter.unwrap().clone())
        //     .send()
        //     .await
        //     .unwrap();
        // println!("output {:#?}", resp);
    }

    #[test]
    #[ignore]
    fn test_load_from_timestamp() {}
}
