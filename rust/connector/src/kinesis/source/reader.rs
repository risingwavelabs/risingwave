use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_kinesis::error::GetRecordsError;
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::{Client as kinesis_client, SdkError};

use crate::base::SourceReader;
use crate::kinesis::config::AwsConfigInfo;
use crate::kinesis::source::message::KinesisMessage;
use crate::kinesis::split::KinesisSplit;

pub struct KinesisSplitReader {
    client: kinesis_client,
    stream_name: String,
    shard_id: String,
    latest_sequence_num: String,
    shard_iter: String,
}

#[async_trait]
impl SourceReader for KinesisSplitReader {
    type Message = KinesisMessage;
    type Split = KinesisSplit;

    // TODO(tabVersion)
    async fn next(&mut self) -> Result<Option<Vec<Self::Message>>> {
        Ok(Some(vec![KinesisMessage::default()]))
    }

    // TODO(tabVersion)
    async fn assign_split(&mut self, _split: Self::Split) -> Result<()> {
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
}
