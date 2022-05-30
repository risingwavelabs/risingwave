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

use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::common::Status as PbStatus;
use risingwave_pb::data::DataChunk;
use risingwave_pb::task_service::{ExecuteResponse, GetDataResponse};
use tonic::Status;

type ExchangeDataSender<T> = tokio::sync::mpsc::Sender<std::result::Result<T, Status>>;

pub trait CreateDataResponse: Send {
    fn create_data_response(status: PbStatus, data_chunk: DataChunk) -> Self;
}

impl CreateDataResponse for GetDataResponse {
    fn create_data_response(status: PbStatus, data_chunk: DataChunk) -> Self {
        Self {
            status: Some(status),
            record_batch: Some(data_chunk),
        }
    }
}

impl CreateDataResponse for ExecuteResponse {
    fn create_data_response(status: PbStatus, data_chunk: DataChunk) -> Self {
        Self {
            status: Some(status),
            record_batch: Some(data_chunk),
        }
    }
}

#[async_trait::async_trait]
pub trait ExchangeWriter<T: CreateDataResponse>: Send {
    async fn write(&mut self, resp: T) -> Result<()>;
}

pub struct GrpcExchangeWriter<T: CreateDataResponse> {
    sender: ExchangeDataSender<T>,
    written_chunks: usize,
}

impl<T: CreateDataResponse> GrpcExchangeWriter<T> {
    pub fn new(sender: ExchangeDataSender<T>) -> Self {
        Self {
            sender,
            written_chunks: 0,
        }
    }

    pub fn written_chunks(&self) -> usize {
        self.written_chunks
    }
}

#[async_trait::async_trait]
impl<T: CreateDataResponse> ExchangeWriter<T> for GrpcExchangeWriter<T> {
    async fn write(&mut self, data: T) -> Result<()> {
        self.written_chunks += 1;
        self.sender
            .send(Ok(data))
            .await
            .to_rw_result_with(|| "failed to write data to ExchangeWriter".into())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::task_service::GetDataResponse;

    use crate::rpc::service::exchange::{ExchangeWriter, GrpcExchangeWriter};

    #[tokio::test]
    async fn test_exchange_writer() {
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let mut writer = GrpcExchangeWriter::new(tx);
        writer.write(GetDataResponse::default()).await.unwrap();
        assert_eq!(writer.written_chunks(), 1);
    }

    #[tokio::test]
    async fn test_write_to_closed_channel() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        drop(rx);
        let mut writer = GrpcExchangeWriter::new(tx);
        let res = writer.write(GetDataResponse::default()).await;
        assert!(res.is_err());
    }
}
