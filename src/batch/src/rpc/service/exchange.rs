// Copyright 2023 RisingWave Labs
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

use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::task_service::GetDataResponse;
use tonic::Status;

pub type GetDataResponseResult = std::result::Result<GetDataResponse, Status>;

type ExchangeDataSender = tokio::sync::mpsc::Sender<GetDataResponseResult>;

pub trait ExchangeWriter: Send {
    async fn write(&mut self, resp: GetDataResponseResult) -> Result<()>;
}

pub struct GrpcExchangeWriter {
    sender: ExchangeDataSender,
    written_chunks: usize,
}

impl GrpcExchangeWriter {
    pub fn new(sender: ExchangeDataSender) -> Self {
        Self {
            sender,
            written_chunks: 0,
        }
    }

    pub fn written_chunks(&self) -> usize {
        self.written_chunks
    }
}

impl ExchangeWriter for GrpcExchangeWriter {
    async fn write(&mut self, data: GetDataResponseResult) -> Result<()> {
        self.written_chunks += 1;
        self.sender
            .send(data)
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
        writer.write(Ok(GetDataResponse::default())).await.unwrap();
        assert_eq!(writer.written_chunks(), 1);
    }

    #[tokio::test]
    async fn test_write_to_closed_channel() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        drop(rx);
        let mut writer = GrpcExchangeWriter::new(tx);
        let res = writer.write(Ok(GetDataResponse::default())).await;
        assert!(res.is_err());
    }
}
