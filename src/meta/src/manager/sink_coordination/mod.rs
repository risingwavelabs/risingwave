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

mod coordinator_worker;
mod manager;

use futures::stream::BoxStream;
pub(crate) use manager::SinkCoordinatorManager;
use risingwave_common::buffer::Bitmap;
use risingwave_connector::sink::SinkParam;
use risingwave_pb::connector_service::{CoordinateRequest, CoordinateResponse};
use tokio::sync::mpsc::Sender;
use tonic::Status;

pub(crate) type SinkWriterRequestStream = BoxStream<'static, Result<CoordinateRequest, Status>>;
pub(crate) type SinkCoordinatorResponseSender = Sender<Result<CoordinateResponse, Status>>;

pub(crate) struct NewSinkWriterRequest {
    pub(crate) request_stream: SinkWriterRequestStream,
    pub(crate) response_tx: SinkCoordinatorResponseSender,
    pub(crate) param: SinkParam,
    pub(crate) vnode_bitmap: Bitmap,
}
