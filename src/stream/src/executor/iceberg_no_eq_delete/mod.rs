// Copyright 2025 RisingWave Labs
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

//! Iceberg V3 Sink without Equality Delete.
//!
//! This module implements three core executors for the Iceberg V3 sink that uses
//! Deletion Vectors (DVs) instead of Equality Delete files:
//!
//! 1. **Writer Executor** (Stateful): Maintains a PK index mapping primary keys to
//!    (`data_file_path`, `row_position`). Writes data files for inserts and emits delete
//!    position messages to the DV Merger for deletes.
//!
//! 2. **Compaction Executor** (Stateless): Receives compaction tasks from meta,
//!    reads data files and applies delete vectors, emits upsert rows to the Writer.
//!
//! 3. **DV Merger Executor** (Stateless, Singleton): Merges delete position messages
//!    from the Writer with historical DVs, produces merged DV files and reports to meta.

mod dv_blob;
mod dv_handler_impl;
mod dv_merger;
mod writer;
mod writer_impl;

pub use dv_handler_impl::DvHandlerImpl;
pub use dv_merger::DvMergerExecutor;
use risingwave_common::bitmap::Bitmap;
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::mock_coordination_client::SinkCoordinationRpcClientEnum;
use risingwave_pb::connector_service::CoordinationRole;
use risingwave_rpc_client::CoordinatorStreamHandle;
pub use writer::{IcebergWriter, RowPosition, WriterExecutor};
pub use writer_impl::IcebergWriterImpl;

use crate::executor::{StreamExecutorError, StreamExecutorResult};

pub struct CoordinatorStreamHandleInit {
    pub coordination_client: SinkCoordinationRpcClientEnum,
    pub sink_param: SinkParam,
    pub vnode_bitmap: Bitmap,
    pub role: CoordinationRole,
}

impl CoordinatorStreamHandleInit {
    pub(crate) async fn create_handle(
        self,
    ) -> StreamExecutorResult<(CoordinatorStreamHandle, Option<u64>)> {
        self.coordination_client
            .new_stream_handle(&self.sink_param, self.vnode_bitmap, self.role)
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_param.sink_id))
    }
}
