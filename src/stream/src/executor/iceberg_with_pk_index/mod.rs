// Copyright 2026 RisingWave Labs
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

//! Iceberg V3 Sink with Primary Key Index
//!
//! This module implements three core executors for the Iceberg V3 sink that uses
//! Deletion Vectors (DVs) instead of Equality Delete files:
//!
//! 1. **Writer Executor** (Stateful): Maintains a PK index mapping primary keys to
//!    (`data_file_path`, `row_position`). Writes data files for inserts and emits
//!    file-scoped payload messages to the DV Merger for deletes and partition metadata.
//!
//! 2. **DV Merger Executor** (Stateless): Consumes the Writer's payload messages,
//!    merges delete positions with historical DVs, and reports the resulting DV files to meta.

mod dv_handler_impl;
mod dv_merger;
mod writer;
mod writer_impl;

use bincode::{Decode, Encode};
pub use dv_handler_impl::DvHandlerImpl;
pub use dv_merger::DvMergerExecutor;
use iceberg::spec::{Literal, Struct, StructType, Type};
use risingwave_common::bitmap::Bitmap;
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::mock_coordination_client::SinkCoordinationRpcClientEnum;
use risingwave_pb::connector_service::coordinate_request::CoordinationRole;
use risingwave_rpc_client::CoordinatorStreamHandle;
pub use writer::WriterExecutor;
pub use writer_impl::IcebergWriterImpl;

use crate::executor::{StreamExecutorError, StreamExecutorResult};

/// Payload encoded into the writer's `[file_path, payload]` output stream.
/// The payload can be either:
/// 1. Position information for delete operations, or
/// 2. Partition metadata for the written data file (if the table is partitioned).
#[derive(Debug, Clone, Encode, Decode)]
pub enum Payload {
    Position(i64),
    PartitionInfo(Vec<u8>),
}

impl Payload {
    pub fn encode_position(pos: i64) -> anyhow::Result<Vec<u8>> {
        let res = bincode::encode_to_vec(Payload::Position(pos), bincode::config::standard())?;
        Ok(res)
    }

    pub fn encode_partition_info(partition_info: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let res = bincode::encode_to_vec(
            Payload::PartitionInfo(partition_info),
            bincode::config::standard(),
        )?;
        Ok(res)
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let (payload, _): (Payload, _) =
            bincode::decode_from_slice(bytes, bincode::config::standard())?;
        Ok(payload)
    }
}

pub struct CoordinatorStreamHandleInit {
    pub coordination_client: SinkCoordinationRpcClientEnum,
    pub sink_param: SinkParam,
    pub vnode_bitmap: Bitmap,
    pub role: CoordinationRole,
}

impl CoordinatorStreamHandleInit {
    /// Create a coordinator stream handle without consuming self, allowing retries.
    pub(crate) async fn try_create_handle(
        &self,
    ) -> StreamExecutorResult<(CoordinatorStreamHandle, Option<u64>)> {
        self.coordination_client
            .clone()
            .new_stream_handle(&self.sink_param, self.vnode_bitmap.clone(), self.role)
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_param.sink_id))
    }
}

pub fn serialize_partition_struct(
    partition_struct: &Struct,
    partition_type: &StructType,
) -> anyhow::Result<Vec<u8>> {
    let json = Literal::Struct(partition_struct.clone())
        .try_into_json(&Type::Struct(partition_type.clone()))?;
    let res = serde_json::to_vec(&json)?;
    Ok(res)
}

pub fn deserialize_partition_struct(
    bytes: &[u8],
    partition_type: &StructType,
) -> anyhow::Result<Struct> {
    let json: serde_json::Value = serde_json::from_slice(bytes)?;
    let literal = Literal::try_from_json(json, &Type::Struct(partition_type.clone()))?;
    if let Some(Literal::Struct(partition_struct)) = literal {
        Ok(partition_struct)
    } else {
        Ok(Struct::empty())
    }
}
