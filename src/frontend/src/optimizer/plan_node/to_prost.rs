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

use anyhow::anyhow;
use risingwave_pb::batch_plan::plan_node as pb_batch_node;
use risingwave_pb::stream_plan::stream_node as pb_stream_node;

use super::*;

pub trait TryToBatchPb {
    fn try_to_batch_prost_body(&self) -> SchedulerResult<pb_batch_node::NodeBody> {
        // Originally we panic in the following way
        // panic!("convert into distributed is only allowed on batch plan")
        Err(anyhow!(
            "Node {} cannot be convert to batch node",
            std::any::type_name::<Self>()
        )
        .into())
    }
}

pub trait ToBatchPb {
    fn to_batch_prost_body(&self) -> pb_batch_node::NodeBody;
}

impl<T: ToBatchPb> TryToBatchPb for T {
    fn try_to_batch_prost_body(&self) -> SchedulerResult<pb_batch_node::NodeBody> {
        Ok(self.to_batch_prost_body())
    }
}

pub trait TryToStreamPb {
    fn try_to_stream_prost_body(
        &self,
        _state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<pb_stream_node::NodeBody> {
        // Originally we panic in the following way
        // panic!("convert into distributed is only allowed on stream plan")
        Err(anyhow!(
            "Node {} cannot be convert to stream node",
            std::any::type_name::<Self>()
        )
        .into())
    }
}

impl<T: StreamNode> TryToStreamPb for T {
    fn try_to_stream_prost_body(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<pb_stream_node::NodeBody> {
        Ok(self.to_stream_prost_body(state))
    }
}

pub trait StreamNode {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState)
    -> pb_stream_node::NodeBody;
}
