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

use risingwave_pb::stream_plan::PbDispatchOutputMapping;

use crate::executor::prelude::*;

#[derive(Debug)]
pub enum DispatchOutputMapping {
    Simple(Vec<usize>),
}

impl DispatchOutputMapping {
    pub(super) fn from_protobuf(proto: PbDispatchOutputMapping) -> Self {
        Self::Simple(
            proto
                .into_simple_indices()
                .iter()
                .map(|&i| i as usize)
                .collect(),
        )
    }

    pub(super) fn apply(&self, chunk: StreamChunk) -> StreamChunk {
        match self {
            Self::Simple(indices) => {
                if indices.len() < chunk.columns().len() {
                    chunk.project(indices).eliminate_adjacent_noop_update()
                } else {
                    chunk.project(indices)
                }
            }
        }
    }

    pub(super) fn apply_watermark(&self, watermark: Watermark) -> Option<Watermark> {
        match self {
            Self::Simple(indices) => watermark.transform_with_indices(indices),
        }
    }
}
