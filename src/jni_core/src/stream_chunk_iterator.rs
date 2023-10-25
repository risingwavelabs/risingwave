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

use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_pb::data::Op;

pub(crate) type StreamChunkRowIterator = impl Iterator<Item = (Op, OwnedRow)> + 'static;

pub(crate) fn into_iter(stream_chunk: StreamChunk) -> StreamChunkRowIterator {
    stream_chunk
        .rows()
        .map(|(op, row_ref)| (op.to_protobuf(), row_ref.to_owned_row()))
        .collect_vec()
        .into_iter()
}
