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

use risingwave_common::error::Result;
use risingwave_pb::stream_plan::StreamNode;

use crate::storage::MetaStore;
use crate::stream::graph::StreamFragment;
use crate::stream::StreamFragmenter;

impl<S> StreamFragmenter<S>
where
    S: MetaStore,
{
    pub fn rewrite_delta_join(&mut self, _node: &StreamNode) -> Result<StreamFragment> {
        todo!()
    }
}
