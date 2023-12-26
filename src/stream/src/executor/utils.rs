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

use futures::StreamExt;
use risingwave_common::catalog::Schema;
use risingwave_common::metrics::LabelGuardedIntCounter;

use crate::executor::monitor::StreamingMetrics;
use crate::executor::{BoxedMessageStream, Executor, ExecutorInfo, PkIndicesRef};
use crate::task::{ActorId, FragmentId};

#[derive(Default)]
pub struct DummyExecutor {
    pub info: ExecutorInfo,
}

impl DummyExecutor {
    pub fn new(info: ExecutorInfo) -> Self {
        Self { info }
    }
}

impl Executor for DummyExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        futures::stream::pending().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

pub(crate) struct ActorInputMetrics {
    pub(crate) actor_in_record_cnt: LabelGuardedIntCounter<3>,
    pub(crate) actor_input_buffer_blocking_duration_ns: LabelGuardedIntCounter<3>,
}

impl ActorInputMetrics {
    pub(crate) fn new(
        metrics: &StreamingMetrics,
        actor_id: ActorId,
        fragment_id: FragmentId,
        upstream_fragment_id: FragmentId,
    ) -> Self {
        let actor_id_str = actor_id.to_string();
        let fragment_id_str = fragment_id.to_string();
        let upstream_fragment_id_str = upstream_fragment_id.to_string();
        Self {
            actor_in_record_cnt: metrics.actor_in_record_cnt.with_guarded_label_values(&[
                &actor_id_str,
                &fragment_id_str,
                &upstream_fragment_id_str,
            ]),
            actor_input_buffer_blocking_duration_ns: metrics
                .actor_input_buffer_blocking_duration_ns
                .with_guarded_label_values(&[
                    &actor_id_str,
                    &fragment_id_str,
                    &upstream_fragment_id_str,
                ]),
        }
    }
}
