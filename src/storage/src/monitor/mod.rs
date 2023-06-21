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

mod hummock_state_store_metrics;
mod traced_store;
use futures::Future;
pub use hummock_state_store_metrics::*;
mod monitored_store;
pub use monitored_store::*;
mod hummock_metrics;
pub use hummock_metrics::*;

mod monitored_storage_metrics;
pub use monitored_storage_metrics::*;

mod compactor_metrics;
pub use compactor_metrics::*;

mod local_metrics;
pub use local_metrics::*;
pub use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;

pub trait HummockTraceFutureExt: Sized + Future {
    type TraceOutput;
    fn may_trace_hummock(self) -> Self::TraceOutput;
}

impl<F: Future> HummockTraceFutureExt for F {
    type TraceOutput = impl Future<Output = F::Output>;

    fn may_trace_hummock(self) -> Self::TraceOutput {
        #[cfg(not(all(not(madsim), any(hm_trace, feature = "hm-trace"))))]
        {
            self
        }
        #[cfg(all(not(madsim), any(hm_trace, feature = "hm-trace")))]
        {
            use risingwave_hummock_trace::hummock_trace_scope;
            hummock_trace_scope(self)
        }
    }
}
