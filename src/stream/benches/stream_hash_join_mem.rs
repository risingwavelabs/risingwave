// Copyright 2024 RisingWave Labs
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

#![feature(let_chains)]

//! To run this benchmark you can use the following command:
//! ```sh
//! cargo bench --features dhat-heap --bench stream_hash_join_mem
//! ```
//!
//! You may also specify the amplification size, e.g. 40000
//! ```sh
//! cargo bench --features dhat-heap --bench stream_hash_join_mem -- 40000
//! ```

use std::env;
use risingwave_pb::plan_common::JoinType;

use risingwave_stream::executor::test_utils::hash_join_executor::*;

risingwave_expr_impl::enable!();

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[tokio::main]
async fn main() {
    let args: Vec<_> = env::args().collect();
    let amp = if let Some(raw_arg) = args.get(1)
        && let Ok(arg) = raw_arg.parse()
    {
        arg
    } else {
        100_000
    };
    let workload = HashJoinWorkload::NotInCache;
    let join_type = JoinType::Inner;
    let (tx_l, tx_r, out) = setup_bench_stream_hash_join(amp, workload, join_type).await;
    {
        // Start the profiler later, after we have ingested the data for hash join build-side.
        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        handle_streams(workload, join_type, amp, tx_l, tx_r, out).await;
    }
}
