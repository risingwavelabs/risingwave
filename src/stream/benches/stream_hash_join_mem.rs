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

//! Specify the amplification_size,workload,join_type e.g. 40000
//! ```sh
//! ARGS=40000,NotInCache,Inner cargo bench --features dhat-heap --bench stream_hash_join_mem
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
    let arg = env::var("ARGS");
    let (amp, workload, join_type) = if let Ok(raw_arg) = arg
    {
        let parts = raw_arg.split(',').collect::<Vec<_>>();
        let amp = parts[0].parse::<usize>().expect(format!("invalid amplification_size: {}", parts[0]).as_str());
        let workload = match parts[1] {
            "NotInCache" => HashJoinWorkload::NotInCache,
            "InCache" => HashJoinWorkload::InCache,
            _ => panic!("Invalid workload: {}", parts[1]),
        };
        let join_type = match parts[2] {
            "Inner" => JoinType::Inner,
            "LeftOuter" => JoinType::LeftOuter,
            _ => panic!("Invalid join type: {}", parts[2]),
        };
        (amp, workload, join_type)
    } else {
        panic!("invalid ARGS: {:?}", arg);
    };

    let (tx_l, tx_r, out) = setup_bench_stream_hash_join(amp, workload, join_type).await;
    {
        // Start the profiler later, after we have ingested the data for hash join build-side.
        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        handle_streams(workload, join_type, amp, tx_l, tx_r, out).await;
        let stats= dhat::HeapStats::get();
        println!("max_blocks: {}", stats.max_blocks);
        println!("max_bytes: {}", stats.max_bytes);
    }
}
