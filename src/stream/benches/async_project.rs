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

//! Finite, single-executor latency model. See docs/dev/src/design/async-projection.md.

use std::time::Duration;

use risingwave_stream::executor::test_utils::async_project::{
    MockProjectBatch, MockProjectWorkload, run_mock_project,
};
use serde_json::json;

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("invalid {name}: {value}"))
    })
}

fn main() {
    let chunks = setting("PROJECT_BENCH_CHUNKS", 64);
    let rows = setting("PROJECT_BENCH_ROWS", 128);
    let latency_ms = setting("PROJECT_BENCH_DELAY_MS", 10);
    let repeats = setting("PROJECT_BENCH_REPEATS", 3);
    assert!(chunks > 0 && rows > 0 && repeats > 0);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut cases = Vec::new();
        for chunks in [1, 2, 4, 8, 16] {
            cases.push(("chunk_sweep", chunks, 0, "fixed"));
        }
        for budget in [1, rows, rows * 2, rows * 4, rows * 8, 0] {
            cases.push(("row_sweep", 16, budget, "fixed"));
        }
        cases.extend([
            ("interaction", 8, rows * 2, "jitter"),
            ("interaction", 8, rows * 2, "variable_visibility"),
            ("interaction", 8, rows * 2, "slow_first"),
            ("interaction", 8, 0, "slow_first"),
            ("baseline", 1, 0, "no_wait"),
            ("baseline", 8, 0, "no_wait"),
            ("baseline", 0, 0, "no_wait"),
        ]);
        for (suite, chunk_limit, row_limit, profile) in cases {
            let batches = (0..chunks)
                .map(|id| {
                    let capacity = if profile == "variable_visibility" {
                        rows * [1, 4, 2, 1][id % 4]
                    } else {
                        rows
                    };
                    let visible_rows = if profile == "variable_visibility" && id % 2 == 1 {
                        capacity / 4
                    } else {
                        capacity
                    };
                    let delay = match profile {
                        "no_wait" => 0,
                        "jitter" => latency_ms * (1 + id % 5),
                        "slow_first" if id == 0 => latency_ms * 10,
                        _ => latency_ms,
                    };
                    MockProjectBatch {
                        capacity,
                        visible_rows,
                        latency: Duration::from_millis(delay as u64),
                    }
                })
                .collect();
            let workload = MockProjectWorkload {
                chunk_limit,
                row_limit,
                batches,
                barrier_every: 8,
            };
            // Warm up construction and execution once per case; report every measured repetition.
            let _ = run_mock_project(&workload).await;
            for repeat in 0..repeats {
                let result = run_mock_project(&workload).await;
                println!(
                    "{}",
                    json!({
                        "suite": suite, "profile": profile, "repeat": repeat,
                        "chunk_limit": chunk_limit, "row_limit": row_limit,
                        "chunks": chunks, "base_rows": rows, "base_latency_ms": latency_ms,
                            "debug_assertions": cfg!(debug_assertions), "runtime": "current_thread",
                        "result": result,
                    })
                );
            }
        }
    });
}
