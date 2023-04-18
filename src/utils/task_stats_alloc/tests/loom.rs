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

#![cfg(all(loom, enable_task_local_alloc))]

/// Note this test is not running in CI, due to the re-compile time cost. Add it when it is
/// necessary. Run `RUSTFLAGS="--cfg loom" cargo test --test loom` to test.
use loom::sync::Arc;
use loom::thread;
use task_stats_alloc::TaskLocalBytesAllocated;

#[test]
fn test_to_avoid_double_drop() {
    loom::model(|| {
        let bytes_num = 3;
        let num = Arc::new(TaskLocalBytesAllocated::new());

        let threads: Vec<_> = (0..bytes_num)
            .map(|_| {
                let num = num.clone();
                thread::spawn(move || {
                    num.add(1);
                    num.sub(1)
                })
            })
            .collect();

        // How many times the bytes have been dropped.
        let mut drop_num = 0;
        for t in threads {
            if t.join().unwrap() {
                drop_num += 1;
            }
        }

        // Ensure the counter is dropped.
        assert_eq!(drop_num, 1);
    });
}
