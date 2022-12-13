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

#![cfg(loom)]

/// Run `RUSTFLAGS="--cfg loom" cargo test --test loom` to test.
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;
use task_stats_alloc::TaskLocalBytesAllocated;

#[test]
fn test_to_avoid_double_drop() {
    loom::model(|| {
        let bytes_num = 3;
        let num = Arc::new(TaskLocalBytesAllocated::new_for_test(3));

        // Add the flag value when counter drop so we can observe.
        let flag_num = Arc::new(AtomicUsize::new(0));

        let threads: Vec<_> = (0..bytes_num)
            .map(|_| {
                let num = num.clone();
                let flag_num = flag_num.clone();
                thread::spawn(move || {
                    num.sub_for_test(1, flag_num);
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        // Ensure the counter is dropped.
        assert_eq!(flag_num.load(Ordering::Relaxed), 1);
    });
}
