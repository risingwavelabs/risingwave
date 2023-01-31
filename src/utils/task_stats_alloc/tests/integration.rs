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

use std::alloc::System;
use std::hint::black_box;
use std::time::Duration;

use task_stats_alloc::*;

#[global_allocator]
static GLOBAL: TaskLocalAlloc<System> = TaskLocalAlloc(System);

#[tokio::test]
async fn test_basic() {
    let mut bytes = None;
    let mut placeholder = Box::new(10u64);

    {
        let bytes = &mut bytes;
        let placeholder = &mut placeholder;
        allocation_stat(
            async move {
                *bytes = Some(BYTES_ALLOCATED.get());
                let bytes = bytes.unwrap();
                let base1 = bytes.val();
                {
                    let _a1 = black_box(Box::new(114514_u64));

                    let base2 = bytes.val();
                    {
                        let _a2 = black_box(Vec::<u32>::with_capacity(1024));
                    }
                    assert_eq!(bytes.val(), base2);

                    let _a3 = black_box(Box::new(1145166666666666666666666666666666u128));
                    let _v = Vec::<u8>::with_capacity(128);
                }
                assert_eq!(bytes.val(), base1);
                // Leak the box out of the task.
                *placeholder = Box::new(187u64);
            },
            Duration::from_secs(1),
            |_| {},
        )
        .await;
    }
    // There should only one u64 held by `placeholder` not dropped in the task local allocator.
    assert_eq!(bytes.unwrap().val(), std::mem::size_of::<u64>());
    // The placeholder was consumed by `black_box`.
    black_box(placeholder);
    // The counter should be dropped, but there is no way to test it.
}
