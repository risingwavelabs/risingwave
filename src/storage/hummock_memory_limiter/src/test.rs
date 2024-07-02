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

#![cfg(test)]

#[cfg(not(loom))]
mod normal_test {
    use std::future::{poll_fn, Future};
    use std::sync::Arc;
    use std::task::Poll;

    use futures::future::join_all;
    use futures::FutureExt;
    use rand::random;

    use crate::MemoryLimiter;

    async fn assert_pending(future: &mut (impl Future + Unpin)) {
        for _ in 0..10 {
            assert!(poll_fn(|cx| Poll::Ready(future.poll_unpin(cx)))
                .await
                .is_pending());
        }
    }

    #[tokio::test]
    async fn test_loose_memory_limiter() {
        let quota = 5;
        let memory_limiter = MemoryLimiter::new(quota);
        drop(memory_limiter.require_memory(6).await);
        let tracker1 = memory_limiter.require_memory(3).await;
        assert_eq!(3, memory_limiter.get_memory_usage());
        let tracker2 = memory_limiter.require_memory(4).await;
        assert_eq!(7, memory_limiter.get_memory_usage());
        let mut future = memory_limiter.require_memory(5).boxed();
        assert_pending(&mut future).await;
        assert_eq!(7, memory_limiter.get_memory_usage());
        drop(tracker1);
        let tracker3 = future.await;
        assert_eq!(9, memory_limiter.get_memory_usage());
        drop(tracker2);
        assert_eq!(5, memory_limiter.get_memory_usage());
        drop(tracker3);
        assert_eq!(0, memory_limiter.get_memory_usage());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_multi_thread_acquire_memory() {
        const QUOTA: u64 = 10;
        let memory_limiter = Arc::new(MemoryLimiter::new(200));
        let mut handles = vec![];
        for _ in 0..40 {
            let limiter = memory_limiter.clone();
            let h = tokio::spawn(async move {
                let mut buffers = vec![];
                let mut current_buffer_usage = (random::<usize>() % 8) + 2;
                for _ in 0..1000 {
                    if buffers.len() < current_buffer_usage
                        && let Some(tracker) = limiter.try_require_memory(QUOTA)
                    {
                        buffers.push(tracker);
                    } else {
                        buffers.clear();
                        current_buffer_usage = (random::<usize>() % 8) + 2;
                        let req = limiter.require_memory(QUOTA);
                        match tokio::time::timeout(std::time::Duration::from_millis(1), req).await {
                            Ok(tracker) => {
                                buffers.push(tracker);
                            }
                            Err(_) => {
                                continue;
                            }
                        }
                    }
                    let sleep_time = random::<u64>() % 3 + 1;
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_time)).await;
                }
            });
            handles.push(h);
        }
        let h = join_all(handles);
        let _ = h.await;
    }
}

#[cfg(loom)]
mod loom_test {
    use std::pin::pin;

    use ::loom::future::block_on;
    use futures::FutureExt;

    use crate::MemoryLimiter;

    #[test]
    fn test_memory_limiter_drop() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(4);
        builder.check(|| {
            let memory_limiter = MemoryLimiter::new(1);
            let initial_holder = block_on(memory_limiter.require_memory(2));
            let (tx, rx) = tokio::sync::oneshot::channel();

            let join_handle1 = loom::thread::spawn(move || {
                let fut = memory_limiter.require_memory(1);
                let mut fut = pin!(fut);
                assert!(fut.as_mut().now_or_never().is_none());
                tx.send(()).unwrap();
                drop(fut);
            });

            let join_handle2 = loom::thread::spawn(move || {
                block_on(rx).unwrap();
                drop(initial_holder);
            });

            join_handle1.join().unwrap();
            join_handle2.join().unwrap();
        });
    }
}
