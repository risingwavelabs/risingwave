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

use std::time::Duration;

use futures::future::{join_all, select_all};
use futures::StreamExt;
use futures_async_stream::stream;
use tokio::sync::watch;

use super::*;

async fn sleep(time: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(time)).await;
    println!("slept {time}ms");
}

async fn sleep_nested() {
    join_all([
        sleep(1500).stack_trace("sleep nested 1500"),
        sleep(2500).stack_trace("sleep nested 2500"),
    ])
    .await;
}

async fn multi_sleep() {
    sleep(400).await;

    sleep(800).stack_trace("sleep another in multi sleep").await;
}

#[stream(item = ())]
async fn stream1() {
    loop {
        sleep(150).await;
        yield;
    }
}

#[stream(item = ())]
async fn stream2() {
    sleep(200).await;
    yield;
    join_all([
        sleep(400).stack_trace("sleep nested 400"),
        sleep(600).stack_trace("sleep nested 600"),
    ])
    .stack_trace("sleep nested another in stream 2")
    .await;
    yield;
}

async fn hello() {
    async move {
        // Join
        join_all([
            sleep(1000).boxed().stack_trace(format!("sleep {}", 1000)),
            sleep(2000).boxed().stack_trace("sleep 2000"),
            sleep_nested().boxed().stack_trace("sleep nested"),
            multi_sleep().boxed().stack_trace("multi sleep"),
        ])
        .await;

        // Join another
        join_all([
            sleep(1200).stack_trace("sleep 1200"),
            sleep(2200).stack_trace("sleep 2200"),
        ])
        .await;

        // Cancel
        select_all([
            sleep(666).boxed().stack_trace("sleep 666"),
            sleep_nested()
                .boxed()
                .stack_trace("sleep nested (should be cancelled)"),
        ])
        .await;

        // Check whether cleaned up
        sleep(233).stack_trace("sleep 233").await;

        // Check stream next drop
        {
            let mut stream1 = stream1().fuse().boxed();
            let mut stream2 = stream2().fuse().boxed();
            let mut count = 0;

            'outer: loop {
                tokio::select! {
                    _ = stream1.next().stack_trace(format!("stream1 next {count}")) => {},
                    r = stream2.next().stack_trace(format!("stream2 next {count}")) => {
                        if r.is_none() { break 'outer }
                    },
                }
                count += 1;
                sleep(50).stack_trace("sleep before next stream poll").await;
            }
        }

        // Check whether cleaned up
        sleep(233).stack_trace("sleep 233").await;

        // TODO: add tests on sending the future to another task or context.
    }
    .stack_trace("hello")
    .await;

    // Aborted futures have been cleaned up. There should only be a single active node of root.
    assert_eq!(with_context(|c| c.active_node_count()), 1);
}

#[tokio::test]
async fn test_stack_trace_display() {
    let (watch_tx, mut watch_rx) = watch::channel(Default::default());

    let collector = tokio::spawn(async move {
        while watch_rx.changed().await.is_ok() {
            println!("{}", &*watch_rx.borrow());
        }
    });

    TraceReporter { tx: watch_tx }
        .trace(hello(), "actor 233", true, Duration::from_millis(50))
        .await;

    collector.await.unwrap();
}
