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

use std::process::{Command, Output};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use risingwave_cmd_all::playground;
use risingwave_common::util::sync_point;
use risingwave_common::util::sync_point::{Signal, WaitForSignal};
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{parse_remote_object_store, ObjectStoreImpl};
use risingwave_pb::common::WorkerType;
use risingwave_rpc_client::MetaClient;
use serial_test::serial;

pub fn setup_env() {
    let current_dir =
        std::env::var("RW_WORKSPACE").expect("set env RW_WORKSPACE to project root path");
    std::env::set_current_dir(current_dir).unwrap();
    std::env::set_var("RW_META_ADDR", "http://127.0.0.1:5690");
    std::env::set_var("FORCE_SHARED_HUMMOCK_IN_MEM", "1");
    std::env::set_var("PLAYGROUND_PROFILE", "playground-test");
    std::env::set_var("OBJECT_STORE_URL", "memory-shared");
    std::env::set_var("OBJECT_STORE_BUCKET", "hummock_001");
}

pub async fn start_cluster() -> (JoinHandle<()>, std::sync::mpsc::Sender<()>) {
    wipe_object_store().await;
    sync_point::activate_sync_point(
        "CLUSTER_READY",
        vec![sync_point::Action::EmitSignal(
            "SIG_CLUSTER_READY".to_owned(),
        )],
        1,
    );

    let (tx, rx) = std::sync::mpsc::channel();
    let join_handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::spawn(async { playground().await });
            rx.recv().unwrap();
        });
    });
    // It will find "SIG_CLUSTER_READY" even when it is reached after "SIG_CLUSTER_READY" has been
    // emitted.
    wait_now("SIG_CLUSTER_READY".to_owned(), Duration::from_secs(30))
        .await
        .unwrap();
    (join_handle, tx)
}

pub async fn stop_cluster(join_handle: JoinHandle<()>, shutdown_tx: std::sync::mpsc::Sender<()>) {
    shutdown_tx.send(()).unwrap();
    join_handle.join().unwrap();
    wipe_object_store().await;
}

pub fn run_slt() -> Output {
    Command::new("./risedev")
        .args([
            "slt",
            "-d",
            "dev",
            "-p",
            "4566",
            "src/tests/sync_point/slt/tpch_snapshot_no_drop.slt",
        ])
        .spawn()
        .unwrap()
        .wait_with_output()
        .unwrap()
}

pub async fn get_object_store_client() -> ObjectStoreImpl {
    let url = std::env::var("OBJECT_STORE_URL").unwrap();
    parse_remote_object_store(&url, Arc::new(ObjectStoreMetrics::unused())).await
}

pub fn get_object_store_bucket() -> String {
    std::env::var("OBJECT_STORE_BUCKET").unwrap()
}

async fn wipe_object_store() {
    // TODO: wipe content of object store
}

pub async fn get_meta_client() -> MetaClient {
    let meta_addr = std::env::var("RW_META_ADDR").unwrap();
    let mut client = MetaClient::new(&meta_addr).await.unwrap();
    let worker_id = client
        .register(WorkerType::Generic, &"127.0.0.1:2333".parse().unwrap(), 0)
        .await
        .unwrap();
    client.set_worker_id(worker_id);
    client
}

pub async fn wait_now(signal: Signal, timeout: Duration) -> Result<(), sync_point::Error> {
    sync_point::activate_sync_point(
        "NOW",
        vec![sync_point::Action::WaitForSignal(WaitForSignal {
            signal,
            relay_signal: false,
            timeout,
        })],
        1,
    );
    sync_point::on_sync_point("NOW").await
}

pub async fn emit_now(signal: Signal) {
    sync_point::activate_sync_point("NOW", vec![sync_point::Action::EmitSignal(signal)], 1);
    sync_point::on_sync_point("NOW").await.unwrap();
}

#[tokio::test]
#[serial]
async fn test_wait_for_signal_timeout() {
    sync_point::activate_sync_point(
        "TEST_SETUP_TIMEOUT",
        vec![sync_point::Action::WaitForSignal(WaitForSignal {
            signal: "SIG_NEVER_EMIT".to_owned(),
            relay_signal: false,
            timeout: Duration::from_secs(1),
        })],
        1,
    );

    // timeout
    sync_point::on_sync_point("TEST_SETUP_TIMEOUT")
        .await
        .unwrap_err();
}

#[tokio::test]
#[serial]
async fn test_launch_cluster() {
    setup_env();
    let (join_handle, tx) = start_cluster().await;
    stop_cluster(join_handle, tx).await;
}
