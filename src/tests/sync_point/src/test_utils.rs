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
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{parse_remote_object_store, ObjectStoreImpl};
use risingwave_pb::common::WorkerType;
use risingwave_rpc_client::MetaClient;
use serial_test::serial;

pub fn setup_env() {
    sync_point::reset();
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
    // It will find "CLUSTER_READY" even when it is reached after "CLUSTER_READY" has been emitted.
    sync_point::wait_timeout("CLUSTER_READY", Duration::from_secs(30))
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
    let url = std::env::var("OBJECT_STORE_URL").unwrap();
    assert_eq!(url, "memory-shared");
    risingwave_object_store::object::InMemObjectStore::reset_shared();
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

#[tokio::test]
#[serial]
#[should_panic]
async fn test_wait_timeout() {
    sync_point::hook("TEST_SETUP_TIMEOUT", || async {
        sync_point::wait_timeout("SIG_NEVER_EMIT", Duration::from_secs(1))
            .await
            .unwrap();
    });

    // timeout
    sync_point::on("TEST_SETUP_TIMEOUT").await;
}

#[tokio::test]
#[serial]
async fn test_launch_cluster() {
    setup_env();
    let (join_handle, tx) = start_cluster().await;
    stop_cluster(join_handle, tx).await;
}
