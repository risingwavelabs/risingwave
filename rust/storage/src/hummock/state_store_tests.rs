use std::sync::Arc;

use bytes::Bytes;
use hyper::{Body, Request, Response};
use prometheus::{Encoder, Registry, TextEncoder};

use super::iterator::UserIterator;
use super::{HummockOptions, HummockStorage};
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
use crate::object::InMemObjectStore;

fn default_hummock_options() -> HummockOptions {
    HummockOptions {
        sstable_size: 256 * (1 << 20),
        block_size: 64 * (1 << 10),
        bloom_false_positive: 0.1,
        remote_dir: "hummock_001".to_string(),
        checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
        block_cache_capacity: 100,
    }
}

async fn prometheus_service(
    _req: Request<Body>,
    registry: &Registry,
) -> Result<Response<Body>, hyper::Error> {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    let mf = registry.gather();
    encoder.encode(&mf, &mut buffer).unwrap();
    let response = Response::builder()
        .header(hyper::header::CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}

#[tokio::test]
async fn test_prometheus_endpoint_hummock() {
    // Create a hummock instance so as to fill the registry.
    let hummock_options = default_hummock_options();
    let object_client = Arc::new(InMemObjectStore::new());
    let local_version_manager = Arc::new(LocalVersionManager::new(
        object_client.clone(),
        &hummock_options.remote_dir,
        None,
    ));
    let _hummock_storage = HummockStorage::new(
        object_client,
        hummock_options,
        local_version_manager,
        Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        ))),
    )
    .await
    .unwrap();

    // Send a request to the prometheus service func.
    let registry = prometheus::default_registry();
    let mut response = prometheus_service(
        Request::get("/metrics").body(Body::default()).unwrap(),
        registry,
    )
    .await
    .unwrap();

    let bytes = hyper::body::to_bytes(response.body_mut()).await.unwrap();
    let s = std::str::from_utf8(&bytes[..]).unwrap();

    println!("\n---{}---\n", s);

    assert!(s.contains("state_store_batched_write_counts"));
    assert!(!s.contains("state_store_batched_counts"));
}

#[tokio::test]
/// Fix this when we finished epoch management.
#[ignore]
async fn test_basic() {
    let object_client = Arc::new(InMemObjectStore::new());
    let hummock_options = default_hummock_options();
    let local_version_manager = Arc::new(LocalVersionManager::new(
        object_client.clone(),
        &hummock_options.remote_dir,
        None,
    ));
    let hummock_storage = HummockStorage::new(
        object_client,
        hummock_options,
        local_version_manager,
        Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        ))),
    )
    .await
    .unwrap();
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), Some(Bytes::from("111"))),
        (Bytes::from("bb"), Some(Bytes::from("222"))),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), Some(Bytes::from("333"))),
        (anchor.clone(), Some(Bytes::from("111111"))),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Third batch deletes the anchor
    let mut batch3 = vec![
        (Bytes::from("dd"), Some(Bytes::from("444"))),
        (Bytes::from("ee"), Some(Bytes::from("555"))),
        (anchor.clone(), None),
    ];

    // Make sure the batch is sorted.
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;

    // Write first batch.
    hummock_storage
        .write_batch(
            batch1
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            epoch1,
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(&Bytes::from("ab"), epoch1)
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write second batch.
    let epoch2 = epoch1 + 1;
    hummock_storage
        .write_batch(
            batch2
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            epoch2,
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));

    // Write third batch.
    let epoch3 = epoch2 + 1;
    hummock_storage
        .write_batch(
            batch3
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            epoch3,
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch3).await.unwrap();
    assert_eq!(value, None);

    // Get non-existent maximum key.
    let value = hummock_storage
        .get(&Bytes::from("ff"), epoch3)
        .await
        .unwrap();
    assert_eq!(value, None);

    // write aa bb
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch1)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));
    // update aa, write cc
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch2)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);

    // delete aa, write dd,ee
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch3)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 4);
}

async fn count_iter(iter: &mut UserIterator<'_>) -> usize {
    let mut c: usize = 0;
    while iter.is_valid() {
        c += 1;
        iter.next().await.unwrap();
    }
    c
}

#[tokio::test]
/// Fix this when we finished epoch management.
#[ignore]
async fn test_reload_storage() {
    let mem_objstore = Arc::new(InMemObjectStore::new());
    let hummock_options = default_hummock_options();
    let local_version_manager = Arc::new(LocalVersionManager::new(
        mem_objstore.clone(),
        &hummock_options.remote_dir,
        None,
    ));
    let hummock_meta_client = Arc::new(MockHummockMetaClient::new(Arc::new(
        MockHummockMetaService::new(),
    )));

    let hummock_storage = HummockStorage::new(
        mem_objstore.clone(),
        hummock_options,
        local_version_manager.clone(),
        hummock_meta_client.clone(),
    )
    .await
    .unwrap();
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), Some(Bytes::from("111"))),
        (Bytes::from("bb"), Some(Bytes::from("222"))),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), Some(Bytes::from("333"))),
        (anchor.clone(), Some(Bytes::from("111111"))),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;

    // Write first batch.
    hummock_storage
        .write_batch(
            batch1
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            epoch1,
        )
        .await
        .unwrap();

    // Mock somthing happened to storage internal, and storage is reloaded.
    drop(hummock_storage);
    let hummock_storage = HummockStorage::new(
        mem_objstore,
        default_hummock_options(),
        local_version_manager,
        hummock_meta_client,
    )
    .await
    .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(&Bytes::from("ab"), epoch1)
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write second batch.
    let epoch2 = epoch1 + 1;
    hummock_storage
        .write_batch(
            batch2
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            epoch2,
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));

    // write aa bb
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch1)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));
    // update aa, write cc
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch2)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);
}
