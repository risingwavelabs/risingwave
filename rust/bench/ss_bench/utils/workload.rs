use std::sync::atomic::AtomicU64;

use bytes::{Bytes, BytesMut};
use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use risingwave_storage::StateStore;

use crate::Opts;

pub(crate) struct Workload {
    pub batch: Vec<(Bytes, Option<Bytes>)>,
    pub prefixes: Vec<Bytes>,
}

impl Workload {
    pub(crate) fn new_sorted_workload(opts: &Opts, seed: Option<u64>) -> Workload {
        let base_seed = seed.unwrap_or(233);

        // get ceil result
        let prefix_num = (opts.kvs_per_batch + opts.key_prefix_frequency - 1) as u64
            / opts.key_prefix_frequency as u64;

        let prefixes: Vec<Bytes> = (0..prefix_num)
            .into_iter()
            .map(|i| {
                // set random seed to make bench reproducable
                let prefix: String = StdRng::seed_from_u64(base_seed + i)
                    .sample_iter(&Alphanumeric)
                    .take(opts.key_prefix_size as usize)
                    .map(char::from)
                    .collect();

                Bytes::from(prefix)
            })
            .collect();

        let mut batch: Vec<(Bytes, Option<Bytes>)> = (0..opts.kvs_per_batch as u64)
            .into_iter()
            .map(|i| {
                // set random seed to make bench reproducable
                let pure_key: String = StdRng::seed_from_u64(base_seed + i + 1)
                    .sample_iter(&Alphanumeric)
                    .take(opts.key_size as usize)
                    .map(char::from)
                    .collect();
                let mut key =
                    BytesMut::with_capacity((opts.key_prefix_size + opts.key_size) as usize);
                // make sure prefixes are evenly distributed
                key.extend_from_slice(&prefixes[i as usize % prefixes.len()]);
                key.extend_from_slice(pure_key.as_ref());

                let value: String = StdRng::seed_from_u64(base_seed + i + 2)
                    .sample_iter(&Alphanumeric)
                    .take(opts.value_size as usize)
                    .map(char::from)
                    .collect();

                (key.freeze(), Some(Bytes::from(value)))
            })
            .collect();
        batch.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        // As duplication rate is low, ignore filling data after deduplicating.
        batch.dedup_by(|(k1, _), (k2, _)| k1 == k2);

        Workload { batch, prefixes }
    }

    pub(crate) async fn del_batch(store: &impl StateStore, batch: Vec<(Bytes, Option<Bytes>)>) {
        let del_batch = batch.into_iter().map(|(k, _)| (k, None)).collect();
        store.ingest_batch(del_batch, get_epoch()).await.unwrap();
    }
}

pub(crate) fn get_epoch() -> u64 {
    static EPOCH: AtomicU64 = AtomicU64::new(0);
    EPOCH.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}
