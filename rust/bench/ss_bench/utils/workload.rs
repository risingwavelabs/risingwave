use std::sync::atomic::AtomicU64;

use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use risingwave_storage::hummock::key::next_key;

use crate::WorkloadType::*;
use crate::{Opts, WorkloadType};

type Prefixes = Vec<Bytes>;
type Keys = Vec<Bytes>;
type Values = Vec<Option<Bytes>>;

pub struct Workload; // (Prefixes, Keys, Values);
                     //     prefixes: Vec<Bytes>,
                     //     pub keys: Vec<Bytes>,
                     //     pub values: Vec<Option<Bytes>>,
                     // }

impl Workload {
    pub(crate) fn make_batch(
        keys: Vec<Bytes>,
        values: Vec<Option<Bytes>>,
    ) -> Vec<(Bytes, Option<Bytes>)> {
        let mut batch = keys.into_iter().zip_eq(values.into_iter()).collect_vec();
        batch.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        // As duplication rate is low, ignore filling data after deduplicating.
        batch.dedup_by(|(k1, _), (k2, _)| k1 == k2);
        batch
    }

    pub(crate) fn gen(
        opts: &Opts,
        workload_type: WorkloadType,
        seed: Option<u64>,
    ) -> (Prefixes, Keys, Values) {
        let base_seed = seed.unwrap_or(233);

        // get ceil result
        let prefix_num =
            (opts.batch_size + opts.keys_per_prefix - 1) as u64 / opts.keys_per_prefix as u64;
        let (prefixes, keys) = match workload_type {
            WriteBatch | PrefixScanRandom | DeleteRandom => {
                Self::new_random_keys(opts, base_seed, prefix_num)
            }
            GetSeq | DeleteSeq => Self::new_sequential_keys(opts, prefix_num),
        };

        let values = match workload_type {
            DeleteRandom | DeleteSeq => vec![None; keys.len()],
            _ => Self::new_values(opts, base_seed),
        };

        (prefixes, keys, values)
    }

    fn new_values(opts: &Opts, base_seed: u64) -> Vec<Option<Bytes>> {
        let str_dist = Uniform::new_inclusive(0, 255);
        let value_num = opts.batch_size as u64;
        (0..value_num)
            .into_iter()
            .map(|i| {
                // set random seed to make bench reproducable
                let value = StdRng::seed_from_u64(base_seed + i)
                    .sample_iter(&str_dist)
                    .take(opts.value_size as usize)
                    .map(u8::from)
                    .collect_vec();

                Some(Bytes::from(value))
            })
            .collect()
    }

    fn new_random_keys(opts: &Opts, base_seed: u64, prefix_num: u64) -> (Prefixes, Keys) {
        // --- get prefixes ---
        let str_dist = Uniform::new_inclusive(0, 255);

        let prefixes = (0..prefix_num)
            .into_iter()
            .map(|i| {
                // set random seed to make bench reproducable
                let prefix = StdRng::seed_from_u64(base_seed + i)
                    .sample_iter(&str_dist)
                    .take(opts.key_prefix_size as usize)
                    .map(u8::from)
                    .collect_vec();

                Bytes::from(prefix)
            })
            .collect_vec();

        // --- get keys ---
        let keys = (0..opts.batch_size as u64)
            .into_iter()
            .map(|i| {
                // set random seed to make bench reproducable
                let user_key = StdRng::seed_from_u64(base_seed + i + 1)
                    .sample_iter(&str_dist)
                    .take(opts.key_size as usize)
                    .map(u8::from)
                    .collect_vec();
                let mut key =
                    BytesMut::with_capacity((opts.key_prefix_size + opts.key_size) as usize);
                // make sure prefixes are evenly distributed
                key.extend_from_slice(&prefixes[i as usize % prefixes.len()]);
                key.extend_from_slice(user_key.as_ref());

                key.freeze()
            })
            .collect();

        (prefixes, keys)
    }

    fn new_sequential_keys(opts: &Opts, prefix_num: u64) -> (Prefixes, Keys) {
        // --- get prefixes ---
        let mut prefixes = Vec::with_capacity(prefix_num as usize);
        let mut prefix = vec![b'\0'; opts.key_prefix_size as usize];
        for _ in 0..prefix_num as u64 {
            prefix = next_key(&prefix);
            // ensure next prefix exist
            assert_ne!(prefix.len(), 0);
            prefixes.push(Bytes::from(prefix.clone()));
        }

        // --- get keys ---
        let mut keys = Vec::with_capacity(opts.batch_size as usize);
        let mut user_key = vec![b'\0'; opts.key_size as usize];

        for _ in 0..opts.keys_per_prefix as u64 {
            user_key = next_key(&user_key);
            // ensure next key exist
            assert_ne!(user_key.len(), 0);

            // keys in a keyspace should be sequential
            for prefix in &prefixes {
                let mut key =
                    BytesMut::with_capacity((opts.key_prefix_size + opts.key_size) as usize);
                // make sure prefixes are evenly distributed
                key.extend_from_slice(prefix);
                key.extend_from_slice(user_key.as_ref());

                keys.push(key.freeze());
            }
        }

        (prefixes, keys)
    }
}

pub(crate) fn get_epoch() -> u64 {
    static EPOCH: AtomicU64 = AtomicU64::new(0);
    EPOCH.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}
