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

use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::rngs::StdRng;
use rand::Rng;
use risingwave_common::storage::key::next_key;

use crate::Opts;

type Prefixes = Vec<Bytes>;
type Keys = Vec<Bytes>;

pub struct Workload;

type Batch = Vec<(Bytes, Option<Bytes>)>;

impl Workload {
    pub(crate) fn make_batches(
        opts: &Opts,
        keys: Vec<Bytes>,
        values: Vec<Option<Bytes>>,
    ) -> Vec<Batch> {
        let mut batches = vec![];
        let mut batch = vec![];

        let pairs = keys.into_iter().zip_eq(values.into_iter()).collect_vec();
        for (k, v) in pairs {
            batch.push((k, v));
            if batch.len() == opts.batch_size as usize {
                batch.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
                // As duplication rate is low, ignore filling data after deduplicating.
                batch.dedup_by(|(k1, _), (k2, _)| k1 == k2);

                batches.push(batch);
                batch = vec![];
            }
        }
        if !batch.is_empty() {
            batch.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
            batch.dedup_by(|(k1, _), (k2, _)| k1 == k2);
            batches.push(batch);
        }

        batches
    }

    /// Generate the values of given number
    pub(crate) fn new_values(opts: &Opts, value_num: u64, rng: &mut StdRng) -> Vec<Option<Bytes>> {
        let str_dist = Uniform::new_inclusive(0, 255);
        (0..value_num)
            .into_iter()
            .map(|_| {
                let value = rng
                    .sample_iter(&str_dist)
                    .take(opts.value_size as usize)
                    .map(u8::from)
                    .collect_vec();

                Some(Bytes::from(value))
            })
            .collect()
    }

    /// Determine the prefix number of given keys
    fn prefix_num(opts: &Opts, keys_num: u64) -> u64 {
        // get ceil result
        (keys_num + opts.keys_per_prefix as u64 - 1) / opts.keys_per_prefix as u64
    }

    /// Generate the random prefixes of given number
    pub(crate) fn new_random_prefixes(opts: &Opts, prefix_num: u64, rng: &mut StdRng) -> Prefixes {
        let str_dist = Uniform::new_inclusive(0, 255);

        (0..prefix_num)
            .into_iter()
            .map(|_| {
                let prefix = rng
                    .sample_iter(&str_dist)
                    .take(opts.key_prefix_size as usize)
                    .map(u8::from)
                    .collect_vec();

                Bytes::from(prefix)
            })
            .collect_vec()
    }

    /// Generate the random keys of given number
    pub(crate) fn new_random_keys(opts: &Opts, key_num: u64, rng: &mut StdRng) -> (Prefixes, Keys) {
        // --- get prefixes ---
        let prefix_num = Self::prefix_num(opts, key_num);
        let prefixes = Workload::new_random_prefixes(opts, prefix_num, rng);

        // --- get keys ---
        let str_dist = Uniform::new_inclusive(0, 255);
        let keys = (0..key_num as u64)
            .into_iter()
            .map(|i| {
                let user_key = rng
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

    /// Generate the sequential keys of given number
    pub(crate) fn new_sequential_keys(opts: &Opts, key_num: u64) -> (Prefixes, Keys) {
        // --- get prefixes ---
        let prefix_num = Self::prefix_num(opts, key_num);
        let mut prefixes = Vec::with_capacity(prefix_num as usize);
        let mut prefix = vec![b'\0'; opts.key_prefix_size as usize];
        for _ in 0..prefix_num as u64 {
            prefix = next_key(&prefix);
            // ensure next prefix exist
            assert!(!prefix.is_empty());
            prefixes.push(Bytes::from(prefix.clone()));
        }

        // --- get keys ---
        let mut keys = Vec::with_capacity(key_num as usize);
        let mut user_key = vec![b'\0'; opts.key_size as usize];

        for _ in 0..opts.keys_per_prefix as u64 {
            user_key = next_key(&user_key);
            // ensure next key exist
            assert!(!user_key.is_empty());

            // keys in a key range should be sequential
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
