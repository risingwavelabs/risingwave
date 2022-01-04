use bytes::Bytes;
use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::Opts;

pub fn gen_workload(opts: &Opts) -> Vec<(Bytes, Option<Bytes>)> {
    let mut batch: Vec<(Bytes, Option<Bytes>)> = (0..opts.kvs_per_batch as u64)
        .into_iter()
        .map(|i| {
            // set random seed to make bench reproducable
            let key: String = StdRng::seed_from_u64(i)
                .sample_iter(&Alphanumeric)
                .take(opts.key_size as usize)
                .map(char::from)
                .collect();

            let value: String = StdRng::seed_from_u64(i)
                .sample_iter(&Alphanumeric)
                .take(opts.value_size as usize)
                .map(char::from)
                .collect();

            (Bytes::from(key), Some(Bytes::from(value)))
        })
        .collect();
    batch.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // As duplication rate is low, ignore filling data after deduplicating.
    batch.dedup_by(|(k1, _), (k2, _)| k1 == k2);

    batch
}
