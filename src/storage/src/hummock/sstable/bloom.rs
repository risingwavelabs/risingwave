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

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::f64;

use bytes::BufMut;

use super::filter::FilterBuilder;
use super::Sstable;

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

/// Bloom implements Bloom filter functionalities over a bit-slice of data.
#[allow(dead_code)]
#[derive(Clone)]
pub struct BloomFilterReader {
    /// data of filter in bits
    data: Vec<u8>,
    /// number of hash functions
    k: u8,
}

impl BloomFilterReader {
    /// Creates a Bloom filter from a byte slice
    #[allow(dead_code)]
    pub fn new(mut buf: Vec<u8>) -> Self {
        if buf.len() <= 1 {
            return Self { data: vec![], k: 0 };
        }
        let k = buf[buf.len() - 1];
        buf.resize(buf.len() - 1, 0);
        Self { data: buf, k }
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[allow(dead_code)]
    pub fn get_raw_data(&self) -> &[u8] {
        &self.data
    }

    /// Judges whether the hash value is in the table with the given false positive rate.
    ///
    /// Note:
    ///   - if the return value is false, then the table surely does not have the user key that has
    ///     the hash;
    ///   - if the return value is true, then the table may or may not have the user key that has
    ///     the hash actually, a.k.a. we don't know the answer.
    #[allow(dead_code)]
    pub fn may_match(&self, mut h: u32) -> bool {
        if self.k > 30 || self.k == 00 {
            // potential new encoding for short Bloom filters
            true
        } else {
            let nbits = self.data.bit_len();
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bit_pos = h % (nbits as u32);
                if !self.data.get_bit(bit_pos as usize) {
                    return false;
                }
                h = h.wrapping_add(delta);
            }
            true
        }
    }
}

pub struct BloomFilterBuilder {
    key_hash_entries: Vec<u32>,
    bits_per_key: usize,
}

impl BloomFilterBuilder {
    pub fn new(bloom_false_positive: f64, capacity: usize) -> Self {
        let key_hash_entries = if capacity > 0 {
            Vec::with_capacity(capacity)
        } else {
            vec![]
        };
        let bits_per_key = bloom_bits_per_key(capacity, bloom_false_positive);
        Self {
            key_hash_entries,
            bits_per_key,
        }
    }
}

/// Gets Bloom filter bits per key from entries count and FPR
pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
    let size = -1.0 * (entries as f64) * false_positive_rate.ln() / f64::consts::LN_2.powi(2);
    let locs = (size / (entries as f64)).ceil();
    locs as usize
}

impl FilterBuilder for BloomFilterBuilder {
    fn add_key(&mut self, key: &[u8], table_id: u32) {
        self.key_hash_entries
            .push(Sstable::hash_for_bloom_filter_u32(key, table_id));
    }

    fn approximate_len(&self) -> usize {
        self.key_hash_entries.len() * 4
    }

    fn finish(&mut self) -> Vec<u8> {
        // 0.69 is approximately ln(2)
        let k = ((self.bits_per_key as f64) * 0.69) as u32;
        // limit k in [1, 30]
        let k = k.clamp(1, 30);
        // For small len(keys), we set a minimum Bloom filter length to avoid high FPR
        let nbits = (self.key_hash_entries.len() * self.bits_per_key).max(64);
        let nbytes = (nbits + 7) / 8;
        // nbits is always multiplication of 8
        let nbits = nbytes * 8;
        let mut filter = Vec::with_capacity(nbytes + 1);
        filter.resize(nbytes, 0);
        for h in &self.key_hash_entries {
            let mut h = *h;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..k {
                let bit_pos = (h as usize) % nbits;
                filter.set_bit(bit_pos, true);
                h = h.wrapping_add(delta);
            }
        }
        filter.put_u8(k as u8);
        self.key_hash_entries.clear();
        filter
    }

    fn create(fpr: f64, capacity: usize) -> Self {
        BloomFilterBuilder::new(fpr, capacity)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::BitXor;

    use bytes::Bytes;
    use xxhash_rust::xxh32;

    use super::*;

    #[test]
    fn test_small_bloom_filter() {
        let mut builder = BloomFilterBuilder::new(0.01, 2);
        builder.add_key(b"hello", 0);
        builder.add_key(b"world", 0);
        let buf = builder.finish();

        let check_hash: Vec<u32> = vec![
            b"hello".to_vec(),
            b"world".to_vec(),
            b"x".to_vec(),
            b"fool".to_vec(),
        ]
        .into_iter()
        .map(|x| xxh32::xxh32(&x, 0).bitxor(0))
        .collect();

        let f = BloomFilterReader::new(buf);
        assert_eq!(f.k, 6);

        assert!(f.may_match(check_hash[0]));
        assert!(f.may_match(check_hash[1]));
        assert!(!f.may_match(check_hash[2]));
        assert!(!f.may_match(check_hash[3]));
    }

    fn false_positive_rate_case(
        preset_key_count: usize,
        test_key_count: usize,
        expected_false_positive_rate: f64,
    ) {
        let mut builder = BloomFilterBuilder::new(expected_false_positive_rate, preset_key_count);
        for i in 0..preset_key_count {
            let k = Bytes::from(format!("{:032}", i));
            builder.add_key(&k, 0);
        }

        let data = builder.finish();
        let filter = BloomFilterReader::new(data);

        let mut true_count = 0;
        for i in preset_key_count..preset_key_count + test_key_count {
            let k = Bytes::from(format!("{:032}", i));
            let h = xxh32::xxh32(&k, 0);
            if !filter.may_match(h) {
                true_count += 1;
            }
        }

        let false_positive_rate = 1_f64 - true_count as f64 / test_key_count as f64;
        assert!(false_positive_rate < 3_f64 * expected_false_positive_rate);
    }

    #[test]
    #[ignore]
    fn test_false_positive_rate() {
        const KEY_COUNT: usize = 1300000;
        const TEST_KEY_COUNT: usize = 100000;
        false_positive_rate_case(KEY_COUNT, TEST_KEY_COUNT, 0.1);
        false_positive_rate_case(KEY_COUNT, TEST_KEY_COUNT, 0.01);
        false_positive_rate_case(KEY_COUNT, TEST_KEY_COUNT, 0.001);
    }
}
