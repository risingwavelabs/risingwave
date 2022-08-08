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

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::f64;

use bytes::BufMut;

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
pub struct Bloom<'a> {
    /// data of filter in bits
    filter: &'a [u8],
    /// number of hash functions
    k: u8,
}

impl<'a> Bloom<'a> {
    /// Creates a Bloom filter from a byte slice
    pub fn new(buf: &'a [u8]) -> Self {
        let filter = &buf[..buf.len() - 1];
        let k = buf[buf.len() - 1];
        Self { filter, k }
    }

    /// Gets Bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size = -1.0 * (entries as f64) * false_positive_rate.ln() / f64::consts::LN_2.powi(2);
        let locs = (f64::consts::LN_2 * size / (entries as f64)).ceil();
        locs as usize
    }

    /// Builds Bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Vec<u8> {
        // 0.69 is approximately ln(2)
        let k = ((bits_per_key as f64) * 0.69) as u32;
        // limit k in [1, 30]
        let k = k.min(30).max(1);
        // For small len(keys), we set a minimum Bloom filter length to avoid high FPR
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) / 8;
        // nbits is always multiplication of 8
        let nbits = nbytes * 8;
        let mut filter = Vec::with_capacity(nbytes + 1);
        filter.resize(nbytes, 0);
        for h in keys {
            let mut h = *h;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..k {
                let bit_pos = (h as usize) % nbits;
                filter.set_bit(bit_pos, true);
                h = h.wrapping_add(delta);
            }
        }
        filter.put_u8(k as u8);
        filter
    }

    /// Judges whether the hash value is in the table with the given false positive rate.
    ///
    /// Note:
    ///   - if the return value is true, then the table surely does not have the user key that has
    ///     the hash;
    ///   - if the return value is false, then the table may or may not have the user key that has
    ///     the hash actually, a.k.a. we don't know the answer.
    pub fn surely_not_have_hash(&self, mut h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short Bloom filters
            false
        } else {
            let nbits = self.filter.bit_len();
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bit_pos = h % (nbits as u32);
                if !self.filter.get_bit(bit_pos as usize) {
                    return true;
                }
                h = h.wrapping_add(delta);
            }
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_bloom_filter() {
        let hash: Vec<u32> = vec![b"hello".to_vec(), b"world".to_vec()]
            .into_iter()
            .map(|x| farmhash::fingerprint32(&x))
            .collect();
        let buf = Bloom::build_from_key_hashes(&hash, 10);

        let check_hash: Vec<u32> = vec![
            b"hello".to_vec(),
            b"world".to_vec(),
            b"x".to_vec(),
            b"fool".to_vec(),
        ]
        .into_iter()
        .map(|x| farmhash::fingerprint32(&x))
        .collect();

        let f = Bloom::new(&buf);
        assert_eq!(f.k, 6);

        assert!(!f.surely_not_have_hash(check_hash[0]));
        assert!(!f.surely_not_have_hash(check_hash[1]));
        assert!(f.surely_not_have_hash(check_hash[2]));
        assert!(f.surely_not_have_hash(check_hash[3]));
    }
}
