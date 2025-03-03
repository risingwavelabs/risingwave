// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use risingwave_common::util::iter_util::ZipEqFast;

#[derive(Debug, Clone)]
pub struct Hitmap<const N: usize> {
    /// For [`Hitmap`] is rarely access in multi-thread pattern,
    /// the cons of false-sharing can be ignored.
    data: Arc<[AtomicU64; N]>,
}

impl<const N: usize> Default for Hitmap<N> {
    fn default() -> Self {
        let data = [(); N].map(|_| AtomicU64::default());
        let data = Arc::new(data);
        Self { data }
    }
}

impl<const N: usize> Hitmap<N> {
    pub const fn bits() -> usize {
        N * u64::BITS as usize
    }

    pub const fn bytes() -> usize {
        N * 8
    }

    pub fn report(&self, local: &mut LocalHitmap<N>) {
        for (global, local) in self.data.iter().zip_eq_fast(local.data.iter()) {
            global.fetch_or(*local, Ordering::Relaxed);
        }
        local.reset();
    }

    pub fn ones(&self) -> usize {
        let mut res = 0;
        for elem in &*self.data {
            res += elem.load(Ordering::Relaxed).count_ones() as usize;
        }
        res
    }

    pub fn zeros(&self) -> usize {
        Self::bits() - self.ones()
    }

    pub fn ratio(&self) -> f64 {
        self.ones() as f64 / Self::bits() as f64
    }

    #[cfg(test)]
    pub fn to_hex_vec(&self) -> Vec<String> {
        use itertools::Itertools;
        self.data
            .iter()
            .map(|elem| elem.load(Ordering::Relaxed))
            .map(|v| format!("{v:016x}"))
            .collect_vec()
    }
}

#[derive(Debug)]
pub struct LocalHitmap<const N: usize> {
    data: Box<[u64; N]>,
}

impl<const N: usize> Default for LocalHitmap<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> LocalHitmap<N> {
    pub const fn bits() -> usize {
        N * u64::BITS as usize
    }

    pub const fn bytes() -> usize {
        N * 8
    }

    pub fn new() -> Self {
        Self {
            data: Box::new([0; N]),
        }
    }

    pub fn reset(&mut self) {
        for elem in &mut *self.data {
            *elem = 0;
        }
    }

    pub fn merge(&mut self, other: &mut Self) {
        for (elem, e) in self.data.iter_mut().zip_eq_fast(other.data.iter()) {
            *elem |= *e;
        }
        other.reset();
    }

    pub fn fill(&mut self, start_bit: usize, end_bit: usize) {
        const MASK: usize = (1 << 6) - 1;

        let end_bit = end_bit.clamp(start_bit + 1, Self::bits());

        let head_bits = start_bit & MASK;
        let tail_bits_rev = end_bit & MASK;

        let head_elem = start_bit >> 6;
        let tail_elem = end_bit >> 6;

        for i in head_elem..=std::cmp::min(tail_elem, N - 1) {
            let elem = &mut self.data[i];
            let mut umask = 0u64;
            if i == head_elem {
                umask |= (1u64 << head_bits) - 1;
            }
            if i == tail_elem {
                umask |= !((1u64 << tail_bits_rev) - 1);
            }
            *elem |= !umask;
        }
    }

    pub fn fill_with_range(&mut self, start: usize, end: usize, len: usize) {
        let start_bit = Self::bits() * start / len;
        let end_bit = Self::bits() * end / len;
        self.fill(start_bit, end_bit)
    }

    pub fn ones(&self) -> usize {
        let mut res = 0;
        for elem in &*self.data {
            res += elem.count_ones() as usize;
        }
        res
    }

    pub fn zeros(&self) -> usize {
        Self::bits() - self.ones()
    }

    pub fn ratio(&self) -> f64 {
        self.ones() as f64 / Self::bits() as f64
    }

    #[cfg(test)]
    pub fn to_hex_vec(&self) -> Vec<String> {
        use itertools::Itertools;
        self.data.iter().map(|v| format!("{v:016x}")).collect_vec()
    }
}

#[cfg(debug_assertions)]
impl<const N: usize> Drop for LocalHitmap<N> {
    fn drop(&mut self) {
        if self.ones() > 0 {
            panic!("LocalHitmap is not reported!");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hitmap() {
        // hex: high <== low
        let g = Hitmap::<4>::default();

        let mut h = LocalHitmap::new();
        assert_eq!(
            h.to_hex_vec(),
            vec![
                "0000000000000000",
                "0000000000000000",
                "0000000000000000",
                "0000000000000000",
            ]
        );
        assert_eq!(h.ones(), 0);
        h.fill(16, 24);
        assert_eq!(
            h.to_hex_vec(),
            vec![
                "0000000000ff0000",
                "0000000000000000",
                "0000000000000000",
                "0000000000000000",
            ]
        );
        assert_eq!(h.ones(), 8);
        h.fill(32, 64);
        assert_eq!(
            h.to_hex_vec(),
            vec![
                "ffffffff00ff0000",
                "0000000000000000",
                "0000000000000000",
                "0000000000000000",
            ]
        );
        assert_eq!(h.ones(), 40);
        h.fill(96, 224);
        assert_eq!(
            h.to_hex_vec(),
            vec![
                "ffffffff00ff0000",
                "ffffffff00000000",
                "ffffffffffffffff",
                "00000000ffffffff",
            ]
        );
        assert_eq!(h.ones(), 168);
        h.fill(0, 256);
        assert_eq!(
            h.to_hex_vec(),
            vec![
                "ffffffffffffffff",
                "ffffffffffffffff",
                "ffffffffffffffff",
                "ffffffffffffffff",
            ]
        );
        assert_eq!(h.ones(), 256);
        g.report(&mut h);
        assert_eq!(
            g.to_hex_vec(),
            vec![
                "ffffffffffffffff",
                "ffffffffffffffff",
                "ffffffffffffffff",
                "ffffffffffffffff",
            ]
        );
        assert_eq!(g.ones(), 256);
    }
}
