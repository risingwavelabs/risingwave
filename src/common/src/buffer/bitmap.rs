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

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines a bitmap, which is used to track which values in an Arrow array are null.
//! This is called a "validity bitmap" in the Arrow documentation.
//! This file is adapted from [arrow-rs](https://github.com/apache/arrow-rs)

// allow `zip` for performance reasons
#![allow(clippy::disallowed_methods)]

use std::iter::{self, TrustedLen};
use std::mem::size_of;
use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, Not, RangeInclusive};

use risingwave_pb::common::buffer::CompressionType;
use risingwave_pb::common::PbBuffer;

use crate::estimate_size::EstimateSize;

#[derive(Default, Debug)]
pub struct BitmapBuilder {
    len: usize,
    data: Vec<usize>,
    count_ones: usize,
}

const BITS: usize = usize::BITS as usize;

impl BitmapBuilder {
    /// Creates a new empty bitmap with at least the specified capacity.
    pub fn with_capacity(capacity: usize) -> BitmapBuilder {
        BitmapBuilder {
            len: 0,
            data: Vec::with_capacity(Bitmap::vec_len(capacity)),
            count_ones: 0,
        }
    }

    /// Creates a new bitmap with all bits set to 0.
    pub fn zeroed(len: usize) -> BitmapBuilder {
        BitmapBuilder {
            len,
            data: vec![0; Bitmap::vec_len(len)],
            count_ones: 0,
        }
    }

    /// Writes a new value into a single bit.
    pub fn set(&mut self, n: usize, val: bool) {
        assert!(n < self.len);

        let byte = &mut self.data[n / BITS];
        let mask = 1 << (n % BITS);
        match (*byte & mask != 0, val) {
            (true, false) => {
                *byte &= !mask;
                self.count_ones -= 1;
            }
            (false, true) => {
                *byte |= mask;
                self.count_ones += 1;
            }
            _ => {}
        }
    }

    /// Tests a single bit.
    pub fn is_set(&self, n: usize) -> bool {
        assert!(n < self.len);
        let byte = &self.data[n / BITS];
        let mask = 1 << (n % BITS);
        *byte & mask != 0
    }

    /// Appends a single bit to the back.
    pub fn append(&mut self, bit_set: bool) -> &mut Self {
        if self.len % BITS == 0 {
            self.data.push(0);
        }
        self.data[self.len / BITS] |= (bit_set as usize) << (self.len % BITS);
        self.count_ones += bit_set as usize;
        self.len += 1;
        self
    }

    /// Appends `n` bits to the back.
    pub fn append_n(&mut self, mut n: usize, bit_set: bool) -> &mut Self {
        while n != 0 && self.len % BITS != 0 {
            self.append(bit_set);
            n -= 1;
        }
        self.len += n;
        self.data.resize(
            Bitmap::vec_len(self.len),
            if bit_set { usize::MAX } else { 0 },
        );
        if bit_set && self.len % BITS != 0 {
            // remove tailing 1s
            *self.data.last_mut().unwrap() &= (1 << (self.len % BITS)) - 1;
        }
        if bit_set {
            self.count_ones += n;
        }
        self
    }

    /// Removes the last bit.
    pub fn pop(&mut self) -> Option<()> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        self.data.truncate(Bitmap::vec_len(self.len));
        if self.len % BITS != 0 {
            *self.data.last_mut().unwrap() &= (1 << (self.len % BITS)) - 1;
        }
        Some(())
    }

    /// Appends a bitmap to the back.
    pub fn append_bitmap(&mut self, other: &Bitmap) -> &mut Self {
        if self.len % BITS == 0 {
            // self is aligned, so just append the bytes
            self.len += other.len();
            self.data.extend_from_slice(&other.bits);
            self.count_ones += other.count_ones;
        } else {
            for bit in other.iter() {
                self.append(bit);
            }
        }
        self
    }

    pub fn finish(self) -> Bitmap {
        Bitmap {
            num_bits: self.len(),
            bits: self.data.into(),
            count_ones: self.count_ones,
        }
    }

    fn len(&self) -> usize {
        self.len
    }
}

/// An immutable bitmap. Use [`BitmapBuilder`] to build it.
#[derive(Clone, PartialEq, Eq)]
pub struct Bitmap {
    /// The useful bits in the bitmap. The total number of bits will usually
    /// be larger than the useful bits due to byte-padding.
    num_bits: usize,

    /// The number of high bits in the bitmap.
    count_ones: usize,

    /// Bits are stored in a compact form.
    /// They are packed into `usize`s.
    bits: Box<[usize]>,
}

impl EstimateSize for Bitmap {
    fn estimated_heap_size(&self) -> usize {
        self.bits.len() * size_of::<usize>()
    }
}

impl std::fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for data in self.iter() {
            write!(f, "{}", data as u8)?;
        }
        Ok(())
    }
}

impl Bitmap {
    /// Creates a new bitmap with all bits set to 0.
    pub fn zeros(len: usize) -> Self {
        BitmapBuilder::zeroed(len).finish()
    }

    /// Creates a new bitmap with all bits set to 1.
    pub fn ones(num_bits: usize) -> Self {
        let len = Self::vec_len(num_bits);
        let mut bits = vec![usize::MAX; len];
        if num_bits % BITS != 0 {
            bits[len - 1] &= (1 << (num_bits % BITS)) - 1;
        }
        Self {
            bits: bits.into(),
            num_bits,
            count_ones: num_bits,
        }
    }

    /// Creates a new bitmap from vector.
    fn from_vec_with_len(buf: Vec<usize>, num_bits: usize) -> Self {
        debug_assert_eq!(buf.len(), Self::vec_len(num_bits));
        let count_ones = buf.iter().map(|&x| x.count_ones()).sum::<u32>() as usize;
        debug_assert!(count_ones <= num_bits);
        Self {
            num_bits,
            bits: buf.into(),
            count_ones,
        }
    }

    /// Creates a new bitmap from bytes.
    pub fn from_bytes(buf: &[u8]) -> Self {
        let num_bits = buf.len() * 8;
        let mut bits = Vec::with_capacity(Self::vec_len(num_bits));
        let slice = unsafe {
            bits.set_len(bits.capacity());
            std::slice::from_raw_parts_mut(bits.as_ptr() as *mut u8, bits.len() * (BITS / 8))
        };
        slice[..buf.len()].copy_from_slice(buf);
        slice[buf.len()..].fill(0);
        Self::from_vec_with_len(bits, num_bits)
    }

    /// Creates a new bitmap from a slice of `bool`.
    pub fn from_bool_slice(bools: &[bool]) -> Self {
        // use SIMD to speed up
        use std::simd::ToBitMask;
        let mut iter = bools.array_chunks::<BITS>();
        let mut bits = Vec::with_capacity(Self::vec_len(bools.len()));
        for chunk in iter.by_ref() {
            let bitmask = std::simd::Mask::<i8, BITS>::from_array(*chunk).to_bitmask() as usize;
            bits.push(bitmask);
        }
        if !iter.remainder().is_empty() {
            let mut bitmask = 0;
            for (i, b) in iter.remainder().iter().enumerate() {
                bitmask |= (*b as usize) << i;
            }
            bits.push(bitmask);
        }
        Self::from_vec_with_len(bits, bools.len())
    }

    /// Return the next set bit index on or after `bit_idx`.
    pub fn next_set_bit(&self, bit_idx: usize) -> Option<usize> {
        (bit_idx..self.len()).find(|&idx| unsafe { self.is_set_unchecked(idx) })
    }

    /// Counts the number of bits set to 1.
    pub fn count_ones(&self) -> usize {
        self.count_ones
    }

    /// Returns the length of vector to store `num_bits` bits.
    fn vec_len(num_bits: usize) -> usize {
        (num_bits + BITS - 1) / BITS
    }

    /// Returns the number of valid bits in the bitmap,
    /// also referred to its 'length'.
    #[inline]
    pub fn len(&self) -> usize {
        self.num_bits
    }

    /// Returns true if the `Bitmap` has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.num_bits == 0
    }

    /// Returns true if the bit at `idx` is set, without doing bounds checking.
    ///
    /// # Safety
    ///
    /// Index must be in range.
    pub unsafe fn is_set_unchecked(&self, idx: usize) -> bool {
        self.bits.get_unchecked(idx / BITS) & (1 << (idx % BITS)) != 0
    }

    /// Returns true if the bit at `idx` is set.
    pub fn is_set(&self, idx: usize) -> bool {
        assert!(idx < self.len());
        unsafe { self.is_set_unchecked(idx) }
    }

    /// Tests if every bit is set to 1.
    pub fn all(&self) -> bool {
        self.count_ones == self.len()
    }

    /// Produces an iterator over each bit.
    pub fn iter(&self) -> BitmapIter<'_> {
        BitmapIter {
            bits: &self.bits,
            idx: 0,
            num_bits: self.num_bits,
            current_usize: 0,
        }
    }

    /// Performs bitwise saturate subtract on two equal-length bitmaps.
    ///
    /// For example, lhs = [01110] and rhs = [00111], then
    /// `bit_saturate_subtract(lhs, rhs)` results in [01000]
    pub fn bit_saturate_subtract(&self, rhs: &Bitmap) -> Bitmap {
        assert_eq!(self.num_bits, rhs.num_bits);
        let bits = (self.bits.iter())
            .zip(rhs.bits.iter())
            .map(|(&a, &b)| (!(a & b)) & a)
            .collect();
        Bitmap::from_vec_with_len(bits, self.num_bits)
    }

    /// Enumerates the index of each bit set to 1.
    pub fn iter_ones(&self) -> BitmapOnesIter<'_> {
        if self.num_bits > 0 {
            BitmapOnesIter {
                bitmap: self,
                cur_idx: 0,
                cur_bits: Some(self.bits[0]),
            }
        } else {
            BitmapOnesIter {
                bitmap: self,
                cur_idx: 0,
                cur_bits: None,
            }
        }
    }

    /// Returns an iterator which yields the position ranges of continuous high bits.
    pub fn high_ranges(&self) -> impl Iterator<Item = RangeInclusive<usize>> + '_ {
        let mut start = None;

        self.iter()
            .chain(iter::once(false))
            .enumerate()
            .filter_map(move |(i, bit)| match (bit, start) {
                // A new high range starts.
                (true, None) => {
                    start = Some(i);
                    None
                }
                // The current high range ends.
                (false, Some(s)) => {
                    start = None;
                    Some(s..=(i - 1))
                }
                _ => None,
            })
    }

    #[cfg(test)]
    fn assert_valid(&self) {
        assert_eq!(
            self.iter().map(|x| x as usize).sum::<usize>(),
            self.count_ones
        )
    }
}

impl<'a, 'b> BitAnd<&'b Bitmap> for &'a Bitmap {
    type Output = Bitmap;

    fn bitand(self, rhs: &'b Bitmap) -> Bitmap {
        assert_eq!(self.num_bits, rhs.num_bits);
        let bits = (self.bits.iter())
            .zip(rhs.bits.iter())
            .map(|(&a, &b)| a & b)
            .collect();
        Bitmap::from_vec_with_len(bits, self.num_bits)
    }
}

impl<'a> BitAnd<Bitmap> for &'a Bitmap {
    type Output = Bitmap;

    fn bitand(self, rhs: Bitmap) -> Self::Output {
        self.bitand(&rhs)
    }
}

impl<'b> BitAnd<&'b Bitmap> for Bitmap {
    type Output = Bitmap;

    fn bitand(self, rhs: &'b Bitmap) -> Self::Output {
        rhs.bitand(self)
    }
}

impl BitAnd for Bitmap {
    type Output = Bitmap;

    fn bitand(self, rhs: Bitmap) -> Self::Output {
        (&self).bitand(&rhs)
    }
}

impl BitAndAssign<&Bitmap> for Bitmap {
    fn bitand_assign(&mut self, rhs: &Bitmap) {
        assert_eq!(self.num_bits, rhs.num_bits);
        let mut count_ones = 0;
        for (a, &b) in self.bits.iter_mut().zip(rhs.bits.iter()) {
            *a &= b;
            count_ones += a.count_ones();
        }
        self.count_ones = count_ones as usize;
    }
}

impl<'a, 'b> BitOr<&'b Bitmap> for &'a Bitmap {
    type Output = Bitmap;

    fn bitor(self, rhs: &'b Bitmap) -> Bitmap {
        assert_eq!(self.num_bits, rhs.num_bits);
        let bits = (self.bits.iter())
            .zip(rhs.bits.iter())
            .map(|(&a, &b)| a | b)
            .collect();
        Bitmap::from_vec_with_len(bits, self.num_bits)
    }
}

impl<'a> BitOr<Bitmap> for &'a Bitmap {
    type Output = Bitmap;

    fn bitor(self, rhs: Bitmap) -> Self::Output {
        self.bitor(&rhs)
    }
}

impl<'b> BitOr<&'b Bitmap> for Bitmap {
    type Output = Bitmap;

    fn bitor(self, rhs: &'b Bitmap) -> Self::Output {
        rhs.bitor(self)
    }
}

impl BitOrAssign<&Bitmap> for Bitmap {
    fn bitor_assign(&mut self, rhs: &Bitmap) {
        assert_eq!(self.num_bits, rhs.num_bits);
        let mut count_ones = 0;
        for (a, &b) in self.bits.iter_mut().zip(rhs.bits.iter()) {
            *a |= b;
            count_ones += a.count_ones();
        }
        self.count_ones = count_ones as usize;
    }
}

impl BitOrAssign<Bitmap> for Bitmap {
    fn bitor_assign(&mut self, rhs: Bitmap) {
        *self |= &rhs;
    }
}

impl BitOr for Bitmap {
    type Output = Bitmap;

    fn bitor(self, rhs: Bitmap) -> Self::Output {
        (&self).bitor(&rhs)
    }
}

impl BitXor for &Bitmap {
    type Output = Bitmap;

    fn bitxor(self, rhs: &Bitmap) -> Self::Output {
        assert_eq!(self.num_bits, rhs.num_bits);
        let bits = (self.bits.iter())
            .zip(rhs.bits.iter())
            .map(|(&a, &b)| a ^ b)
            .collect();
        Bitmap::from_vec_with_len(bits, self.num_bits)
    }
}

impl<'a> Not for &'a Bitmap {
    type Output = Bitmap;

    fn not(self) -> Self::Output {
        let mut bits: Vec<usize> = self.bits.iter().map(|b| !b).collect();
        if self.num_bits % BITS != 0 {
            bits[self.num_bits / BITS] &= (1 << (self.num_bits % BITS)) - 1;
        }
        Bitmap {
            num_bits: self.num_bits,
            count_ones: self.num_bits - self.count_ones,
            bits: bits.into(),
        }
    }
}

impl Not for Bitmap {
    type Output = Bitmap;

    fn not(mut self) -> Self::Output {
        self.bits.iter_mut().for_each(|x| *x = !*x);
        if self.num_bits % BITS != 0 {
            self.bits[self.num_bits / BITS] &= (1 << (self.num_bits % BITS)) - 1;
        }
        self.count_ones = self.num_bits - self.count_ones;
        self
    }
}

impl FromIterator<bool> for Bitmap {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        let vec = iter.into_iter().collect::<Vec<_>>();
        Self::from_bool_slice(&vec)
    }
}

impl FromIterator<Option<bool>> for Bitmap {
    fn from_iter<T: IntoIterator<Item = Option<bool>>>(iter: T) -> Self {
        iter.into_iter().map(|b| b.unwrap_or(false)).collect()
    }
}

impl Bitmap {
    pub fn to_protobuf(&self) -> PbBuffer {
        let mut body = Vec::with_capacity((self.num_bits + 7) % 8 + 1);
        body.push((self.num_bits % 8) as u8);
        body.extend_from_slice(unsafe {
            std::slice::from_raw_parts(self.bits.as_ptr() as *const u8, (self.num_bits + 7) / 8)
        });
        PbBuffer {
            body,
            compression: CompressionType::None as i32,
        }
    }
}

impl From<&PbBuffer> for Bitmap {
    fn from(buf: &PbBuffer) -> Self {
        let last_byte_num_bits = buf.body[0];
        let num_bits = ((buf.body.len() - 1) * 8) - ((8 - last_byte_num_bits) % 8) as usize;

        let mut bitmap = Self::from_bytes(&buf.body[1..]);
        bitmap.num_bits = num_bits;
        bitmap
    }
}

/// Bitmap iterator.
///
/// TODO: add `count_ones` to make it [`ExactSizeIterator`]?
pub struct BitmapIter<'a> {
    bits: &'a [usize],
    idx: usize,
    num_bits: usize,
    current_usize: usize,
}

impl<'a> BitmapIter<'a> {
    fn next_always_load_usize(&mut self) -> Option<bool> {
        if self.idx >= self.num_bits {
            return None;
        }

        // Offset of the bit within the usize.
        let usize_offset = self.idx % BITS;

        // Get the index of usize which the bit is located in
        let usize_index = self.idx / BITS;
        self.current_usize = unsafe { *self.bits.get_unchecked(usize_index) };

        let bit_mask = 1 << usize_offset;
        let bit_flag = self.current_usize & bit_mask != 0;
        self.idx += 1;
        Some(bit_flag)
    }
}

impl<'a> iter::Iterator for BitmapIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.num_bits {
            return None;
        }

        // Offset of the bit within the usize.
        let usize_offset = self.idx % BITS;

        if usize_offset == 0 {
            // Get the index of usize which the bit is located in
            let usize_index = self.idx / BITS;
            self.current_usize = unsafe { *self.bits.get_unchecked(usize_index) };
        }

        let bit_mask = 1 << usize_offset;

        let bit_flag = self.current_usize & bit_mask != 0;
        self.idx += 1;
        Some(bit_flag)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.num_bits - self.idx;
        (remaining, Some(remaining))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.idx += n;
        self.next_always_load_usize()
    }
}

impl ExactSizeIterator for BitmapIter<'_> {}
unsafe impl TrustedLen for BitmapIter<'_> {}

pub struct BitmapOnesIter<'a> {
    bitmap: &'a Bitmap,
    cur_idx: usize,
    cur_bits: Option<usize>,
}

impl<'a> iter::Iterator for BitmapOnesIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while self.cur_bits == Some(0) {
            self.cur_idx += 1;
            self.cur_bits = if self.cur_idx == (self.bitmap.len() + BITS - 1) / BITS {
                None
            } else {
                Some(self.bitmap.bits[self.cur_idx])
            }
        }
        match self.cur_bits {
            Some(bits) => {
                let low_bit = bits & bits.wrapping_neg();
                let low_bit_idx = bits.trailing_zeros();
                self.cur_bits = self.cur_bits.map(|bits| bits ^ low_bit);
                Some(self.cur_idx * BITS + low_bit_idx as usize)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_builder() {
        let bitmap1 = {
            let mut builder = BitmapBuilder::default();
            let bits = [
                false, true, true, false, true, false, true, false, true, false, true, true, false,
                true, false, true,
            ];
            for bit in bits {
                builder.append(bit);
            }
            for (idx, bit) in bits.iter().enumerate() {
                assert_eq!(builder.is_set(idx), *bit);
            }
            builder.finish()
        };
        let byte1 = 0b0101_0110_u8;
        let byte2 = 0b1010_1101_u8;
        let expected = Bitmap::from_bytes(&[byte1, byte2]);
        assert_eq!(bitmap1, expected);
        assert_eq!(
            bitmap1.count_ones(),
            (byte1.count_ones() + byte2.count_ones()) as usize
        );

        let bitmap2 = {
            let mut builder = BitmapBuilder::default();
            let bits = [false, true, true, false, true, false, true, false];
            for bit in bits {
                builder.append(bit);
            }
            for (idx, bit) in bits.iter().enumerate() {
                assert_eq!(builder.is_set(idx), *bit);
            }
            builder.finish()
        };
        let byte1 = 0b0101_0110_u8;
        let expected = Bitmap::from_bytes(&[byte1]);
        assert_eq!(bitmap2, expected);
    }

    #[test]
    fn test_bitmap_get_size() {
        let bitmap1 = {
            let mut builder = BitmapBuilder::default();
            let bits = [
                false, true, true, false, true, false, true, false, true, false, true, true, false,
                true, false, true,
            ];
            for bit in bits {
                builder.append(bit);
            }
            for (idx, bit) in bits.iter().enumerate() {
                assert_eq!(builder.is_set(idx), *bit);
            }
            builder.finish()
        };
        assert_eq!(8, bitmap1.estimated_heap_size());
        assert_eq!(40, bitmap1.estimated_size());
    }

    #[test]
    fn test_bitmap_all_high() {
        let num_bits = 3;
        let bitmap = Bitmap::ones(num_bits);
        assert_eq!(bitmap.len(), num_bits);
        assert!(bitmap.all());
        for i in 0..num_bits {
            assert!(bitmap.is_set(i));
        }
        // Test to and from protobuf is OK.
        assert_eq!(bitmap, Bitmap::from(&bitmap.to_protobuf()));
    }

    #[test]
    fn test_bitwise_and() {
        #[rustfmt::skip]
        let cases = [(
            vec![],
            vec![],
            vec![],
        ), (
            vec![0, 1, 1, 0, 1, 0, 1, 0],
            vec![0, 1, 0, 0, 1, 1, 1, 0],
            vec![0, 1, 0, 0, 1, 0, 1, 0],
        ), (
            vec![0, 1, 0, 1, 0],
            vec![1, 1, 1, 1, 0],
            vec![0, 1, 0, 1, 0],
        )];

        for (input1, input2, expected) in cases {
            let bitmap1: Bitmap = input1.into_iter().map(|x| x != 0).collect();
            let bitmap2: Bitmap = input2.into_iter().map(|x| x != 0).collect();
            let res = bitmap1 & bitmap2;
            res.assert_valid();
            assert_eq!(res.iter().map(|x| x as i32).collect::<Vec<_>>(), expected,);
        }
    }

    #[test]
    fn test_bitwise_not() {
        #[rustfmt::skip]
        let cases = [(
            vec![1, 0, 1, 0, 1],
            vec![0, 1, 0, 1, 0],
        ), (
            vec![],
            vec![],
        ), (
            vec![1, 0, 1, 1, 0, 0, 1, 1],
            vec![0, 1, 0, 0, 1, 1, 0, 0],
        ), (
            vec![1, 0, 0, 1, 1, 1, 0, 0, 1, 0],
            vec![0, 1, 1, 0, 0, 0, 1, 1, 0, 1],
        )];

        for (input, expected) in cases {
            let bitmap: Bitmap = input.into_iter().map(|x| x != 0).collect();
            let res = !bitmap;
            res.assert_valid();
            assert_eq!(res.iter().map(|x| x as i32).collect::<Vec<_>>(), expected);
        }
    }

    #[test]
    fn test_bitwise_or() {
        let bitmap1 = Bitmap::from_bytes(&[0b01101010]);
        let bitmap2 = Bitmap::from_bytes(&[0b01001110]);
        assert_eq!(Bitmap::from_bytes(&[0b01101110]), (bitmap1 | bitmap2));
    }

    #[test]
    fn test_bitwise_saturate_subtract() {
        let bitmap1 = Bitmap::from_bytes(&[0b01101010]);
        let bitmap2 = Bitmap::from_bytes(&[0b01001110]);
        assert_eq!(
            Bitmap::from_bytes(&[0b00100000]),
            Bitmap::bit_saturate_subtract(&bitmap1, &bitmap2)
        );
    }

    #[test]
    fn test_bitmap_is_set() {
        let bitmap = Bitmap::from_bytes(&[0b01001010]);
        assert!(!bitmap.is_set(0));
        assert!(bitmap.is_set(1));
        assert!(!bitmap.is_set(2));
        assert!(bitmap.is_set(3));
        assert!(!bitmap.is_set(4));
        assert!(!bitmap.is_set(5));
        assert!(bitmap.is_set(6));
        assert!(!bitmap.is_set(7));
    }

    #[test]
    fn test_bitmap_iter() {
        {
            let bitmap = Bitmap::from_bytes(&[0b01001010]);
            let mut booleans = vec![];
            for b in bitmap.iter() {
                booleans.push(b as u8);
            }
            assert_eq!(booleans, vec![0u8, 1, 0, 1, 0, 0, 1, 0]);
        }
        {
            let bitmap: Bitmap = vec![true; 5].into_iter().collect();
            for b in bitmap.iter() {
                assert!(b);
            }
        }
    }

    #[test]
    fn test_bitmap_high_ranges_iter() {
        fn test(bits: impl IntoIterator<Item = bool>, expected: Vec<RangeInclusive<usize>>) {
            let bitmap = Bitmap::from_iter(bits);
            let high_ranges = bitmap.high_ranges().collect::<Vec<_>>();
            assert_eq!(high_ranges, expected);
        }

        test(
            vec![
                true, true, true, false, false, true, true, true, false, true,
            ],
            vec![0..=2, 5..=7, 9..=9],
        );
        test(vec![true, true, true], vec![0..=2]);
        test(vec![false, false, false], vec![]);
    }

    #[test]
    fn test_bitmap_from_protobuf() {
        let bitmap_bytes = vec![3u8 /* len % BITS */, 0b0101_0010, 0b110];
        let buf = PbBuffer {
            body: bitmap_bytes,
            compression: CompressionType::None as _,
        };
        let bitmap: Bitmap = (&buf).try_into().unwrap();
        let actual_bytes: Vec<u8> = bitmap.iter().map(|b| b as u8).collect();

        assert_eq!(actual_bytes, vec![0, 1, 0, 0, 1, 0, 1, 0, /*  */ 0, 1, 1]); // in reverse order
        assert_eq!(bitmap.count_ones(), 5);
    }

    #[test]
    fn test_bitmap_from_buffer() {
        let byte1 = 0b0110_1010_u8;
        let byte2 = 0b1011_0101_u8;
        let bitmap = Bitmap::from_bytes(&[0b0110_1010, 0b1011_0101]);
        let expected = Bitmap::from_iter(vec![
            false, true, false, true, false, true, true, false, true, false, true, false, true,
            true, false, true,
        ]);
        let count_ones = (byte1.count_ones() + byte2.count_ones()) as usize;
        assert_eq!(expected, bitmap);
        assert_eq!(bitmap.count_ones(), count_ones);
        assert_eq!(expected.count_ones(), count_ones);
    }

    #[test]
    fn test_bitmap_eq() {
        let b1: Bitmap = Bitmap::zeros(3);
        let b2: Bitmap = Bitmap::zeros(5);
        assert_ne!(b1, b2);

        let b1: Bitmap = [true, false].iter().cycle().cloned().take(10000).collect();
        let b2: Bitmap = [true, false].iter().cycle().cloned().take(10000).collect();
        assert_eq!(b1, b2);
    }

    #[test]
    fn test_bitmap_set() {
        let mut b = BitmapBuilder::zeroed(10);
        assert_eq!(b.count_ones, 0);

        b.set(0, true);
        b.set(7, true);
        b.set(8, true);
        b.set(9, true);
        assert_eq!(b.count_ones, 4);

        b.set(7, false);
        b.set(8, false);
        assert_eq!(b.count_ones, 2);

        b.append(true);
        assert_eq!(b.len, 11);
        assert_eq!(b.count_ones, 3);

        let b = b.finish();
        assert_eq!(&b.bits[..], &[0b0110_0000_0001]);
    }

    #[test]
    fn test_bitmap_pop() {
        let mut b = BitmapBuilder::zeroed(7);

        {
            b.append(true);
            assert!(b.is_set(b.len() - 1));
            b.pop();
            assert!(!b.is_set(b.len() - 1));
        }

        {
            b.append(false);
            assert!(!b.is_set(b.len() - 1));
            b.pop();
            assert!(!b.is_set(b.len() - 1));
        }

        {
            b.append(true);
            b.append(false);
            assert!(!b.is_set(b.len() - 1));
            b.pop();
            assert!(b.is_set(b.len() - 1));
            b.pop();
            assert!(!b.is_set(b.len() - 1));
        }
    }

    #[test]
    fn test_bitmap_iter_ones() {
        let mut builder = BitmapBuilder::zeroed(1000);
        builder.append_n(1000, true);
        let bitmap = builder.finish();
        let mut iter = bitmap.iter_ones();
        for i in 0..1000 {
            let item = iter.next();
            assert!(item == Some(i + 1000));
        }
        for _ in 0..5 {
            let item = iter.next();
            assert!(item.is_none());
        }
    }
}
