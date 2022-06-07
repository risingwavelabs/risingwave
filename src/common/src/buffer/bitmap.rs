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

use std::ops::{BitAnd, BitOr};

use bytes::Bytes;
use itertools::Itertools;
use risingwave_pb::data::buffer::CompressionType;
use risingwave_pb::data::Buffer as ProstBuffer;

use crate::array::{Array, BoolArray};
use crate::error::{Result, RwError};
use crate::util::bit_util;

#[derive(Default, Debug)]
pub struct BitmapBuilder {
    len: usize,
    data: Vec<u8>,
    num_high_bits: usize,

    // `head` is 'dirty' bitmap data and will be flushed to `self.data` when `self.len%8==0`.
    head: u8,
}

impl BitmapBuilder {
    pub fn with_capacity(capacity: usize) -> BitmapBuilder {
        BitmapBuilder {
            len: 0,
            data: Vec::with_capacity((capacity + 7) / 8),
            num_high_bits: 0,
            head: 0,
        }
    }

    pub fn append(&mut self, bit_set: bool) -> &mut Self {
        self.head |= (bit_set as u8) << (self.len % 8);
        self.num_high_bits += bit_set as usize;
        self.len += 1;
        if self.len % 8 == 0 {
            self.data.push(self.head);
            self.head = 0;
        }
        self
    }

    pub fn append_bitmap(&mut self, other: &Bitmap) -> &mut Self {
        for bit in other.iter() {
            self.append(bit);
        }
        self
    }

    pub fn finish(mut self) -> Bitmap {
        if self.len % 8 != 0 {
            self.data.push(self.head);
        }
        let num_high_bits = self.num_high_bits;

        Bitmap {
            num_bits: self.len(),
            bits: self.data.into(),
            num_high_bits,
        }
    }

    fn len(&self) -> usize {
        self.len
    }
}

#[derive(Clone)]
pub struct Bitmap {
    pub bits: Bytes,

    // The useful bits in the bitmap. The total number of bits will usually
    // be larger than the useful bits due to byte-padding.
    num_bits: usize,

    // The number of high bits in the bitmap.
    num_high_bits: usize,
}

impl std::fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let mut is_first = true;
        for data in self.iter() {
            if is_first {
                write!(f, "{}", data)?;
            } else {
                write!(f, ", {}", data)?;
            }
            is_first = false;
        }
        write!(f, "]")
    }
}

impl Bitmap {
    pub fn new(num_bits: usize) -> Result<Self> {
        let len = Self::num_of_bytes(num_bits);
        Ok(Self {
            bits: vec![0; len].into(),
            num_bits,
            num_high_bits: 0,
        })
    }

    pub fn from_bytes_with_num_bits(buf: Bytes, num_bits: usize) -> Self {
        assert!(num_bits <= buf.len() << 3);

        let num_high_bits = buf.iter().map(|x| x.count_ones()).sum::<u32>() as usize;
        Self {
            num_bits,
            bits: buf,
            num_high_bits,
        }
    }

    #[cfg(test)]
    pub fn from_bytes(buf: Bytes) -> Self {
        let num_bits = buf.len() << 3;
        Self::from_bytes_with_num_bits(buf, num_bits)
    }

    /// Return the next set bit index on or after `bit_idx`.
    pub fn next_set_bit(&self, bit_idx: usize) -> Option<usize> {
        for idx in bit_idx..self.len() {
            // Since `self.len()` guards the range, we can safely call unsafe function here.
            if unsafe { self.is_set_unchecked(idx) } {
                return Some(idx);
            }
        }
        None
    }

    pub fn num_high_bits(&self) -> usize {
        self.num_high_bits
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    pub fn num_of_bytes(num_bits: usize) -> usize {
        let num_bytes = num_bits / 8 + if num_bits % 8 > 0 { 1 } else { 0 };
        let r = num_bytes % 64;
        if r == 0 {
            num_bytes
        } else {
            num_bytes + 64 - r
        }
    }

    /// Returns the number of valid bits in the bitmap,
    /// also referred to its 'length'.
    #[inline]
    pub fn len(&self) -> usize {
        self.num_bits
    }

    fn num_of_buffer_bytes(&self) -> usize {
        self.bits.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    /// # Safety
    ///
    /// Makes clippy happy.
    pub unsafe fn is_set_unchecked(&self, idx: usize) -> bool {
        bit_util::get_bit_raw(self.bits.as_ptr(), idx)
    }

    pub fn is_set(&self, idx: usize) -> Result<bool> {
        self.check_idx(idx)?;

        // Justification
        // We've already checked index here, so it's ok to use unsafe.
        Ok(unsafe { self.is_set_unchecked(idx) })
    }

    pub fn bytes_ref(&self) -> &Bytes {
        &self.bits
    }

    pub fn into_bytes(self) -> Bytes {
        self.bits
    }

    pub fn iter(&self) -> BitmapIter<'_> {
        BitmapIter {
            bits: &self.bits,
            idx: 0,
            num_bits: self.num_bits,
        }
    }

    /// Returns an iterator which starts from `offset`.
    pub fn iter_from(&self, offset: usize) -> Result<BitmapIter<'_>> {
        self.check_idx(offset)?;
        Ok(BitmapIter {
            bits: &self.bits,
            idx: offset,
            num_bits: self.num_bits,
        })
    }

    fn check_idx(&self, idx: usize) -> Result<()> {
        ensure!(idx < self.len());
        Ok(())
    }
}

impl<'a, 'b> BitAnd<&'b Bitmap> for &'a Bitmap {
    type Output = Result<Bitmap>;

    fn bitand(self, rhs: &'b Bitmap) -> Result<Bitmap> {
        assert_eq!(self.num_bits, rhs.num_bits);
        let bits = self
            .bits
            .iter()
            .zip_eq(rhs.bits.iter())
            .map(|(&a, &b)| a & b)
            .collect();
        Ok(Bitmap::from_bytes_with_num_bits(bits, self.num_bits))
    }
}

impl<'a, 'b> BitOr<&'b Bitmap> for &'a Bitmap {
    type Output = Result<Bitmap>;

    fn bitor(self, rhs: &'b Bitmap) -> Result<Bitmap> {
        assert_eq!(self.num_bits, rhs.num_bits);
        let bits = self
            .bits
            .iter()
            .zip_eq(rhs.bits.iter())
            .map(|(&a, &b)| a | b)
            .collect();
        Ok(Bitmap::from_bytes_with_num_bits(bits, self.num_bits))
    }
}

impl TryFrom<&BoolArray> for Bitmap {
    type Error = RwError;

    fn try_from(bools: &BoolArray) -> Result<Bitmap> {
        let mut builder = BitmapBuilder::default();
        bools.iter().for_each(|e| {
            builder.append(e.unwrap_or(false));
        });
        Ok(builder.finish())
    }
}

impl TryFrom<Vec<bool>> for Bitmap {
    type Error = RwError;

    fn try_from(bools: Vec<bool>) -> Result<Bitmap> {
        let mut builder = BitmapBuilder::default();
        bools.iter().for_each(|e| {
            builder.append(*e);
        });
        Ok(builder.finish())
    }
}

impl Bitmap {
    pub fn to_protobuf(&self) -> ProstBuffer {
        let last_byte_num_bits = ((self.num_bits % 8) as u8).to_be_bytes();
        let body = last_byte_num_bits
            .into_iter()
            .chain(self.bits.iter().copied())
            .collect();

        ProstBuffer {
            body,
            compression: CompressionType::None as i32,
        }
    }
}

impl TryFrom<&ProstBuffer> for Bitmap {
    type Error = RwError;

    fn try_from(buf: &ProstBuffer) -> Result<Bitmap> {
        let last_byte_num_bits = u8::from_be_bytes(buf.body[..1].try_into().unwrap());
        let bits = Bytes::copy_from_slice(&buf.body[1..]); // TODO: avoid this allocation
        let num_bits = (bits.len() << 3) - ((8 - last_byte_num_bits) % 8) as usize;

        Ok(Self::from_bytes_with_num_bits(bits, num_bits))
    }
}

impl PartialEq for Bitmap {
    fn eq(&self, other: &Self) -> bool {
        // buffer equality considers capacity, but here we want to only compare
        // actual data contents
        if self.num_bits != other.num_bits {
            return false;
        }
        // assume unset bits are always 0, and num_bits is always consistent with bits length.
        // Note: If you new a Buffer without init, the PartialEq may have UB due to uninit mem cuz
        // we are comparing bytes by bytes instead of bits by bits.
        let length = (self.num_bits + 7) / 8;
        self.bits[..length] == other.bits[..length]
    }
}

pub struct BitmapIter<'a> {
    bits: &'a Bytes,
    idx: usize,
    num_bits: usize,
}

impl<'a> BitmapIter<'a> {
    pub fn try_from(value: &'a Bytes, num_bits: usize) -> Result<Self> {
        ensure!(value.len() >= Bitmap::num_of_bytes(num_bits));
        Ok(Self {
            bits: value,
            idx: 0,
            num_bits,
        })
    }
}

impl<'a> std::iter::Iterator for BitmapIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.num_bits {
            return None;
        }
        let b = unsafe { bit_util::get_bit_raw(self.bits.as_ptr(), self.idx) };
        self.idx += 1;
        Some(b)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_bitmap_builder() {
        let bitmap1 = {
            let mut builder = BitmapBuilder::default();
            builder
                .append(false)
                .append(true)
                .append(true)
                .append(false)
                .append(true)
                .append(false)
                .append(true)
                .append(false)
                .append(true)
                .append(false)
                .append(true)
                .append(true)
                .append(false)
                .append(true)
                .append(false)
                .append(true);
            builder.finish()
        };
        let byte1 = 0b0101_0110_u8;
        let byte2 = 0b1010_1101_u8;
        let expected = Bitmap::from_bytes(Bytes::copy_from_slice(&[byte1, byte2]));
        assert_eq!(bitmap1, expected);
        assert_eq!(
            bitmap1.num_high_bits(),
            (byte1.count_ones() + byte2.count_ones()) as usize
        );

        let bitmap2 = {
            let mut builder = BitmapBuilder::default();
            builder
                .append(false)
                .append(true)
                .append(true)
                .append(false)
                .append(true)
                .append(false)
                .append(true)
                .append(false);
            builder.finish()
        };
        let byte1 = 0b0101_0110_u8;
        let expected = Bitmap::from_bytes(Bytes::copy_from_slice(&[byte1]));
        assert_eq!(bitmap2, expected);
    }

    #[test]
    fn test_bitmap_length() {
        assert_eq!(64, Bitmap::new(63 * 8).unwrap().num_of_buffer_bytes());
        assert_eq!(64, Bitmap::new(64 * 8).unwrap().num_of_buffer_bytes());
        assert_eq!(128, Bitmap::new(65 * 8).unwrap().num_of_buffer_bytes());
    }

    #[test]
    fn test_bitwise_and() {
        let bitmap1 = Bitmap::from_bytes(Bytes::from_static(&[0b01101010]));
        let bitmap2 = Bitmap::from_bytes(Bytes::from_static(&[0b01001110]));
        assert_eq!(
            Bitmap::from_bytes(Bytes::from_static(&[0b01001010])),
            (&bitmap1 & &bitmap2).unwrap()
        );
    }

    #[test]
    fn test_bitwise_or() {
        let bitmap1 = Bitmap::from_bytes(Bytes::from_static(&[0b01101010]));
        let bitmap2 = Bitmap::from_bytes(Bytes::from_static(&[0b01001110]));
        assert_eq!(
            Bitmap::from_bytes(Bytes::from_static(&[0b01101110])),
            (&bitmap1 | &bitmap2).unwrap()
        );
    }

    #[test]
    fn test_bitmap_is_set() {
        let bitmap = Bitmap::from_bytes(Bytes::from_static(&[0b01001010]));
        assert!(!bitmap.is_set(0).unwrap());
        assert!(bitmap.is_set(1).unwrap());
        assert!(!bitmap.is_set(2).unwrap());
        assert!(bitmap.is_set(3).unwrap());
        assert!(!bitmap.is_set(4).unwrap());
        assert!(!bitmap.is_set(5).unwrap());
        assert!(bitmap.is_set(6).unwrap());
        assert!(!bitmap.is_set(7).unwrap());
    }

    #[test]
    fn test_bitmap_iter() -> Result<()> {
        {
            let bitmap = Bitmap::from_bytes(Bytes::from_static(&[0b01001010]));
            let mut booleans = vec![];
            for b in bitmap.iter() {
                booleans.push(b as u8);
            }
            assert_eq!(booleans, vec![0u8, 1, 0, 1, 0, 0, 1, 0]);
        }
        {
            let bitmap: Bitmap = vec![true; 5].try_into()?;
            for b in bitmap.iter() {
                assert!(b);
            }
        }
        Ok(())
    }

    #[test]
    fn test_bitmap_from_protobuf() {
        let bitmap_bytes = vec![3u8 /* len % 8 */, 0b0101_0010, 0b110];
        let buf = ProstBuffer {
            body: bitmap_bytes,
            compression: CompressionType::None as _,
        };
        let bitmap: Bitmap = (&buf).try_into().unwrap();
        let actual_bytes: Vec<u8> = bitmap.iter().map(|b| b as u8).collect();

        assert_eq!(actual_bytes, vec![0, 1, 0, 0, 1, 0, 1, 0, /*  */ 0, 1, 1]); // in reverse order
        assert_eq!(bitmap.num_high_bits(), 5);
    }

    #[test]
    fn test_bitmap_from_buffer() {
        let byte1 = 0b0110_1010_u8;
        let byte2 = 0b1011_0101_u8;
        let bitmap = Bitmap::from_bytes(Bytes::from_static(&[0b0110_1010, 0b1011_0101]));
        let expected = Bitmap::try_from(vec![
            false, true, false, true, false, true, true, false, true, false, true, false, true,
            true, false, true,
        ])
        .unwrap();
        let num_high_bits = (byte1.count_ones() + byte2.count_ones()) as usize;
        assert_eq!(expected, bitmap);
        assert_eq!(bitmap.num_high_bits(), num_high_bits);
        assert_eq!(expected.num_high_bits(), num_high_bits);
    }

    #[test]
    fn test_bitmap_eq() {
        let b1: Bitmap = (vec![false; 3]).try_into().unwrap();
        let b2: Bitmap = (vec![false; 5]).try_into().unwrap();
        assert_ne!(b1, b2);

        let b1: Bitmap = [true, false]
            .iter()
            .cycle()
            .cloned()
            .take(10000)
            .collect_vec()
            .try_into()
            .unwrap();
        let b2: Bitmap = [true, false]
            .iter()
            .cycle()
            .cloned()
            .take(10000)
            .collect_vec()
            .try_into()
            .unwrap();
        assert_eq!(b1, b2);
    }

    #[test]
    fn test_bitmap_init() {
        let bm1 = Bitmap::new(1).unwrap();
        let bm2 = (vec![false]).try_into().unwrap();
        assert_eq!(bm1, bm2);
    }
}
