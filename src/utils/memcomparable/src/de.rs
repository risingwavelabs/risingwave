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

use bytes::Buf;
use serde::de::{
    self, DeserializeSeed, EnumAccess, IntoDeserializer, SeqAccess, VariantAccess, Visitor,
};

use crate::error::{Error, Result};

const DECIMAL_FLAG_LOW_BOUND: u8 = 0x6;
const DECIMAL_FLAG_UP_BOUND: u8 = 0x23;
const BYTES_CHUNK_SIZE: usize = 8;
const BYTES_CHUNK_UNIT_SIZE: usize = BYTES_CHUNK_SIZE + 1;

/// A structure that deserializes memcomparable bytes into Rust values.
pub struct Deserializer<B: Buf> {
    input: MaybeFlip<B>,
    input_len: usize,
}

impl<B: Buf> Deserializer<B> {
    /// Creates a deserializer from a buffer.
    pub fn new(input: B) -> Self {
        Deserializer {
            input_len: input.remaining(),
            input: MaybeFlip { input, flip: false },
        }
    }

    /// Set whether data is serialized in reverse order.
    pub fn set_reverse(&mut self, reverse: bool) {
        self.input.flip = reverse;
    }

    /// Unwrap the inner buffer from the `Deserializer`.
    pub fn into_inner(self) -> B {
        self.input.input
    }

    /// Check if the inner buffer still has remaining data.
    pub fn has_remaining(&self) -> bool {
        self.input.input.has_remaining()
    }

    /// Return the position of inner buffer from the `Deserializer`.
    pub fn position(&self) -> usize {
        self.input_len - self.input.input.remaining()
    }

    /// Advance the position of inner buffer from the `Deserializer`.
    pub fn advance(&mut self, cnt: usize) {
        self.input.input.advance(cnt)
    }
}

/// Deserialize an instance of type `T` from a memcomparable bytes.
pub fn from_slice<'a, T>(bytes: &'a [u8]) -> Result<T>
where
    T: serde::Deserialize<'a>,
{
    let mut deserializer = Deserializer::new(bytes);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(Error::TrailingCharacters)
    }
}

/// A wrapper around `Buf` that can flip bits when getting data.
struct MaybeFlip<B: Buf> {
    input: B,
    flip: bool,
}

macro_rules! def_method {
    ($name:ident, $ty:ty) => {
        fn $name(&mut self) -> $ty {
            let v = self.input.$name();
            if self.flip {
                !v
            } else {
                v
            }
        }
    };
}

impl<B: Buf> MaybeFlip<B> {
    def_method!(get_u8, u8);

    def_method!(get_u16, u16);

    def_method!(get_u32, u32);

    def_method!(get_u64, u64);

    def_method!(get_i32, i32);

    def_method!(get_i64, i64);

    fn copy_to_slice(&mut self, dst: &mut [u8]) {
        self.input.copy_to_slice(dst);
        if self.flip {
            dst.iter_mut().for_each(|x| *x = !*x);
        }
    }

    fn is_empty(&self) -> bool {
        self.input.remaining() == 0
    }
}

impl<B: Buf> Deserializer<B> {
    fn read_bytes(&mut self) -> Result<Vec<u8>> {
        match self.input.get_u8() {
            0 => return Ok(vec![]), // empty slice
            1 => {}                 // non-empty slice
            v => return Err(Error::InvalidBytesEncoding(v)),
        }
        let mut bytes = vec![];
        let mut chunk = [0u8; BYTES_CHUNK_UNIT_SIZE]; // chunk + chunk_len
        loop {
            self.input.copy_to_slice(&mut chunk);
            match chunk[8] {
                len @ 1..=8 => {
                    bytes.extend_from_slice(&chunk[..len as usize]);
                    return Ok(bytes);
                }
                9 => bytes.extend_from_slice(&chunk[..8]),
                v => return Err(Error::InvalidBytesEncoding(v)),
            }
        }
    }

    fn read_decimal(&mut self) -> Result<Vec<u8>> {
        let flag = self.input.get_u8();
        if !(DECIMAL_FLAG_LOW_BOUND..=DECIMAL_FLAG_UP_BOUND).contains(&flag) {
            return Err(Error::InvalidBytesEncoding(flag));
        }
        let mut byte_array = vec![flag];
        loop {
            let byte = self.input.get_u8();
            if byte == 0 {
                break;
            }
            byte_array.push(byte);
        }
        Ok(byte_array)
    }

    /// Read bytes_len without copy, it will consume offset
    pub fn read_bytes_len(&mut self) -> Result<usize> {
        use core::cmp;
        let mut result: usize = 0;

        match self.input.get_u8() {
            0 => return Ok(0), // empty slice
            1 => {}            // non-empty slice
            v => return Err(Error::InvalidBytesEncoding(v)),
        }

        loop {
            {
                // calc advance
                let mut offset = 0;
                while offset < BYTES_CHUNK_SIZE {
                    let src = self.input.input.chunk();
                    let cnt = cmp::min(src.len(), BYTES_CHUNK_SIZE - offset);
                    offset += cnt;
                    self.advance(cnt);
                }
            }

            let chunk_len = if self.input.flip {
                !self.input.input.chunk()[0]
            } else {
                self.input.input.chunk()[0]
            };
            self.advance(1);

            match chunk_len {
                len @ 1..=8 => {
                    result += len as usize;
                    // self.advance(len as usize);
                    return Ok(result);
                }
                9 => {
                    result += 8;
                }
                v => return Err(Error::InvalidBytesEncoding(v)),
            }
        }
    }

    /// Read decimal_len without copy, it will consume offset
    pub fn read_decimal_len(&mut self) -> Result<usize> {
        let mut len: usize = 0;

        let flag = self.input.get_u8();
        if !(DECIMAL_FLAG_LOW_BOUND..=DECIMAL_FLAG_UP_BOUND).contains(&flag) {
            return Err(Error::InvalidBytesEncoding(flag));
        }
        loop {
            let byte = self.input.get_u8();
            if byte == 0 {
                break;
            }

            len += 1;
        }

        Ok(len)
    }

    /// Read struct_and_list without copy, it will consume offset
    pub fn read_struct_and_list_len(&mut self) -> Result<usize> {
        let len = self.input.get_u32() as usize;
        self.advance(len);
        Ok(len)
    }
}

// Format Reference:
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
// https://haxisnake.github.io/2020/11/06/TIDB源码学习笔记-基本类型编解码方案/
impl<'de, 'a, B: Buf + 'de> de::Deserializer<'de> for &'a mut Deserializer<B> {
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(Error::NotSupported("deserialize_any"))
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.input.get_u8() {
            1 => visitor.visit_bool(true),
            0 => visitor.visit_bool(false),
            value => Err(Error::InvalidBoolEncoding(value)),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let v = (self.input.get_u8() ^ (1 << 7)) as i8;
        visitor.visit_i8(v)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let v = (self.input.get_u16() ^ (1 << 15)) as i16;
        visitor.visit_i16(v)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let v = (self.input.get_u32() ^ (1 << 31)) as i32;
        visitor.visit_i32(v)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let v = (self.input.get_u64() ^ (1 << 63)) as i64;
        visitor.visit_i64(v)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(self.input.get_u8())
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(self.input.get_u16())
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(self.input.get_u32())
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(self.input.get_u64())
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let u = self.input.get_u32();
        let u = if u & (1 << 31) != 0 {
            u & !(1 << 31)
        } else {
            !u
        };
        visitor.visit_f32(f32::from_bits(u))
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let u = self.input.get_u64();
        let u = if u & (1 << 63) != 0 {
            u & !(1 << 63)
        } else {
            !u
        };
        visitor.visit_f64(f64::from_bits(u))
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let u = self.input.get_u32();
        visitor.visit_char(char::from_u32(u).ok_or(Error::InvalidCharEncoding(u))?)
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(Error::NotSupported("borrowed str"))
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let bytes = self.read_bytes()?;
        visitor.visit_string(String::from_utf8(bytes)?)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let bytes = self.read_bytes()?;
        visitor.visit_bytes(&bytes)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let bytes = self.read_bytes()?;
        visitor.visit_byte_buf(bytes)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.input.get_u8() {
            0 => visitor.visit_none(),
            1 => visitor.visit_some(self),
            t => Err(Error::InvalidTagEncoding(t as usize)),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain. That means not
    // parsing anything other than the contained value.
    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        struct Access<'a, B: Buf> {
            deserializer: &'a mut Deserializer<B>,
        }
        impl<'de, 'a, B: Buf + 'de> SeqAccess<'de> for Access<'a, B> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: DeserializeSeed<'de>,
            {
                match self.deserializer.input.get_u8() {
                    1 => Ok(Some(DeserializeSeed::deserialize(
                        seed,
                        &mut *self.deserializer,
                    )?)),
                    0 => Ok(None),
                    value => Err(Error::InvalidSeqEncoding(value)),
                }
            }
        }

        visitor.visit_seq(Access { deserializer: self })
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        struct Access<'a, B: Buf> {
            deserializer: &'a mut Deserializer<B>,
            len: usize,
        }

        impl<'de, 'a, B: Buf + 'de> SeqAccess<'de> for Access<'a, B> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: DeserializeSeed<'de>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let value = DeserializeSeed::deserialize(seed, &mut *self.deserializer)?;
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        visitor.visit_seq(Access {
            deserializer: self,
            len,
        })
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(Error::NotSupported("map"))
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        impl<'de, 'a, B: Buf + 'de> EnumAccess<'de> for &'a mut Deserializer<B> {
            type Error = Error;
            type Variant = Self;

            fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
            where
                V: DeserializeSeed<'de>,
            {
                let idx = self.input.get_u8() as u32;
                let val: Result<_> = seed.deserialize(idx.into_deserializer());
                Ok((val?, self))
            }
        }

        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(Error::NotSupported("deserialize_identifier"))
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(Error::NotSupported("deserialize_ignored_any"))
    }
}

// `VariantAccess` is provided to the `Visitor` to give it the ability to see
// the content of the single variant that it decided to deserialize.
impl<'de, 'a, B: Buf + 'de> VariantAccess<'de> for &'a mut Deserializer<B> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        serde::de::Deserializer::deserialize_tuple(self, len, visitor)
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        serde::de::Deserializer::deserialize_tuple(self, fields.len(), visitor)
    }
}

impl<B: Buf> Deserializer<B> {
    /// Deserialize a decimal value. Returns `(mantissa, scale)`.
    pub fn deserialize_decimal(&mut self) -> Result<(i128, u8)> {
        let mut byte_array = self.read_decimal()?;

        // indicate the beginning position of mantissa in `byte_array`.
        let mut begin: usize = 2;
        // whether the decimal is negative or not.
        let mut neg: bool = false;
        let exponent = match byte_array[0] {
            DECIMAL_FLAG_LOW_BOUND => {
                // NaN
                return Ok((0, 31));
            }
            0x07 => {
                // Negative INF
                return Ok((0, 29));
            }
            0x08 => {
                neg = true;
                !byte_array[1] as i8
            }
            0x09..=0x13 => {
                begin -= 1;
                neg = true;
                (0x13 - byte_array[0]) as i8
            }
            0x14 => {
                neg = true;
                -(byte_array[1] as i8)
            }
            0x15 => {
                return Ok((0, 0));
            }
            0x16 => -!(byte_array[1] as i8),
            0x17..=0x21 => {
                begin -= 1;
                (byte_array[0] - 0x17) as i8
            }
            0x22 => byte_array[1] as i8,
            DECIMAL_FLAG_UP_BOUND => {
                // Positive INF
                return Ok((0, 30));
            }
            invalid_byte => {
                return Err(Error::InvalidBytesEncoding(invalid_byte));
            }
        };
        if neg {
            byte_array = byte_array.into_iter().map(|item| !item).collect();
        }

        // decode mantissa.
        let mut mantissa: i128 = 0;
        let bytes_len = byte_array.len() - begin;
        let mut exp = bytes_len;
        for item in byte_array.iter().skip(begin) {
            exp -= 1;
            mantissa += ((item - 1) / 2) as i128 * 100i128.pow(exp as u32);
        }
        mantissa += 1;

        // get scale
        let mut scale = (bytes_len as i8 - exponent) * 2;
        if scale <= 0 {
            // e.g. 1(mantissa) + 2(exponent) (which is 100).
            for _i in 0..-scale {
                mantissa *= 10;
            }
            scale = 0;
        } else if mantissa % 10 == 0 {
            // Remove unnecessary zeros.
            // e.g. 0.01_11_10 should be 0.01_11_1
            mantissa /= 10;
            scale -= 1;
        }

        if neg {
            mantissa = -mantissa;
        }
        Ok((mantissa, scale as u8))
    }

    /// Deserialize a NaiveDateWrapper value. Returns `days`.
    pub fn deserialize_naivedate(&mut self) -> Result<i32> {
        let days = self.input.get_i32() ^ (1 << 31);
        Ok(days)
    }

    /// Deserialize a NaiveTimeWrapper value. Returns `(secs, nano)`.
    pub fn deserialize_naivetime(&mut self) -> Result<(u32, u32)> {
        let secs = self.input.get_u32();
        let nano = self.input.get_u32();
        Ok((secs, nano))
    }

    /// Deserialize a NaiveDateTimeWrapper value. Returns `(secs, nsecs)`.
    pub fn deserialize_naivedatetime(&mut self) -> Result<(i64, u32)> {
        let secs = self.input.get_i64() ^ (1 << 63);
        let nsecs = self.input.get_u32();
        Ok((secs, nsecs))
    }

    /// Deserialize struct and list value. Returns `bytes`.
    pub fn deserialize_struct_or_list(&mut self) -> Result<Vec<u8>> {
        let len = self.input.get_u32();
        let mut bytes = vec![0; len as usize];
        self.input.copy_to_slice(&mut bytes);
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::iter::zip;
    use std::str::FromStr;

    use rust_decimal::Decimal;
    use serde::Deserialize;

    use super::*;

    #[test]
    fn test_unit() {
        assert_eq!(from_slice::<()>(&[]), Ok(()));
        assert_eq!(from_slice::<()>(&[0]), Err(Error::TrailingCharacters));

        #[derive(Debug, PartialEq, Eq, Deserialize)]
        struct UnitStruct;
        assert_eq!(from_slice::<UnitStruct>(&[]).unwrap(), UnitStruct);
    }

    #[test]
    fn test_bool() {
        assert_eq!(from_slice::<bool>(&[0]), Ok(false));
        assert_eq!(from_slice::<bool>(&[1]), Ok(true));
        assert_eq!(from_slice::<bool>(&[2]), Err(Error::InvalidBoolEncoding(2)));
    }

    #[test]
    fn test_option() {
        assert_eq!(from_slice::<Option<u8>>(&[0]).unwrap(), None);
        assert_eq!(from_slice::<Option<u8>>(&[1, 0x12]).unwrap(), Some(0x12));
    }

    #[test]
    fn test_tuple() {
        assert_eq!(
            from_slice::<(i8, i16, i32, i64)>(&[
                0x92, 0x92, 0x34, 0x92, 0x34, 0x56, 0x78, 0x92, 0x34, 0x56, 0x78, 0x87, 0x65, 0x43,
                0x21
            ])
            .unwrap(),
            (0x12, 0x1234, 0x12345678, 0x1234_5678_8765_4321)
        );

        #[derive(Debug, PartialEq, Eq, Deserialize)]
        struct TupleStruct(u8, u16, u32, u64);
        assert_eq!(
            from_slice::<TupleStruct>(&[
                0x12, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x87, 0x65, 0x43,
                0x21
            ])
            .unwrap(),
            TupleStruct(0x12, 0x1234, 0x12345678, 0x1234_5678_8765_4321)
        );

        #[derive(Debug, PartialEq, Eq, Deserialize)]
        struct NewTypeStruct(char);
        assert_eq!(
            from_slice::<NewTypeStruct>(&[0, 0, 0, b'G']).unwrap(),
            NewTypeStruct('G')
        );
    }

    #[test]
    fn test_vec() {
        assert_eq!(
            from_slice::<Vec<u8>>(&[1, 0x01, 1, 0x02, 1, 0x03, 0]).unwrap(),
            vec![1, 2, 3]
        );
        assert_eq!(
            from_slice::<Vec<u8>>(&[1, 0x01, 2]),
            Err(Error::InvalidSeqEncoding(2))
        );
    }

    #[test]
    fn test_enum() {
        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
        enum TestEnum {
            Unit,
            NewType(u8),
            Tuple(u8, u8),
            Struct { a: u8, b: u8 },
        }

        assert_eq!(from_slice::<TestEnum>(&[0]).unwrap(), TestEnum::Unit);
        assert_eq!(
            from_slice::<TestEnum>(&[1, 0x12]).unwrap(),
            TestEnum::NewType(0x12)
        );
        assert_eq!(
            from_slice::<TestEnum>(&[2, 0x12, 0x34]).unwrap(),
            TestEnum::Tuple(0x12, 0x34)
        );
        assert_eq!(
            from_slice::<TestEnum>(&[3, 0x12, 0x34]).unwrap(),
            TestEnum::Struct { a: 0x12, b: 0x34 }
        );
    }

    #[test]
    fn test_struct() {
        #[derive(Debug, PartialEq, PartialOrd, Deserialize)]
        struct Test {
            a: bool,
            b: f32,
            c: f64,
        }
        assert_eq!(
            from_slice::<Test>(&[1, 0x80, 0, 0, 0, 0x80, 0, 0, 0, 0, 0, 0, 0]).unwrap(),
            Test {
                a: true,
                b: 0.0,
                c: 0.0,
            }
        );
    }

    #[test]
    fn test_string() {
        assert_eq!(from_slice::<String>(&[0]).unwrap(), "".to_string());
        assert_eq!(
            from_slice::<String>(&[1, b'1', b'2', b'3', 0, 0, 0, 0, 0, 3]).unwrap(),
            "123".to_string()
        );
        assert_eq!(
            from_slice::<String>(&[1, b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 8]).unwrap(),
            "12345678".to_string()
        );
        assert_eq!(
            from_slice::<String>(&[
                1, b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 9, b'9', b'0', 0, 0, 0, 0, 0, 0,
                2
            ])
            .unwrap(),
            "1234567890".to_string()
        );
        assert_eq!(
            from_slice::<String>(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 10]),
            Err(Error::InvalidBytesEncoding(10))
        );
        assert_eq!(
            from_slice::<String>(&[2]),
            Err(Error::InvalidBytesEncoding(2))
        );
    }

    #[test]
    fn test_decimal() {
        // Notice: decimals like 100.00 will be decoding as 100.

        // Test: -1234_5678_9012_3456_7890_1234, -12_3456_7890.1234, -0.001, 0.001, 100, 0.01111,
        // 12345, 1234_5678_9012_3456_7890_1234, -233.3, 50
        let mantissas: Vec<i128> = vec![
            -1234_5678_9012_3456_7890_1234,
            -12_3456_7890_1234,
            -1,
            1,
            100,
            1111,
            12345,
            1234_5678_9012_3456_7890_1234,
            -2333,
            50,
        ];
        let scales: Vec<u8> = vec![0, 4, 3, 3, 0, 5, 0, 0, 1, 0];
        for (mantissa, scale) in zip(mantissas, scales) {
            assert_eq!(
                (mantissa, scale),
                deserialize_decimal(&serialize_decimal(mantissa, scale))
            );
        }
    }

    #[test]
    fn test_decimal_2() {
        let d = Decimal::from_str("41721.900909090909090909090909").unwrap();
        let (mantissa, scale) = (d.mantissa(), d.scale() as u8);
        let (mantissa0, scale0) = deserialize_decimal(&serialize_decimal(mantissa, scale));
        assert_eq!((mantissa, scale), (mantissa0, scale0));
    }

    fn serialize_decimal(mantissa: i128, scale: u8) -> Vec<u8> {
        let mut serializer = crate::Serializer::new(vec![]);
        serializer.serialize_decimal(mantissa, scale).unwrap();
        serializer.into_inner()
    }

    fn deserialize_decimal(bytes: &[u8]) -> (i128, u8) {
        let mut deserializer = Deserializer::new(bytes);
        deserializer.deserialize_decimal().unwrap()
    }

    #[test]
    fn test_naivedate() {
        let days = 12_3456;
        let days0 = deserialize_naivedate(&serialize_naivedate(days));
        assert_eq!(days, days0);
    }

    fn serialize_naivedate(days: i32) -> Vec<u8> {
        let mut serializer = crate::Serializer::new(vec![]);
        serializer.serialize_naivedate(days).unwrap();
        serializer.into_inner()
    }

    fn deserialize_naivedate(bytes: &[u8]) -> i32 {
        let mut deserializer = Deserializer::new(bytes);
        deserializer.deserialize_naivedate().unwrap()
    }

    #[test]
    fn test_naivetime() {
        let (secs, nano) = (23 * 3600 + 59 * 60 + 59, 1234_5678);
        let (secs0, nano0) = deserialize_naivetime(&serialize_naivetime(secs, nano));
        assert_eq!((secs, nano), (secs0, nano0));
    }

    fn serialize_naivetime(secs: u32, nano: u32) -> Vec<u8> {
        let mut serializer = crate::Serializer::new(vec![]);
        serializer.serialize_naivetime(secs, nano).unwrap();
        serializer.into_inner()
    }

    fn deserialize_naivetime(bytes: &[u8]) -> (u32, u32) {
        let mut deserializer = Deserializer::new(bytes);
        deserializer.deserialize_naivetime().unwrap()
    }

    #[test]
    fn test_naivedatetime() {
        let (secs, nsecs) = (12_3456_7890_1234, 1234_5678);
        let (secs0, nsecs0) = deserialize_naivedatetime(&serialize_naivedatetime(secs, nsecs));
        assert_eq!((secs, nsecs), (secs0, nsecs0));
    }

    fn serialize_naivedatetime(secs: i64, nsecs: u32) -> Vec<u8> {
        let mut serializer = crate::Serializer::new(vec![]);
        serializer.serialize_naivedatetime(secs, nsecs).unwrap();
        serializer.into_inner()
    }

    fn deserialize_naivedatetime(bytes: &[u8]) -> (i64, u32) {
        let mut deserializer = Deserializer::new(bytes);
        deserializer.deserialize_naivedatetime().unwrap()
    }
}
