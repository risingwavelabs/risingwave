use crate::error::{Error, Result};
use bytes::BufMut;
use serde::{ser, Serialize};

/// A structure for serializing Rust values into a memcomparable bytes.
pub struct Serializer<B: BufMut> {
    output: MaybeFlip<B>,
}

impl<B: BufMut> Serializer<B> {
    /// Create a new `Serializer`.
    pub fn new(buffer: B) -> Self {
        Serializer {
            output: MaybeFlip {
                output: buffer,
                flip: false,
            },
        }
    }

    /// Unwrap the inner buffer from the `Serializer`.
    pub fn into_inner(self) -> B {
        self.output.output
    }

    /// Set whether data is serialized in reverse order.
    pub fn set_reverse(&mut self, reverse: bool) {
        self.output.flip = reverse;
    }
}

/// Serialize the given data structure as a memcomparable byte vector.
pub fn to_vec(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut serializer = Serializer::new(vec![]);
    value.serialize(&mut serializer)?;
    Ok(serializer.into_inner())
}

/// A wrapper around `BufMut` that can flip bits when putting data.
struct MaybeFlip<B: BufMut> {
    output: B,
    flip: bool,
}

macro_rules! def_method {
    ($name:ident, $ty:ty) => {
        fn $name(&mut self, value: $ty) {
            self.output.$name(if self.flip { !value } else { value });
        }
    };
}

impl<B: BufMut> MaybeFlip<B> {
    def_method!(put_u8, u8);
    def_method!(put_u16, u16);
    def_method!(put_u32, u32);
    def_method!(put_u64, u64);
    def_method!(put_i32, i32);

    fn put_slice(&mut self, src: &[u8]) {
        for &val in src {
            let val = if self.flip { !val } else { val };
            self.output.put_u8(val);
        }
    }

    fn put_bytes(&mut self, val: u8, cnt: usize) {
        let val = if self.flip { !val } else { val };
        self.output.put_bytes(val, cnt);
    }
}

// Format Reference:
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
// https://haxisnake.github.io/2020/11/06/TIDB源码学习笔记-基本类型编解码方案/
impl<'a, B: BufMut> ser::Serializer for &'a mut Serializer<B> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.serialize_u8(v as u8)
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        let u = v as u8 ^ (1 << 7);
        self.serialize_u8(u)
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        let u = v as u16 ^ (1 << 15);
        self.serialize_u16(u)
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        let u = v as u32 ^ (1 << 31);
        self.serialize_u32(u)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        let u = v as u64 ^ (1 << 63);
        self.serialize_u64(u)
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.output.put_u8(v);
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.output.put_u16(v);
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.output.put_u32(v);
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.output.put_u64(v);
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        let u = v.to_bits();
        let u = if v.is_sign_positive() {
            u | (1 << 31)
        } else {
            !u
        };
        self.output.put_u32(u);
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        let u = v.to_bits();
        let u = if v.is_sign_positive() {
            u | (1 << 63)
        } else {
            !u
        };
        self.output.put_u64(u);
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.serialize_u32(v as u32)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        self.output.put_u8(if v.is_empty() { 0 } else { 1 });
        let mut len = 0;
        for chunk in v.chunks(8) {
            self.output.put_slice(chunk);
            if chunk.len() != 8 {
                self.output.put_bytes(0, 8 - chunk.len());
            }
            len += chunk.len();
            // append an extra byte that signals the number of significant bytes in this chunk
            // 1-8: many bytes were significant and this group is the last group
            // 9: all 8 bytes were significant and there is more data to come
            let extra = if len == v.len() { chunk.len() as u8 } else { 9 };
            self.output.put_u8(extra);
        }
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        self.serialize_u8(0)
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_u8(1)?;
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        assert!(variant_index <= u8::MAX as u32, "too many variants");
        self.serialize_u8(variant_index as u8)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        assert!(variant_index <= u8::MAX as u32, "too many variants");
        self.serialize_u8(variant_index as u8)?;
        value.serialize(&mut *self)?;
        Ok(())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        assert!(variant_index <= u8::MAX as u32, "too many variants");
        self.serialize_u8(variant_index as u8)?;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(Error::NotSupported("map"))
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        assert!(variant_index <= u8::MAX as u32, "too many variants");
        self.serialize_u8(variant_index as u8)?;
        Ok(self)
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<'a, B: BufMut> ser::SerializeSeq for &'a mut Serializer<B> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        use serde::Serializer;
        self.serialize_u8(1)?;
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        use serde::Serializer;
        self.serialize_u8(0)?;
        Ok(())
    }
}

impl<'a, B: BufMut> ser::SerializeTuple for &'a mut Serializer<B> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, B: BufMut> ser::SerializeTupleStruct for &'a mut Serializer<B> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, B: BufMut> ser::SerializeTupleVariant for &'a mut Serializer<B> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, B: BufMut> ser::SerializeMap for &'a mut Serializer<B> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        key.serialize(&mut **self)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, B: BufMut> ser::SerializeStruct for &'a mut Serializer<B> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, B: BufMut> ser::SerializeStructVariant for &'a mut Serializer<B> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<B: BufMut> Serializer<B> {
    /// Serialize a decimal value.
    ///
    /// - `mantissa`: From `rust_decimal::Decimal::mantissa()`. A 96-bits signed integer.
    /// - `scale`: From `rust_decimal::Decimal::scale()`. A power of 10 ranging from 0 to 28.
    ///
    /// The decimal will be encoded to 13 bytes.
    /// It is memcomparable only when two decimals have the same scale.
    pub fn serialize_decimal(&mut self, mantissa: i128, scale: u8) -> Result<()> {
        // TODO(wrj): variable-length encoding
        // https://github.com/pingcap/tidb/blob/fec2938c1379270bf9939822c1abfe3d7244c174/types/mydecimal.go#L1133
        self.output.put_u8(scale);
        self.output.put_i32((mantissa >> 64) as i32 ^ (1 << 31));
        self.output.put_u64(mantissa as u64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{distributions::Alphanumeric, Rng};
    use serde::Serialize;

    #[test]
    fn test_unit() {
        assert_eq!(to_vec(&()).unwrap(), []);

        #[derive(Serialize)]
        struct UnitStruct;
        assert_eq!(to_vec(&UnitStruct).unwrap(), []);
    }

    #[test]
    fn test_option() {
        assert_eq!(to_vec(&(None as Option<u8>)).unwrap(), [0]);
        assert_eq!(to_vec(&Some(0x12u8)).unwrap(), [1, 0x12]);
    }

    #[test]
    fn test_tuple() {
        let tuple: (i8, i16, i32, i64) = (0x12, 0x1234, 0x12345678, 0x1234_5678_8765_4321);
        assert_eq!(
            to_vec(&tuple).unwrap(),
            [
                0x92, 0x92, 0x34, 0x92, 0x34, 0x56, 0x78, 0x92, 0x34, 0x56, 0x78, 0x87, 0x65, 0x43,
                0x21
            ]
        );

        #[derive(Serialize)]
        struct TupleStruct(u8, u16, u32, u64);
        let tuple = TupleStruct(0x12, 0x1234, 0x12345678, 0x1234_5678_8765_4321);
        assert_eq!(
            to_vec(&tuple).unwrap(),
            [
                0x12, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x87, 0x65, 0x43,
                0x21
            ]
        );

        #[derive(Serialize)]
        struct NewTypeStruct(char);
        let tuple = NewTypeStruct('G');
        assert_eq!(to_vec(&tuple).unwrap(), [0, 0, 0, b'G']);
    }

    #[test]
    fn test_vec() {
        let s: &[u8] = &[1, 2, 3];
        assert_eq!(to_vec(&s).unwrap(), [1, 0x01, 1, 0x02, 1, 0x03, 0]);
    }

    #[test]
    fn test_enum() {
        #[derive(PartialEq, Eq, PartialOrd, Ord, Serialize)]
        enum TestEnum {
            Unit,
            NewType(u8),
            Tuple(u8, u8),
            Struct { a: u8, b: u8 },
        }

        let test = TestEnum::Unit;
        assert_eq!(to_vec(&test).unwrap(), [0]);

        let test = TestEnum::NewType(0x12);
        assert_eq!(to_vec(&test).unwrap(), [1, 0x12]);

        let test = TestEnum::Tuple(0x12, 0x34);
        assert_eq!(to_vec(&test).unwrap(), [2, 0x12, 0x34]);

        let test = TestEnum::Struct { a: 0x12, b: 0x34 };
        assert_eq!(to_vec(&test).unwrap(), [3, 0x12, 0x34]);
    }

    #[derive(PartialEq, PartialOrd, Serialize)]
    struct Test {
        a: bool,
        b: f32,
        c: f64,
    }

    #[test]
    fn test_struct() {
        let test = Test {
            a: true,
            b: 0.0,
            c: 0.0,
        };
        assert_eq!(
            to_vec(&test).unwrap(),
            [1, 0x80, 0, 0, 0, 0x80, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_struct_order() {
        for _ in 0..1000 {
            let mut rng = rand::thread_rng();
            let a = Test {
                a: rng.gen(),
                b: rng.gen(),
                c: rng.gen(),
            };
            let b = Test {
                a: if rng.gen_bool(0.5) { a.a } else { rng.gen() },
                b: if rng.gen_bool(0.5) {
                    a.b
                } else {
                    rng.gen_range(-1.0..1.0)
                },
                c: if rng.gen_bool(0.5) {
                    a.c
                } else {
                    rng.gen_range(-1.0..1.0)
                },
            };
            let ea = to_vec(&a).unwrap();
            let eb = to_vec(&b).unwrap();
            assert_eq!(a.partial_cmp(&b), ea.partial_cmp(&eb));
        }
    }

    #[test]
    fn test_string() {
        assert_eq!(to_vec(&"").unwrap(), [0]);
        assert_eq!(
            to_vec(&"123").unwrap(),
            [1, b'1', b'2', b'3', 0, 0, 0, 0, 0, 3]
        );
        assert_eq!(
            to_vec(&"12345678").unwrap(),
            [1, b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 8]
        );
        assert_eq!(
            to_vec(&"1234567890").unwrap(),
            [
                1, b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 9, b'9', b'0', 0, 0, 0, 0, 0, 0,
                2
            ]
        );
    }

    #[test]
    fn test_string_order() {
        fn to_vec_desc(s: &str) -> Vec<u8> {
            let mut ser = Serializer::new(vec![]);
            ser.set_reverse(true);
            s.serialize(&mut ser).unwrap();
            ser.into_inner()
        }

        for _ in 0..1000 {
            let s = rand_string(0..16);
            let a = s.clone() + &rand_string(0..16);
            let b = s + &rand_string(0..16);

            let ea = to_vec(&a).unwrap();
            let eb = to_vec(&b).unwrap();
            assert_eq!(a.cmp(&b), ea.cmp(&eb));

            let ra = to_vec_desc(&a);
            let rb = to_vec_desc(&b);
            assert_eq!(a.cmp(&b), ra.cmp(&rb).reverse());
        }
    }

    fn rand_string(len_range: std::ops::Range<usize>) -> String {
        let mut rng = rand::thread_rng();
        let len = rng.gen_range(len_range);
        rng.sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    #[test]
    fn test_decimal() {
        let a = serialize_decimal(12_3456_7890_1234, 4);
        let b = serialize_decimal(0, 4);
        let c = serialize_decimal(-12_3456_7890_1234, 4);
        assert!(a > b && b > c);
    }

    fn serialize_decimal(mantissa: i128, scale: u8) -> Vec<u8> {
        let mut serializer = Serializer::new(vec![]);
        serializer.serialize_decimal(mantissa, scale).unwrap();
        serializer.into_inner()
    }

    #[test]
    fn test_reverse_order() {
        // Order: (ASC, DESC)
        let v1 = (0u8, 1i32);
        let v2 = (0u8, -1i32);
        let v3 = (1u8, -1i32);

        fn serialize(v: (u8, i32)) -> Vec<u8> {
            let mut ser = Serializer::new(vec![]);
            v.0.serialize(&mut ser).unwrap();
            ser.set_reverse(true);
            v.1.serialize(&mut ser).unwrap();
            ser.into_inner()
        }
        assert!(serialize(v1) < serialize(v2));
        assert!(serialize(v2) < serialize(v3));
    }
}
