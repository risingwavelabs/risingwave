use crate::error::{Error, Result};
use bytes::Buf;
use serde::de::{
    self, DeserializeSeed, EnumAccess, IntoDeserializer, SeqAccess, VariantAccess, Visitor,
};

/// A structure that deserializes memcomparable bytes into Rust values.
pub struct Deserializer<'de> {
    // This string starts with the input data and characters are truncated off
    // the beginning as data is parsed.
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    /// Creates a deserializer from a `&[u8]`.
    pub fn from_slice(input: &'de [u8]) -> Self {
        Deserializer { input }
    }
}

/// Deserialize an instance of type `T` from a memcomparable bytes.
pub fn from_slice<'a, T>(bytes: &'a [u8]) -> Result<T>
where
    T: serde::Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_slice(bytes);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(Error::TrailingCharacters)
    }
}

impl Deserializer<'_> {
    fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let mut bytes = vec![];
        loop {
            let (chunk, remain) = self.input.split_at(9);
            self.input = remain;
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
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    // Look at the input data to decide what Serde data model type to
    // deserialize as. Not all data formats are able to support this operation.
    // Formats that support `deserialize_any` are known as self-describing.
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
        struct Access<'de, 'a> {
            deserializer: &'a mut Deserializer<'de>,
        }
        impl<'de, 'a> SeqAccess<'de> for Access<'de, 'a> {
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
        struct Access<'de, 'a> {
            deserializer: &'a mut Deserializer<'de>,
            len: usize,
        }

        impl<'de, 'a> SeqAccess<'de> for Access<'de, 'a> {
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
        impl<'de, 'a> EnumAccess<'de> for &'a mut Deserializer<'de> {
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
impl<'de, 'a> VariantAccess<'de> for &'a mut Deserializer<'de> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

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
        assert_eq!(
            from_slice::<String>(&[b'1', b'2', b'3', 0, 0, 0, 0, 0, 3]).unwrap(),
            "123".to_string()
        );
        assert_eq!(
            from_slice::<String>(&[b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 8]).unwrap(),
            "12345678".to_string()
        );
        assert_eq!(
            from_slice::<String>(&[
                b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 9, b'9', b'0', 0, 0, 0, 0, 0, 0, 2
            ])
            .unwrap(),
            "1234567890".to_string()
        );
        assert_eq!(
            from_slice::<String>(&[0, 0, 0, 0, 0, 0, 0, 0, 10]),
            Err(Error::InvalidBytesEncoding(10))
        );
    }
}
