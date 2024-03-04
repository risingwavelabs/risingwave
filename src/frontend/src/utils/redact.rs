// Copyright 2024 RisingWave Labs
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

use core::fmt::{Debug, Display, Formatter};

use risingwave_sqlparser::ast::{Ident, ObjectName, SqlOption, Value};
use serde::ser::{Impossible, StdError};
use serde::{ser, Serialize};

#[derive(Debug)]
pub enum Error {
    Message(String),
    NotSupported(String),
}

impl ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Error::Message(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            Error::Message(msg) => f.write_str(msg),
            Error::NotSupported(msg) => f.write_str(msg),
        }
    }
}

impl StdError for Error {}

#[derive(Default)]
struct ValueSerializer {}

impl serde::Serializer for ValueSerializer {
    type Error = Error;
    type Ok = Option<Value>;
    type SerializeMap = Impossible<Option<Value>, Error>;
    type SerializeSeq = Impossible<Option<Value>, Error>;
    type SerializeStruct = Impossible<Option<Value>, Error>;
    type SerializeStructVariant = Impossible<Option<Value>, Error>;
    type SerializeTuple = Impossible<Option<Value>, Error>;
    type SerializeTupleStruct = Impossible<Option<Value>, Error>;
    type SerializeTupleVariant = Impossible<Option<Value>, Error>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::Boolean(v)))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::Number(v.to_string())))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::Number(v.to_string())))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::Number(v.to_string())))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::Number(v.to_string())))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::Number(v.to_string())))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::Number(v.to_string())))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::Number(v.to_string())))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::Number(v.to_string())))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::Number(v.to_string())))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::Number(v.to_string())))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::SingleQuotedString(v.to_string())))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Some(Value::SingleQuotedString(v.to_string())))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_bytes".into()))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(None)
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_unit".into()))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_unit_struct".into()))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_unit_variant".into()))
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(Error::NotSupported("serialize_newtype_struct".into()))
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(Error::NotSupported("serialize_newtype_variant".into()))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(Error::NotSupported("serialize_seq".into()))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(Error::NotSupported("serialize_tuple".into()))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(Error::NotSupported("serialize_tuple_struct".into()))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(Error::NotSupported("serialize_tuple_variant".into()))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(Error::NotSupported("serialize_map".into()))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(Error::NotSupported("serialize_struct".into()))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Error::NotSupported("serialize_struct_variant".into()))
    }
}

#[derive(Default)]
pub struct SqlOptionVecSerializer {
    output: Vec<SqlOption>,
    last_name: Option<ObjectName>,
}

impl From<SqlOptionVecSerializer> for Vec<SqlOption> {
    fn from(value: SqlOptionVecSerializer) -> Self {
        value.output
    }
}

impl<'a> ser::SerializeMap for &'a mut SqlOptionVecSerializer {
    type Error = Error;
    type Ok = ();

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        assert!(self.last_name.take().is_none());
        let Some(Value::SingleQuotedString(name)) = key.serialize(ValueSerializer::default())?
        else {
            return Err(Error::Message("expect key of string type".into()));
        };
        self.last_name = Some(ObjectName(vec![Ident::new_unchecked(name)]));
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let name = self
            .last_name
            .take()
            .ok_or_else(|| Error::Message("expect name".into()))?;
        if let Some(value) = value.serialize(ValueSerializer::default())? {
            self.output.push(SqlOption { name, value });
        }
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> ser::SerializeStruct for &'a mut SqlOptionVecSerializer {
    type Error = Error;
    type Ok = ();

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let Some(value) = value.serialize(ValueSerializer::default())? else {
            return Ok(());
        };
        let sql_option = SqlOption {
            name: ObjectName(vec![Ident::new_unchecked(key)]),
            value,
        };
        self.output.push(sql_option);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> serde::Serializer for &'a mut SqlOptionVecSerializer {
    type Error = Error;
    type Ok = ();
    type SerializeMap = Self;
    type SerializeSeq = Impossible<(), Error>;
    type SerializeStruct = Self;
    type SerializeStructVariant = Impossible<(), Error>;
    type SerializeTuple = Impossible<(), Error>;
    type SerializeTupleStruct = Impossible<(), Error>;
    type SerializeTupleVariant = Impossible<(), Error>;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_bool".into()))
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_i8".into()))
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_i16".into()))
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_i32".into()))
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_i64".into()))
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_u8".into()))
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_u16".into()))
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_u32".into()))
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_u64".into()))
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_f32".into()))
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_f64".into()))
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_char".into()))
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_str".into()))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_bytes".into()))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_unit".into()))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_unit_struct".into()))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSupported("serialize_unit_variant".into()))
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(Error::NotSupported("serialize_newtype_variant".into()))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(Error::NotSupported("serialize_seq".into()))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(Error::NotSupported("serialize_tuple".into()))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(Error::NotSupported("serialize_tuple_struct".into()))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(Error::NotSupported("serialize_tuple_variant".into()))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(self)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Error::NotSupported("serialize_struct_variant".into()))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::ast::{Ident, ObjectName, SqlOption, Value};
    use serde::Serialize;

    use crate::utils::redact::SqlOptionVecSerializer;

    fn to_object_name(s: &str) -> ObjectName {
        ObjectName(vec![Ident::new_unchecked(s)])
    }

    #[test]
    fn test_serializer_basic() {
        #[derive(Serialize)]
        struct Foo {
            a: String,
            #[serde(rename = "foo.2.b")]
            b: u32,
            c: Option<f32>,
            d: Option<f32>,
        }

        let mut serializer = SqlOptionVecSerializer::default();
        let foo = Foo {
            a: "v_a".to_string(),
            b: 2,
            c: Some(1.5f32),
            d: None,
        };
        foo.serialize(&mut serializer).unwrap();
        let sql_option: Vec<SqlOption> = serializer.into();
        assert_eq!(
            sql_option,
            vec![
                SqlOption {
                    name: to_object_name("a"),
                    value: Value::SingleQuotedString("v_a".into())
                },
                SqlOption {
                    name: to_object_name("foo.2.b"),
                    value: Value::Number("2".into())
                },
                SqlOption {
                    name: to_object_name("c"),
                    value: Value::Number("1.5".into())
                },
            ]
        );
    }

    #[test]
    fn test_serializer_flatten() {
        #[derive(Serialize)]
        struct Foo {
            #[serde(flatten)]
            x: Option<Bar>,
            #[serde(flatten)]
            y: F1,
        }
        #[derive(Serialize)]
        struct Bar {
            y: u32,
            #[serde(flatten)]
            a: Option<F1>,
            #[serde(flatten)]
            b: F2,
        }
        #[derive(Serialize)]
        struct F1 {
            f1_a: u32,
        }

        #[derive(Serialize)]
        struct F2 {
            f2_a: u32,
        }

        let mut serializer = SqlOptionVecSerializer::default();
        let foo = Foo {
            x: Some(Bar {
                y: 1,
                a: Some(F1 { f1_a: 5 }),
                b: F2 { f2_a: 10 },
            }),
            y: F1 { f1_a: 100 },
        };
        foo.serialize(&mut serializer).unwrap();
        let sql_option: Vec<SqlOption> = serializer.into();
        // duplicated name `f1_a` is allowed
        assert_eq!(
            sql_option,
            vec![
                SqlOption {
                    name: to_object_name("y"),
                    value: Value::Number("1".into())
                },
                SqlOption {
                    name: to_object_name("f1_a"),
                    value: Value::Number("5".into())
                },
                SqlOption {
                    name: to_object_name("f2_a"),
                    value: Value::Number("10".into())
                },
                SqlOption {
                    name: to_object_name("f1_a"),
                    value: Value::Number("100".into())
                },
            ]
        );
    }
}
