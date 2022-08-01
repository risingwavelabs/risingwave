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

use bytes::{BytesMut, Bytes, BufMut};
use chrono::{NaiveDate, NaiveTime, NaiveDateTime};
use postgres_types::{Type, ToSql};
use rust_decimal::Decimal;

/// A structure for encoding Rust values into a binary bytes.
pub struct BinaryEncoder {
    output: BytesMut,
    placeholder: Type,
}

impl BinaryEncoder {
    /// Create a new `Serializer`.
    pub fn new() -> Self {
        Self {
            output: BytesMut::new(),
            placeholder: Type::ANY.clone(),
        }
    }

    /// Get the output buffer from the `Serializer`.
    pub fn get_ouput(&mut self) -> Bytes {
        self.output.clone().freeze()
    }

    fn serialize_bool(&mut self, v: bool) {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
    }

    fn serialize_i8(&mut self, v: i8) {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
    }

    fn serialize_i16(&mut self, v: i16)  {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
    }

    fn serialize_i32(&mut self, v: i32) {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
    }

    fn serialize_i64(&mut self, v: i64) {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
    }

    fn serialize_f32(&mut self, v: f32) {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
    }

    fn serialize_f64(&mut self, v: f64)  {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
    }

    fn serialize_str(&mut self, v: &str){
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
    }

    fn serialize_bytes(&mut self, v: &[u8]) {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
    }

    /// Serialize a Decimal value.
    ///
    /// v:Option<Decimal>
    /// Some(v) means v is Normalized Decimal.
    /// None means v is NaN.
    pub fn serialize_decimal(&mut self, v: Option<Decimal>) {
        match v {
            Some(v) => {
                v.to_sql(&self.placeholder, &mut self.output).unwrap();
            }
            None => {
                self.output.reserve(8);
                self.output.put_u16(0);
                self.output.put_i16(0);
                self.output.put_u16(0xC000);
                self.output.put_i16(0);
            }
        }
    
    }

    /// Serialize a NaiveDate value.
    pub fn serialize_naivedate(&mut self, v: NaiveDate){
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
       
    }

    /// Serialize a NaiveTimeWrapper value.
    pub fn serialize_naivetime(&mut self, v: NaiveTime){
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
    }

    /// Serialize a NaiveDateTime value.
    pub fn serialize_naivedatetime(&mut self, v: NaiveDateTime) {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
    
    }
}
impl Default for BinaryEncoder {
    fn default() -> Self {
        Self::new()
    }
}