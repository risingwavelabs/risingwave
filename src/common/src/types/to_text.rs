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

use super::{DatumRef, ScalarRefImpl};

// Used to convert ScalarRef to text format
pub trait ToText {
    fn to_text(&self) -> String;
}

macro_rules! implement_using_to_string {
    ($({ $scalar_type:ty } ),*) => {
        $(
            impl ToText for $scalar_type {
                fn to_text(&self) -> String {
                    self.to_string()
                }
            }
        )*
    };
}

implement_using_to_string! {
    { i16 },
    { i32 },
    { i64 },
    { String },
    { &str },
    { crate::types::OrderedF32 },
    { crate::types::OrderedF64 }
}

impl ToText for bool {
    fn to_text(&self) -> String {
        if *self {
            "t".to_string()
        } else {
            "f".to_string()
        }
    }
}

impl ToText for ScalarRefImpl<'_> {
    fn to_text(&self) -> String {
        match self {
            ScalarRefImpl::Bool(b) => b.to_text(),
            ScalarRefImpl::Int16(i) => i.to_text(),
            ScalarRefImpl::Int32(i) => i.to_text(),
            ScalarRefImpl::Int64(i) => i.to_text(),
            ScalarRefImpl::Float32(f) => f.to_text(),
            ScalarRefImpl::Float64(f) => f.to_text(),
            ScalarRefImpl::Decimal(d) => d.to_text(),
            ScalarRefImpl::Interval(i) => i.to_text(),
            ScalarRefImpl::NaiveDate(d) => d.to_text(),
            ScalarRefImpl::NaiveTime(t) => t.to_text(),
            ScalarRefImpl::NaiveDateTime(dt) => dt.to_text(),
            ScalarRefImpl::List(l) => l.to_text(),
            ScalarRefImpl::Struct(s) => s.to_text(),
            ScalarRefImpl::Utf8(v) => v.to_text(),
        }
    }
}

impl ToText for DatumRef<'_> {
    fn to_text(&self) -> String {
        match self {
            Some(data) => data.to_text(),
            None => "NULL".to_string(),
        }
    }
}
