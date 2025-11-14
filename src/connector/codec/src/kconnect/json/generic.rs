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

pub trait JsonRead: Sized {
    fn is_null(&self) -> bool;
    fn as_str(&self) -> Option<&str>;
    fn as_bool(&self) -> Option<bool>;
    fn as_i32(&self) -> Option<i32>;
    fn as_array(&self) -> Option<&[Self]>;
    fn as_kv_iter(&self) -> Option<impl ExactSizeIterator<Item = (&str, &Self)>>;
    fn get(&self, key: &str) -> Option<&Self>;

    fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.as_str())
    }
    fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(|v| v.as_bool())
    }
    fn get_i32(&self, key: &str) -> Option<i32> {
        self.get(key).and_then(|v| v.as_i32())
    }
    fn get_array(&self, key: &str) -> Option<&[Self]> {
        self.get(key).and_then(|v| v.as_array())
    }
    fn get_kv_iter(&self, key: &str) -> Option<impl ExactSizeIterator<Item = (&str, &Self)>> {
        self.get(key).and_then(|v| v.as_kv_iter())
    }
}

impl JsonRead for serde_json::Value {
    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    fn as_i32(&self) -> Option<i32> {
        match self {
            Self::Number(n) => n.as_i64().and_then(|v| v.try_into().ok()),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<&[Self]> {
        match self {
            Self::Array(arr) => Some(arr),
            _ => None,
        }
    }

    fn as_kv_iter(&self) -> Option<impl ExactSizeIterator<Item = (&str, &Self)>> {
        match self {
            Self::Object(map) => Some(map.iter().map(|(k, v)| (k.as_str(), v))),
            _ => None,
        }
    }

    fn get(&self, key: &str) -> Option<&Self> {
        match self {
            Self::Object(map) => map.get(key),
            _ => None,
        }
    }
}

impl JsonRead for simd_json::BorrowedValue<'_> {
    fn is_null(&self) -> bool {
        matches!(self, Self::Static(simd_json::StaticNode::Null))
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Static(simd_json::StaticNode::Bool(b)) => Some(*b),
            _ => None,
        }
    }

    fn as_i32(&self) -> Option<i32> {
        use simd_json::prelude::ValueAsScalar as _;
        match self {
            Self::Static(n) => n.as_i64().and_then(|v| v.try_into().ok()),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<&[Self]> {
        match self {
            Self::Array(arr) => Some(arr),
            _ => None,
        }
    }

    fn as_kv_iter(&self) -> Option<impl ExactSizeIterator<Item = (&str, &Self)>> {
        match self {
            Self::Object(map) => Some(map.iter().map(|(k, v)| (k.as_ref(), v))),
            _ => None,
        }
    }

    fn get(&self, key: &str) -> Option<&Self> {
        match self {
            Self::Object(map) => map.get(key),
            _ => None,
        }
    }
}
