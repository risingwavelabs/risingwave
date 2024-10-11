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

//! @kwannoel:
//! This module implements Serde for the Pretty struct. Why not implement it directly on our plan nodes?
//! That's because Pretty already summarizes the fields that are important to us.
//! You can see that when `explain()` is called, we directly return the `Pretty` struct.
//! The _proper way_ to do this would be to create a new data structure that plan nodes get converted into,
//! and then implement `Serialize` and `Deserialize` on that data structure (including to `Pretty`).
//! But that's a lot of refactoring work.
//! So we just wrap `Pretty` in a newtype and implement `Serialize` and `Deserialize` on that,
//! since it's a good enough intermediate representation.

use pretty_xmlish::Pretty;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub struct PrettySerde<'a>(pub Pretty<'a>);

impl<'a> Serialize for PrettySerde<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("")
    }
}
