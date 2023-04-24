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

use std::string::ToString;

use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{json, Value};

use super::DEFAULT_LENGTH;
use crate::types::{Datum, Scalar};

pub struct VarcharRandomVariableLengthField {
    seed: u64,
}

impl VarcharRandomVariableLengthField {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }

    pub fn generate_string(&mut self, offset: u64) -> String {
        StdRng::seed_from_u64(offset ^ self.seed)
            .sample_iter(&Alphanumeric)
            .map(char::from)
            .collect()
    }

    pub fn generate(&mut self, offset: u64) -> Value {
        json!(self.generate_string(offset))
    }

    pub fn generate_datum(&mut self, offset: u64) -> Datum {
        let s = self.generate_string(offset);
        Some(s.into_boxed_str().to_scalar_value())
    }
}

pub struct VarcharRandomFixedLengthField {
    length: usize,
    seed: u64,
}

impl VarcharRandomFixedLengthField {
    pub fn new(length_option: &Option<usize>, seed: u64) -> Self {
        let length = if let Some(length) = length_option {
            *length
        } else {
            DEFAULT_LENGTH
        };
        Self { length, seed }
    }

    pub fn generate(&mut self, offset: u64) -> Value {
        let s: String = StdRng::seed_from_u64(offset ^ self.seed)
            .sample_iter(&Alphanumeric)
            .take(self.length)
            .map(char::from)
            .collect();
        json!(s)
    }

    pub fn generate_datum(&mut self, offset: u64) -> Datum {
        let s: String = StdRng::seed_from_u64(offset ^ self.seed)
            .sample_iter(&Alphanumeric)
            .take(self.length)
            .map(char::from)
            .collect();
        Some(s.into_boxed_str().to_scalar_value())
    }
}

pub struct VarcharConstant {}

impl VarcharConstant {
    const CONSTANT_STRING: &'static str = "2022-03-03";

    pub fn generate_json() -> Value {
        json!(Self::CONSTANT_STRING)
    }

    pub fn generate_datum() -> Datum {
        Some(
            Self::CONSTANT_STRING
                .to_string()
                .into_boxed_str()
                .to_scalar_value(),
        )
    }
}
