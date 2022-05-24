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

use anyhow::Result;
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde_json::{json, Value};

pub struct VarcharField {
    length: usize,
}

impl VarcharField {
    pub fn new(length_option: Option<String>) -> Result<Self> {
        let length = if let Some(length_option) = length_option {
            length_option.parse::<usize>()?
        } else {
            100
        };
        Ok(Self { length })
    }

    pub fn generate(&mut self) -> Value {
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(self.length)
            .map(char::from)
            .collect();
        json!(s)
    }
}
