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

mod field_generator;
mod generator;
mod reader;
use anyhow::Result;
pub use reader::*;
use serde_json::Value;
pub trait FieldGenerator {
    fn with_sequence(min: Option<String>, max: Option<String>) -> Result<Self>
    where
        Self: Sized;
    fn with_random(start: Option<String>, end: Option<String>) -> Result<Self>
    where
        Self: Sized;
    fn generate(&mut self) -> Value;
}

// Generator of this '#' field. Can be 'sequence' or 'random'.
pub enum FieldKind {
    Sequence,
    Random,
}

impl Default for FieldKind {
    fn default() -> Self {
        FieldKind::Random
    }
}
