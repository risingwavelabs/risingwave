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

pub mod agg;
pub mod arithmetic_op;
pub mod array_access;
pub mod ascii;
pub mod bitwise_op;
pub mod cast;
pub mod cmp;
pub mod concat_op;
pub mod conjunction;
pub mod extract;
pub mod length;
pub mod like;
pub mod lower;
pub mod ltrim;
pub mod md5;
pub mod overlay;
pub mod position;
pub mod repeat;
pub mod replace;
pub mod round;
pub mod rtrim;
pub mod split_part;
pub mod substr;
pub mod timestampz;
pub mod to_char;
pub mod translate;
pub mod trim;
pub mod trim_characters;
pub mod tumble;
pub mod upper;

#[cfg(test)]
mod tests;
