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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ValueEncodingError {
    #[error("Invalid bool value encoding: {0}")]
    InvalidBoolEncoding(u8),
    #[error("Invalid UTF8 value encoding: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("Invalid NaiveDate value encoding: days: {0}")]
    InvalidNaiveDateEncoding(i32),
    #[error("invalid NaiveDateTime value encoding: secs: {0} nsecs: {1}")]
    InvalidNaiveDateTimeEncoding(i64, u32),
    #[error("invalid NaiveTime value encoding: secs: {0} nano: {1}")]
    InvalidNaiveTimeEncoding(u32, u32),
    #[error("Invalid null tag value encoding: {0}")]
    InvalidTagEncoding(u8),
    #[error("Invalid struct encoding: {0}")]
    InvalidStructEncoding(crate::array::ArrayError),
    #[error("Invalid list encoding: {0}")]
    InvalidListEncoding(crate::array::ArrayError),
}
