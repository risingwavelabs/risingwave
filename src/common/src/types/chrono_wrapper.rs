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

use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io::Write;

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use crate::error::ErrorCode::{InternalError, IoError};
use crate::error::{Result, RwError};
use crate::util::value_encoding::error::ValueEncodingError;
/// The same as `NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce()`.
/// Minus this magic number to store the number of days since 1970-01-01.
pub const UNIX_EPOCH_DAYS: i32 = 719_163;

macro_rules! impl_chrono_wrapper {
    ($({ $variant_name:ident, $chrono:ty, $_array:ident, $_builder:ident }),*) => {
        $(
            #[derive(Clone, Copy, Debug, Eq, PartialOrd, Ord)]
            #[repr(transparent)]
            pub struct $variant_name(pub $chrono);

            impl $variant_name {
                pub fn new(data: $chrono) -> Self {
                    $variant_name(data)
                }
            }

            impl Hash for $variant_name {
                fn hash<H: Hasher>(&self, state: &mut H) {
                    self.0.hash(state);
                }
            }

            impl PartialEq for $variant_name {
                fn eq(&self, other: &Self) -> bool {
                    self.0 == other.0
                }
            }

            impl Display for $variant_name {
                fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                    Display::fmt(&self.0, f)
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! for_all_chrono_variants {
    ($macro:ident) => {
        $macro! {
            { NaiveDateWrapper, NaiveDate, NaiveDateArray, NaiveDateArrayBuilder },
            { NaiveDateTimeWrapper, NaiveDateTime, NaiveDateTimeArray, NaiveDateTimeArrayBuilder },
            { NaiveTimeWrapper, NaiveTime, NaiveTimeArray, NaiveTimeArrayBuilder }
        }
    };
}

for_all_chrono_variants! { impl_chrono_wrapper }

impl Default for NaiveDateWrapper {
    fn default() -> Self {
        NaiveDateWrapper(NaiveDate::from_ymd(1970, 1, 1))
    }
}

impl Default for NaiveTimeWrapper {
    fn default() -> Self {
        NaiveTimeWrapper(NaiveTime::from_hms(0, 0, 0))
    }
}

impl Default for NaiveDateTimeWrapper {
    fn default() -> Self {
        NaiveDateTimeWrapper(NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0))
    }
}

impl NaiveDateWrapper {
    pub fn with_days(days: i32) -> memcomparable::Result<Self> {
        Ok(NaiveDateWrapper::new(
            NaiveDate::from_num_days_from_ce_opt(days)
                .ok_or(memcomparable::Error::InvalidNaiveDateEncoding(days))?,
        ))
    }

    pub fn new_with_days_value_encoding(days: i32) -> Result<Self> {
        Ok(NaiveDateWrapper::new(
            NaiveDate::from_num_days_from_ce_opt(days)
                .ok_or(ValueEncodingError::InvalidNaiveDateEncoding(days))?,
        ))
    }

    /// Converted to the number of days since 1970.1.1 for compatibility with existing Java
    /// frontend. TODO: Save days directly when using Rust frontend.
    pub fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&(self.0.num_days_from_ce() - UNIX_EPOCH_DAYS).to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    pub fn from_protobuf(days: i32) -> Result<Self> {
        Self::with_days(days + UNIX_EPOCH_DAYS)
            .map_err(|e| RwError::from(InternalError(e.to_string())))
    }
}

impl NaiveTimeWrapper {
    pub fn with_secs_nano(secs: u32, nano: u32) -> memcomparable::Result<Self> {
        Ok(NaiveTimeWrapper::new(
            NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                .ok_or(memcomparable::Error::InvalidNaiveTimeEncoding(secs, nano))?,
        ))
    }

    pub fn new_with_secs_nano_value_encoding(secs: u32, nano: u32) -> Result<Self> {
        Ok(NaiveTimeWrapper::new(
            NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                .ok_or(ValueEncodingError::InvalidNaiveTimeEncoding(secs, nano))?,
        ))
    }

    /// Converted to microsecond timestamps for compatibility with existing Java frontend.
    /// TODO: Save nanoseconds directly when using Rust frontend.
    pub fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(
                &(self.0.num_seconds_from_midnight() as i64 * 1_000_000
                    + self.0.nanosecond() as i64 / 1000)
                    .to_be_bytes(),
            )
            .map_err(|e| RwError::from(IoError(e)))
    }

    pub fn from_protobuf(timestamp_micro: i64) -> Result<Self> {
        let secs = (timestamp_micro / 1_000_000) as u32;
        let nano = (timestamp_micro % 1_000_000) as u32 * 1000;
        Self::with_secs_nano(secs, nano).map_err(|e| RwError::from(InternalError(e.to_string())))
    }
}

impl NaiveDateTimeWrapper {
    pub fn with_secs_nsecs(secs: i64, nsecs: u32) -> memcomparable::Result<Self> {
        Ok(NaiveDateTimeWrapper::new({
            NaiveDateTime::from_timestamp_opt(secs, nsecs).ok_or(
                memcomparable::Error::InvalidNaiveDateTimeEncoding(secs, nsecs),
            )?
        }))
    }

    pub fn new_with_secs_nsecs_value_encoding(secs: i64, nsecs: u32) -> Result<Self> {
        Ok(NaiveDateTimeWrapper::new({
            NaiveDateTime::from_timestamp_opt(secs, nsecs).ok_or(
                ValueEncodingError::InvalidNaiveDateTimeEncoding(secs, nsecs),
            )?
        }))
    }

    /// Although `NaiveDateTime` takes 12 bytes, we drop 4 bytes in protobuf encoding.
    /// Converted to microsecond timestamps for compatibility with existing Java frontend.
    /// TODO: Consider another way to save when using Rust frontend. Nanosecond timestamp can only
    /// represent about 584 years
    pub fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&(self.0.timestamp_nanos() / 1000).to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    pub fn from_protobuf(timestamp_micro: i64) -> Result<Self> {
        let secs = timestamp_micro / 1_000_000;
        let nsecs = (timestamp_micro % 1_000_000) as u32 * 1000;
        Self::with_secs_nsecs(secs, nsecs).map_err(|e| RwError::from(InternalError(e.to_string())))
    }

    pub fn parse_from_str(s: &str) -> Result<Self> {
        Ok(NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .map_err(|e| RwError::from(InternalError(e.to_string())))?,
        ))
    }

    pub fn sub(&self, rhs: NaiveDateTimeWrapper) -> Duration {
        self.0 - rhs.0
    }

    pub fn add(&self, duration: Duration) -> Self {
        NaiveDateTimeWrapper::new(self.0 + duration)
    }
}

impl TryFrom<NaiveDateWrapper> for NaiveDateTimeWrapper {
    type Error = RwError;

    fn try_from(date: NaiveDateWrapper) -> Result<Self> {
        Ok(NaiveDateTimeWrapper::new(date.0.and_hms(0, 0, 0)))
    }
}
