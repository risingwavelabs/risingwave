use std::hash::{Hash, Hasher};
use std::io::Write;

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use crate::error::ErrorCode::{InternalError, IoError};
use crate::error::{Result, RwError};
use crate::vector_op::cast::UNIX_EPOCH_DAYS;

macro_rules! impl_chrono_wrapper {
    ($({ $variant_name:ident, $chrono:ty, $array:ident, $builder:ident }),*) => {
        $(
            #[derive(Clone, Copy, Debug, Eq, PartialOrd, Ord)]
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

            impl ToString for $variant_name {
                fn to_string(&self) -> String {
                    self.0.to_string()
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! for_all_chrono_variants {
    ($macro:tt) => {
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
    pub fn new_with_days(days: i32) -> memcomparable::Result<Self> {
        Ok(NaiveDateWrapper::new(
            NaiveDate::from_num_days_from_ce_opt(days)
                .ok_or(memcomparable::Error::InvalidNaiveDateEncoding(days))?,
        ))
    }

    /// Converted to the number of days since 1970.1.1 for compatibility with existing code.
    pub fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&(self.0.num_days_from_ce() - UNIX_EPOCH_DAYS).to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    pub fn from_protobuf(days: i32) -> Result<Self> {
        Self::new_with_days(days + UNIX_EPOCH_DAYS)
            .map_err(|e| RwError::from(InternalError(e.to_string())))
    }
}

impl NaiveTimeWrapper {
    pub fn new_with_secs_nano(secs: u32, nano: u32) -> memcomparable::Result<Self> {
        Ok(NaiveTimeWrapper::new(
            NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                .ok_or(memcomparable::Error::InvalidNaiveTimeEncoding(secs, nano))?,
        ))
    }

    /// Converted to microsecond timestamps for compatibility with existing code.
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
        Self::new_with_secs_nano(secs, nano)
            .map_err(|e| RwError::from(InternalError(e.to_string())))
    }
}

impl NaiveDateTimeWrapper {
    pub fn new_with_secs_nsecs(secs: i64, nsecs: u32) -> memcomparable::Result<Self> {
        Ok(NaiveDateTimeWrapper::new({
            NaiveDateTime::from_timestamp_opt(secs, nsecs).ok_or(
                memcomparable::Error::InvalidNaiveDateTimeEncoding(secs, nsecs),
            )?
        }))
    }

    /// Although `NaiveDateTime` takes 12 bytes, we drop 4 bytes in protobuf encoding.
    /// Converted to microsecond timestamps for compatibility with existing code.
    pub fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&(self.0.timestamp_nanos() / 1000).to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    pub fn from_protobuf(timestamp_micro: i64) -> Result<Self> {
        let secs = timestamp_micro / 1_000_000;
        let nsecs = (timestamp_micro % 1_000_000) as u32 * 1000;
        Self::new_with_secs_nsecs(secs, nsecs)
            .map_err(|e| RwError::from(InternalError(e.to_string())))
    }
}
