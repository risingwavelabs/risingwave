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

use risingwave_common::types::{IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper};

use crate::{ExprError, Result};

#[inline(always)]
pub fn tumble_start_date(
    time: NaiveDateWrapper,
    window: IntervalUnit,
    offset: Option<IntervalUnit>,
) -> Result<NaiveDateTimeWrapper> {
    tumble_start_date_time(time.into(), window, offset)
}

#[inline(always)]
pub fn tumble_start_date_time(
    time: NaiveDateTimeWrapper,
    window: IntervalUnit,
    offset: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    let diff = time.0.timestamp_micros();
    let window_start = tm_diff_bin(diff, window)?;
    Ok(NaiveDateTimeWrapper::from_timestamp_uncheck(
        window_start / 1_000_000,
        (window_start % 1_000_000 * 1000) as u32,
    ))
}

#[inline(always)]
pub fn tumble_start_timestamptz(time: i64, window: IntervalUnit) -> Result<i64> {
    // Actually directly calls into the helper `tm_diff_bin`. But we keep the shared utility and
    // enduser function separate.
    let diff = time;
    let window_start = tm_diff_bin(diff, window)?;
    Ok(window_start)
}

/// The common part of PostgreSQL function `timestamp_bin` and `timestamptz_bin`.
#[inline(always)]
fn tm_diff_bin(diff_usecs: i64, window: IntervalUnit) -> Result<i64> {
    if window.get_months() != 0 {
        return Err(ExprError::InvalidParam {
            name: "window",
            reason: "unimplemented: tumble_start only support days or milliseconds".to_string(),
        });
    }
    let window_usecs = window.get_days() as i64 * 24 * 60 * 60 * 1_000_000 + window.get_ms() * 1000;

    if window_usecs <= 0 {
        return Err(ExprError::InvalidParam {
            name: "window",
            reason: "window must be positive".to_string(),
        });
    }

    let delta_usecs = diff_usecs - diff_usecs % window_usecs;
    Ok(delta_usecs)
}

#[inline(always)]
pub fn tumble_offset_date(
    time: NaiveDateWrapper,
    offset: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    tumble_offset_date_time(time.into(), offset)
}

#[inline(always)]
pub fn tumble_offset_date_time(
    time: NaiveDateTimeWrapper,
    offset: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    let time = time.0.timestamp_micros();
    let window_offset = tm_subtracts(time, offset)?;
    Ok(NaiveDateTimeWrapper::from_timestamp_uncheck(
        window_offset / 1_000_000,
        (window_offset % 1_000_000 * 1000) as u32,
    ))
}

#[inline(always)]
pub fn tumble_offset_timestamptz(time: i64, offset: IntervalUnit) -> Result<i64> {
    Ok(tm_subtracts(time, offset)?)
}

#[inline(always)]
fn tm_subtracts(time: i64, offset: IntervalUnit) -> Result<i64> {
    const DAY_MS: i64 = 86400000;
    let offset_usecs = offset.get_days() as i64 * DAY_MS + offset.get_ms() * 1000;
    let time_usecs = time - offset_usecs;
    if time_usecs < 0 {
        Ok(-time_usecs)
    } else {
        Ok(time_usecs)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{offset, Datelike, Timelike};
    use risingwave_common::types::test_utils::IntervalUnitTestExt;
    use risingwave_common::types::{IntervalUnit, NaiveDateWrapper};

    use super::{tm_subtracts, tumble_start_date_time};

    #[test]
    fn test_tumble_start_date_time() {
        let dt = NaiveDateWrapper::from_ymd_uncheck(2022, 2, 22).and_hms_uncheck(22, 22, 22);
        let interval = IntervalUnit::from_minutes(30);
        let offset = IntervalUnit::from_minutes(0);
        println!("{}", dt);
        println!("{}", interval);
        let w = tumble_start_date_time(dt, interval, offset).unwrap().0;
        println!("{}", w);
        assert_eq!(w.year(), 2022);
        assert_eq!(w.month(), 2);
        assert_eq!(w.day(), 22);
        assert_eq!(w.hour(), 22);
        assert_eq!(w.minute(), 0);
        assert_eq!(w.second(), 0);
    }

    #[test]
    fn test_tm_subtracts() {
        let time = NaiveDateWrapper::from_ymd_uncheck(2022, 2, 22)
            .and_hms_uncheck(22, 22, 22)
            .0
            .timestamp_micros();
        let offset = IntervalUnit::from_minutes(30);

        assert_eq!(
            tm_subtracts(time, offset).unwrap(),
            NaiveDateWrapper::from_ymd_uncheck(2022, 2, 22)
                .and_hms_uncheck(21, 52, 22)
                .0
                .timestamp_micros()
        )
    }
    #[test]
    fn test_tm_subtracts_overflow() {
        let time = NaiveDateWrapper::from_ymd_uncheck(1970, 1, 1)
            .and_hms_uncheck(0, 0, 0)
            .0
            .timestamp_micros();
        let offset = IntervalUnit::from_minutes(30);

        assert_eq!(
            tm_subtracts(time, offset).unwrap(),
            NaiveDateWrapper::from_ymd_uncheck(1970, 1, 1)
                .and_hms_uncheck(0, 30, 0)
                .0
                .timestamp_micros()
        )
    }
}
