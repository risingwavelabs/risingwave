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

use chrono::NaiveDateTime;
use risingwave_common::types::{IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper};

use crate::{ExprError, Result};

#[inline(always)]
pub fn tumble_start_date(
    time: NaiveDateWrapper,
    window: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    tumble_start_date_time(time.into(), window)
}

#[inline(always)]
pub fn tumble_start_date_time(
    time: NaiveDateTimeWrapper,
    window: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    let diff = time.0.timestamp_micros();
    let window_start = tm_diff_bin(diff, window)?;
    Ok(NaiveDateTimeWrapper(NaiveDateTime::from_timestamp(
        window_start / 1_000_000,
        (window_start % 1_000_000 * 1000) as u32,
    )))
}

#[inline(always)]
pub fn tumble_start_timestampz(time: i64, window: IntervalUnit) -> Result<i64> {
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

#[cfg(test)]
mod tests {
    use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
    use risingwave_common::types::{IntervalUnit, NaiveDateTimeWrapper};

    use super::tumble_start_date_time;

    #[test]
    fn test_tumble_start_date_time() {
        let d = NaiveDate::from_ymd(2022, 2, 22);
        let t = NaiveTime::from_hms(22, 22, 22);
        let dt = NaiveDateTime::new(d, t);
        let interval = IntervalUnit::new(0, 0, 30 * 60 * 1000);
        let w = tumble_start_date_time(NaiveDateTimeWrapper(dt), interval)
            .unwrap()
            .0;
        assert_eq!(w.year(), 2022);
        assert_eq!(w.month(), 2);
        assert_eq!(w.day(), 22);
        assert_eq!(w.hour(), 22);
        assert_eq!(w.minute(), 0);
        assert_eq!(w.second(), 0);
    }
}
