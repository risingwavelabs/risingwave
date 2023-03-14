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

use crate::Result;

#[inline(always)]
pub fn tumble_start_date(
    timestamp: NaiveDateWrapper,
    window_size: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    tumble_start_date_time(timestamp.into(), window_size)
}

#[inline(always)]
pub fn tumble_start_date_time(
    timestamp: NaiveDateTimeWrapper,
    window_size: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    let timestamp_micro_second = timestamp.0.timestamp_micros();
    let window_start = get_window_start(timestamp_micro_second, window_size)?;
    Ok(NaiveDateTimeWrapper::from_timestamp_uncheck(
        window_start / 1_000_000,
        (window_start % 1_000_000 * 1000) as u32,
    ))
}

#[inline(always)]
pub fn tumble_start_timestamptz(
    timestamp_micro_second: i64,
    window_size: IntervalUnit,
) -> Result<i64> {
    let timestamp_micro_second = timestamp_micro_second;
    let window_size = window_size;
    Ok(get_window_start(timestamp_micro_second, window_size)?)
}

/// The common part of PostgreSQL function `timestamp_bin` and `timestamptz_bin`.
#[inline(always)]
fn get_window_start(timestamp_micro_second: i64, window_size: IntervalUnit) -> Result<i64> {
    const DAY_MICOR_SECOND: i64 = 86400000000;
    const MONTH_MICOR_SECOND: i64 = 30 * DAY_MICOR_SECOND;

    let window_size_micro_second = window_size.get_months() as i64 * MONTH_MICOR_SECOND
        + window_size.get_days() as i64 * DAY_MICOR_SECOND
        + window_size.get_ms() * 1000;

    // Inspired by https://issues.apache.org/jira/browse/FLINK-26334
    let remainder = (timestamp_micro_second + window_size_micro_second) % window_size_micro_second;
    if remainder < 0 {
        Ok(timestamp_micro_second - (remainder + window_size_micro_second))
    } else {
        Ok(timestamp_micro_second - remainder)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, Timelike};
    use risingwave_common::types::test_utils::IntervalUnitTestExt;
    use risingwave_common::types::{IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper};

    use crate::vector_op::tumble::{get_window_start, tumble_start_date_time};

    #[test]
    fn test_tumble_start_date_time() {
        let dt = NaiveDateWrapper::from_ymd_uncheck(2022, 2, 22).and_hms_uncheck(22, 22, 22);
        let interval = IntervalUnit::from_minutes(30);
        let w = tumble_start_date_time(dt, interval).unwrap().0;
        assert_eq!(w.year(), 2022);
        assert_eq!(w.month(), 2);
        assert_eq!(w.day(), 22);
        assert_eq!(w.hour(), 22);
        assert_eq!(w.minute(), 0);
        assert_eq!(w.second(), 0);
    }

    #[test]
    fn test_remainder_necessary() {
        let mut wrong_cnt = 0;
        for i in -30..30 {
            let timestamp_micro_second = IntervalUnit::from_minutes(i).get_ms() * 1000;
            let window_size = IntervalUnit::from_minutes(5);
            let window_start = get_window_start(timestamp_micro_second, window_size).unwrap();

            const DAY_MICOR_SECOND: i64 = 86400000000;
            const MONTH_MICOR_SECOND: i64 = 30 * DAY_MICOR_SECOND;

            let window_size_micro_second = window_size.get_months() as i64 * MONTH_MICOR_SECOND
                + window_size.get_days() as i64 * DAY_MICOR_SECOND
                + window_size.get_ms() * 1000;

            let default_window_start = timestamp_micro_second
                - (timestamp_micro_second + window_size_micro_second) % window_size_micro_second;

            if timestamp_micro_second < default_window_start {
                // which is wrong
                wrong_cnt = wrong_cnt + 1;
            }

            assert!(timestamp_micro_second >= window_start)
        }
        assert_ne!(wrong_cnt, 0);
    }
}
