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

use chrono::{TimeZone, Utc};
use chrono_tz::Tz;
use num_traits::ToPrimitive;
use risingwave_common::types::{NaiveDateTimeWrapper, OrderedF64};

use crate::{ExprError, Result};

/// Just a wrapper to reuse the `map_err` logic.
#[inline(always)]
fn parse_time_zone(time_zone: &str) -> Result<Tz> {
    Tz::from_str_insensitive(time_zone).map_err(|e| ExprError::InvalidParam {
        name: "time_zone",
        reason: e,
    })
}

#[inline(always)]
pub fn f64_sec_to_timestampz(elem: OrderedF64) -> Result<i64> {
    // TODO(#4515): handle +/- infinity
    (elem * 1e6)
        .round() // TODO(#5576): should round to even
        .to_i64()
        .ok_or(ExprError::NumericOutOfRange)
}

#[inline(always)]
pub fn timestamp_at_time_zone(input: NaiveDateTimeWrapper, time_zone: &str) -> Result<i64> {
    let time_zone = parse_time_zone(time_zone)?;
    // https://www.postgresql.org/docs/current/datetime-invalid-input.html
    // Special cases:
    // * invalid time during daylight forward
    //   * PostgreSQL uses UTC offset before the transition
    //   * We report an error (FIXME)
    // * ambiguous time during daylight backward
    //   * We follow PostgreSQL to use UTC offset after the transition
    let instant_local = input
        .0
        .and_local_timezone(time_zone)
        .latest()
        .ok_or_else(|| ExprError::InvalidParam {
            name: "local timestamp",
            reason: format!(
                "fail to interpret local timestamp \"{}\" in time zone \"{}\"",
                input, time_zone
            ),
        })?;
    let usec = instant_local.timestamp_micros();
    Ok(usec)
}

#[inline(always)]
pub fn timestampz_at_time_zone(input: i64, time_zone: &str) -> Result<NaiveDateTimeWrapper> {
    let time_zone = parse_time_zone(time_zone)?;
    let instant_utc = Utc.timestamp(input / 1_000_000, (input % 1_000_000 * 1000) as u32);
    let instant_local = instant_utc.with_timezone(&time_zone);
    let naive = instant_local.naive_local();
    Ok(NaiveDateTimeWrapper(naive))
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use itertools::Itertools;

    use super::*;
    use crate::vector_op::cast::{str_to_timestamp, str_to_timestampz};

    #[test]
    fn test_time_zone_conversion() {
        let zones = ["US/Pacific", "ASIA/SINGAPORE", "europe/zurich"];
        #[rustfmt::skip]
        let test_cases = [
            // winter
            ["2022-01-01 00:00:00Z", "2021-12-31 16:00:00", "2022-01-01 08:00:00", "2022-01-01 01:00:00"],
            // summer
            ["2022-07-01 00:00:00Z", "2022-06-30 17:00:00", "2022-07-01 08:00:00", "2022-07-01 02:00:00"],
            // before and after PST -> PDT, where [02:00, 03:00) are invalid
            ["2022-03-13 09:59:00Z", "2022-03-13 01:59:00", "2022-03-13 17:59:00", "2022-03-13 10:59:00"],
            ["2022-03-13 10:00:00Z", "2022-03-13 03:00:00", "2022-03-13 18:00:00", "2022-03-13 11:00:00"],
            // before and after CET -> CEST, where [02:00. 03:00) are invalid
            ["2022-03-27 00:59:00Z", "2022-03-26 17:59:00", "2022-03-27 08:59:00", "2022-03-27 01:59:00"],
            ["2022-03-27 01:00:00Z", "2022-03-26 18:00:00", "2022-03-27 09:00:00", "2022-03-27 03:00:00"],
            // before and after CEST -> CET, where [02:00, 03:00) are ambiguous
            ["2022-10-29 23:59:00Z", "2022-10-29 16:59:00", "2022-10-30 07:59:00", "2022-10-30 01:59:00"],
            ["2022-10-30 02:00:00Z", "2022-10-29 19:00:00", "2022-10-30 10:00:00", "2022-10-30 03:00:00"],
            // before and after PDT -> PST, where [01:00, 02:00) are ambiguous
            ["2022-11-06 07:59:00Z", "2022-11-06 00:59:00", "2022-11-06 15:59:00", "2022-11-06 08:59:00"],
            ["2022-11-06 10:00:00Z", "2022-11-06 02:00:00", "2022-11-06 18:00:00", "2022-11-06 11:00:00"],
        ];
        for case in test_cases {
            let usecs = str_to_timestampz(case[0]).unwrap();
            case.iter().skip(1).zip_eq(zones).for_each(|(local, zone)| {
                let local = str_to_timestamp(local).unwrap();

                let actual = timestampz_at_time_zone(usecs, zone).unwrap();
                assert_eq!(local, actual);

                let actual = timestamp_at_time_zone(local, zone).unwrap();
                assert_eq!(usecs, actual);
            });
        }
    }

    #[test]
    fn test_time_zone_conversion_daylight_forward() {
        for (local, zone) in [
            ("2022-03-13 02:00:00", "US/Pacific"),
            ("2022-03-13 02:59:00", "US/Pacific"),
            ("2022-03-27 02:00:00", "europe/zurich"),
            ("2022-03-27 02:59:00", "europe/zurich"),
        ] {
            let local = str_to_timestamp(local).unwrap();

            let actual = timestamp_at_time_zone(local, zone);
            assert_matches!(actual, Err(_));
        }
    }

    #[test]
    fn test_time_zone_conversion_daylight_backward() {
        #[rustfmt::skip]
        let test_cases = [
            ("2022-10-30 00:00:00Z", "2022-10-30 02:00:00", "europe/zurich", false),
            ("2022-10-30 00:59:00Z", "2022-10-30 02:59:00", "europe/zurich", false),
            ("2022-10-30 01:00:00Z", "2022-10-30 02:00:00", "europe/zurich", true),
            ("2022-10-30 01:59:00Z", "2022-10-30 02:59:00", "europe/zurich", true),
            ("2022-11-06 08:00:00Z", "2022-11-06 01:00:00", "US/Pacific", false),
            ("2022-11-06 08:59:00Z", "2022-11-06 01:59:00", "US/Pacific", false),
            ("2022-11-06 09:00:00Z", "2022-11-06 01:00:00", "US/Pacific", true),
            ("2022-11-06 09:59:00Z", "2022-11-06 01:59:00", "US/Pacific", true),
        ];
        for (instant, local, zone, preferred) in test_cases {
            let usecs = str_to_timestampz(instant).unwrap();
            let local = str_to_timestamp(local).unwrap();

            let actual = timestampz_at_time_zone(usecs, zone).unwrap();
            assert_eq!(local, actual);

            if preferred {
                let actual = timestamp_at_time_zone(local, zone).unwrap();
                assert_eq!(usecs, actual)
            }
        }
    }
}
