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

use std::borrow::Cow;

use chrono::{DateTime, Utc};

const PARSE_ERROR_STR_WITH_TIME_ZONE_TO_TIMESTAMPTZ: &str = concat!(
    "Can't cast string to timestamp with time zone (expected format is YYYY-MM-DD HH:MM:SS[.D+{up to 6 digits}] followed by +hh:mm or literal Z)"
    , "\nFor example: '2021-04-01 00:00:00+00:00'"
);

/// Timestamptz is represented as i64 in `ScalarImpl` now.
#[inline(always)]
pub fn str_with_time_zone_to_timestamptz(elem: &str) -> Result<i64, Cow<'static, str>> {
    Ok(elem
        .parse::<DateTime<Utc>>()
        .map(|ret| ret.timestamp_micros())
        .map_err(|_| PARSE_ERROR_STR_WITH_TIME_ZONE_TO_TIMESTAMPTZ)?)
}

#[cfg(test)]
mod test {
    use crate::types::timestamptz::{
        str_with_time_zone_to_timestamptz, PARSE_ERROR_STR_WITH_TIME_ZONE_TO_TIMESTAMPTZ,
    };

    #[test]
    fn parse() {
        assert_eq!(
            str_with_time_zone_to_timestamptz("2022-08-03 10:34:02Z").unwrap(),
            str_with_time_zone_to_timestamptz("2022-08-03 02:34:02-08:00").unwrap()
        );

        assert_eq!(
            str_with_time_zone_to_timestamptz("1999-01-08 04:05:06")
                .unwrap_err()
                .to_string(),
            PARSE_ERROR_STR_WITH_TIME_ZONE_TO_TIMESTAMPTZ.to_string()
        );
    }
}
