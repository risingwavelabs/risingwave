use chrono::{Datelike, Timelike};

use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::types::{Decimal, NaiveDateTimeWrapper, NaiveDateWrapper};

fn extract_time<T>(time: T, time_unit: &str) -> Result<Decimal>
where
    T: Timelike,
{
    match time_unit {
        "HOUR" => Ok(time.hour().into()),
        "MINUTE" => Ok(time.minute().into()),
        "SECOND" => Ok(time.second().into()),
        _ => Err(RwError::from(InternalError(format!(
            "Unsupported time unit {} in extract function",
            time_unit
        )))),
    }
}

fn extract_date<T>(date: T, time_unit: &str) -> Result<Decimal>
where
    T: Datelike,
{
    match time_unit {
        "DAY" => Ok(date.day().into()),
        "MONTH" => Ok(date.month().into()),
        "YEAR" => Ok(date.year().into()),
        // Sun = 0 and Sat = 6
        "DOW" => Ok(date.weekday().num_days_from_sunday().into()),
        "DOY" => Ok(date.ordinal().into()),
        _ => Err(RwError::from(InternalError(format!(
            "Unsupported time unit {} in extract function",
            time_unit
        )))),
    }
}

pub fn extract_from_date(time_unit: &str, date: NaiveDateWrapper) -> Result<Decimal> {
    extract_date(date.0, time_unit)
}

pub fn extract_from_timestamp(time_unit: &str, timestamp: NaiveDateTimeWrapper) -> Result<Decimal> {
    let time = timestamp.0;
    let mut res = extract_date(time, time_unit);
    if res.is_err() {
        res = extract_time(time, time_unit);
    }
    res
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime};

    use super::*;

    #[test]
    fn test_date() {
        let date =
            NaiveDateWrapper::new(NaiveDate::parse_from_str("2021-11-22", "%Y-%m-%d").unwrap());
        assert_eq!(extract_from_date("DAY", date).unwrap(), 22.into());
        assert_eq!(extract_from_date("MONTH", date).unwrap(), 11.into());
        assert_eq!(extract_from_date("YEAR", date).unwrap(), 2021.into());
        assert_eq!(extract_from_date("DOW", date).unwrap(), 1.into());
        assert_eq!(extract_from_date("DOY", date).unwrap(), 326.into());
    }

    #[test]
    fn test_time() {
        let time = NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("2021-11-22 12:4:2", "%Y-%m-%d %H:%M:%S").unwrap(),
        );
        assert_eq!(extract_from_timestamp("HOUR", time).unwrap(), 12.into());
        assert_eq!(extract_from_timestamp("MINUTE", time).unwrap(), 4.into());
        assert_eq!(extract_from_timestamp("SECOND", time).unwrap(), 2.into());
    }
}
