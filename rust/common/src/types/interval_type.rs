use risingwave_pb::{data::data_type::TypeName, ToProto};

use crate::array::interval_array::IntervalArrayBuilder;

use super::*;

/// Every interval can be represented by a `IntervalUnit`.
/// Note that the difference between Interval and Instant.
/// For example, `5 yrs 1 month 25 days 23:22:57` is a interval (Can be interpreted by Interval Unit
/// with month = 61, days = 25, seconds = (57 + 23 * 3600 + 22 * 60) * 1000),
/// `1970-01-01 04:05:06` is a Instant or Timestamp
/// One month may contain 28/31 days. One day may contain 23/25 hours.
/// This internals is learned from PG:
/// <https://www.postgresql.org/docs/9.1/datatype-datetime.html#:~:text=field%20is%20negative.-,Internally,-interval%20values%20are>
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Hash, Default)]
pub struct IntervalUnit {
    months: i32,
    days: i32,
    ms: i64,
}

impl IntervalUnit {
    pub fn new(months: i32, days: i32, ms: i64) -> Self {
        IntervalUnit { months, days, ms }
    }
    pub fn get_days(&self) -> i32 {
        self.days
    }

    pub fn get_months(&self) -> i32 {
        self.months
    }

    pub fn get_years(&self) -> i32 {
        self.months / 12
    }

    pub fn get_ms(&self) -> i64 {
        self.ms
    }

    pub fn negative(&self) -> Self {
        IntervalUnit {
            months: -self.months,
            days: -self.days,
            ms: -self.ms,
        }
    }

    pub fn from_ymd(year: i32, month: i32, days: i32) -> Self {
        let months = year * 12 + month;
        let days = days;
        let ms = 0;
        IntervalUnit { months, days, ms }
    }

    pub fn from_month(months: i32) -> Self {
        IntervalUnit {
            months,
            days: 0,
            ms: 0,
        }
    }
    pub fn from_millis(ms: i64) -> Self {
        IntervalUnit {
            months: 0,
            days: 0,
            ms,
        }
    }
}
#[derive(Debug, Eq, PartialEq)]
pub struct IntervalType {
    nullable: bool,
}

impl DataType for IntervalType {
    fn data_type_kind(&self) -> DataTypeKind {
        DataTypeKind::Interval
    }

    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn create_array_builder(self: Arc<Self>, capacity: usize) -> Result<ArrayBuilderImpl> {
        IntervalArrayBuilder::new(capacity).map(|x| x.into())
    }

    fn to_protobuf(&self) -> Result<risingwave_proto::data::DataType> {
        self.to_prost()
            .map(|x| x.to_proto::<risingwave_proto::data::DataType>())
    }

    fn to_prost(&self) -> Result<DataTypeProto> {
        let proto = DataTypeProto {
            type_name: TypeName::Boolean as i32,
            is_nullable: self.nullable,
            ..Default::default()
        };
        Ok(proto)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_size(&self) -> DataSize {
        DataSize::Variable
    }
}

impl IntervalType {
    pub fn new(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn create(nullable: bool) -> DataTypeRef {
        Arc::new(Self::new(nullable))
    }
}

impl<'a> TryFrom<&'a DataTypeProto> for IntervalType {
    type Error = RwError;

    fn try_from(proto: &'a DataTypeProto) -> Result<Self> {
        ensure!(proto.get_type_name() == TypeName::Interval);
        Ok(IntervalType::new(proto.get_is_nullable()))
    }
}
