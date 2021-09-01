use crate::array::interval_array::IntervalArrayBuilder;
use crate::array::BoxedArrayBuilder;
use crate::error::{Result, RwError};
use crate::types::{DataType, DataTypeKind, DataTypeRef};
use risingwave_proto::data::{DataType as DataTypeProto, DataType_IntervalType, DataType_TypeName};
use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

/// If want to define a interval holds all kinds of time interval, now the frontend Java plan's Interval Type should be marked as Interval.
#[derive(Debug)]
pub(crate) struct IntervalType {
    nullable: bool,
    // Used for precision of seconds.
    precision: u32,
    // None: the column can be hold any interval (microseconds/days/months/years)
    // Some(): If Interval Unit is set to YEAR, insert values except years/month will be recorded as 00::00::00.
    inner_type: Option<IntervalUnit>,
}

impl IntervalType {
    fn new(nullable: bool, precision: u32, inner_type: Option<IntervalUnit>) -> Self {
        IntervalType {
            nullable,
            precision,
            inner_type,
        }
    }

    pub(crate) fn create(
        nullable: bool,
        precision: u32,
        inner_type: Option<IntervalUnit>,
    ) -> DataTypeRef {
        Arc::new(Self::new(nullable, precision, inner_type)) as DataTypeRef
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum IntervalUnit {
    Year,
    Day,
    // TODO: other fileds in https://www.postgresql.org/docs/current/datatype-datetime.html
}

impl DataType for IntervalType {
    fn data_type_kind(&self) -> DataTypeKind {
        DataTypeKind::Interval
    }

    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn create_array_builder(
        self: Arc<Self>,
        capacity: usize,
    ) -> crate::error::Result<BoxedArrayBuilder> {
        match self.inner_type {
            // FIXME: change signed integer to unsigned.
            Some(IntervalUnit::Year) => IntervalArrayBuilder::new(
                Arc::new(IntervalType::new(
                    self.nullable,
                    self.precision,
                    self.inner_type,
                )),
                capacity,
            )
            .map(|elem| Box::new(elem) as BoxedArrayBuilder),
            _ => {
                todo!()
            }
        }
    }

    fn to_protobuf(&self) -> crate::error::Result<DataTypeProto> {
        let mut proto = DataTypeProto::new();
        proto.set_type_name(DataType_TypeName::INTERVAL);
        proto.set_interval_type(IntervalType::as_proto_interval_type(self.inner_type));
        proto.set_is_nullable(self.nullable);
        proto.set_precision(self.precision);
        Ok(proto)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl IntervalType {
    fn as_proto_interval_type(interval_type: Option<IntervalUnit>) -> DataType_IntervalType {
        match interval_type {
            Some(IntervalUnit::Year) => DataType_IntervalType::YEAR,
            Some(IntervalUnit::Day) => DataType_IntervalType::DAY,
            None => unimplemented!(),
        }
    }

    fn from_proto_interval_type(proto_interval: DataType_IntervalType) -> Option<IntervalUnit> {
        match proto_interval {
            DataType_IntervalType::YEAR => Some(IntervalUnit::Year),
            DataType_IntervalType::DAY => Some(IntervalUnit::Day),
            _ => unimplemented!(),
        }
    }
}

impl<'a> TryFrom<&'a DataTypeProto> for IntervalType {
    type Error = RwError;

    fn try_from(proto: &'a DataTypeProto) -> Result<Self> {
        ensure!(proto.get_type_name() == DataType_TypeName::INTERVAL);
        Ok(Self {
            nullable: proto.get_is_nullable(),
            precision: proto.get_precision(),
            inner_type: IntervalType::from_proto_interval_type(proto.get_interval_type()),
        })
    }
}
