use crate::array::array_data::ArrayData;
use crate::array::{Array, ArrayBuilder, ArrayRef};
use crate::buffer::{Bitmap, Buffer};
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::expr::Datum;
use crate::types::{DataType, DataTypeKind, DataTypeRef};
use crate::util::downcast_ref;
use protobuf::well_known_types::Any as AnyProto;
use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

pub(crate) struct IntervalArrayBuilder {
    data_type: DataTypeRef,
    month_buffer: Vec<i32>,
    day_buffer: Vec<i32>,
    // microseconds
    ms_buffer: Vec<i64>,
    null_bitmap_buffer: Vec<bool>,
    null_count: usize,
}

impl AsRef<dyn Any> for IntervalArrayBuilder {
    fn as_ref(&self) -> &dyn Any {
        self
    }
}

impl AsMut<dyn Any> for IntervalArrayBuilder {
    fn as_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl ArrayBuilder for IntervalArrayBuilder {
    fn append(&mut self, datum: &Datum) -> Result<()> {
        match datum {
            Datum::Interval(interval_val) => self.append_interval(Some(interval_val)),
            _ => Err(InternalError(format!("Incorrect datum for interval: {:?}", datum)).into()),
        }
    }

    fn append_array(&mut self, source: &dyn Array) -> Result<()> {
        let input: &IntervalArray = downcast_ref(source)?;
        for v in input.as_iter()? {
            self.append_interval(v.as_ref())?;
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<ArrayRef> {
        // Result array should contains 3 buffers.
        let cardinality = self.null_bitmap_buffer.len() - 1;
        let months_buffer = Buffer::from_slice(self.month_buffer)?;
        let days_buffer = Buffer::from_slice(self.day_buffer)?;
        let ms_buffer = Buffer::from_slice(self.ms_buffer)?;
        let null_bitmap = Bitmap::from_vec(self.null_bitmap_buffer)?;
        let array_data = ArrayData::builder()
            .data_type(self.data_type)
            .cardinality(cardinality)
            .null_count(self.null_count)
            .buffers(vec![months_buffer, days_buffer, ms_buffer])
            .null_bitmap(null_bitmap)
            .build();

        IntervalArray::try_from(array_data).map(|arr| Arc::new(arr) as ArrayRef)
    }
}

impl IntervalArrayBuilder {
    pub(crate) fn new(data_type: DataTypeRef, capacity: usize) -> Result<Self> {
        Ok(Self {
            data_type,
            month_buffer: Vec::with_capacity(capacity),
            day_buffer: Vec::with_capacity(capacity),
            ms_buffer: Vec::with_capacity(capacity),
            null_bitmap_buffer: Vec::with_capacity(capacity),
            null_count: 0,
        })
    }

    fn append_interval(&mut self, value: Option<&IntervalValue>) -> Result<()> {
        match value {
            Some(v) => {
                self.month_buffer.push(v.get_months());
                self.day_buffer.push(v.get_days());
                self.ms_buffer.push(v.get_ms());
                self.null_bitmap_buffer.push(true);
                Ok(())
            }

            None => {
                self.month_buffer.push(0);
                self.day_buffer.push(0);
                self.ms_buffer.push(0);
                self.null_bitmap_buffer.push(false);
                self.null_count += 1;
                Ok(())
            }
        }
    }
}

/// The layout of Interval Array:
/// 3 data buffers: one for number of month, one for number of days, one for number of microseconds.
pub(crate) struct IntervalArray {
    data: ArrayData,
}

impl AsRef<dyn Any> for IntervalArray {
    fn as_ref(&self) -> &dyn Any {
        self
    }
}

impl AsMut<dyn Any> for IntervalArray {
    fn as_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl TryFrom<ArrayData> for IntervalArray {
    type Error = RwError;

    fn try_from(value: ArrayData) -> Result<Self> {
        IntervalArray::new(value)
    }
}

impl IntervalArray {
    fn new(data: ArrayData) -> Result<Self> {
        ensure!(data.data_type().data_type_kind() == DataTypeKind::Interval);
        ensure!(data.buffers().len() == 3);

        Ok(Self { data })
    }

    fn as_iter(&self) -> Result<impl Iterator<Item = Option<IntervalValue>> + '_> {
        IntervalArrayIter::new(self)
    }

    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<IntervalValue> {
        if self.is_null_unchecked(idx) {
            None
        } else {
            let month: i32 = self.data.buffer_at_unchecked(0).typed_data()[idx];
            let days = self.data.buffer_at_unchecked(1).typed_data()[idx];
            let ms = self.data.buffer_at_unchecked(2).typed_data()[idx];
            Some(IntervalValue { month, days, ms })
        }
    }
}

impl Array for IntervalArray {
    fn data_type(&self) -> &dyn DataType {
        self.data.data_type()
    }

    fn data_type_ref(&self) -> DataTypeRef {
        self.data.data_type_ref()
    }

    fn array_data(&self) -> &ArrayData {
        &self.data
    }

    fn len(&self) -> usize {
        self.data.cardinality()
    }

    fn to_protobuf(&self) -> Result<AnyProto> {
        todo!()
    }
}

// Every interval value can be represented by a Interval Value.
#[derive(Debug, Clone)]
pub(crate) struct IntervalValue {
    month: i32,
    days: i32,
    ms: i64,
}

impl IntervalValue {
    fn get_days(&self) -> i32 {
        self.days
    }

    fn get_months(&self) -> i32 {
        self.month
    }

    fn get_years(&self) -> i32 {
        self.month / 12
    }

    fn get_ms(&self) -> i64 {
        self.ms
    }

    fn from_ymd(year: i32, month: i32, days: i32) -> Self {
        let month = year * 12 + month;
        let days = days;
        let ms = 0;
        IntervalValue { month, days, ms }
    }
}

struct IntervalArrayIter<'a> {
    array: &'a IntervalArray,
    cur_pos: usize,
    end_pos: usize,
}

impl<'a> IntervalArrayIter<'a> {
    fn new(array: &'a IntervalArray) -> Result<Self> {
        Ok(Self {
            array,
            cur_pos: 0,
            end_pos: array.len(),
        })
    }
}

impl Iterator for IntervalArrayIter<'_> {
    type Item = Option<IntervalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pos >= self.end_pos {
            None
        } else {
            let old_pos = self.cur_pos;
            self.cur_pos += 1;

            // Justification
            // We've already checked pos.
            unsafe { Some(self.array.value_at_unchecked(old_pos)) }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::array::interval_array::{IntervalArray, IntervalValue};
    use crate::expr::Datum;
    use crate::types::interval_type::IntervalType;
    use crate::types::interval_type::IntervalUnit;
    use crate::types::DataType;
    use crate::util::downcast_ref;

    #[test]
    fn test_interval_array() {
        let data_type = IntervalType::create(false, 0, Some(IntervalUnit::Year));
        let cardinality = 5;
        let mut array_builder =
            DataType::create_array_builder(data_type.clone(), cardinality).unwrap();
        for _ in 0..cardinality {
            let v = IntervalValue::from_ymd(1, 0, 0);
            array_builder.append(&Datum::Interval(v)).unwrap();
        }
        let ret_arr = array_builder.finish().unwrap();
        let casted_arr: &IntervalArray = downcast_ref(ret_arr.as_ref()).unwrap();
        casted_arr
            .as_iter()
            .map(|mut elem| {
                if let Some(next) = elem.next() {
                    if let Some(v) = next {
                        assert_eq!(v.get_years(), 1);
                        assert_eq!(v.get_months(), 12);
                        assert_eq!(v.get_days(), 0);
                    };
                };
            })
            .unwrap();
    }
}
