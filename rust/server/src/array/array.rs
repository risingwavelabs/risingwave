use crate::array::array_data::ArrayData;
use crate::types::DataType;
use std::sync::Arc;

pub(crate) trait Array {
    fn data_type(&self) -> &dyn DataType;
    fn array_data(&self) -> &ArrayData;
    fn len(&self) -> usize {
        self.array_data().cardinality()
    }
}

pub(crate) type ArrayRef = Arc<dyn Array>;
