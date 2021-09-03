use crate::buffer::Bitmap;
use crate::buffer::Buffer;
use crate::error::Result;
use crate::types::{DataType, DataTypeRef};
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct ArrayData {
    data_type: DataTypeRef,
    cardinality: usize,
    null_count: usize,
    buffers: Vec<Buffer>,
    #[builder(default, setter(strip_option))]
    null_bitmap: Option<Bitmap>, // TODO: remove Option, since null_bitmap always exists.
}

impl ArrayData {
    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub fn data_type(&self) -> &dyn DataType {
        &*self.data_type
    }

    pub fn data_type_ref(&self) -> DataTypeRef {
        self.data_type.clone()
    }

    pub fn null_bitmap(&self) -> Option<&Bitmap> {
        self.null_bitmap.as_ref()
    }

    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    pub fn buffer_at(&self, idx: usize) -> Result<&Buffer> {
        self.check_buffer_idx(idx)?;
        // Justification:
        // We already checked idx before.
        Ok(unsafe { self.buffer_at_unchecked(idx) })
    }

    pub unsafe fn buffer_at_unchecked(&self, idx: usize) -> &Buffer {
        self.buffers.get_unchecked(idx)
    }

    pub fn is_null(&self, idx: usize) -> Result<bool> {
        self.null_bitmap
            .as_ref()
            .map(|b| b.is_set(idx).map(|v| !v))
            .unwrap_or(Ok(false))
    }

    pub unsafe fn is_null_unchecked(&self, idx: usize) -> bool {
        self.null_bitmap
            .as_ref()
            .map(|b| !b.is_set_unchecked(idx))
            .unwrap_or(false)
    }

    pub fn check_idx(&self, idx: usize) -> Result<()> {
        ensure!(idx < self.cardinality);
        Ok(())
    }

    pub fn check_buffer_idx(&self, idx: usize) -> Result<()> {
        ensure!(idx < self.buffers.len());
        Ok(())
    }
}
