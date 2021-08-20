use crate::buffer::Bitmap;
use crate::buffer::Buffer;
use crate::error::Result;
use crate::types::{DataType, DataTypeRef};
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub(crate) struct ArrayData {
    data_type: DataTypeRef,
    cardinality: usize,
    null_count: usize,
    buffers: Vec<Buffer>,
    #[builder(default, setter(strip_option))]
    null_bitmap: Option<Bitmap>, // TODO: remove Option, since null_bitmap always exists.
}

impl ArrayData {
    pub(crate) fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub(crate) fn data_type(&self) -> &dyn DataType {
        &*self.data_type
    }

    pub(crate) fn null_bitmap(&self) -> Option<&Bitmap> {
        self.null_bitmap.as_ref()
    }

    pub(crate) fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    pub(crate) fn buffer_at(&self, idx: usize) -> Result<&Buffer> {
        self.check_buffer_idx(idx)?;
        // Justification:
        // We already checked idx before.
        Ok(unsafe { self.buffer_at_unchecked(idx) })
    }

    pub(crate) unsafe fn buffer_at_unchecked(&self, idx: usize) -> &Buffer {
        self.buffers.get_unchecked(idx)
    }

    pub(crate) fn is_null(&self, idx: usize) -> Result<bool> {
        self.null_bitmap
            .as_ref()
            .map(|b| b.is_set(idx).map(|v| !v))
            .unwrap_or(Ok(false))
    }

    pub(crate) unsafe fn is_null_unchecked(&self, idx: usize) -> bool {
        self.null_bitmap
            .as_ref()
            .map(|b| !b.is_set_unchecked(idx))
            .unwrap_or(false)
    }

    pub(crate) fn check_idx(&self, idx: usize) -> Result<()> {
        ensure!(idx < self.cardinality);
        Ok(())
    }

    pub(crate) fn check_buffer_idx(&self, idx: usize) -> Result<()> {
        ensure!(idx < self.buffers.len());
        Ok(())
    }
}
