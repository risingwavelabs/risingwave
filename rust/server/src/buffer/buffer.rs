use std::ops::BitAnd;
use std::ops::BitOr;
use std::ops::Not;
use std::ptr::NonNull;
use std::slice::from_raw_parts;
use std::slice::from_raw_parts_mut;

use crate::alloc::alloc_aligned;
use crate::alloc::free_aligned;
use crate::error::ErrorCode;
use crate::error::Result;

#[derive(Debug)]
pub(crate) struct Buffer {
  ptr: NonNull<u8>,
  len: usize,
}

impl Drop for Buffer {
  fn drop(&mut self) {
    free_aligned(self.len, &self.ptr)
  }
}

impl Buffer {
  pub(crate) fn new(size: usize) -> Result<Buffer> {
    alloc_aligned(size).map(|ptr| Buffer { ptr, len: size })
  }

  pub(crate) fn as_slice_mut(&mut self) -> &mut [u8] {
    unsafe { from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
  }

  pub(crate) fn as_slice(&self) -> &[u8] {
    unsafe { from_raw_parts(self.ptr.as_ptr(), self.len) }
  }

  pub(crate) fn len(&self) -> usize {
    self.len
  }

  pub(crate) fn capacity(&self) -> usize {
    self.len
  }

  pub(crate) fn is_empty(&self) -> bool {
    self.len == 0
  }

  pub(crate) fn as_ptr(&self) -> *const u8 {
    self.ptr.as_ptr()
  }

  pub(crate) fn try_from<T: AsRef<[u8]>>(src: T) -> Result<Self> {
    let mut buffer = Buffer::new(src.as_ref().len())?;
    let to_slice = buffer.as_slice_mut();
    to_slice.copy_from_slice(src.as_ref());
    Ok(buffer)
  }

  fn buffer_bin_op<F>(left: &Buffer, right: &Buffer, op: F) -> Result<Buffer>
  where
    F: Fn(u8, u8) -> u8,
  {
    ensure!(left.len() == right.len());
    let ret: Vec<u8> = left
      .as_slice()
      .iter()
      .zip(right.as_slice())
      .map(|a| op(*a.0, *a.1))
      .collect();

    Buffer::try_from(ret)
  }

  fn unary_op<F>(mut self, op: F) -> Buffer
  where
    F: Fn(u8) -> u8,
  {
    self.as_slice_mut().iter_mut().for_each(|b| *b = op(*b));

    self
  }
}

impl<'a, 'b> BitAnd<&'b Buffer> for &'a Buffer {
  type Output = Result<Buffer>;

  fn bitand(self, rhs: &'b Buffer) -> Result<Buffer> {
    if self.len() != rhs.len() {
      return Err(
        ErrorCode::InternalError("Buffers must be the same size to apply Bitwise AND.".to_string())
          .into(),
      );
    }

    Buffer::buffer_bin_op(self, rhs, |a, b| a & b)
  }
}

impl<'a, 'b> BitOr<&'b Buffer> for &'a Buffer {
  type Output = Result<Buffer>;

  fn bitor(self, rhs: &'b Buffer) -> Result<Buffer> {
    if self.len() != rhs.len() {
      return Err(
        ErrorCode::InternalError("Buffers must be the same size to apply Bitwise OR.".to_string())
          .into(),
      );
    }

    Buffer::buffer_bin_op(self, rhs, |a, b| a | b)
  }
}

impl Not for Buffer {
  type Output = Buffer;

  fn not(self) -> Buffer {
    self.unary_op(|a| !a)
  }
}
