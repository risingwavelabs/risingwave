use std::any::type_name;
use std::any::Any;

pub use proto::*;

use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};

pub mod bit_util;

#[macro_use]
pub mod proto;

pub fn downcast_ref<S, T>(source: &S) -> Result<&T>
where
    S: AsRef<dyn Any> + ?Sized,
    T: 'static,
{
    source.as_ref().downcast_ref::<T>().ok_or_else(|| {
        RwError::from(InternalError(format!(
            "Failed to cast to {}",
            type_name::<T>()
        )))
    })
}

pub fn downcast_mut<S, T>(source: &mut S) -> Result<&mut T>
where
    S: AsMut<dyn Any> + ?Sized,
    T: 'static,
{
    source.as_mut().downcast_mut::<T>().ok_or_else(|| {
        RwError::from(InternalError(format!(
            "Failed to cast to {}",
            type_name::<T>()
        )))
    })
}
