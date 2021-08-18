use std::any::type_name;
use std::any::Any;

pub(crate) use proto::*;

use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};

pub(crate) mod bit_util;
pub(crate) mod proto;

pub(crate) fn downcast_ref<S, T>(source: &S) -> Result<&T>
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

pub(crate) fn downcast_mut<S, T>(source: &mut S) -> Result<&mut T>
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
