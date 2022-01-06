use std::any::{type_name, Any};
use std::sync::Arc;

pub use self::prost::*;
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};

pub mod addr;
pub mod bit_util;
pub mod chunk_coalesce;
pub mod encoding_for_comparison;
pub mod hash_util;
pub mod ordered;
pub mod prost;
pub mod sort_util;
#[macro_use]
pub mod try_match;

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

pub fn downcast_arc<T>(source: Arc<dyn Any + Send + Sync>) -> Result<Arc<T>>
where
    T: 'static + Send + Sync,
{
    source.downcast::<T>().map_err(|_| {
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
