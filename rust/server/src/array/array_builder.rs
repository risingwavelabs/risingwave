use crate::array::{Array, ArrayRef};
use crate::error::Result;
use crate::expr::Datum;
use std::any::Any;

pub trait ArrayBuilder: AsRef<dyn Any> + AsMut<dyn Any> {
    fn append(&mut self, datum: &Datum) -> Result<()>;
    fn append_array(&mut self, source: &dyn Array) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<ArrayRef>;
}

pub type BoxedArrayBuilder = Box<dyn ArrayBuilder>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::array::{ArrayBuilder, PrimitiveArrayBuilder};
    use crate::error::Result;
    use crate::expr::Datum;
    use crate::types::Int32Type;

    #[test]
    fn test_array_builder_i32() -> Result<()> {
        let mut i32_builder = Box::new(PrimitiveArrayBuilder::<Int32Type>::new(
            Arc::new(Int32Type::new(false)),
            1,
        ));
        i32_builder.append(&Datum::Int32(1))?;
        let arr = i32_builder.finish()?;
        assert_eq!(arr.len(), 1);
        Ok(())
    }

    #[test]
    fn test_array_null_bitmap() -> Result<()> {
        let mut i32_builder = Box::new(PrimitiveArrayBuilder::<Int32Type>::new(
            Arc::new(Int32Type::new(false)),
            5,
        ));
        for i in 0..5 {
            i32_builder.append(&Datum::Int32(i))?;
        }
        let arr = i32_builder.finish()?;
        assert_eq!(arr.len(), 5);
        let bm = arr.array_data().null_bitmap().unwrap();
        for b in bm.iter() {
            assert_eq!(b, true);
        }
        Ok(())
    }
}
