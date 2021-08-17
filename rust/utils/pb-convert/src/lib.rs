//! This create provides trait definition of `FromProtobuf` and `IntoProtobuf`.
pub trait FromProtobuf<PbType>: Sized {
    fn from_protobuf(pb: PbType) -> anyhow::Result<Self>;
}

pub trait IntoProtobuf<PbType> {
    fn into_protobuf(self) -> anyhow::Result<PbType>;
}

impl<S, T> FromProtobuf<S> for T
where
    T: From<S>,
{
    fn from_protobuf(pb: S) -> anyhow::Result<Self> {
        Ok(T::from(pb))
    }
}

impl<S, T> IntoProtobuf<T> for S
where
    S: Into<T>,
{
    fn into_protobuf(self) -> anyhow::Result<T> {
        Ok(self.into())
    }
}
