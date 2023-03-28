// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::io::Write;

use super::{F32, F64};
use crate::array::serial_array::Serial;
use crate::array::ArrayResult;

pub trait NativeType:
    PartialOrd + PartialEq + Debug + Copy + Send + Sync + Sized + Default + 'static
{
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize>;
}

impl NativeType for i16 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write(&self.to_be_bytes()).map_err(Into::into)
    }
}

impl NativeType for i32 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write(&self.to_be_bytes()).map_err(Into::into)
    }
}

impl NativeType for i64 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write(&self.to_be_bytes()).map_err(Into::into)
    }
}

impl NativeType for Serial {
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output
            .write(&self.into_inner().to_be_bytes())
            .map_err(Into::into)
    }
}

impl NativeType for F32 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write(&self.to_be_bytes()).map_err(Into::into)
    }
}

impl NativeType for F64 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write(&self.to_be_bytes()).map_err(Into::into)
    }
}

impl NativeType for u8 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write(&self.to_be_bytes()).map_err(Into::into)
    }
}

impl NativeType for u16 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write(&self.to_be_bytes()).map_err(Into::into)
    }
}

impl NativeType for u32 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write(&self.to_be_bytes()).map_err(Into::into)
    }
}

impl NativeType for u64 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write(&self.to_be_bytes()).map_err(Into::into)
    }
}
