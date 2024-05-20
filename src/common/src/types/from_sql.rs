// Copyright 2024 RisingWave Labs
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

use postgres_types::{to_sql_checked, FromSql, IsNull, Kind, Type};

use crate::types::{ScalarImpl, ScalarRefImpl};

impl<'a> FromSql<'a> for ScalarRefImpl<'_> {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(match ty.kind() {
            Kind::Simple => match *ty {
                Type::BOOL => ScalarImpl::from(bool::from_sql(ty, raw)?),
                Type::INT2 => ScalarImpl::from(i16::from_sql(ty, raw)?),
                Type::INT4 => ScalarImpl::from(i32::from_sql(ty, raw)?),
                Type::INT8 => ScalarImpl::from(i64::from_sql(ty, raw)?),
                Type::FLOAT4 => ScalarImpl::from(f32::from_sql(ty, raw)?).as_scalar_ref_impl(),
                Type::FLOAT8 => ScalarImpl::from(f64::from_sql(ty, raw)?).as_scalar_ref_impl(),
                _ => bail_not_implemented!("the postgres decoding for {ty} is unsupported"),
            },
            _ => bail_not_implemented!("the postgres decoding for {ty} is unsupported"),
        })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::TIMESTAMPTZ)
    }
}
