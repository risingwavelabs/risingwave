// Copyright 2025 RisingWave Labs
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

use risingwave_common::types::{DataType, DatumCow, ScalarRefImpl, ToDatumRef};

use super::{Access, ChangeEvent};
use crate::parser::unified::ChangeEventOperation;
use crate::source::SourceColumnDesc;

pub const MAXWELL_INSERT_OP: &str = "insert";
pub const MAXWELL_UPDATE_OP: &str = "update";
pub const MAXWELL_DELETE_OP: &str = "delete";

pub struct MaxwellChangeEvent<A>(A);

impl<A> MaxwellChangeEvent<A> {
    pub fn new(accessor: A) -> Self {
        Self(accessor)
    }
}

impl<A> ChangeEvent for MaxwellChangeEvent<A>
where
    A: Access,
{
    fn op(&self) -> std::result::Result<super::ChangeEventOperation, super::AccessError> {
        const OP: &str = "type";
        if let Some(ScalarRefImpl::Utf8(op)) =
            self.0.access(&[OP], &DataType::Varchar)?.to_datum_ref()
        {
            match op {
                MAXWELL_INSERT_OP | MAXWELL_UPDATE_OP => return Ok(ChangeEventOperation::Upsert),
                MAXWELL_DELETE_OP => return Ok(ChangeEventOperation::Delete),
                _ => (),
            }
        }
        Err(super::AccessError::Undefined {
            name: "op".into(),
            path: Default::default(),
        })
    }

    fn access_field(&self, desc: &SourceColumnDesc) -> super::AccessResult<DatumCow<'_>> {
        const DATA: &str = "data";
        self.0.access(&[DATA, &desc.name], &desc.data_type)
    }
}
