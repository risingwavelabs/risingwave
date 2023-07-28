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

use itertools::Itertools;
use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::user::grant_privilege::Object;

use crate::catalog::system_catalog::{get_acl_items, BuiltinTable, SysCatalogReaderImpl};

pub const RW_DATABASES: BuiltinTable = BuiltinTable {
    name: "rw_databases",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "id"),
        (DataType::Varchar, "name"),
        (DataType::Int32, "owner"),
        (DataType::Varchar, "acl"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub fn read_rw_database_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();

        Ok(reader
            .iter_databases()
            .map(|db| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(db.id() as i32)),
                    Some(ScalarImpl::Utf8(db.name().into())),
                    Some(ScalarImpl::Int32(db.owner() as i32)),
                    Some(ScalarImpl::Utf8(
                        get_acl_items(&Object::DatabaseId(db.id()), &users, username_map).into(),
                    )),
                ])
            })
            .collect_vec())
    }
}
