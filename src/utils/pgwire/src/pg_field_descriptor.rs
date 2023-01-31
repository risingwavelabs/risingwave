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

#[derive(Debug, Clone)]
pub struct PgFieldDescriptor {
    name: String,
    table_oid: i32,
    col_attr_num: i16,

    // NOTE: Static code for data type. To see the oid of a specific type in Postgres,
    // use the following command:
    //   SELECT oid FROM pg_type WHERE typname = 'int4';
    type_oid: i32,

    type_len: i16,
    type_modifier: i32,
    format_code: i16,
}

impl PgFieldDescriptor {
    pub fn new(name: String, type_oid: i32, type_len: i16) -> Self {
        let type_modifier = -1;
        let format_code = 0;
        let table_oid = 0;
        let col_attr_num = 0;

        Self {
            name,
            table_oid,
            col_attr_num,
            type_oid,
            type_len,
            type_modifier,
            format_code,
        }
    }

    /// Set the format code as binary format.
    /// NOTE: Format code is text format by default.
    pub fn set_to_binary(&mut self) {
        self.format_code = 1;
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_table_oid(&self) -> i32 {
        self.table_oid
    }

    pub fn get_col_attr_num(&self) -> i16 {
        self.col_attr_num
    }

    pub fn get_type_oid(&self) -> i32 {
        self.type_oid
    }

    pub fn get_type_len(&self) -> i16 {
        self.type_len
    }

    pub fn get_type_modifier(&self) -> i32 {
        self.type_modifier
    }

    pub fn get_format_code(&self) -> i16 {
        self.format_code
    }
}
