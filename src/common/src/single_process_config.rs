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

//! This module defines default configurations for single node mode.

use std::sync::LazyLock;

use home::home_dir;

pub static DEFAULT_DATA_DIRECTORY: LazyLock<String> = LazyLock::new(|| {
    let mut home_path = home_dir().unwrap();
    home_path.push(".risingwave");
    let home_path = home_path.to_str().unwrap();
    home_path.to_string()
});

pub static DEFAULT_SINGLE_NODE_SQLITE_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{}/single_node.db", DEFAULT_DATA_DIRECTORY.clone()));
