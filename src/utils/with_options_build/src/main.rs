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

use std::io::Write;

use with_options_build::{connector_crate_path, update_with_options_yaml};

fn main() {
    let yaml_str = update_with_options_yaml();
    let mut file =
        std::fs::File::create(connector_crate_path().unwrap().join("with_options.yaml")).unwrap();
    file.write_all(yaml_str.as_bytes()).unwrap();
}
