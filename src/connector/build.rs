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

fn main() {
    // This function extracts WITH options from the Rust code and maintains
    // them in a single source of truth `with_options.yaml`.
    with_options_build::update_with_options_yaml();

    let proto_dir = "./src/test_data/proto_recursive";
    let proto_files = ["recursive"];
    let protos: Vec<String> = proto_files
        .iter()
        .map(|f| format!("{}/{}.proto", proto_dir, f))
        .collect();
    prost_build::Config::new()
        .out_dir("./src/parser/protobuf")
        .compile_protos(&protos, &Vec::<String>::new())
        .unwrap();
}
