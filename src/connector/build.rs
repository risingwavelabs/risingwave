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
    let proto_dir = "./src/test_data/proto_recursive";

    println!("cargo:rerun-if-changed={}", proto_dir);

    let proto_files = ["recursive"];
    let protos: Vec<String> = proto_files
        .iter()
        .map(|f| format!("{}/{}.proto", proto_dir, f))
        .collect();
    prost_build::Config::new()
        .out_dir("./src/parser/protobuf")
        .compile_protos(&protos, &Vec::<String>::new())
        .unwrap();

    let proto_include_path = protobuf_src::include();
    println!(
        "cargo:rustc-env=PROTO_INCLUDE={}",
        proto_include_path.to_str().unwrap()
    );

    let paths: Vec<_> = glob::glob(proto_include_path.join("**/*.proto").to_str().unwrap())
        .unwrap()
        // Shall errors here be fatal or ignored?
        .filter_map(|p| {
            Some(
                p.ok()?
                    .strip_prefix(proto_include_path.join("google/protobuf"))
                    .ok()?
                    .to_owned(),
            )
        })
        .collect();
    let out_dir = std::env::var_os("OUT_DIR").unwrap();
    let dest_path = std::path::Path::new(&out_dir).join("for_all_wkts.rs");
    std::fs::write(
        dest_path,
        format!(
            r#"macro_rules! for_all_wkts {{
                ($macro:ident) => {{
                    $macro! {paths:?}
                }};
            }}"#
        ),
    )
    .unwrap();
}
