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

use std::path::{Path, PathBuf};

const DEBUG: bool = false;

macro_rules! debug {
    ($($tokens: tt)*) => {
        if DEBUG {
            println!("cargo:warning={}", format!($($tokens)*))
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    println!("cargo:rerun-if-changed={}", proto_dir);

    // Only include core protobuf files that contain data structures
    let proto_files = vec![
        "common",
        "secret", 
        "expr",
        "data", 
        "plan_common",
    ];
    let protos: Vec<String> = proto_files
        .iter()
        .map(|f| format!("{}/{}.proto", proto_dir, f))
        .collect();

    // Paths to generate `BTreeMap` for protobuf maps.
    let btree_map_paths = [
        ".plan_common.ExternalTableDesc",
    ];

    // Build protobuf structs.

    // We first put generated files to `OUT_DIR`, then copy them to `/src` only if they are changed.
    // This is to avoid unexpected recompilation due to the timestamps of generated files to be
    // changed by different kinds of builds. See https://github.com/risingwavelabs/risingwave/issues/11449 for details.
    //
    // Put generated files in /src may also benefit IDEs https://github.com/risingwavelabs/risingwave/pull/2581
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR envvar is missing"));
    let file_descriptor_set_path: PathBuf = out_dir.join("file_descriptor_set.bin");

    let tonic_config = tonic_build::configure()
        .file_descriptor_set_path(file_descriptor_set_path.as_path())
        .compile_well_known_types(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute("rex_node", "#[derive(::enum_as_inner::EnumAsInner)]")
        .btree_map(btree_map_paths)
        // `Udf` is 248 bytes, while 2nd largest field is 32 bytes.
        .boxed(".expr.ExprNode.rex_node.udf");

    // If any configuration for `prost_build` is not exposed by `tonic_build`, specify it here.
    let mut prost_config = prost_build::Config::new();
    prost_config.skip_debug([
        "plan_common.ColumnDesc",
        "data.DataType",
    ]);
    // Compile the proto files.
    tonic_config
        .out_dir(out_dir.as_path())
        .compile_with_config(prost_config, &protos, &[proto_dir.to_owned()])
        .expect("Failed to compile grpc!");

    // Implement `serde::Serialize` on those structs.
    let descriptor_set = fs_err::read(file_descriptor_set_path)?;
    pbjson_build::Builder::new()
        .btree_map(btree_map_paths)
        .register_descriptors(&descriptor_set)?
        .out_dir(out_dir.as_path())
        .build(&["."])
        .expect("Failed to compile serde");

    // Tweak the serde files so that they can be compiled in our project.
    // By adding a `use crate::module::*`
    let rewrite_files = proto_files;
    for serde_proto_file in &rewrite_files {
        let out_file = out_dir.join(format!("{}.serde.rs", serde_proto_file));
        let file_content = String::from_utf8(fs_err::read(&out_file)?)?;
        let module_path_id = serde_proto_file.replace('.', "::");
        fs_err::write(
            &out_file,
            format!("use crate::{}::*;\n{}", module_path_id, file_content),
        )?;
    }

    compare_and_copy(&out_dir, &PathBuf::from("./src")).unwrap_or_else(|_| {
        panic!(
            "Failed to copy generated files from {} to ./src",
            out_dir.display()
        )
    });

    Ok(())
}

/// Copy all files from `src_dir` to `dst_dir` only if they are changed.
fn compare_and_copy(src_dir: &Path, dst_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    debug!(
        "copying files from {} to {}",
        src_dir.display(),
        dst_dir.display()
    );
    let mut updated = false;
    let t1 = std::time::Instant::now();
    for entry in walkdir::WalkDir::new(src_dir) {
        let entry = entry?;
        if entry.file_type().is_file() {
            let src_file = entry.path();
            let dst_file = dst_dir.join(src_file.strip_prefix(src_dir)?);
            if dst_file.exists() {
                let src_content = fs_err::read(src_file)?;
                let dst_content = fs_err::read(dst_file.as_path())?;
                if src_content == dst_content {
                    continue;
                }
            }
            updated = true;
            debug!("copying {} to {}", src_file.display(), dst_file.display());
            fs_err::create_dir_all(dst_file.parent().unwrap())?;
            fs_err::copy(src_file, dst_file)?;
        }
    }
    debug!(
        "Finished generating risingwave_prost in {:?}{}",
        t1.elapsed(),
        if updated { "" } else { ", no file is updated" }
    );

    Ok(())
}
