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

use std::path::{Path, PathBuf};

use itertools::Itertools;

macro_rules! embed_wkts {
    [$( $path:literal ),+ $(,)?] => {
        &[$(
            (
                concat!("google/protobuf/", $path),
                include_bytes!(concat!(env!("PROTO_INCLUDE"), "/google/protobuf/", $path)).as_slice(),
            )
        ),+]
    };
}
const WELL_KNOWN_TYPES: &[(&str, &[u8])] = embed_wkts![
    "any.proto",
    "api.proto",
    "compiler/plugin.proto",
    "descriptor.proto",
    "duration.proto",
    "empty.proto",
    "field_mask.proto",
    "source_context.proto",
    "struct.proto",
    "timestamp.proto",
    "type.proto",
    "wrappers.proto",
];

#[derive(Debug, thiserror::Error)]
pub enum PbCompileError {
    #[error("build_file_descriptor_set failed\n{}", errs.iter().map(|e| format!("\t{e}")).join("\n"))]
    Build {
        errs: Vec<protobuf_native::compiler::FileLoadError>,
    },
    #[error("serialize descriptor set failed")]
    Serialize,
}

pub fn compile_pb(
    main_file: (PathBuf, Vec<u8>),
    dependencies: impl IntoIterator<Item = (PathBuf, Vec<u8>)>,
) -> Result<Vec<u8>, PbCompileError> {
    use protobuf_native::compiler::{
        SimpleErrorCollector, SourceTreeDescriptorDatabase, VirtualSourceTree,
    };
    use protobuf_native::MessageLite;

    let root = main_file.0.clone();

    let mut source_tree = VirtualSourceTree::new();
    for (path, bytes) in std::iter::once(main_file).chain(dependencies.into_iter()) {
        source_tree.as_mut().add_file(&path, bytes);
    }
    for (path, bytes) in WELL_KNOWN_TYPES {
        source_tree
            .as_mut()
            .add_file(Path::new(path), bytes.to_vec());
    }

    let mut error_collector = SimpleErrorCollector::new();
    // `db` needs to be dropped before we can iterate on `error_collector`.
    let fds = {
        let mut db = SourceTreeDescriptorDatabase::new(source_tree.as_mut());
        db.as_mut().record_errors_to(error_collector.as_mut());
        db.as_mut().build_file_descriptor_set(&[root])
    }
    .map_err(|_| PbCompileError::Build {
        errs: error_collector.as_mut().collect(),
    })?;
    fds.serialize().map_err(|_| PbCompileError::Serialize)
}
