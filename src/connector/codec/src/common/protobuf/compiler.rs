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

use std::collections::HashMap;

use prost_types::FileDescriptorSet;
use protox::Error;
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};

// name -> content
pub fn compile_pb(
    main_file: (String, String),
    dependencies: impl IntoIterator<Item = (String, String)>,
) -> Result<FileDescriptorSet, Error> {
    struct MyResolver {
        map: HashMap<String, String>,
    }

    impl MyResolver {
        fn new(
            main_file: (String, String),
            dependencies: impl IntoIterator<Item = (String, String)>,
        ) -> Self {
            let map = std::iter::once(main_file).chain(dependencies).collect();

            Self { map }
        }
    }

    impl FileResolver for MyResolver {
        fn open_file(&self, name: &str) -> Result<File, Error> {
            if let Some(content) = self.map.get(name) {
                Ok(File::from_source(name, content)?)
            } else {
                Err(Error::file_not_found(name))
            }
        }
    }

    let main_file_name = main_file.0.clone();

    let mut resolver = ChainFileResolver::new();
    resolver.add(GoogleFileResolver::new());
    resolver.add(MyResolver::new(main_file, dependencies));

    let fd = protox::Compiler::with_file_resolver(resolver)
        .include_imports(true)
        .open_file(&main_file_name)?
        .file_descriptor_set();

    Ok(fd)
}
