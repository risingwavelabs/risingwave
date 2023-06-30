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
use std::path::{Path, PathBuf};

use anyhow::Result;
use itertools::Itertools;
use tracing::*;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Position {
    filepath: PathBuf,
    line_no: usize,
}

struct SltBlock {
    position: Position,
    content: String,
}

/// Extracts all `slt` code blocks from a file.
fn extract_slt(filepath: &Path) -> Vec<SltBlock> {
    let content = std::fs::read_to_string(filepath).unwrap();

    let mut blocks = vec![];
    let mut iter = content.lines().enumerate();
    'block: while let Some((i, line)) = iter.next() {
        if !line.trim_end().ends_with("```slt") {
            continue;
        }
        let mut content = String::new();
        loop {
            let Some((i, mut line)) = iter.next() else {
                error!("unexpected end of file at {}", filepath.display());
                break 'block;
            };
            line = line.trim();
            // skip empty lines
            if line.is_empty() {
                continue;
            }
            if !(line.starts_with("///") || line.starts_with("//!")) {
                error!("expect /// or //! at {}:{}", filepath.display(), i + 1);
                continue 'block;
            }
            line = line[3..].trim();
            if line == "```" {
                break;
            }
            content += line;
            content += "\n";
        }
        blocks.push(SltBlock {
            position: Position {
                filepath: filepath.into(),
                line_no: i + 1,
            },
            content,
        });
    }
    blocks
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .without_time()
        .with_target(false)
        .with_level(true)
        .init();

    // output directory
    let slt_dir = PathBuf::from("e2e_test/generated/docslt");
    fs_err::remove_dir_all(&slt_dir).ok();
    fs_err::create_dir_all(&slt_dir)?;

    for entry in glob::glob("src/**/*.rs")? {
        let path = match entry {
            Ok(path) => path,
            Err(e) => {
                error!("{:?}", e);
                continue;
            }
        };
        let blocks = extract_slt(&path);
        if blocks.is_empty() {
            continue;
        }
        info!("found {} blocks at {}", blocks.len(), path.display());

        let slt_filename = path
            .components()
            .map(|comp| comp.as_os_str().to_str().unwrap())
            .filter(|name| *name != "src")
            .join("__")
            .replace(".rs", ".slt");
        let mut slt_file = fs_err::File::create(slt_dir.join(slt_filename))?;
        write!(
            slt_file,
            "\
            # DO NOT MODIFY THIS FILE\n\
            # This file is generated from `{}` at {}.\n\
            \n\
            statement ok\n\
            set RW_IMPLICIT_FLUSH to true;\n\
            \n\
            statement ok\n\
            set CREATE_COMPACTION_GROUP_FOR_MV to true;\n",
            path.display(),
            chrono::Utc::now()
        )?;
        for block in blocks {
            write!(
                slt_file,
                "\n\
                # ==== `slt` @ L{} ====\n\
                \n\
                {}\n",
                block.position.line_no, block.content
            )?;
        }
    }
    Ok(())
}
