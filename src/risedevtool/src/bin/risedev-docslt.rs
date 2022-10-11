// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(path_file_prefix)]

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::Parser;
use console::style;
use itertools::Itertools;
use serde_json::Value as JsonValue;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
#[clap(infer_subcommands = true)]
pub struct RiseDevDocSltOpts {
    /// Specify the package name to extract DocSlt from.
    #[clap(short, long)]
    package: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SltBlockType {
    Setup,
    General,
    Teardown,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FilePosition {
    filepath: PathBuf,
    line_no: usize,
    item_name: String,
}

struct SltBlock {
    position: Arc<FilePosition>,
    typ: SltBlockType,
    content: String,
}

fn extract_slt_blocks(markdown: &str, position: FilePosition) -> Vec<SltBlock> {
    let position = Arc::new(position);
    let parser = pulldown_cmark::Parser::new(markdown);
    let mut slt_blocks = Vec::new();
    let mut curr_typ = None;
    let mut curr_content = String::new();
    for event in parser {
        match event {
            pulldown_cmark::Event::Start(pulldown_cmark::Tag::CodeBlock(
                pulldown_cmark::CodeBlockKind::Fenced(lang),
            )) => {
                curr_typ = Some(match &*lang {
                    "slt" => SltBlockType::General,
                    "slt-setup" => SltBlockType::Setup,
                    "slt-teardown" => SltBlockType::Teardown,
                    _ => continue,
                });
                curr_content.clear();
            }
            pulldown_cmark::Event::Text(text) => {
                if curr_typ.is_some() {
                    curr_content.push_str(&text);
                    curr_content.push('\n');
                }
            }
            pulldown_cmark::Event::End(pulldown_cmark::Tag::CodeBlock(
                pulldown_cmark::CodeBlockKind::Fenced(_),
            )) => {
                if let Some(typ) = curr_typ.take() {
                    slt_blocks.push(SltBlock {
                        position: position.clone(),
                        typ,
                        content: curr_content.trim().to_string(),
                    });
                }
            }
            _ => {}
        }
    }
    slt_blocks
}

fn generate_slt_files(package_name: &str) -> Result<()> {
    print!("Generating SLT files for package `{package_name}`...");
    std::io::stdout().flush().unwrap();

    let rustdoc_output = Command::new("cargo")
        .args([
            "rustdoc",
            "-p",
            package_name,
            "--lib",
            "--",
            "-Zunstable-options", // for json format
            "--output-format",
            "json",
            "--document-private-items",
        ])
        .output()?;
    if !rustdoc_output.status.success() {
        let stderr = String::from_utf8(rustdoc_output.stderr)?;
        if stderr.contains("no library targets found") {
            println!("{}", style("IGNORED").yellow().bold());
            return Ok(());
        } else {
            println!("{}", style("FAILED").red().bold());
            println!("{}\n{}", style("Error output:").red().bold(), stderr);
            return Err(anyhow!(
                "`cargo rustdoc` failed with exit code {}",
                rustdoc_output.status.code().unwrap()
            ));
        }
    }

    let rustdoc: JsonValue = serde_json::from_reader(std::io::BufReader::new(
        std::fs::File::open(format!("target/doc/{}.json", package_name))?,
    ))?;
    let index = rustdoc["index"]
        .as_object()
        .ok_or_else(|| anyhow!("failed to access `index` field as object"))?;

    // slt blocks in each file
    let mut slt_blocks_per_file: HashMap<String, Vec<SltBlock>> = HashMap::new();

    for item in index.values() {
        let docs = item["docs"].as_str().unwrap_or("");
        if docs.contains("```slt") {
            let filename = item["span"]["filename"]
                .as_str()
                .ok_or_else(|| anyhow!("docslt blocks are expected to be in files"))?;
            let line_no = item["span"]["begin"][0]
                .as_u64()
                .ok_or_else(|| anyhow!("docslt blocks are expected to have line number"))?
                as usize;
            let item_name = item["name"].as_str().unwrap_or("").to_string();
            let slt_blocks = extract_slt_blocks(
                docs,
                FilePosition {
                    filepath: PathBuf::from(filename),
                    line_no,
                    item_name,
                },
            );
            if slt_blocks.is_empty() {
                continue;
            }
            slt_blocks_per_file
                .entry(filename.to_string())
                .or_default()
                .extend(slt_blocks);
        }
    }

    for slt_blocks in slt_blocks_per_file.values_mut() {
        slt_blocks.sort_by_key(|block| (block.typ, block.position.line_no));
    }

    let slt_dir = PathBuf::from(format!("e2e_test/generated/docslt/{}", package_name));
    std::fs::remove_dir_all(&slt_dir).ok();
    if !slt_blocks_per_file.is_empty() {
        std::fs::create_dir_all(&slt_dir)?;
    }

    for filename in slt_blocks_per_file.keys() {
        let mut filepath = PathBuf::from(filename);
        filepath.set_extension("slt");
        let slt_filename = filepath
            .components()
            .filter_map(|comp| comp.as_os_str().to_str().filter(|s| *s != "src"))
            .join("__");
        let mut slt_file = std::fs::File::create(slt_dir.join(slt_filename))?;
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
            filename,
            chrono::Utc::now()
        )?;
        let blocks = &slt_blocks_per_file[filename];
        for block in blocks.iter() {
            write!(
                slt_file,
                "\n\
                # ==== `{}` @ L{} ====\n\
                \n\
                {}\n",
                block.position.item_name, block.position.line_no, block.content
            )?;
        }
    }

    println!("{}", style("OK").green().bold());
    Ok(())
}

fn main() -> Result<()> {
    let opts = RiseDevDocSltOpts::parse();
    let packages = if let Some(ref package) = opts.package {
        vec![package.as_ref()]
    } else {
        let default_packages = vec![
            "risingwave_common",
            "risingwave_frontend",
            "risingwave_stream",
            "risingwave_batch",
            "risingwave_expr",
        ];
        println!(
            "Extracting DocSlt for default packages: {:#?}",
            default_packages
        );
        default_packages
    };
    for package in packages {
        generate_slt_files(package)?;
    }
    Ok(())
}
