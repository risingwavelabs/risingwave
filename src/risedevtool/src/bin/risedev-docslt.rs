#![feature(path_file_prefix)]

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::Parser;
use serde_json::Value as JsonValue;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
#[clap(infer_subcommands = true)]
pub struct RiseDevDocSltOpts {
    #[clap(short, long)]
    package: String,
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

fn generate_slt_files_for_package(package_name: &str) -> Result<()> {
    println!("Generating SLT files for package `{package_name}`...");

    let rustdoc_status = Command::new("cargo")
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
        .status()?;
    if !rustdoc_status.success() {
        return Err(anyhow!(
            "`cargo rustdoc` failed with exit code {}",
            rustdoc_status.code().unwrap()
        ));
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
        let filename_digest = format!("{:x}", md5::compute(filename));
        let file_name = Path::new(filename).file_name().unwrap().to_str().unwrap();
        let file_basename = Path::new(file_name)
            .file_prefix()
            .unwrap()
            .to_str()
            .unwrap();
        let slt_filename = format!("{file_basename}_{filename_digest}.slt");
        let mut slt_file = std::fs::File::create(slt_dir.join(slt_filename))?;
        writeln!(
            slt_file,
            "# DO NOT MODIFY THIS FILE\n# This SLT file is generated from `{}`.",
            filename
        )?;
        let blocks = &slt_blocks_per_file[filename];
        for block in blocks.iter() {
            writeln!(
                slt_file,
                "\n# ==== `{}` @ L{} ====",
                block.position.item_name, block.position.line_no
            )?;
            writeln!(slt_file, "\n{}", block.content)?;
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let opts = RiseDevDocSltOpts::parse();
    // TODO: tmp
    // let packages = if opts.package == "all" {
    //     let metadata_output = Command::new("cargo").arg("metadata").output()?;
    //     if !metadata_output.status.success() {
    //         return Err(anyhow!(
    //             "cargo metadata failed with exit code {}",
    //             metadata_output.status.code().unwrap()
    //         ));
    //     }
    //     let metadata_json = String::from_utf8(metadata_output.stdout)?;
    //     let metadata: JsonValue = serde_json::from_str(&metadata_json)?;
    //     let workspace_members = metadata["workspace_members"]
    //         .as_array()
    //         .ok_or_else(|| anyhow!("cargo metadata output did not contain
    // `workspace_members`"))?;     workspace_members
    //         .iter()
    //         .map(|member| {
    //             let member_info = member.as_str().unwrap();
    //             member_info.split_once(" ").unwrap().0.to_string()
    //         })
    //         .filter(|package_name| package_name.starts_with("risingwave_"))
    //         .collect_vec()
    // } else {
    //     vec![opts.package.clone()]
    // };
    let packages = vec![opts.package.clone()];
    for package_name in packages {
        generate_slt_files_for_package(&package_name)?;
    }
    Ok(())
}
