#![feature(path_file_prefix)]

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::process::Command;

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
enum DocSltBlockType {
    Setup,
    General,
    Teardown,
}

struct DocSltBlock {
    typ: DocSltBlockType,
    content: String,
}

fn extract_docslt_blocks(markdown: &str) -> Vec<DocSltBlock> {
    let parser = pulldown_cmark::Parser::new(markdown);
    let mut docslt_blocks = Vec::new();
    let mut curr_docslt_typ = None;
    let mut curr_docslt = String::new();
    for event in parser {
        match event {
            pulldown_cmark::Event::Start(pulldown_cmark::Tag::CodeBlock(
                pulldown_cmark::CodeBlockKind::Fenced(lang),
            )) => {
                curr_docslt_typ = Some(match &*lang {
                    "slt" => DocSltBlockType::General,
                    "slt-setup" => DocSltBlockType::Setup,
                    "slt-teardown" => DocSltBlockType::Teardown,
                    _ => continue,
                });
                curr_docslt.clear();
            }
            pulldown_cmark::Event::Text(text) => {
                if curr_docslt_typ.is_some() {
                    curr_docslt.push_str(&text);
                    curr_docslt.push('\n');
                }
            }
            pulldown_cmark::Event::End(pulldown_cmark::Tag::CodeBlock(
                pulldown_cmark::CodeBlockKind::Fenced(_),
            )) => {
                if let Some(typ) = curr_docslt_typ.take() {
                    docslt_blocks.push(DocSltBlock {
                        typ,
                        content: curr_docslt.trim().to_string(),
                    });
                }
            }
            _ => {}
        }
    }
    docslt_blocks
}

fn generate_slt_for_package(package_name: &str) -> Result<()> {
    println!("Generating SQL logic tests for package `{package_name}`...");

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

    // docslt blocks in each file
    let mut docslt_map: HashMap<String, Vec<DocSltBlock>> = HashMap::new();

    for item in index.values() {
        let docs = item["docs"].as_str().unwrap_or("");
        if docs.contains("```slt") {
            let docslt_blocks = extract_docslt_blocks(docs);
            if docslt_blocks.is_empty() {
                continue;
            }
            // TODO: tmp
            // let id = item["id"]
            //     .as_str()
            //     .ok_or_else(|| anyhow!("failed to access `id` field as string"))?;
            // let name = item["name"].as_str().unwrap_or("");
            let filename = item["span"]["filename"]
                .as_str()
                .ok_or_else(|| anyhow!("docslt blocks are expected to be in files"))?;
            docslt_map
                .entry(filename.to_string())
                .or_default()
                .extend(docslt_blocks);
        }
    }

    for docslt_blocks in docslt_map.values_mut() {
        docslt_blocks.sort_by_key(|block| block.typ);
    }

    let slt_dir = format!("e2e_test/generated/docslt/{}", package_name);
    std::fs::remove_dir_all(&slt_dir).ok();
    if !docslt_map.is_empty() {
        std::fs::create_dir_all(&slt_dir)?;
    }

    for filename in docslt_map.keys() {
        let filename_digest = format!("{:x}", md5::compute(filename));
        let file_name = Path::new(filename).file_name().unwrap().to_str().unwrap();
        let file_basename = Path::new(file_name)
            .file_prefix()
            .unwrap()
            .to_str()
            .unwrap();
        let slt_filename = format!("{file_basename}_{filename_digest}.slt");
        let mut slt_file = std::fs::File::create(format!("{slt_dir}/{slt_filename}"))?;
        writeln!(slt_file, "# file: {}", filename)?;
        for docslt in docslt_map[filename].iter() {
            writeln!(slt_file, "\n{}", docslt.content)?;
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
        generate_slt_for_package(&package_name)?;
    }
    Ok(())
}
