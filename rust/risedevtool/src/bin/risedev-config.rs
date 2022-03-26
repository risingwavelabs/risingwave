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

#![feature(let_else)]

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};

use anyhow::{anyhow, Context, Result};
use console::style;
use dialoguer::MultiSelect;
use enum_iterator::IntoEnumIterator;
use itertools::Itertools;

#[derive(Clone, Copy, Debug, IntoEnumIterator, PartialEq)]
pub enum Components {
    MinIO,
    PrometheusAndGrafana,
    Etcd,
    Tracing,
    ComputeNodeAndMetaNode,
    Frontend,
    Dashboard,
    Release,
    AllInOne,
    Sanitizer,
}

impl Components {
    pub fn title(&self) -> String {
        match self {
            Self::MinIO => "[Component] Hummock: MinIO + MinIO-CLI",
            Self::PrometheusAndGrafana => "[Component] Metrics: Prometheus + Grafana",
            Self::Etcd => "[Component] Etcd",
            Self::ComputeNodeAndMetaNode => "[Build] Rust components",
            Self::Frontend => "[Build] Java frontend",
            Self::Dashboard => "[Build] Dashboard v2",
            Self::Tracing => "[Component] Tracing: Jaeger",
            Self::Release => "[Build] Enable release mode",
            Self::AllInOne => "[Build] Enable all-in-one binary",
            Self::Sanitizer => "[Build] Enable sanitizer",
        }
        .into()
    }

    pub fn description(&self) -> String {
        match self {
            Self::MinIO => {
                "
Required by Hummock state store."
            }
            Self::PrometheusAndGrafana => {
                "
Required if you want to view metrics."
            }
            Self::Etcd => {
                "
Required if you want to persistent meta-node data.
                "
            }
            Self::ComputeNodeAndMetaNode => {
                "
Required if you want to build compute-node and meta-node.
Otherwise you will need to manually download and copy it
to RiseDev directory."
            }
            Self::Frontend => {
                "
Required if you want to build frontend. Otherwise you will
need to manually download and copy it to RiseDev directory."
            }
            Self::Dashboard => {
                "
Required if you want to build dashboard v2 from source."
            }
            Self::Tracing => {
                "
Required if you want to use tracing. This option will help
you download Jaeger."
            }
            Self::Release => {
                "
Build RisingWave in release mode"
            }
            Self::AllInOne => {
                "
With this option enabled, RiseDev will help you create
symlinks to `risingwave` all-in-one binary, so as to build
and use `risingwave` in all-in-one mode."
            }
            Self::Sanitizer => {
                "
With this option enabled, RiseDev will build Rust components
with thread sanitizer. The built binaries will be at
`target/<arch-triple>/(debug|release)` instead of simply at
`target/debug`. RiseDev will help link binaries when starting
a dev cluster.
"
            }
        }
        .into()
    }

    pub fn from_env(env: impl AsRef<str>) -> Option<Self> {
        match env.as_ref() {
            "ENABLE_MINIO" => Some(Self::MinIO),
            "ENABLE_PROMETHEUS_GRAFANA" => Some(Self::PrometheusAndGrafana),
            "ENABLE_ETCD" => Some(Self::Etcd),
            "ENABLE_BUILD_RUST" => Some(Self::ComputeNodeAndMetaNode),
            "ENABLE_BUILD_FRONTEND" => Some(Self::Frontend),
            "ENABLE_COMPUTE_TRACING" => Some(Self::Tracing),
            "ENABLE_RELEASE_PROFILE" => Some(Self::Release),
            "ENABLE_ALL_IN_ONE" => Some(Self::AllInOne),
            "ENABLE_SANITIZER" => Some(Self::Sanitizer),
            _ => None,
        }
    }

    pub fn env(&self) -> String {
        match self {
            Self::MinIO => "ENABLE_MINIO",
            Self::PrometheusAndGrafana => "ENABLE_PROMETHEUS_GRAFANA",
            Self::Etcd => "ENABLE_ETCD",
            Self::ComputeNodeAndMetaNode => "ENABLE_BUILD_RUST",
            Self::Frontend => "ENABLE_BUILD_FRONTEND",
            Self::Dashboard => "ENABLE_BUILD_DASHBOARD_V2",
            Self::Tracing => "ENABLE_COMPUTE_TRACING",
            Self::Release => "ENABLE_RELEASE_PROFILE",
            Self::AllInOne => "ENABLE_ALL_IN_ONE",
            Self::Sanitizer => "ENABLE_SANITIZER",
        }
        .into()
    }

    pub fn default_enabled() -> &'static [Self] {
        &[Self::ComputeNodeAndMetaNode, Self::Frontend]
    }
}

fn configure(chosen: &[Components]) -> Result<Vec<Components>> {
    println!("=== Configure RiseDev ===");
    println!();
    println!("RiseDev includes several components. You can select the ones you need, so as to reduce build time.");
    println!();
    println!(
        "Use {} to navigate, use {} to go to next page, and use {} to select. Press {} to continue.",
        style("arrow up / down").bold(),
        style("arrow left / right").bold(),
        style("space").bold(),
        style("enter").bold()
    );
    println!();

    let all_components = Components::into_enum_iter().collect_vec();

    const ITEMS_PER_PAGE: usize = 6;

    let items = all_components
        .iter()
        .enumerate()
        .map(|(idx, c)| {
            let title = c.title();
            let desc = style(
                ("\n".to_string() + c.description().trim())
                    .split('\n')
                    .join("\n      "),
            )
            .dim();

            let instruction = if (idx + 1) % ITEMS_PER_PAGE == 0 || idx == all_components.len() - 1
            {
                format!(
                    "\n\n  page {}/{}, use {} to navigate",
                    style(((idx + ITEMS_PER_PAGE - 1) / ITEMS_PER_PAGE).to_string()).bold(),
                    (all_components.len() + ITEMS_PER_PAGE - 1) / ITEMS_PER_PAGE,
                    style("arrow key left / right").blue(),
                )
            } else {
                String::new()
            };

            (format!("{title}{desc}{instruction}",), chosen.contains(c))
        })
        .collect_vec();

    let chosen_indices: Vec<usize> = MultiSelect::new()
        .items_checked(&items)
        .max_length(ITEMS_PER_PAGE)
        .interact_opt()?
        .ok_or_else(|| anyhow!("no selection made"))?;

    let chosen = chosen_indices
        .into_iter()
        .map(|i| all_components[i])
        .collect_vec();

    Ok(chosen)
}

fn main() -> Result<()> {
    let file_path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("expect argv[1] to be component env file path"))?;

    let chosen = {
        if let Ok(file) = OpenOptions::new().read(true).open(&file_path) {
            let reader = BufReader::new(file);
            let mut enabled = vec![];
            for line in reader.lines() {
                let line = line?;
                let Some((component, val)) = line.split_once('=') else {
                    println!("invalid config line {}, discarded", line);
                    continue;
                };
                if component == "RISEDEV_CONFIGURED" {
                    continue;
                }
                match Components::from_env(&component) {
                    Some(component) => {
                        if val == "true" {
                            enabled.push(component);
                        }
                    }
                    None => {
                        println!("unknown configure {}, discarded", component);
                        continue;
                    }
                }
            }
            enabled
        } else {
            println!(
                "RiseDev component config not found, generating {}",
                file_path
            );
            Components::default_enabled().to_vec()
        }
    };

    let chosen = if let Some(true) = std::env::args().nth(2).map(|x| x == "--default") {
        println!("Using default config");
        Components::default_enabled().to_vec()
    } else {
        configure(&chosen)?
    };

    for component in Components::into_enum_iter() {
        println!(
            "{}: {}",
            component.title(),
            if chosen.contains(&component) {
                style("enabled").green()
            } else {
                style("disabled").dim()
            }
        );
    }
    println!();

    println!("Writing configuration into {}...", file_path);

    let mut file = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&file_path)
            .context(format!("failed to open component config at {}", file_path))?,
    );

    writeln!(file, "RISEDEV_CONFIGURED=true")?;
    writeln!(file)?;

    for component in Components::into_enum_iter() {
        writeln!(file, "# {}", component.title())?;
        writeln!(
            file,
            "# {}",
            component.description().trim().split('\n').join("\n# ")
        )?;
        if chosen.contains(&component) {
            writeln!(file, "{}=true", component.env())?;
        } else {
            writeln!(file, "# {}=true", component.env())?;
        }
        writeln!(file)?;
    }

    file.flush()?;

    Ok(())
}
