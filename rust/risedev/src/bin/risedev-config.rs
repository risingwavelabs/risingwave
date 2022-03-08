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
    Release,
}

impl Components {
    pub fn title(&self) -> String {
        match self {
            Self::MinIO => "MinIO / MinIO-CLI",
            Self::PrometheusAndGrafana => "Prometheus / Grafana",
            Self::Etcd => "Etcd",
            Self::ComputeNodeAndMetaNode => "Build compute-node / meta-node",
            Self::Frontend => "Build frontend",
            Self::Tracing => "Tracing / Jaeger",
            Self::Release => "Enable release mode",
        }
        .into()
    }

    pub fn description(&self) -> String {
        match self {
            Self::MinIO => {
                "
Required by Hummock state store.
(Also required by ./risedev dev)"
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
            Self::Tracing => {
                "
Required if you want to use tracing. This option will help
you download Jaeger."
            }
            Self::Release => {
                "
Build RisingWave in release mode"
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
            Self::Tracing => "ENABLE_COMPUTE_TRACING",
            Self::Release => "ENABLE_RELEASE_PROFILE",
        }
        .into()
    }

    pub fn default_enabled() -> &'static [Self] {
        &[
            Self::MinIO,
            Self::PrometheusAndGrafana,
            Self::ComputeNodeAndMetaNode,
            Self::Frontend,
        ]
    }
}

fn configure(chosen: &[Components]) -> Result<Vec<Components>> {
    println!("=== Configure RiseDev ===");
    println!();
    println!("RiseDev includes several components. You can select the ones you need, so as to reduce build time.");
    println!();
    println!(
        "Use {} to navigate, and use {} to select. Press {} to continue.",
        style("arrow keys").bold(),
        style("space").bold(),
        style("enter").bold()
    );
    println!();

    let all_components = Components::into_enum_iter().collect_vec();

    let items = all_components
        .iter()
        .map(|c| {
            (
                format!(
                    "{}{}",
                    c.title(),
                    style(
                        ("\n".to_string() + c.description().trim())
                            .split('\n')
                            .join("\n      ")
                    )
                    .dim()
                ),
                chosen.contains(c),
            )
        })
        .collect_vec();

    let chosen_indices: Vec<usize> = MultiSelect::new()
        .items_checked(&items)
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
    for component in chosen {
        writeln!(file, "{}=true", component.env())?;
    }

    file.flush()?;

    Ok(())
}
