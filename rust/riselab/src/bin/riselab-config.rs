use std::fs::OpenOptions;
use std::io::{BufWriter, Write};

use anyhow::{anyhow, Context, Result};
use console::style;
use dialoguer::MultiSelect;
use enum_iterator::IntoEnumIterator;
use itertools::Itertools;

#[derive(Clone, Debug, IntoEnumIterator)]
pub enum Components {
    MinIO,
    PrometheusAndGrafana,
    ComputeNodeAndMetaNode,
    Frontend,
}

impl Components {
    pub fn title(&self) -> String {
        match self {
            Self::MinIO => "MinIO / MinIO-CLI",
            Self::PrometheusAndGrafana => "Prometheus / Grafana",
            Self::ComputeNodeAndMetaNode => "Build compute-node / meta-node",
            Self::Frontend => "Build frontend",
        }
        .into()
    }

    pub fn description(&self) -> String {
        match self {
      Self::MinIO => "Required by Hummock state store.",
      Self::PrometheusAndGrafana => "Required if you want to view metrics.",
      Self::ComputeNodeAndMetaNode => "Required if you want to build compute-node and meta-node. Otherwise you will need to manually download and copy it to RiseLAB directory.",
      Self::Frontend => "Required if you want to build frontend. Otherwise you will need to manually download and copy it to RiseLAB directory.",
    }
    .into()
    }

    pub fn env(&self) -> String {
        match self {
            Self::MinIO => "ENABLE_MINIO",
            Self::PrometheusAndGrafana => "ENABLE_PROMETHEUS_GRAFANA",
            Self::ComputeNodeAndMetaNode => "ENABLE_BUILD_RUST",
            Self::Frontend => "ENABLE_BUILD_FRONTEND",
        }
        .into()
    }
}

fn main() -> Result<()> {
    let file_path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("expect argv[1] to be component env file path"))?;

    println!(
        "RiseLAB component config not found, generating {}",
        file_path
    );

    println!("=== Configure RiseLAB ===");
    println!();
    println!("RiseLAB includes several components. You can select the ones you need, so as to reduce build time.");
    println!();
    println!(
        "Use {} to navigate, and use {} to select. Press {} to continue.",
        style("arrow keys").bold(),
        style("space").bold(),
        style("enter").bold()
    );
    println!();

    let items = Components::into_enum_iter()
        .map(|c| {
            (
                format!("{}\n      {}", c.title(), style(c.description()).dim()),
                true,
            )
        })
        .collect_vec();

    let chosen: Vec<usize> = MultiSelect::new()
        .items_checked(&items)
        .interact_opt()?
        .ok_or_else(|| anyhow!("no selection made"))?;

    println!("Writing configuration into {}...", file_path);

    let mut file = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&file_path)
            .context(format!("failed to open component config at {}", file_path))?,
    );

    writeln!(file, "RISELAB_CONFIGURED=true")?;
    for (idx, item) in Components::into_enum_iter().enumerate() {
        if chosen.contains(&idx) {
            writeln!(file, "{}=true", item.env())?;
        }
    }

    file.flush()?;

    Ok(())
}
