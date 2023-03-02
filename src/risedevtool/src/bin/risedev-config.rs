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

#![allow(clippy::needless_question_mark)]

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use console::style;
use dialoguer::MultiSelect;
use enum_iterator::{all, Sequence};
use itertools::Itertools;
use risedev::RISEDEV_CONFIG_FILE;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
#[clap(infer_subcommands = true)]
pub struct RiseDevConfigOpts {
    #[clap(subcommand)]
    command: Option<Commands>,
    #[clap(short, long)]
    file: String,
}

#[derive(Subcommand)]
#[clap(infer_subcommands = true)]
enum Commands {
    /// Enable one component
    Enable {
        /// Component to enable
        #[clap(value_enum)]
        component: Components,
    },
    /// Disable one component
    Disable {
        /// Component to disable
        #[clap(value_enum)]
        component: Components,
    },
    /// Use default configuration
    Default,
}

#[derive(Clone, Copy, Debug, Sequence, PartialEq, Eq, ValueEnum)]
pub enum Components {
    #[clap(name = "minio")]
    Minio,
    Hdfs,
    PrometheusAndGrafana,
    Etcd,
    Kafka,
    Pubsub,
    Redis,
    ConnectorNode,
    Tracing,
    RustComponents,
    Dashboard,
    Release,
    AllInOne,
    Sanitizer,
}

impl Components {
    pub fn title(&self) -> String {
        match self {
            Self::Minio => "[Component] Hummock: MinIO + MinIO-CLI",
            Self::Hdfs => "[Component] Hummock: Hdfs Backend",
            Self::PrometheusAndGrafana => "[Component] Metrics: Prometheus + Grafana",
            Self::Etcd => "[Component] Etcd",
            Self::Kafka => "[Component] Kafka",
            Self::Pubsub => "[Component] Google Pubsub",
            Self::Redis => "[Component] Redis",
            Self::ConnectorNode => "[Component] RisingWave Connector",
            Self::RustComponents => "[Build] Rust components",
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
            Self::Minio => {
                "
Required by Hummock state store."
            }
            Self::Hdfs => {
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
            Self::Kafka => {
                "
Required if you want to create source from Kafka.
                "
            }
            Self::Pubsub => {
                "
Required if you want to create source from Emulated Google Pub/sub.
                "
            }
            Self::RustComponents => {
                "
Required if you want to build compute-node and meta-node.
Otherwise you will need to manually download and copy it
to RiseDev directory."
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
            Self::Redis => {
                "
Required if you want to sink data to redis.
                "
            }
            Self::ConnectorNode => {
                "
Required if you want to create CDC source from external Databases.
                "
            }
        }
        .into()
    }

    pub fn from_env(env: impl AsRef<str>) -> Option<Self> {
        match env.as_ref() {
            "ENABLE_MINIO" => Some(Self::Minio),
            "ENABLE_HDFS" => Some(Self::Hdfs),
            "ENABLE_PROMETHEUS_GRAFANA" => Some(Self::PrometheusAndGrafana),
            "ENABLE_ETCD" => Some(Self::Etcd),
            "ENABLE_KAFKA" => Some(Self::Kafka),
            "ENABLE_PUBSUB" => Some(Self::Pubsub),
            "ENABLE_BUILD_RUST" => Some(Self::RustComponents),
            "ENABLE_BUILD_DASHBOARD_V2" => Some(Self::Dashboard),
            "ENABLE_COMPUTE_TRACING" => Some(Self::Tracing),
            "ENABLE_RELEASE_PROFILE" => Some(Self::Release),
            "ENABLE_ALL_IN_ONE" => Some(Self::AllInOne),
            "ENABLE_SANITIZER" => Some(Self::Sanitizer),
            "ENABLE_REDIS" => Some(Self::Redis),
            "ENABLE_RW_CONNECTOR" => Some(Self::ConnectorNode),
            _ => None,
        }
    }

    pub fn env(&self) -> String {
        match self {
            Self::Minio => "ENABLE_MINIO",
            Self::Hdfs => "ENABLE_HDFS",
            Self::PrometheusAndGrafana => "ENABLE_PROMETHEUS_GRAFANA",
            Self::Etcd => "ENABLE_ETCD",
            Self::Kafka => "ENABLE_KAFKA",
            Self::Pubsub => "ENABLE_PUBSUB",
            Self::Redis => "ENABLE_REDIS",
            Self::RustComponents => "ENABLE_BUILD_RUST",
            Self::Dashboard => "ENABLE_BUILD_DASHBOARD_V2",
            Self::Tracing => "ENABLE_COMPUTE_TRACING",
            Self::Release => "ENABLE_RELEASE_PROFILE",
            Self::AllInOne => "ENABLE_ALL_IN_ONE",
            Self::Sanitizer => "ENABLE_SANITIZER",
            Self::ConnectorNode => "ENABLE_RW_CONNECTOR",
        }
        .into()
    }

    pub fn default_enabled() -> &'static [Self] {
        &[Self::RustComponents]
    }
}

fn configure(chosen: &[Components]) -> Result<Option<Vec<Components>>> {
    println!("=== Configure RiseDev ===");

    let all_components = all::<Components>().collect_vec();

    const ITEMS_PER_PAGE: usize = 6;

    let items = all_components
        .iter()
        .map(|c| {
            let title = c.title();
            let desc = style(
                ("\n".to_string() + c.description().trim())
                    .split('\n')
                    .join("\n      "),
            )
            .dim();

            (format!("{title}{desc}",), chosen.contains(c))
        })
        .collect_vec();

    let Some(chosen_indices) = MultiSelect::new()
        .with_prompt(
            format!(
                "RiseDev includes several components. You can select the ones you need, so as to reduce build time\n\n{}: navigate\n{}: confirm and save   {}: quit without saving\n\nPick items with {}",
                style("↑ / ↓ / ← / → ").reverse(),
                style("Enter").reverse(),
                style("Esc / q").reverse(),
                style("Space").reverse(),
            )
        )
        .items_checked(&items)
        .max_length(ITEMS_PER_PAGE)
        .interact_opt()? else {
        return Ok(None);
    };

    let chosen = chosen_indices
        .into_iter()
        .map(|i| all_components[i])
        .collect_vec();

    Ok(Some(chosen))
}

fn main() -> Result<()> {
    let opts = RiseDevConfigOpts::parse();
    let file_path = opts.file;

    let chosen = {
        if let Ok(file) = OpenOptions::new().read(true).open(&file_path) {
            let reader = BufReader::new(file);
            let mut enabled = vec![];
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() || line.trim().starts_with('#') {
                    continue;
                }
                let Some((component, val)) = line.split_once('=') else {
                    println!("invalid config line {}, discarded", line);
                    continue;
                };
                if component == "RISEDEV_CONFIGURED" {
                    continue;
                }
                match Components::from_env(component) {
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

    let chosen = match &opts.command {
        Some(Commands::Default) => {
            println!("Using default config");
            Components::default_enabled().to_vec()
        }
        Some(Commands::Enable { component }) => {
            let mut chosen = chosen;
            chosen.push(*component);
            chosen
        }
        Some(Commands::Disable { component }) => {
            chosen.into_iter().filter(|x| x != component).collect()
        }
        None => match configure(&chosen)? {
            Some(chosen) => chosen,
            None => {
                println!("Quit without saving");
                println!("=========================");
                return Ok(());
            }
        },
    };

    println!("=== Enabled Components ===");
    for component in all::<Components>() {
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

    println!("Configuration saved at {}", file_path);
    println!("=========================");

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

    for component in all::<Components>() {
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

    println!(
        "RiseDev will {} the components you've enabled.",
        style("only download").bold()
    );
    println!(
        "If you want to use these components, please {} in {} to start that component.",
        style("modify the cluster config").yellow().bold(),
        style(RISEDEV_CONFIG_FILE).bold(),
    );
    println!("See CONTRIBUTING.md or RiseDev's readme for more information.");

    Ok(())
}
