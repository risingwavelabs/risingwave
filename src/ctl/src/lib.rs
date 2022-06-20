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

use anyhow::Result;
use clap::{Parser, Subcommand};
use cmd_impl::bench::BenchCommands;
mod cmd_impl;
pub(crate) mod common;

/// risectl provides internal access to the RisingWave cluster. Generally, you will need
/// to provide the meta address and the state store URL to enable risectl to access the cluster. You
/// must start RisingWave in full cluster mode (e.g. enable MinIO and compactor in risedev.yml)
/// instead of playground mode to use this tool. risectl will read environment variables
/// `RW_META_ADDR` and `RW_HUMMOCK_URL` to configure itself.
#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
#[clap(infer_subcommands = true)]
pub struct CliOpts {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
#[clap(infer_subcommands = true)]
enum Commands {
    /// Commands for Hummock
    #[clap(subcommand)]
    Hummock(HummockCommands),
    /// Commands for Tables
    #[clap(subcommand)]
    Table(TableCommands),
    /// Commands for Benchmarks
    #[clap(subcommand)]
    Bench(BenchCommands),
}

#[derive(Subcommand)]
enum HummockCommands {
    /// list latest Hummock version on meta node
    ListVersion,
    /// list all Hummock key-value pairs
    ListKv {
        #[clap(short, long = "epoch", default_value_t = u64::MAX)]
        epoch: u64,

        #[clap(short, long = "table-id")]
        table_id: Option<u32>,
    },
    SstDump,
    /// trigger a targeted compaction through compaction_group_id
    TriggerManualCompaction {
        #[clap(short, long = "compaction-group-id", default_value_t = 2)]
        compaction_group_id: u64,

        #[clap(short, long = "table-id", default_value_t = 0)]
        table_id: u32,

        #[clap(short, long = "level", default_value_t = 1)]
        level: u32,
    },
}

#[derive(Subcommand)]
enum TableCommands {
    /// benchmark state table
    Scan {
        /// name of the materialized view to operate on
        mv_name: String,
    },
}

pub async fn start(opts: CliOpts) -> Result<()> {
    match opts.command {
        Commands::Hummock(HummockCommands::ListVersion) => {
            tokio::spawn(cmd_impl::hummock::list_version()).await??;
        }
        Commands::Hummock(HummockCommands::ListKv { epoch, table_id }) => {
            tokio::spawn(cmd_impl::hummock::list_kv(epoch, table_id)).await??;
        }
        Commands::Hummock(HummockCommands::SstDump) => cmd_impl::hummock::sst_dump().await.unwrap(),
        Commands::Hummock(HummockCommands::TriggerManualCompaction {
            compaction_group_id,
            table_id,
            level,
        }) => {
            tokio::spawn(cmd_impl::hummock::trigger_manual_compaction(
                compaction_group_id,
                table_id,
                level,
            ))
            .await??
        }
        Commands::Table(TableCommands::Scan { mv_name }) => {
            tokio::spawn(cmd_impl::table::scan(mv_name)).await??
        }
        Commands::Bench(cmd) => tokio::spawn(cmd_impl::bench::do_bench(cmd)).await??,
    }
    Ok(())
}
