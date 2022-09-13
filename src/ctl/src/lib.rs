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
    /// Commands for Meta
    #[clap(subcommand)]
    Meta(MetaCommands),
    /// Commands for Benchmarks
    #[clap(subcommand)]
    Bench(BenchCommands),
    /// Commands for tracing the compute nodes
    Trace,
    // TODO(yuhao): profile other nodes
    /// Commands for profilng the compute nodes
    Profile {
        #[clap(short, long = "sleep")]
        sleep: u64,
    },
}

#[derive(Subcommand)]
enum HummockCommands {
    /// list latest Hummock version on meta node
    ListVersion,

    /// list hummock version deltas in the meta store
    ListVersionDeltas {
        #[clap(short, long = "start-version-delta-id", default_value_t = 0)]
        start_id: u64,

        #[clap(short, long = "num-epochs", default_value_t = 100)]
        num_epochs: u32,
    },
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
    /// trigger a full GC for SSTs that is not in version and with timestamp <= now -
    /// sst_retention_time_sec.
    TriggerFullGc {
        #[clap(short, long = "sst_retention_time_sec", default_value_t = 259200)]
        sst_retention_time_sec: u64,
    },
}

#[derive(Subcommand)]
enum TableCommands {
    /// scan a state table with MV name
    Scan {
        /// name of the materialized view to operate on
        mv_name: String,
    },
    /// scan a state table using Id
    ScanById {
        /// id of the state table to operate on
        table_id: u32,
    },
    /// list all state tables
    List,
}

#[derive(Subcommand)]
enum MetaCommands {
    /// pause the stream graph
    Pause,
    /// resume the stream graph
    Resume,
    /// get cluster info
    ClusterInfo,
    /// Reschedule the parallel unit in the stream graph
    ///
    /// The format is `fragment_id-[removed]+[added]`
    /// You can provide either `removed` only or `added` only, but `removed` should be preceded by
    /// `added` when both are provided.
    ///
    /// For example, for plan `100-[1,2,3]+[4,5]` the follow request will be generated:
    /// {
    ///     100: Reschedule {
    ///         added_parallel_units: [4,5],
    ///         removed_parallel_units: [1,2,3],
    ///     }
    /// }
    /// Use ; to separate multiple fragment
    #[clap(verbatim_doc_comment)]
    Reschedule {
        /// Plan of reschedule
        #[clap(long)]
        plan: String,
        /// Show the plan only, no actual operation
        #[clap(long)]
        dry_run: bool,
    },
}

pub async fn start(opts: CliOpts) -> Result<()> {
    match opts.command {
        Commands::Hummock(HummockCommands::ListVersion) => {
            cmd_impl::hummock::list_version().await?;
        }
        Commands::Hummock(HummockCommands::ListVersionDeltas {
            start_id,
            num_epochs,
        }) => {
            cmd_impl::hummock::list_version_deltas(start_id, num_epochs).await?;
        }
        Commands::Hummock(HummockCommands::ListKv { epoch, table_id }) => {
            cmd_impl::hummock::list_kv(epoch, table_id).await?;
        }
        Commands::Hummock(HummockCommands::SstDump) => cmd_impl::hummock::sst_dump().await.unwrap(),
        Commands::Hummock(HummockCommands::TriggerManualCompaction {
            compaction_group_id,
            table_id,
            level,
        }) => {
            cmd_impl::hummock::trigger_manual_compaction(compaction_group_id, table_id, level)
                .await?
        }
        Commands::Hummock(HummockCommands::TriggerFullGc {
            sst_retention_time_sec,
        }) => cmd_impl::hummock::trigger_full_gc(sst_retention_time_sec).await?,
        Commands::Table(TableCommands::Scan { mv_name }) => cmd_impl::table::scan(mv_name).await?,
        Commands::Table(TableCommands::ScanById { table_id }) => {
            cmd_impl::table::scan_id(table_id).await?
        }
        Commands::Table(TableCommands::List) => cmd_impl::table::list().await?,
        Commands::Bench(cmd) => cmd_impl::bench::do_bench(cmd).await?,
        Commands::Meta(MetaCommands::Pause) => cmd_impl::meta::pause().await?,
        Commands::Meta(MetaCommands::Resume) => cmd_impl::meta::resume().await?,
        Commands::Meta(MetaCommands::ClusterInfo) => cmd_impl::meta::cluster_info().await?,
        Commands::Meta(MetaCommands::Reschedule { plan, dry_run }) => {
            cmd_impl::meta::reschedule(plan, dry_run).await?
        }
        Commands::Trace => cmd_impl::trace::trace().await?,
        Commands::Profile { sleep } => cmd_impl::profile::profile(sleep).await?,
    }
    Ok(())
}
