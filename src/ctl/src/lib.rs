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

use anyhow::Result;
use clap::{Parser, Subcommand};
use cmd_impl::bench::BenchCommands;
use cmd_impl::hummock::SstDumpArgs;

use crate::cmd_impl::hummock::{
    build_compaction_config_vec, list_pinned_snapshots, list_pinned_versions,
};
use crate::common::CtlContext;

pub mod cmd_impl;
pub mod common;

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
    /// Commands for Compute
    #[clap(subcommand)]
    Compute(ComputeCommands),
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
enum ComputeCommands {
    /// Show all the configuration parameters on compute node
    ShowConfig { host: String },
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
    /// Forbid hummock commit new epochs, which is a prerequisite for compaction deterministic test
    DisableCommitEpoch,
    /// list all Hummock key-value pairs
    ListKv {
        #[clap(short, long = "epoch", default_value_t = u64::MAX)]
        epoch: u64,

        #[clap(short, long = "table-id")]
        table_id: u32,
    },
    SstDump(SstDumpArgs),
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
    /// List pinned versions of each worker.
    ListPinnedVersions {},
    /// List pinned snapshots of each worker.
    ListPinnedSnapshots {},
    /// List all compaction groups.
    ListCompactionGroup,
    /// Update compaction config for compaction groups.
    UpdateCompactionConfig {
        #[clap(long)]
        compaction_group_ids: Vec<u64>,
        #[clap(long)]
        max_bytes_for_level_base: Option<u64>,
        #[clap(long)]
        max_bytes_for_level_multiplier: Option<u64>,
        #[clap(long)]
        max_compaction_bytes: Option<u64>,
        #[clap(long)]
        sub_level_max_compaction_bytes: Option<u64>,
        #[clap(long)]
        level0_tier_compact_file_number: Option<u64>,
        #[clap(long)]
        target_file_size_base: Option<u64>,
        #[clap(long)]
        compaction_filter_mask: Option<u32>,
        #[clap(long)]
        max_sub_compaction: Option<u32>,
        #[clap(long)]
        level0_stop_write_threshold_sub_level_number: Option<u64>,
    },
    /// Split given compaction group into two. Moves the given tables to the new group.
    SplitCompactionGroup {
        #[clap(long)]
        compaction_group_id: u64,
        #[clap(long)]
        table_ids: Vec<u32>,
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
    /// backup meta by taking a meta snapshot
    BackupMeta,
    /// delete meta snapshots
    DeleteMetaSnapshots { snapshot_ids: Vec<u64> },

    /// Create a new connection object
    CreateConnection {
        #[clap(long)]
        provider: String,
        #[clap(long)]
        service_name: String,
        #[clap(long)]
        availability_zones: String,
    },

    /// List all existing connections in the catalog
    ListConnections,

    /// Drop a connection by its name
    DropConnection {
        #[clap(long)]
        connection_name: String,
    },
}

pub async fn start(opts: CliOpts) -> Result<()> {
    let context = CtlContext::default();
    let result = start_impl(opts, &context).await;
    context.try_close().await;
    result
}

pub async fn start_impl(opts: CliOpts, context: &CtlContext) -> Result<()> {
    match opts.command {
        Commands::Compute(ComputeCommands::ShowConfig { host }) => {
            cmd_impl::compute::show_config(&host).await?
        }
        Commands::Hummock(HummockCommands::DisableCommitEpoch) => {
            cmd_impl::hummock::disable_commit_epoch(context).await?
        }
        Commands::Hummock(HummockCommands::ListVersion) => {
            cmd_impl::hummock::list_version(context).await?;
        }
        Commands::Hummock(HummockCommands::ListVersionDeltas {
            start_id,
            num_epochs,
        }) => {
            cmd_impl::hummock::list_version_deltas(context, start_id, num_epochs).await?;
        }
        Commands::Hummock(HummockCommands::ListKv { epoch, table_id }) => {
            cmd_impl::hummock::list_kv(context, epoch, table_id).await?;
        }
        Commands::Hummock(HummockCommands::SstDump(args)) => {
            cmd_impl::hummock::sst_dump(context, args).await.unwrap()
        }
        Commands::Hummock(HummockCommands::TriggerManualCompaction {
            compaction_group_id,
            table_id,
            level,
        }) => {
            cmd_impl::hummock::trigger_manual_compaction(
                context,
                compaction_group_id,
                table_id,
                level,
            )
            .await?
        }
        Commands::Hummock(HummockCommands::TriggerFullGc {
            sst_retention_time_sec,
        }) => cmd_impl::hummock::trigger_full_gc(context, sst_retention_time_sec).await?,
        Commands::Hummock(HummockCommands::ListPinnedVersions {}) => {
            list_pinned_versions(context).await?
        }
        Commands::Hummock(HummockCommands::ListPinnedSnapshots {}) => {
            list_pinned_snapshots(context).await?
        }
        Commands::Hummock(HummockCommands::ListCompactionGroup) => {
            cmd_impl::hummock::list_compaction_group(context).await?
        }
        Commands::Hummock(HummockCommands::UpdateCompactionConfig {
            compaction_group_ids,
            max_bytes_for_level_base,
            max_bytes_for_level_multiplier,
            max_compaction_bytes,
            sub_level_max_compaction_bytes,
            level0_tier_compact_file_number,
            target_file_size_base,
            compaction_filter_mask,
            max_sub_compaction,
            level0_stop_write_threshold_sub_level_number,
        }) => {
            cmd_impl::hummock::update_compaction_config(
                context,
                compaction_group_ids,
                build_compaction_config_vec(
                    max_bytes_for_level_base,
                    max_bytes_for_level_multiplier,
                    max_compaction_bytes,
                    sub_level_max_compaction_bytes,
                    level0_tier_compact_file_number,
                    target_file_size_base,
                    compaction_filter_mask,
                    max_sub_compaction,
                    level0_stop_write_threshold_sub_level_number,
                ),
            )
            .await?
        }
        Commands::Hummock(HummockCommands::SplitCompactionGroup {
            compaction_group_id,
            table_ids,
        }) => {
            cmd_impl::hummock::split_compaction_group(context, compaction_group_id, &table_ids)
                .await?;
        }
        Commands::Table(TableCommands::Scan { mv_name }) => {
            cmd_impl::table::scan(context, mv_name).await?
        }
        Commands::Table(TableCommands::ScanById { table_id }) => {
            cmd_impl::table::scan_id(context, table_id).await?
        }
        Commands::Table(TableCommands::List) => cmd_impl::table::list(context).await?,
        Commands::Bench(cmd) => cmd_impl::bench::do_bench(context, cmd).await?,
        Commands::Meta(MetaCommands::Pause) => cmd_impl::meta::pause(context).await?,
        Commands::Meta(MetaCommands::Resume) => cmd_impl::meta::resume(context).await?,
        Commands::Meta(MetaCommands::ClusterInfo) => cmd_impl::meta::cluster_info(context).await?,
        Commands::Meta(MetaCommands::Reschedule { plan, dry_run }) => {
            cmd_impl::meta::reschedule(context, plan, dry_run).await?
        }
        Commands::Meta(MetaCommands::BackupMeta) => cmd_impl::meta::backup_meta(context).await?,
        Commands::Meta(MetaCommands::DeleteMetaSnapshots { snapshot_ids }) => {
            cmd_impl::meta::delete_meta_snapshots(context, &snapshot_ids).await?
        }
        Commands::Meta(MetaCommands::CreateConnection {
            provider,
            service_name,
            availability_zones,
        }) => {
            cmd_impl::meta::create_connection(context, provider, service_name, availability_zones)
                .await?
        }
        Commands::Meta(MetaCommands::ListConnections) => {
            cmd_impl::meta::list_connections(context).await?
        }
        Commands::Meta(MetaCommands::DropConnection { connection_name }) => {
            cmd_impl::meta::drop_connection(context, connection_name).await?
        }
        Commands::Trace => cmd_impl::trace::trace(context).await?,
        Commands::Profile { sleep } => cmd_impl::profile::profile(context, sleep).await?,
    }
    Ok(())
}
