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

#![feature(let_chains)]
#![feature(hash_extract_if)]

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use cmd_impl::bench::BenchCommands;
use cmd_impl::hummock::SstDumpArgs;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_meta::backup_restore::RestoreOpts;
use risingwave_pb::meta::update_worker_node_schedulability_request::Schedulability;

use crate::cmd_impl::hummock::{
    build_compaction_config_vec, list_pinned_snapshots, list_pinned_versions,
};
use crate::cmd_impl::throttle::apply_throttle;
use crate::common::CtlContext;

pub mod cmd_impl;
pub mod common;

/// risectl provides internal access to the RisingWave cluster. Generally, you will need
/// to provide the meta address and the state store URL to enable risectl to access the cluster. You
/// must start RisingWave in full cluster mode (e.g. enable MinIO and compactor in risedev.yml)
/// instead of playground mode to use this tool. risectl will read environment variables
/// `RW_META_ADDR` and `RW_HUMMOCK_URL` to configure itself.
#[derive(Parser)]
#[clap(version, about = "The DevOps tool that provides internal access to the RisingWave cluster", long_about = None)]
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
    /// Commands for Scaling
    #[clap(subcommand)]
    Scale(ScaleCommands),
    /// Commands for Benchmarks
    #[clap(subcommand)]
    Bench(BenchCommands),
    /// Commands for Debug
    #[clap(subcommand)]
    Debug(DebugCommands),
    /// Dump the await-tree of compute nodes and compactors
    #[clap(visible_alias("trace"))]
    AwaitTree,
    // TODO(yuhao): profile other nodes
    /// Commands for profilng the compute nodes
    #[clap(subcommand)]
    Profile(ProfileCommands),
    #[clap(subcommand)]
    Throttle(ThrottleCommands),
}

#[derive(clap::ValueEnum, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum DebugCommonKind {
    Worker,
    User,
    Table,
    MetaMember,
    SourceCatalog,
    SinkCatalog,
    IndexCatalog,
    FunctionCatalog,
    ViewCatalog,
    ConnectionCatalog,
    DatabaseCatalog,
    SchemaCatalog,
    TableCatalog,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum DebugCommonOutputFormat {
    Json,
    Yaml,
}

#[derive(clap::Args, Debug, Clone)]
pub struct DebugCommon {
    /// The address of the etcd cluster
    #[clap(long, value_delimiter = ',', default_value = "localhost:2388")]
    etcd_endpoints: Vec<String>,

    /// The username for etcd authentication, used if `--enable-etcd-auth` is set
    #[clap(long)]
    etcd_username: Option<String>,

    /// The password for etcd authentication, used if `--enable-etcd-auth` is set
    #[clap(long)]
    etcd_password: Option<String>,

    /// Whether to enable etcd authentication
    #[clap(long, default_value_t = false, requires_all = &["etcd_username", "etcd_password"])]
    enable_etcd_auth: bool,

    /// Kinds of debug info to dump
    #[clap(value_enum, value_delimiter = ',')]
    kinds: Vec<DebugCommonKind>,

    /// The output format
    #[clap(value_enum, long = "output", short = 'o', default_value_t = DebugCommonOutputFormat::Yaml)]
    format: DebugCommonOutputFormat,
}

#[derive(Subcommand, Clone, Debug)]
pub enum DebugCommands {
    /// Dump debug info from the raw state store
    Dump {
        #[command(flatten)]
        common: DebugCommon,
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
    ListVersion {
        #[clap(short, long = "verbose", default_value_t = false)]
        verbose: bool,

        #[clap(long = "verbose_key_range", default_value_t = false)]
        verbose_key_range: bool,
    },

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
        #[clap(short, long = "epoch", default_value_t = HummockEpoch::MAX)]
        epoch: u64,

        #[clap(short, long = "table-id")]
        table_id: u32,

        // data directory for hummock state store. None: use default
        data_dir: Option<String>,
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

        #[clap(short, long = "sst-ids")]
        sst_ids: Vec<u64>,
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
        #[clap(long)]
        level0_sub_level_compact_level_count: Option<u32>,
        #[clap(long)]
        max_space_reclaim_bytes: Option<u64>,
        #[clap(long)]
        level0_max_compact_file_number: Option<u64>,
        #[clap(long)]
        level0_overlapping_sub_level_compact_level_count: Option<u32>,
        #[clap(long)]
        enable_emergency_picker: Option<bool>,
        #[clap(long)]
        tombstone_reclaim_ratio: Option<u32>,
    },
    /// Split given compaction group into two. Moves the given tables to the new group.
    SplitCompactionGroup {
        #[clap(long)]
        compaction_group_id: u64,
        #[clap(long)]
        table_ids: Vec<u32>,
    },
    /// Pause version checkpoint, which subsequently pauses GC of delta log and SST object.
    PauseVersionCheckpoint,
    /// Resume version checkpoint, which subsequently resumes GC of delta log and SST object.
    ResumeVersionCheckpoint,
    /// Replay version from the checkpoint one to the latest one.
    ReplayVersion,
    /// List compaction status
    ListCompactionStatus {
        #[clap(short, long = "verbose", default_value_t = false)]
        verbose: bool,
    },
    GetCompactionScore {
        #[clap(long)]
        compaction_group_id: u64,
    },
    /// Validate the current HummockVersion.
    ValidateVersion,
    /// Rebuild table stats
    RebuildTableStats,
}

#[derive(Subcommand)]
enum TableCommands {
    /// scan a state table with MV name
    Scan {
        /// name of the materialized view to operate on
        mv_name: String,
        // data directory for hummock state store. None: use default
        data_dir: Option<String>,
    },
    /// scan a state table using Id
    ScanById {
        /// id of the state table to operate on
        table_id: u32,
        // data directory for hummock state store. None: use default
        data_dir: Option<String>,
    },
    /// list all state tables
    List,
}

#[derive(clap::Args, Debug, Clone)]
pub struct ScaleHorizonCommands {
    /// The worker that needs to be excluded during scheduling, worker_id and worker_host are both
    /// supported
    #[clap(
        long,
        value_delimiter = ',',
        value_name = "worker_id or worker_host, ..."
    )]
    exclude_workers: Option<Vec<String>>,

    /// The worker that needs to be included during scheduling, worker_id and worker_host are both
    /// supported
    #[clap(
        long,
        value_delimiter = ',',
        value_name = "all or worker_id or worker_host, ..."
    )]
    include_workers: Option<Vec<String>>,

    /// The target parallelism, currently, it is used to limit the target parallelism and only
    /// takes effect when the actual parallelism exceeds this value. Can be used in conjunction
    /// with exclude/include_workers.
    #[clap(long)]
    target_parallelism: Option<u32>,

    #[command(flatten)]
    common: ScaleCommon,
}

#[derive(clap::Args, Debug, Clone)]
pub struct ScaleCommon {
    /// Will generate a plan supported by the `reschedule` command and save it to the provided path
    /// by the `--output`.
    #[clap(long, default_value_t = false)]
    generate: bool,

    /// The output file to write the generated plan to, standard output by default
    #[clap(long)]
    output: Option<String>,

    /// Automatic yes to prompts
    #[clap(short = 'y', long, default_value_t = false)]
    yes: bool,

    /// Specify the fragment ids that need to be scheduled.
    /// empty by default, which means all fragments will be scheduled
    #[clap(long, value_delimiter = ',')]
    fragments: Option<Vec<u32>>,
}

#[derive(clap::Args, Debug, Clone)]
pub struct ScaleVerticalCommands {
    #[command(flatten)]
    common: ScaleCommon,

    /// The worker that needs to be scheduled, worker_id and worker_host are both
    /// supported
    #[clap(
        long,
        required = true,
        value_delimiter = ',',
        value_name = "all or worker_id or worker_host, ..."
    )]
    workers: Option<Vec<String>>,

    /// The target parallelism per worker, requires `workers` to be set.
    #[clap(long, required = true)]
    target_parallelism_per_worker: Option<u32>,

    /// It will exclude all other workers to maintain the target parallelism only for the target workers.
    #[clap(long, default_value_t = false)]
    exclusive: bool,
}

#[derive(Subcommand, Debug)]
enum ScaleCommands {
    /// Scale the compute nodes horizontally, alias of `horizon`
    Resize(ScaleHorizonCommands),

    /// Scale the compute nodes horizontally
    Horizon(ScaleHorizonCommands),

    /// Scale the compute nodes vertically
    Vertical(ScaleVerticalCommands),

    /// Mark a compute node as unschedulable
    #[clap(verbatim_doc_comment)]
    Cordon {
        /// Workers that need to be cordoned, both id and host are supported.
        #[clap(
            long,
            required = true,
            value_delimiter = ',',
            value_name = "id or host,..."
        )]
        workers: Vec<String>,
    },
    /// mark a compute node as schedulable. Nodes are schedulable unless they are cordoned
    Uncordon {
        /// Workers that need to be uncordoned, both id and host are supported.
        #[clap(
            long,
            required = true,
            value_delimiter = ',',
            value_name = "id or host,..."
        )]
        workers: Vec<String>,
    },
}

#[derive(Subcommand)]
enum MetaCommands {
    /// pause the stream graph
    Pause,
    /// resume the stream graph
    Resume,
    /// get cluster info
    ClusterInfo,
    /// get source split info
    SourceSplitInfo,
    /// Reschedule the parallel unit in the stream graph
    ///
    /// The format is `fragment_id-[removed]+[added]`
    /// You can provide either `removed` only or `added` only, but `removed` should be preceded by
    /// `added` when both are provided.
    ///
    /// For example, for plan `100-[1,2,3]+[4,5]` the follow request will be generated:
    /// ```text
    /// {
    ///     100: Reschedule {
    ///         added_parallel_units: [4,5],
    ///         removed_parallel_units: [1,2,3],
    ///     }
    /// }
    /// ```
    /// Use ; to separate multiple fragment
    #[clap(verbatim_doc_comment)]
    #[clap(group(clap::ArgGroup::new("input_group").required(true).args(&["plan", "from"])))]
    Reschedule {
        /// Plan of reschedule, needs to be used with `revision`
        #[clap(long, requires = "revision")]
        plan: Option<String>,
        /// Revision of the plan
        #[clap(long)]
        revision: Option<u64>,
        /// Reschedule from a specific file
        #[clap(long, conflicts_with = "revision", value_hint = clap::ValueHint::AnyPath)]
        from: Option<String>,
        /// Show the plan only, no actual operation
        #[clap(long, default_value = "false")]
        dry_run: bool,
        /// Resolve NO_SHUFFLE upstream
        #[clap(long, default_value = "false")]
        resolve_no_shuffle: bool,
    },
    /// backup meta by taking a meta snapshot
    BackupMeta {
        #[clap(long)]
        remarks: Option<String>,
    },
    /// restore meta by recovering from a meta snapshot
    RestoreMeta {
        #[command(flatten)]
        opts: RestoreOpts,
    },
    /// delete meta snapshots
    DeleteMetaSnapshots { snapshot_ids: Vec<u64> },

    /// List all existing connections in the catalog
    ListConnections,

    /// List fragment to parallel units mapping for serving
    ListServingFragmentMapping,

    /// Unregister workers from the cluster
    UnregisterWorkers {
        /// The workers that needs to be unregistered, worker_id and worker_host are both supported
        #[clap(
            long,
            required = true,
            value_delimiter = ',',
            value_name = "worker_id or worker_host, ..."
        )]
        workers: Vec<String>,

        /// Automatic yes to prompts
        #[clap(short = 'y', long, default_value_t = false)]
        yes: bool,

        /// The worker not found will be ignored
        #[clap(long, default_value_t = false)]
        ignore_not_found: bool,
    },

    /// Validate source interface for the cloud team
    ValidateSource {
        /// With properties in json format
        /// If privatelink is used, specify `connection.id` instead of `connection.name`
        #[clap(long)]
        props: String,
    },
}

#[derive(Subcommand, Clone, Debug)]
enum ThrottleCommands {
    Source(ThrottleCommandArgs),
    Mv(ThrottleCommandArgs),
}

#[derive(Clone, Debug, Args)]
pub struct ThrottleCommandArgs {
    id: u32,
    rate: Option<u32>,
}

#[derive(Subcommand, Clone, Debug)]
pub enum ProfileCommands {
    /// CPU profile
    Cpu {
        /// The time to active profiling for (in seconds)
        #[clap(short, long = "sleep")]
        sleep: u64,
    },
    /// Heap profile
    Heap {
        /// The output directory of the dumped file
        #[clap(long = "dir")]
        dir: Option<String>,
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
        Commands::Hummock(HummockCommands::ListVersion {
            verbose,
            verbose_key_range,
        }) => {
            cmd_impl::hummock::list_version(context, verbose, verbose_key_range).await?;
        }
        Commands::Hummock(HummockCommands::ListVersionDeltas {
            start_id,
            num_epochs,
        }) => {
            cmd_impl::hummock::list_version_deltas(context, start_id, num_epochs).await?;
        }
        Commands::Hummock(HummockCommands::ListKv {
            epoch,
            table_id,
            data_dir,
        }) => {
            cmd_impl::hummock::list_kv(context, epoch, table_id, data_dir).await?;
        }
        Commands::Hummock(HummockCommands::SstDump(args)) => {
            cmd_impl::hummock::sst_dump(context, args).await.unwrap()
        }
        Commands::Hummock(HummockCommands::TriggerManualCompaction {
            compaction_group_id,
            table_id,
            level,
            sst_ids,
        }) => {
            cmd_impl::hummock::trigger_manual_compaction(
                context,
                compaction_group_id,
                table_id,
                level,
                sst_ids,
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
            level0_sub_level_compact_level_count,
            max_space_reclaim_bytes,
            level0_max_compact_file_number,
            level0_overlapping_sub_level_compact_level_count,
            enable_emergency_picker,
            tombstone_reclaim_ratio,
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
                    level0_sub_level_compact_level_count,
                    max_space_reclaim_bytes,
                    level0_max_compact_file_number,
                    level0_overlapping_sub_level_compact_level_count,
                    enable_emergency_picker,
                    tombstone_reclaim_ratio,
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
        Commands::Hummock(HummockCommands::PauseVersionCheckpoint) => {
            cmd_impl::hummock::pause_version_checkpoint(context).await?;
        }
        Commands::Hummock(HummockCommands::ResumeVersionCheckpoint) => {
            cmd_impl::hummock::resume_version_checkpoint(context).await?;
        }
        Commands::Hummock(HummockCommands::ReplayVersion) => {
            cmd_impl::hummock::replay_version(context).await?;
        }
        Commands::Hummock(HummockCommands::ListCompactionStatus { verbose }) => {
            cmd_impl::hummock::list_compaction_status(context, verbose).await?;
        }
        Commands::Hummock(HummockCommands::GetCompactionScore {
            compaction_group_id,
        }) => {
            cmd_impl::hummock::get_compaction_score(context, compaction_group_id).await?;
        }
        Commands::Hummock(HummockCommands::ValidateVersion) => {
            cmd_impl::hummock::validate_version(context).await?;
        }
        Commands::Hummock(HummockCommands::RebuildTableStats) => {
            cmd_impl::hummock::rebuild_table_stats(context).await?;
        }
        Commands::Table(TableCommands::Scan { mv_name, data_dir }) => {
            cmd_impl::table::scan(context, mv_name, data_dir).await?
        }
        Commands::Table(TableCommands::ScanById { table_id, data_dir }) => {
            cmd_impl::table::scan_id(context, table_id, data_dir).await?
        }
        Commands::Table(TableCommands::List) => cmd_impl::table::list(context).await?,
        Commands::Bench(cmd) => cmd_impl::bench::do_bench(context, cmd).await?,
        Commands::Meta(MetaCommands::Pause) => cmd_impl::meta::pause(context).await?,
        Commands::Meta(MetaCommands::Resume) => cmd_impl::meta::resume(context).await?,
        Commands::Meta(MetaCommands::ClusterInfo) => cmd_impl::meta::cluster_info(context).await?,
        Commands::Meta(MetaCommands::SourceSplitInfo) => {
            cmd_impl::meta::source_split_info(context).await?
        }
        Commands::Meta(MetaCommands::Reschedule {
            from,
            dry_run,
            plan,
            revision,
            resolve_no_shuffle,
        }) => {
            cmd_impl::meta::reschedule(context, plan, revision, from, dry_run, resolve_no_shuffle)
                .await?
        }
        Commands::Meta(MetaCommands::BackupMeta { remarks }) => {
            cmd_impl::meta::backup_meta(context, remarks).await?
        }
        Commands::Meta(MetaCommands::RestoreMeta { opts }) => {
            risingwave_meta::backup_restore::restore(opts).await?
        }
        Commands::Meta(MetaCommands::DeleteMetaSnapshots { snapshot_ids }) => {
            cmd_impl::meta::delete_meta_snapshots(context, &snapshot_ids).await?
        }
        Commands::Meta(MetaCommands::ListConnections) => {
            cmd_impl::meta::list_connections(context).await?
        }
        Commands::Meta(MetaCommands::ListServingFragmentMapping) => {
            cmd_impl::meta::list_serving_fragment_mappings(context).await?
        }
        Commands::Meta(MetaCommands::UnregisterWorkers {
            workers,
            yes,
            ignore_not_found,
        }) => cmd_impl::meta::unregister_workers(context, workers, yes, ignore_not_found).await?,
        Commands::Meta(MetaCommands::ValidateSource { props }) => {
            cmd_impl::meta::validate_source(context, props).await?
        }
        Commands::AwaitTree => cmd_impl::await_tree::dump(context).await?,
        Commands::Profile(ProfileCommands::Cpu { sleep }) => {
            cmd_impl::profile::cpu_profile(context, sleep).await?
        }
        Commands::Profile(ProfileCommands::Heap { dir }) => {
            cmd_impl::profile::heap_profile(context, dir).await?
        }
        Commands::Scale(ScaleCommands::Horizon(resize))
        | Commands::Scale(ScaleCommands::Resize(resize)) => {
            cmd_impl::scale::resize(context, resize.into()).await?
        }
        Commands::Scale(ScaleCommands::Vertical(resize)) => {
            cmd_impl::scale::resize(context, resize.into()).await?
        }
        Commands::Scale(ScaleCommands::Cordon { workers }) => {
            cmd_impl::scale::update_schedulability(context, workers, Schedulability::Unschedulable)
                .await?
        }
        Commands::Scale(ScaleCommands::Uncordon { workers }) => {
            cmd_impl::scale::update_schedulability(context, workers, Schedulability::Schedulable)
                .await?
        }
        Commands::Debug(DebugCommands::Dump { common }) => cmd_impl::debug::dump(common).await?,
        Commands::Throttle(ThrottleCommands::Source(args)) => {
            apply_throttle(context, risingwave_pb::meta::PbThrottleTarget::Source, args).await?
        }
        Commands::Throttle(ThrottleCommands::Mv(args)) => {
            apply_throttle(context, risingwave_pb::meta::PbThrottleTarget::Mv, args).await?;
        }
    }
    Ok(())
}
