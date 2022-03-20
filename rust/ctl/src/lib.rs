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
//
use clap::{Parser, Subcommand};
mod cmd_impl;
pub(crate) mod common;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
#[clap(infer_subcommands = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
#[clap(infer_subcommands = true)]
enum Commands {
    /// Commands for Hummock
    #[clap(subcommand)]
    Hummock(HummockCommands),
}

#[derive(Subcommand)]
enum HummockCommands {
    /// list latest Hummock version on meta node
    ListVersion,
    /// list all Hummock key-value pairs
    ListKv,
}

pub async fn start() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Hummock(HummockCommands::ListVersion) => {
            cmd_impl::hummock::list_version().await.unwrap()
        }
        Commands::Hummock(HummockCommands::ListKv) => cmd_impl::hummock::list_kv().await.unwrap(),
    }
}
