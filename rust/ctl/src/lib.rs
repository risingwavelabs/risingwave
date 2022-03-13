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
