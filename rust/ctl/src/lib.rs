use clap::{Parser, Subcommand};
mod cmd_impl;
pub(crate) mod common;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// list latest Hummock version on meta node
    ListVersion,
    /// list all Hummock key-value pairs
    ListKv,
}

pub async fn start() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::ListVersion => cmd_impl::list_version().await.unwrap(),
        Commands::ListKv => cmd_impl::list_kv().await.unwrap(),
    }
}
