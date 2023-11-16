use clap::{Arg, ArgMatches, Command};

mod entities;
mod entities_taxi;
mod server_pb;
mod simulation;

#[tokio::main]
async fn main() {
    let args = get_args();
    simulation::main_loop(
        args.get_one::<String>("types")
            .expect("failed to decode brokers")
            .to_string(),
    )
    .await;
}

fn get_args() -> ArgMatches {
    Command::new("simulator")
        .about("The simulator")
        .arg(
            Arg::new("types")
                .short('t')
                .long("types")
                .help("mfa or taxi")
                .num_args(1)
                .default_value("taxi"),
        )
        .get_matches()
}
