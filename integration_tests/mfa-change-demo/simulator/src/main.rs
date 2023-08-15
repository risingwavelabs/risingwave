use clap::{App, Arg, ArgMatches};

mod entities;
mod entities_taxi;
mod server_pb;
mod simulation;

#[tokio::main]
async fn main() {
    let args = get_args();
    simulation::main_loop(
        args.value_of("types")
            .expect("failed to decode brokers")
            .to_string(),
    )
    .await;
}

fn get_args<'a>() -> ArgMatches<'a> {
    App::new("simulator")
        .about("The simulator")
        .arg(
            Arg::with_name("types")
                .short("t")
                .long("types")
                .help("mfa or taxi")
                .takes_value(true)
                .default_value("taxi"),
        )
        .get_matches()
}
