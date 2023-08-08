use std::{env, path::PathBuf};
use std::fs;

fn main() {
    let actor_proto = "./actor.proto";

    tonic_build::configure()
        .build_server(true)
        .out_dir("./src")
        .compile(&[actor_proto], &["."])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));
    fs::copy("./src/server.rs", "../simulator/src/server.rs").unwrap();
}