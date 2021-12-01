fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    println!("cargo:rerun-if-changed={}", proto_dir);

    let proto_files = vec![
        "common",
        "data",
        "expr",
        "meta",
        "plan",
        "task_service",
        "stream_plan",
        "stream_service",
        "hummock",
    ];
    let protos: Vec<String> = proto_files
        .iter()
        .map(|f| format!("{}/{}.proto", proto_dir, f))
        .collect();

    tonic_build::configure()
        .type_attribute(".", "#[derive(prost_helpers::AnyPB)]")
        .out_dir("./src")
        .compile(&protos, &[proto_dir.to_string()])
        .expect("Failed to compile grpc!");
    Ok(())
}
