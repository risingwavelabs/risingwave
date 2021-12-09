use std::path::PathBuf;

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

    let out_dir: PathBuf = PathBuf::from("./src");
    let file_descriptor_set_path: PathBuf = out_dir.join("file_descriptor_set.bin");
    tonic_build::configure()
        .file_descriptor_set_path(file_descriptor_set_path.as_path())
        .type_attribute(".", "#[derive(prost_helpers::AnyPB)]")
        .out_dir(out_dir.as_path())
        .compile(&protos, &[proto_dir.to_string()])
        .expect("Failed to compile grpc!");

    Ok(())
}
