extern crate protoc_grpcio;

fn main() {
    let proto_dir = "../../proto";

    println!("cargo:rerun-if-changed={}", proto_dir);

    let proto_files = vec![
        "common.proto",
        "data.proto",
        "expr.proto",
        "plan.proto",
        "task_service.proto",
        "stream_plan.proto",
        "stream_service.proto",
    ];
    let protos: Vec<String> = proto_files
        .iter()
        .map(|f| format!("{}/{}", proto_dir, f))
        .collect();

    protoc_grpcio::compile_grpc_protos(&protos, &[proto_dir], "src", None).unwrap();
}
