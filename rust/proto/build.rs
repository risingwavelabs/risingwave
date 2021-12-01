fn main() {
    let proto_dir = "../../proto";

    println!("cargo:rerun-if-changed={}", proto_dir);

    let proto_files = vec![
        "common.proto",
        "data.proto",
        "expr.proto",
        "meta.proto",
        "plan.proto",
        "task_service.proto",
        "stream_plan.proto",
        "stream_service.proto",
    ];
    let protos: Vec<String> = proto_files
        .iter()
        .map(|f| format!("{}/{}", proto_dir, f))
        .collect();

    protoc_rust::Codegen::new()
        .out_dir("src")
        .inputs(&protos)
        .include(proto_dir)
        .run()
        .unwrap();
}
