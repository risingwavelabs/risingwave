extern crate protoc_grpcio;

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let proto_dir = "../../proto";
  let proto_files = vec![
    "common.proto",
    "data.proto",
    "expr.proto",
    "plan.proto",
    "task_service.proto",
  ];
  let protos: Vec<String> = proto_files
    .iter()
    .map(|f| format!("{}/{}", proto_dir, f))
    .collect();

  protoc_grpcio::compile_grpc_protos(&protos, &[proto_dir], "src", None)
    .expect("Failed to compile grpc!");
  Ok(())
}
