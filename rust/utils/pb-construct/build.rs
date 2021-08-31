use protoc_rust::Customize;

fn main() {
    let out_dir = std::env::var("OUT_DIR").expect("Unable to get OUT_DIR");

    let customize = Customize {
        gen_mod_rs: Some(true),
        ..Default::default()
    };

    protoc_rust::Codegen::new()
        .out_dir(&out_dir)
        .include("tests/proto")
        .inputs(&["tests/proto/message.proto"])
        .customize(customize)
        .run()
        .expect("Compiling test proto files failed.");
}
