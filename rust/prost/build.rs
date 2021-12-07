use convert_case::{Case, Casing};
use prost::Message;
use prost_types::FileDescriptorSet;
use quote::{format_ident, quote};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
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

    let file_descriptor_set_bytes = fs::read(file_descriptor_set_path).unwrap();
    let file_descriptor_set = FileDescriptorSet::decode(&file_descriptor_set_bytes[..]).unwrap();

    generate_extras(&out_dir, &file_descriptor_set);

    Ok(())
}

fn generate_extras(out_dir: &Path, file_descriptor_set: &FileDescriptorSet) {
    for fd in &file_descriptor_set.file {
        let package = match fd.package {
            Some(ref pkg) => pkg,
            None => continue,
        };

        let gen_path = out_dir.join(format!("{}.rs", package));
        let mut gen_file = OpenOptions::new().append(true).open(gen_path).unwrap();

        for msg in &fd.message_type {
            let name = match msg.name {
                Some(ref name) => name,
                None => continue,
            };

            let type_url = format!("type.googleapis.com/{}.{}", package, name);
            let type_name = name.to_case(Case::UpperCamel);

            gen_type_url(&mut gen_file, &type_url, &type_name);
        }
    }
}

fn gen_type_url(gen_file: &mut File, type_url: &str, type_name: &str) {
    let type_name = format_ident!("{}", type_name);

    let tokens = quote! {
        impl crate::TypeUrl for #type_name {
            fn type_url() -> &'static str {
                #type_url
            }
        }
    };

    writeln!(gen_file).unwrap();
    writeln!(gen_file, "{}", &tokens).unwrap();
}
