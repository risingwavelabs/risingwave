use core::option::Option::Some;
use core::result::Result::{Err, Ok};
use risingwave_pb::connector_service::GetEventStreamResponse;
use std::fs;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use jni::{InitArgsBuilder, JavaVM, JNIVersion};
use tokio::sync::mpsc::Sender;

pub static JVM: LazyLock<Arc<JavaVM>> = LazyLock::new(|| {
    let dir_path = ".risingwave/bin/connector-node/libs/";

    let dir = Path::new(dir_path);

    if !dir.is_dir() {
        panic!("{} is not a directory", dir_path);
    }

    let mut class_vec = vec![];

    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries {
            if let Ok(entry) = entry {
                if let Some(name) = entry.path().file_name() {
                    println!("{:?}", name);
                    class_vec.push(String::from( dir_path.to_owned() + name.to_str().to_owned().unwrap()));
                }
            }
        }
    } else {
        println!("failed to read directory {}", dir_path);
    }

    // Build the VM properties
    let jvm_args = InitArgsBuilder::new()
        // Pass the JNI API version (default is 8)
        .version(JNIVersion::V8)
        // You can additionally pass any JVM options (standard, like a system property,
        // or VM-specific).
        // Here we enable some extra JNI checks useful during development
        // .option("-Xcheck:jni")
        .option("-ea")
        .option(format!("-Djava.class.path={}", class_vec.join(":")) )
        .option("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=9111")
        .build()
        .unwrap();

    // Create a new VM
    let jvm = match JavaVM::new(jvm_args) {
        Err(err) => {
            panic!("{:?}", err)
        },
        Ok(jvm) => jvm,
    };

    Arc::new(jvm)
});


pub struct MyPtr {
    pub ptr: Sender<GetEventStreamResponse>,
    pub num: u64,
}

impl Drop for MyPtr {
    fn drop(&mut self) {
        println!("drop MyPtr, num = {}", self.num);
    }
}