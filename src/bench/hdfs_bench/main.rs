use std::sync::Arc;

use opendal::raw::Accessor;
use opendal::services::hdfs;
use opendal::{Object, Operator};

#[tokio::main]
async fn main() {
    // Create fs backend builder.
    let mut builder = hdfs::Builder::default();
    // Set the name node for hdfs.
    builder.name_node("hdfs://127.0.0.1:9000");
    // Set the root for hdfs, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root("/tmp");

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder.build().unwrap());

    // Create an object handle to start operation on object.
    op.object("test_file").write("Hello, world!").await.unwrap();

    let content = op.object("test_file").read().await.unwrap();
    println!("read: {:?}", String::from_utf8_lossy(&content));

    let meta = op.object("test_file").metadata().await.unwrap();

    println!("meta size = {:?}", meta.content_length());
    op.object("test_file").delete().await.unwrap();

}
