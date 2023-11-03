use std::io::Write;

use with_options_build::{connector_crate_path, update_with_options_yaml};

fn main() {
    let yaml_str = update_with_options_yaml();
    let mut file =
        std::fs::File::create(connector_crate_path().unwrap().join("with_options.yaml")).unwrap();
    file.write_all(yaml_str.as_bytes()).unwrap();
}
