use risingwave_common::config::RwConfig;

fn main() {
    let config = RwConfig::default();
    let toml = toml::to_string(&config).unwrap();
    print!("{}", &toml);
}
