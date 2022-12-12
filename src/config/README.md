# Configurations (`risingwave.toml`)

`risingwave.toml` is the configuration file for risingwave. It can be provided to risingwave nodes via command line argument `--config-path`.

If no configuration file specified, default values in `src/common/src/config.rs` will be used.
Otherwise, items present in `risingwave.toml` will override the default values in the source code.

See `src/common/src/config.rs` for the definition as well as the default values.

## Use `risingwave.toml` in RiseDev

To specify configurations for `risedev dev <profile>`, put the path of configuration file in `config-path` under your profile.
RiseDev will copy this file to the working directory (`.risingwave/` by default) and rename it to `risingwave.toml` on start-up.

```yaml
risedev:
  profile-name:
    config-path: src/config/ci.toml
    steps:
      - use: ...
```

The configuration files defined under `config/` are mainly used for testing and development purposes.
