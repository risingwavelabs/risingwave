## `risingwave.toml`

`risingwave.toml` is the configuration file for risingwave. It can be provided to risingwave nodes via command line argument `--config-path` .

The related code is located at `src/common/src/config.rs` . 

## Use `risingwave.toml` in `RiseDev`

The config files defined here are mainly used for testing and development purposes. 

`./risingwave.toml` is the default config used by `risedev` .

> **Note**
>
> Its value is different from the default values defined in `src/common/src/config.rs` , which is used when the configuration file is not present.

Each folder contains a `risingwave.toml` file for a different scene. It can be used in `risedev.yml` like:

```yaml
risedev:
  profile-name:
    config-path: src/config/ci/risingwave.toml
    steps:
      - use: ...
```

It will be copied to `.risingwave/config/risingwave.toml` , which is the `config-path` arg passed to risingwave nodes by risedev.

> **Note**
>
> You may want to copy the content of `./risingwave.toml` when you want to define a configuration where some values are modified but the others are the same as the default file instead of the default values defined in `src/common/src/config.rs` .
