## `risingwave.toml`

`risingwave.toml` is the configuration file for risingwave. It can be provided to risingwave nodes via command line argument `--config-path` .

The related code is located at `src/common/src/config.rs` . 

## Use `risingwave.toml` in `RiseDev`

The config files defined here are mainly used for testing and development purposes. 

`./risingwave.toml` is the default config used by `risedev` .

> **Note**
>
> Its value is different from the default values defined in `src/common/src/config.rs` , which is used when the configuration file is not present.

Other toml files define configurations for different scenes. It can be used in `risedev.yml` like:

```yaml
risedev:
  profile-name:
    config-path: src/config/ci.toml
    steps:
      - use: ...
```
