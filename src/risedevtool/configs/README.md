Each folder contains a `risingwave.toml` file which can be used by `risedev.yml` . It can be used like:

```yaml
risedev:
  profile-name:
    config-path: src/risedevtool/configs/test/risingwave.toml
    steps:
      - use: ...
```

It will be copied to `.risingwave/config/risingwave.toml` , which will be used for execution.

> **Note**
>
> If `config-path` is not specified, `src/config/risingwave.toml` will be used.
>
> The values defined in `src/config/risingwave.toml` is different from the default values defined in `src/common/src/config.rs` , which is used when the configuration file is not present. Therefore, you may want to copy the content of `src/config/risingwave.toml` when you want to define a configuration where some values are modified but the others are the same as the default file.
