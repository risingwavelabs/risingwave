Each folder contains a `risingwave.toml` file which can be used by `risedev.yml` . It can be used like:

```yaml
risedev:
  profile-name:
    config-path: src/risedevtool/configs/test/risingwave.toml
    steps:
      - use: ...
```

It will be copied to `.risingwave/config/risingwave.toml` , which will be used for execution.
