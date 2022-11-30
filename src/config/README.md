Here defines a sample configuration file `risingwave.toml`. It can be provided to risingwave nodes via command line argument `--config-path`

The related code is located at `src/common/src/config.rs`. 

> **Note**
>
> The config file here is mainly used for testing purpose. It is the default config used by `risedev`.
>
> The values defined here is different from the default values defined in `src/common/src/config.rs` , which is used when the configuration file is not present.
