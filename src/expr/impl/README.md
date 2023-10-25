## Run Microbenchmarks

```sh
# run all benches
cargo bench --bench expr -- --quick
# list all benches
cargo bench --bench expr -- --list
# run specified benches
cargo bench --bench expr -- --quick "add\(int32,int32\)"
```
