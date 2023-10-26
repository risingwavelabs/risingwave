# WASM UDF examples

TODO:
- [ ] error handing
- [ ] schema validation

## Required tools

- [wasm-tools](https://github.com/bytecodealliance/wasm-tools): to create WASM component from WASM module.
- [wit-bindgen](https://github.com/bytecodealliance/wit-bindgen) CLI: to generate guest code from WIT file. (Not required for Rust guest)

```
cargo install wasm-tools@1.0.35
cargo install wit-bindgen-cli@0.8.0
```

> **Note**
>
> WASM component model IS NOT stable and may change. Please use the version specified above.

## Examples for different guest languages

Refer to each language's directory for an example. Some additional notes are listed below.
Generally you will just need to copy the `wit` directory and follow the examples.

It's not guaranteed to work if you used different versions of toolchains and project dependencies.

### Rust

nothing special

### Golang

#### TinyGo

[TinyGo](https://tinygo.org/getting-started/install/) is an alternative Go compiler for small devices. It also supports WASM.

tested under
```
> tinygo version
tinygo version 0.28.1 darwin/amd64 (using go version go1.20.2 and LLVM version 15.0.0)
```

- TinyGo cannot compile the lz4 package ([Add support for reading Go assembly files by aykevl · Pull Request #3103 · tinygo-org/tinygo](https://github.com/tinygo-org/tinygo/pull/3103)), which is used by Arrow. Can workaround by using the forked version of arrow, which removed lz4.

```
replace github.com/apache/arrow/go/v13 => github.com/xxchan/arrow/go/v13 v13.0.0-20230713134335-45002b4934f9
```

#### TODO: Go 1.21

(requires full wasi_snapshot_preview1)
- [Go 1.21 Release Notes - The Go Programming Language](https://tip.golang.org/doc/go1.21)
- [all: add GOOS=wasip1 GOARCH=wasm port · Issue #58141 · golang/go](https://github.com/golang/go/issues/58141)
