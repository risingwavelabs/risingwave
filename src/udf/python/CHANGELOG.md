# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add `workers` option in `@udf` and `@udtf` to specify the number of worker processes for CPU bound functions.

## [0.1.0] - 2023-12-01

### Fixed

- Fix unconstrained decimal type.

## [0.0.12] - 2023-11-28

### Changed

- Change the default struct field name to `f{i}`.

### Fixed

- Fix parsing nested struct type.


## [0.0.11] - 2023-11-06

### Fixed

- Hook SIGTERM to stop the UDF server gracefully.
