# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/undefined-oss/pgTest/compare/internals-v0.1.0...internals-v0.2.0) - 2026-10-09

### Added

- *(all)* Refactored and rearchitectured core worker and postgres ([#22](https://github.com/undefined-oss/pgTest/pull/22))
- *(all)* Added Newtype struct for mostly all options used for config. ([#21](https://github.com/undefined-oss/pgTest/pull/21))
- *(engine)* Database creation executed in batch instead of sequentially. ([#11](https://github.com/undefined-oss/pgTest/pull/11))
- *(postgres)* Migrated from SQLx to Tokyo-postgres ([#10](https://github.com/undefined-oss/pgTest/pull/10))

### Other

- *(refactor)* Removed validation if a value is zero using NonZero Struct from standard library ([#20](https://github.com/undefined-oss/pgTest/pull/20))

## [0.1.0](https://github.com/undefined-oss/pgTest/releases/tag/internals-v0.1.0) - 2026-09-22

### Added

- Version 0.1.0 ([#1](https://github.com/undefined-oss/pgTest/pull/1))

### Other

- CD Pipelines and trimming final rust binary size ([#2](https://github.com/undefined-oss/pgTest/pull/2))
