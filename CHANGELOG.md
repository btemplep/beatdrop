# Changelog

Changelog for `beatdrop`.
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- 
## [Unreleased] - YYYY-MM-DD

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security 
-->

## [0.1.0a9] - 2024-02-19

Fix README

## [0.1.0a8] - 2024-02-19

### Fixed
- pydantic less than 2
- deprecations for use of `datetime.datetime.utcnow()`

### Removed
- support for python 3.7


## [0.1.0a7] - 2023-02-25

### Fixed
- tests 
- docs


## [0.1.0a6] - 2023-02-22

### Added

- `RQScheduler` - Building block scheduler for the RQ (Redis Queue) task backend.
- `RQRedisScheduler` - Complete scheduler with RQ task backend and redis entry storage.
- `RQSQLScheduler` - Complete scheduler with RQ task backend and SQL DB entry storage.

### Fixed

- PYPI logo

## [0.1.0a5] - 2023-02-06

### Fixed

- packaging files
- README Links


## [0.1.0a4] - 2023-02-05

### Fixed

Docstrings updated and documentation added.


## [0.1.0a3] - 2023-01-18

Update for pypi formatting.


## [0.1.0a2] - 2023-01-17

Update for pypi formatting.


## [0.1.0a1] - 2023-01-17

Initial release