# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.0] - 2022-07-05
### Added
- Initial bully leader election algorithm

## [2.0.0] - 2022-07-03
### Changed
- CLI API
- Settings format to parse nodes
- Settings parser

## [1.2.0] - 2022-07-03
### Added
- Dependency resolution for nodes

## [1.1.0] - 2022-07-02
### Added
- Stateless nodes can be replicated

## [1.0.0] - 2022-07-02
### Added
- MOM using RabbitMQ
- Nodes implementation
- Filters and transforms
- Unreplicated coordinator with docker-in-docker, monitoring heartbeats
- Dockerized client and coordinator in compose
- Settings for an initial version of average posts score

### Modified
- Client now uses MOM
- RabbitMQ now gets configuration from file in volume
- Sidecars now use threads instead of processes

## [0.5.0] - 2022-07-01
### Added
- Client that writes straight to rabbit

## [0.4.0] - 2022-07-01
### Added
- Docker utils

## [0.3.0] - 2022-07-01
### Added
- Storage API

## [0.2.1] - 2022-07-01
### Fixed
- Sidecars bugs

## [0.2.0] - 2022-06-30
### Added
- Sidecars for pings and heartbeats

## [0.1.0] - 2022-06-10
### Added
- Initial commit with basic structure
