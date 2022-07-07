# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.1.0] - 2022-07-06

### Changed
- `client/cli` has been refactored to utilize new structures and communicate with server using a custom protocol

### Added
- `server` module, which implements a boundary to client from backend
- `storage` module, which serves as distribuited database for server nodes
- `collector` module, which serves as proxy from server to collector node in DAG
- `protocol` module, which provides common utilities for protocol logic between server and client
- `client` module, which provides an abstraction over client's logic, including server communication
- `file_provider` module, which provides an abstraction over data transmission from client

### Changed
- Bully leader election algorithm now try-catches the send and recv of messages to avoid connection error

## [3.0.0] - 2022-07-06
### Removed
- `ed-comments` no longer receives a list of columns to keep, as to preserve th
  filter semantics
- `filter-comments` no longer receives a list of columns to keep. This could be
  reintroduced.

### Changed
- Moved `join` as a top level command in the CLI
- Pika heartbeat set to 300 seconds
- Tasks now receive the name of the queue where the message was fetched from,
  additionally to the message itself

### Fixed
- Commented out the leader election which was introducing a deadlock
- Bug on message identifiers on node

### Added
- `collector` command on the CLI
- `Collector` task
- Messages are no longer required to carry an `id` field. If missing, it is built
  by hashing the message.
- Script to monitor all containers

## [2.3.1] - 2022-07-06
### Fixed
- Bug in node
- Config for joiner

## [2.3.0] - 2022-07-05
### Added
- Nodes now have an identifier and a storage
- Storage is recreated from local disk on start
- Added identifier to EoS messages
- Added identifier to data messages
- Dependencies values and messages are now persisted to local storage
- Duplicate messages are dropped on reception
- Docker in docker now mounts a data directory to each sub-container


## [2.2.0] - 2022-07-05
### Added
- Generic Node implementation to handle joins
- Sentiment joiner
- Comment filter
- Post mean sentiment
- Best meme downloader
- Student comment filter
- School Joiner

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
