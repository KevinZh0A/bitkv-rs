<div align="center">
<h1>Bitkv</h1>
</div>

<div align="center">



[<img alt="github" src="https://img.shields.io/badge/github-KevinZh0A%2Fbitkv-8da0cb?style=for-the-badge&logo=GitHub&label=github&color=8da0cb" height="22">][Github-url]

[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/KevinZh0A/bitkv-rs/rust.yml?branch=main&style=for-the-badge&logo=Github-Actions&cacheSeconds=3600" height="22">][CI-url]

[<img alt="Codecov" src="https://img.shields.io/codecov/c/gh/KevinZh0A/bitkv-rs?token=6DNS3IF3MO&style=for-the-badge&logo=codecov" height="22">][codecov-url]

<img alt="GitHub License" src="https://img.shields.io/github/license/KevinZh0A/bitkv-rs?style=for-the-badge&logo=license&label=license" height="22">

An efficient key-value storage engine, designed for fast reading and writing, which is inspired by [Bitcask][bitcask_url].

See [Introduction](#introduction), [Installation](#installation) and [Usages](#usages) for more details.

</div>

## Introduction

bitkv-rs is a high-performance key-value storage system base in rust, featuring a log-structured filesystem and append-only write approach strategy. Leveraging the Rust's powerful type system and concurrency control model, bitkv-rs offers both safety and speed for data storage operations. Designed with scalability in mind, it supports efficient data retrieval and storage across multiple crates, making it ideal for a wide range of applications, from embedded systems to large-scale, distributed data stores.

## Features

- **Efficient Key-Value Storage:** Optimized for fast read and write operations with minimal overhead.

- **Low latency per item read or written:**  
    - Write latency:  `~ 7 µs` 
    - Read latency:  `~ 3 ns`

- **Type Safety:** Utilizes Rust's strong typing to ensure data integrity.
- 
- **Concurrency Support:**  fine-grained locking strategy minimizes contentions and ensures atomicity.

## Installation

To use bitkv in your project, add it as a dependency in your Cargo.toml file:

  ```toml
  [dependencies]
  bitkv = "0.2.1"
  ```
Then, run cargo build to download and compile bitkv-rs and its dependencies.

For more detailed setup and compilation instructions, visit the Bitkv-rs GitHub repository.

## Usages
Please see [`examples`].

For detailed usage and API documentation, refer to the [bitkv-rs Documentation](https://docs.rs/bitkv-rs).

## TODO

- [X] Basic error handling
- [X] Merge files during compaction
- [X] Configurable compaction triggers and thresholds
- [X] WriteBactch transaction
- [X] Use mmap to read data file that on disk.
- [ ] Increased use of flatbuffers build options to support faster reading speed
- [ ] Optimize hintfile storage structure to support the memtable build faster 
- [X] Http api server
- [X] Tests
- [X] Benchmark
- [ ] Documentation
- [ ] Handle database corruption
- [ ] Extend protocol support for Redis

#### License

<sup>
Bitkv-rs is licensed under the [MIT license](https://github.com/example/bitkv-rs/blob/main/LICENSE-MIT), permitting use in both open source and private projects.
</sup>
<br>
<sub>
This license grants you the freedom to use bitkv-rs in your own projects, under the condition that the original license and copyright notice are included with any substantial portions of the Bitkv-rs software.
</sub>


[Github-url]: https://github.com/KevinZh0A/bitkv-rs
[CI-url]: https://github.com/KevinZh0A/bitkv-rs/actions/workflows/rust.yml
[doc-url]: https://docs.rs/bitkv

[crates-url]: https://crates.io/crates/bitkv
[codecov-url]: https://app.codecov.io/gh/KevinZh0A/bitkv-rs
[bitcask_url]: https://riak.com/assets/bitcask-intro.pdf
[`examples`]: https://github.com/KevinZh0A/bitkv-rs/tree/main/examples