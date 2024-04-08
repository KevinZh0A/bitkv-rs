<div align="center">
<h1>Bitkv</h1>
</div>

<div align="center">

[<img alt="github" src="https://img.shields.io/badge/github-KevinZh0A%2Fbitkv-8da0cb?style=for-the-badge&logo=GitHub&label=github&color=8da0cb" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/KevinZh0A/bitkv-rs/rust.yml?branch=main&style=for-the-badge&logo=Github-Actions&cacheSeconds= d" height="22">][CI-url]
[<img alt="Codecov" src="https://img.shields.io/codecov/c/gh/KevinZh0A/bitkv-rs?token=6DNS3IF3MO&style=for-the-badge&logo=codecov" height="22">][codecov-url]
<img alt="GitHub License" src="https://img.shields.io/github/license/KevinZh0A/bitkv-rs?style=for-the-badge&logo=license&label=license" height="22">

An efficient key-value storage engine, designed for fast reading and writing, which is inspired by [Bitcask][bitcask_url].

See [Introduction](#introduction), [Installation](#installation) and [Usages](#usages) for more details.

</div>

## Introduction

Bitkv is a high-performance key-value storage system written in Rust. It leverages a log-structured design with an append-only write approach to deliver exceptional speed, reliability, and scalability.

### Features

- **Efficient Key-Value Storage:** Optimized for fast read and write operations with minimal overhead.
- **Diverse Index:** Support BTree, Skiplist, BPlusTree index for multiple index strategies.
- **MemMap files for efficient I/O:**  For fast index reconstruction adn quick startup times
- **Low latency per item read or written:**
    - Write latency:  `~ 7 µs`
    - Read latency:  `~ 3 ns`
- **Concurrency Support:**   fine-grained locking minimizes contentions.
- **WriteBatch transaction:**   commit a batch of write enhance isolation.


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
- [X] Optimize hintfile storage structure to support the memtable build faster 
- [X] Http api server
- [X] Tests
- [X] Benchmark
- [ ] Documentation 
- [ ] Increased use of flatbuffers option to support faster reading speed
- [ ] Extend support for Redis Data Types

## Contribution

Contributions to this project are welcome! If you find any issues or have suggestions for improvements, please raise an issue or submit a pull request.


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