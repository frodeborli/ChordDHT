# ChordDHT: A C# Implementation of Chord Distributed Hash Table

## Table of Contents

 * Introduction
 * Installation
 * Usage
   * Launching a Single Node
   * Launching Multiple Nodes
 * HTTP API
   * GET Request
   * PUT Request
   * DELETE Request
 * Implementation Details
   * Finger Table
   * Address Space
   * Hash Function
   * Chord Hops
 * Limitations
 * Future Work
 * Contributing
 * License

## Introduction

ChordDHT is a C# implementation of the Chord Distributed Hash Table (DHT). This implementation is primarily intended for educational purposes and supports core features such as data storage, retrieval, and deletion.

## Installation

To install, download the latest binary ChordDHT from the releases page and move it to your desired directory.

## Usage

### Launching a Single Node

To start a single node, you can use the following syntax:

```bash
ChordDHT serve my_hostname 8000 othernode1:8000 othernode2:8000 ...
```

### Launching Multiple Nodes

For launching multiple instances on the same machine:

```bash
ChordDHT multiserve my_hostname 8000 16
```

This will start 16 nodes listening on ports 8000 to 8015.

### Running a benchmark

For launching a benchmark:

```bash
ChordDHT benchmark hostname:8000 other_host:8000
```

This will benchmark the cluster by connecting to hostname and other_host on port 8000.

## HTTP API

### GET Request

Retrieve a value by key:

```bash
curl http://any_node:8000/storage/any_key
```

### PUT Request

Store a value with a given key:

```bash
curl -X PUT -d "Hello World" http://any_node:8000/storage/any_key
```

### DELETE Request

Delete a value by key:

```bash
curl -X DELETE http://any_node:8000/storage/any_key
```

## Implementation Details

### Finger Table

The implementation utilizes a finger table with up to 64 nodes.

### Address Space

The system has a 64-bit address space.

### Hash Function

SHA-512 is used for hashing keys.

### Chord Hops

HTTP responses from the nodes include an X-Chord-Hops header that indicates the number of hops needed to find the node.

## Limitations

The network does not support joining or leaving after launch. Finger table limited to 64 nodes.

## Future Work

 * Support for dynamic joining and leaving of nodes.
 * Enhanced security features.

## Contributing

Please read the CONTRIBUTING.md file for details on how to contribute.

## License

This project is licensed under the MIT License.