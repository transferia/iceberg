# Iceberg Provider

This is an incubator project for a new provider in the Transferia ecosystem. It's part of the [Transferia](https://github.com/transferia/transferia) project.

## Overview

Iceberg is a provider implementation that handles data processing and transformation tasks. It's designed to be integrated into the Transferia ecosystem as a new data processing provider.

## Prerequisites

- Go 1.23 or higher
- Docker (for running tests with testcontainers)
- Make

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/transferia/iceberg.git
cd iceberg
```

2. Install dependencies:
```bash
go mod download
```

3. Build the project:
```bash
make build
```

## Testing

Run the test suite:
```bash
make test
```

For detailed test reports:
```bash
make run-tests
```

Test reports will be generated in the `reports/` directory.

## Development

The project uses standard Go tooling and Make for common tasks:

- `make clean` - Remove build artifacts
- `make build` - Build the project
- `make test` - Run tests
- `make run-tests` - Run tests with detailed reporting

## Project Structure

- `cmd/` - Main application entry points, it's custom main file same as in transfer, but with extra plugin
- `reports/` - Test reports
- `binaries/` - Compiled binaries
- `doc/` - Documentation, including design documents
- `...rest` - plugin code base

## Key Features

### Iceberg Table Reading

The Iceberg Provider implements a robust Table Reading mechanism that:

1. Provides efficient data access through optimized manifest processing
2. Ensures data consistency through snapshot-based reading
3. Implements advanced optimization techniques like partition pruning and column projection

For more details, see the [Iceberg Table Reading Design Document](doc/storage.md).

### Snapshot Sink

The Iceberg Provider implements a powerful Snapshot Sink mechanism that:

1. Efficiently transforms incoming data into Parquet files
2. Tracks files generated by each worker
3. Coordinates file registration using a central coordinator
4. Atomically commits all files to the target table in a single transaction

For more details, see the [Snapshot Sink Design Document](doc/snapshot-sink.md).

### Streaming Sink

The Iceberg Provider also implements a Streaming Sink mechanism that:

1. Processes data in real-time as it arrives
2. Maintains continuous data ingestion with minimal latency
3. Provides exactly-once semantics for data delivery
4. Supports automatic schema evolution and data type mapping

**Note**: It's for append-only sources, not for CDC

## Contributing

This project is part of the Transferia ecosystem and follows its contribution guidelines. Please refer to the main [Transferia repository](https://github.com/transferia/transferia) for more information. 