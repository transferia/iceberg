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
- `...rest` - plugin code base

## Contributing

This project is part of the Transferia ecosystem and follows its contribution guidelines. Please refer to the main [Transferia repository](https://github.com/transferia/transferia) for more information. 