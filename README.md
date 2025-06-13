# basepak

[![PyPI - Version](https://img.shields.io/pypi/v/basepak.svg)](https://pypi.org/project/basepak)
[![Security: Bandit](https://img.shields.io/badge/security-bandit-green.svg)](https://bandit.readthedocs.io/en/latest/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/basepak.svg)](https://pypi.org/project/basepak)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/tomaDev/basepak)
-----

Basepak provides a toolset to simplify CLI utility creation for the Iguazio environment

## Installation

```console
uv pip install basepak
```

### Core Capabilities
- **Task Runner**: Simple framework for execution, retries, error handling, logging and monitoring
- **Kubernetes Operations**: Resource management, file transfer, job orchestration, and template generation
- **Iguazio Platform client**: HTTP API client with session management and retry logic
- **Logging module**: Centralized logging with rich output formatting and sensitive data masking
- **Data Types & Utilities**: Custom types for units, ranges, IP addresses, CLI parameters etc.

### Main Modules
- `tasks`: The task framework.
- `k8s_utils`: Kubernetes resource management and operations
- `platform_api`: Iguazio platform HTTP API abstract class
- `igz_mgmt_handler`: context manager for [Iguazio Management SDK](https://iguazio.github.io/igz-mgmt-sdk/)
- `log`: Enhanced logging with security features
- `units`: Data types for measurements and CLI parameters
- `stats`: Task monitoring and system validation
- `locks`: Run lock functionality to prevent unwanted concurrent script runs


## Quick Start

```python
from basepak import log

logger = log.get_logger()
logger.info('hello from basepak!')
```

## Development

### Setup Development Environment

The project uses `hatch` for management

### Available Scripts
- `hatch run lock`: Update dependency lockfile
- `hatch run upgrade`: Upgrade dependencies
- `hatch run release`: Create and push version tag

### Testing 

Run tests across multiple Python versions:
```bash
hatch test --cover --randomize --all --durations=5
```

### Code Quality

- **Security scanning**: `hatch run scan:scan`
- **Type checking**: `hatch run types:check` (WIP)
- **Coverage reporting**: Integrated with pytest

## Release Process

Releases are automated through GitHub Actions when version tags are pushed. The release workflow validates versions, builds packages, and publishes to both GitHub Releases and PyPI.

## License

`basepak` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
