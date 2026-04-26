# Contributing

Thank you for your interest in contributing to the Cursus Python Client!

## Development Setup

```bash
git clone https://github.com/cursus-io/cursus-python.git
cd cursus-python
uv sync --extra dev
```

## Running Tests

```bash
uv run pytest tests/unit/ -v
```

## Code Quality

```bash
uv run ruff check src/ tests/
uv run ruff format src/ tests/
uv run mypy src/cursus/ --ignore-missing-imports
```

## Commit Messages

Use conventional commit format:

```
feat: add snappy compression support
fix: resolve consumer offset tracking for multi-partition
docs: update configuration reference
test: add producer retry integration test
```

## Pull Requests

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Ensure all tests pass and lint is clean
5. Submit a pull request

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
