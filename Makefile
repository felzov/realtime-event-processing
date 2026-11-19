.RECIPEPREFIX = >

.PHONY: install lint format typecheck test check

install:
> uv sync

lint:
> uv run --locked ruff check .
> uv run --locked ruff format --check .

format:
> uv run --locked ruff format .
> uv run --locked ruff check --fix .

typecheck:
> uv run --locked mypy

# Unit tests; no database or broker needed.
test:
> uv run --locked pytest -m "not integration"

# What CI runs on each Python version (CI also scans git history for secrets).
check: lint typecheck test
