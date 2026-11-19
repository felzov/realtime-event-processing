import tomllib
from pathlib import Path

import event_pipeline

PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"


def test_installed_version_matches_pyproject() -> None:
    # Proves the package imports and its installed metadata resolves. A stale install only
    # shows up here in a direct or IDE pytest run: `make` (`uv run`) re-syncs first, and CI's
    # `uv sync --locked` stops on a stale lock before pytest starts.
    with PYPROJECT.open("rb") as f:
        expected = tomllib.load(f)["project"]["version"]

    assert event_pipeline.__version__ == expected
