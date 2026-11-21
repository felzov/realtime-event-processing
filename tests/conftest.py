import pytest

from event_pipeline.config import Settings


@pytest.fixture(autouse=True)
def _no_settings_from_the_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    # A variable set in the developer's shell (or CI) would otherwise change what `Settings` sees
    # in every test. Derived from the model, so a new setting is covered automatically.
    for name in Settings.model_fields:
        monkeypatch.delenv(name.upper(), raising=False)
