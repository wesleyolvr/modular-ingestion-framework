import pytest
from unittest.mock import MagicMock

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.pipeline import Pipeline, ValidationError, PipelineError
from core.base_connector import BaseConnector
from core.base_loader import BaseLoader
from core.base_validator import BaseValidator, ValidationResult


# ── Fakes ──────────────────────────────────────────────────────────────────

class FakeConnector(BaseConnector):
    def __init__(self, data):
        super().__init__(name="fake")
        self._data = data

    def fetch(self, **kwargs):
        return self._data


class FakeLoader(BaseLoader):
    def __init__(self):
        self.loaded = None

    def load(self, data):
        self.loaded = data


class FakeValidator(BaseValidator):
    def __init__(self, valid: bool, errors=None):
        self._valid = valid
        self._errors = errors or []

    def validate(self, data):
        return ValidationResult(valid=self._valid, errors=self._errors)


# ── Testes ──────────────────────────────────────────────────────────────────

class TestPipelineSuccess:
    def test_run_without_validator_or_transform(self):
        data = [{"id": 1}]
        loader = FakeLoader()

        pipeline = Pipeline(
            name="test",
            connector=FakeConnector(data),
            loader=loader,
        )

        metrics = pipeline.run()

        assert metrics.success is True
        assert loader.loaded == data
        assert metrics.records_fetched == 1
        assert metrics.records_loaded == 1

    def test_run_with_transform(self):
        data = [{"id": 1, "nome": "Parnaíba"}]
        loader = FakeLoader()

        pipeline = Pipeline(
            name="test",
            connector=FakeConnector(data),
            loader=loader,
            transform=lambda records: [{"id": r["id"]} for r in records],
        )

        metrics = pipeline.run()

        assert loader.loaded == [{"id": 1}]
        assert metrics.success is True

    def test_run_with_valid_validator(self):
        data = [{"id": 1}]
        loader = FakeLoader()

        pipeline = Pipeline(
            name="test",
            connector=FakeConnector(data),
            loader=loader,
            validator=FakeValidator(valid=True),
        )

        metrics = pipeline.run()
        assert metrics.success is True

    def test_metrics_duration_is_set(self):
        loader = FakeLoader()
        pipeline = Pipeline(
            name="test",
            connector=FakeConnector([]),
            loader=loader,
        )
        metrics = pipeline.run()
        assert metrics.duration_seconds >= 0


class TestPipelineValidationFailure:
    def test_raises_validation_error(self):
        pipeline = Pipeline(
            name="test",
            connector=FakeConnector([{"id": 1}]),
            loader=FakeLoader(),
            validator=FakeValidator(valid=False, errors=["campo ausente"]),
        )

        with pytest.raises(ValidationError) as exc_info:
            pipeline.run()

        assert "campo ausente" in exc_info.value.errors

    def test_loader_not_called_on_validation_failure(self):
        loader = FakeLoader()
        pipeline = Pipeline(
            name="test",
            connector=FakeConnector([{"id": 1}]),
            loader=loader,
            validator=FakeValidator(valid=False),
        )

        with pytest.raises(ValidationError):
            pipeline.run()

        assert loader.loaded is None


class TestPipelineConnectorFailure:
    def test_raises_pipeline_error_on_connector_exception(self):
        class BrokenConnector(BaseConnector):
            def __init__(self):
                super().__init__(name="broken")
            def fetch(self, **kwargs):
                raise ConnectionError("timeout")

        pipeline = Pipeline(
            name="test",
            connector=BrokenConnector(),
            loader=FakeLoader(),
        )

        with pytest.raises(PipelineError):
            pipeline.run()
