import pytest
from pathlib import Path
from datetime import datetime, timezone

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pydantic import ValidationError
from audit.metrics import PipelineMetrics


class TestPipelineMetricsDefaults:
    def test_creation_with_defaults(self):
        metrics = PipelineMetrics(pipeline="test")
        assert metrics.pipeline == "test"
        assert metrics.success is False
        assert metrics.records_fetched == 0
        assert metrics.records_loaded == 0
        assert metrics.records_failed == 0
        assert metrics.duration_seconds == 0.0
        assert metrics.stage_failed is None
        assert metrics.error_message is None

    def test_started_at_has_default(self):
        metrics = PipelineMetrics(pipeline="test")
        assert metrics.started_at is not None
        assert isinstance(metrics.started_at, datetime)

    def test_finished_at_none_by_default(self):
        metrics = PipelineMetrics(pipeline="test")
        assert metrics.finished_at is None


class TestPipelineMetricsValidation:
    def test_pipeline_empty_raises(self):
        with pytest.raises(ValidationError):
            PipelineMetrics(pipeline="")

    def test_pipeline_whitespace_raises(self):
        with pytest.raises(ValidationError):
            PipelineMetrics(pipeline="   ")

    def test_negative_records_fetched_raises(self):
        with pytest.raises(ValidationError):
            PipelineMetrics(pipeline="test", records_fetched=-1)

    def test_negative_records_loaded_raises(self):
        with pytest.raises(ValidationError):
            PipelineMetrics(pipeline="test", records_loaded=-1)

    def test_negative_duration_raises(self):
        with pytest.raises(ValidationError):
            PipelineMetrics(pipeline="test", duration_seconds=-1.0)

    def test_loaded_greater_than_fetched_raises(self):
        with pytest.raises(ValidationError):
            PipelineMetrics(pipeline="test", records_fetched=10, records_loaded=20)

    def test_loaded_equal_fetched_valid(self):
        metrics = PipelineMetrics(pipeline="test", records_fetched=10, records_loaded=10)
        assert metrics.records_loaded == 10

    def test_loaded_less_than_fetched_valid(self):
        metrics = PipelineMetrics(pipeline="test", records_fetched=10, records_loaded=5)
        assert metrics.records_loaded == 5


class TestPipelineMetricsOutput:
    def test_to_dict(self):
        metrics = PipelineMetrics(pipeline="test", success=True, records_fetched=10, records_loaded=10)
        d = metrics.to_dict()
        assert isinstance(d, dict)
        assert d["pipeline"] == "test"
        assert d["success"] is True
        assert d["records_fetched"] == 10

    def test_repr_success(self):
        metrics = PipelineMetrics(pipeline="test", success=True, records_fetched=10, records_loaded=10)
        text = repr(metrics)
        assert "✓" in text
        assert "test" in text
        assert "fetched=10" in text

    def test_repr_failed(self):
        metrics = PipelineMetrics(pipeline="test", success=False)
        text = repr(metrics)
        assert "✗" in text

    def test_repr_with_stage_failed(self):
        metrics = PipelineMetrics(pipeline="test", success=False, stage_failed="validation")
        text = repr(metrics)
        assert "stage=validation" in text

    def test_repr_with_records_failed(self):
        metrics = PipelineMetrics(pipeline="test", records_failed=3)
        text = repr(metrics)
        assert "failed=3" in text

    def test_str_equals_repr(self):
        metrics = PipelineMetrics(pipeline="test")
        assert str(metrics) == repr(metrics)
