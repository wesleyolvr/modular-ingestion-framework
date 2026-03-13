import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.exceptions import PipelineError, ValidationError


class TestPipelineError:
    def test_creation(self):
        error = PipelineError("algo deu errado")
        assert str(error) == "algo deu errado"

    def test_is_exception(self):
        assert issubclass(PipelineError, Exception)


class TestValidationError:
    def test_creation_with_errors(self):
        error = ValidationError(["campo ausente", "tipo inválido"])
        assert error.errors == ["campo ausente", "tipo inválido"]

    def test_inherits_from_pipeline_error(self):
        error = ValidationError(["erro"])
        assert isinstance(error, PipelineError)

    def test_message_format(self):
        error = ValidationError(["campo ausente"])
        assert "Validation failed" in str(error)
        assert "campo ausente" in str(error)

    def test_errors_attribute(self):
        errors = ["erro 1", "erro 2", "erro 3"]
        error = ValidationError(errors)
        assert len(error.errors) == 3
        assert error.errors[0] == "erro 1"

    def test_can_be_caught_as_pipeline_error(self):
        with pytest.raises(PipelineError):
            raise ValidationError(["teste"])
