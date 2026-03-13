from datetime import datetime, timezone
import time
from typing import Any, Callable
from core.base_connector import BaseConnector
from core.base_loader import BaseLoader
from core.base_validator import BaseValidator
from audit.logger import get_logger
from audit.metrics import PipelineMetrics
from core.exceptions import PipelineError, ValidationError
from transformers.base_transformer import BaseTransformer
class Pipeline:
    """
    Orquestra o fluxo: fetch → validate → transform → load.

    Parâmetros:
        name        : identificador do pipeline (usado nos logs)
        connector   : como buscar os dados (BaseConnector)
        loader      : onde salvar os dados (BaseLoader)
        validator   : como validar os dados (BaseValidator, opcional)
        transform   : função de transformação (callable ou BaseTransformer, opcional)
    """

    def __init__(
        self,
        name: str,
        connector: BaseConnector,
        loader: BaseLoader,
        validator: BaseValidator | None = None,
        transform: Callable[[Any], Any] | BaseTransformer | None = None,
    ):
        self.name = name
        self.connector = connector
        self.loader = loader
        self.validator = validator
        self.transform = transform
        self.logger = get_logger(pipeline=name)

    def run(self, **fetch_kwargs) -> PipelineMetrics:
        """
        Executa o pipeline completo.
        Retorna métricas de execução.
        Lança PipelineError em caso de falha.
        """
        metrics = PipelineMetrics(pipeline=self.name)
        start_time = time.monotonic()
        self.logger.info("pipeline_started")
        current_stage = "fetch"

        try:
            # 1. Fetch
            self.logger.info("fetching_data", connector=self.connector.name)
            raw_data = self.connector.fetch(**fetch_kwargs)
            metrics.records_fetched = self._count(raw_data)
            self.logger.info("fetch_complete", records=metrics.records_fetched)
            current_stage = "validation"

            # 2. Validate
            if self.validator:
                self.logger.info("validating_data")
                result = self.validator.validate(raw_data)
                if not result:
                    self.logger.error("validation_failed", errors=result.errors)
                    raise ValidationError(result.errors)
                self.logger.info("validation_passed")
            current_stage = "transform"

            # 3. Transform
            data = raw_data
            if self.transform:
                self.logger.info("transforming_data", transform=self.transform.__class__.__name__)
                # Suporta tanto callable quanto BaseTransformer
                if hasattr(self.transform, 'transform'):
                    # É um BaseTransformer
                    data = self.transform.transform(data)
                elif callable(self.transform):
                    # É uma função callable
                    data = self.transform(data)
                else:
                    raise TypeError(f"transform deve ser callable ou BaseTransformer, recebido: {type(self.transform)}")
                self.logger.info("transform_complete", records=self._count(data), transform=self.transform.__class__.__name__)
            current_stage = "load"

            # 4. Load
            self.logger.info("loading_data", loader=self.loader.__class__.__name__, records=metrics.records_fetched)
            self.loader.load(data)
            metrics.records_loaded = self._count(data)
            self.logger.info("load_complete", records=metrics.records_loaded, loader=self.loader.__class__.__name__)

            metrics.success = True

        except ValidationError as e:
            metrics.success = False
            metrics.stage_failed = "validation"
            metrics.error_message = str(e)
            self.logger.error("validation_failed", errors=e.errors)
            raise

        except PipelineError as e:
            metrics.success = False
            metrics.stage_failed = getattr(e, "stage_failed", "unknown")
            metrics.error_message = str(e)
            self.logger.error("pipeline_error", exc_info=True, error_message=str(e))
            raise

        except Exception as e:
            # Converte qualquer exceção não tratada em PipelineError
            metrics.success = False
            metrics.stage_failed = current_stage
            metrics.error_message = str(e)
            self.logger.error("pipeline_error", exc_info=True, error_message=str(e))
            raise PipelineError(f"Pipeline failed at stage '{current_stage}': {str(e)}") from e

        finally:
            metrics.duration_seconds = round(time.monotonic() - start_time, 3)
            metrics.finished_at = datetime.now(timezone.utc)
            status = "success" if metrics.success else "failed"
            extra = {"stage_failed": metrics.stage_failed} if metrics.stage_failed else {}
            self.logger.info("pipeline_finished", status=status, duration=metrics.duration_seconds, **extra)

        return metrics

    @staticmethod
    def _count(data: Any) -> int:
        try:
            return len(data)
        except TypeError:
            return 0
