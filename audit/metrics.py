from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator, model_validator


class PipelineMetrics(BaseModel):
    """
    Métricas de execução de um pipeline.

    Usa Pydantic para garantir tipos e validações em runtime.
    Campos numéricos não podem ser negativos.
    records_loaded não pode ser maior que records_fetched.
    """

    pipeline: str
    success: bool = False
    records_fetched: int = Field(default=0, ge=0)
    records_loaded: int = Field(default=0, ge=0)
    records_failed: int = Field(default=0, ge=0)
    duration_seconds: float = Field(default=0.0, ge=0)
    started_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    finished_at: datetime | None = None
    stage_failed: str | None = None
    error_message: str | None = None

    model_config = {"arbitrary_types_allowed": True, "repr": False}

    @field_validator("pipeline")
    @classmethod
    def pipeline_nao_pode_ser_vazio(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("pipeline não pode ser vazio")
        return v

    @model_validator(mode="after")
    def records_loaded_nao_pode_ser_maior_que_fetched(self) -> "PipelineMetrics":
        if self.records_loaded > self.records_fetched and self.records_fetched > 0:
            raise ValueError(
                f"records_loaded ({self.records_loaded}) não pode ser maior "
                f"que records_fetched ({self.records_fetched})"
            )
        return self

    def to_dict(self) -> dict:
        return self.model_dump(mode="json")

    def __repr__(self) -> str:
        status = "✓" if self.success else "✗"
        failed = f" failed={self.records_failed}" if self.records_failed > 0 else ""
        stage = f" stage={self.stage_failed}" if self.stage_failed else ""
        return (
            f"PipelineMetrics({status} {self.pipeline} | "
            f"fetched={self.records_fetched} loaded={self.records_loaded}"
            f"{failed}{stage} duration={self.duration_seconds}s)"
        )
        
    __str__ = __repr__