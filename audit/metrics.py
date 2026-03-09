from dataclasses import dataclass, field
from datetime import datetime, timezone

@dataclass
class PipelineMetrics:
    """
    Data class para metrics de pipeline.
    """
    pipeline: str
    success: bool = False
    records_fetched: int = 0
    records_loaded: int = 0
    duration_seconds: float = 0.0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "success": self.success,
            "records_fetched": self.records_fetched,
            "records_loaded": self.records_loaded,
            "duration_seconds": self.duration_seconds,
            "started_at": self.started_at.isoformat(),
        }

    def __repr__(self) -> str:
        status = "✓" if self.success else "✗"
        return (
            f"PipelineMetrics({status} {self.pipeline} | "
            f"fetched={self.records_fetched} loaded={self.records_loaded} "
            f"duration={self.duration_seconds}s)"
        )
