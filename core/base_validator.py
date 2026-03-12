from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ValidationResult:
    valid: bool
    errors: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.valid


class BaseValidator(ABC):
    """
    Interface base para todos os validadores.

    Todo validador recebe dados brutos e retorna
    um ValidationResult indicando se os dados são válidos.
    """

    @abstractmethod
    def validate(self, data: Any) -> ValidationResult:
        """
        Valida os dados recebidos.
        Retorna ValidationResult com status e lista de erros.
        """
        ...
