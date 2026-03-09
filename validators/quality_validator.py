from typing import Any

from core.base_validator import BaseValidator, ValidationResult


class QualityValidator(BaseValidator):
    """
    Valida qualidade dos dados:
    - campos nulos
    - duplicatas por chave
    - valores fora de range

    Parâmetros:
        not_null    : lista de campos que não podem ser nulos
        unique_by   : campo usado para detectar duplicatas
        ranges      : dict de campo → (min, max)
    """

    def __init__(
        self,
        not_null: list[str] | None = None,
        unique_by: str | None = None,
        ranges: dict[str, tuple] | None = None,
    ):
        self.not_null = not_null or []
        self.unique_by = unique_by
        self.ranges = ranges or {}

    def validate(self, data: Any) -> ValidationResult:
        errors: list[str] = []
        records = data if isinstance(data, list) else [data]

        seen_keys: set = set()

        for i, record in enumerate(records):
            if not isinstance(record, dict):
                continue

            # Nulos
            for field in self.not_null:
                if record.get(field) is None:
                    errors.append(f"Record {i}: campo '{field}' não pode ser nulo")

            # Duplicatas
            if self.unique_by:
                key = record.get(self.unique_by)
                if key in seen_keys:
                    errors.append(
                        f"Record {i}: valor duplicado em '{self.unique_by}' = {key!r}"
                    )
                else:
                    seen_keys.add(key)

            # Ranges
            for field, (min_val, max_val) in self.ranges.items():
                value = record.get(field)
                if value is not None:
                    if value < min_val or value > max_val:
                        errors.append(
                            f"Record {i}: '{field}' = {value} fora do range [{min_val}, {max_val}]"
                        )

        return ValidationResult(valid=len(errors) == 0, errors=errors)
