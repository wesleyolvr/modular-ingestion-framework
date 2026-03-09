from typing import Any

from core.base_validator import BaseValidator, ValidationResult


class SchemaValidator(BaseValidator):
    """
    Valida que os dados contêm os campos obrigatórios
    e que os tipos batem com o schema definido.

    schema: dict mapeando nome do campo → tipo esperado
    Exemplo: {"id": int, "nome": str, "valor": float}
    """

    def __init__(self, schema: dict[str, type], required: list[str] | None = None):
        self.schema = schema
        self.required = required or list(schema.keys())

    def validate(self, data: Any) -> ValidationResult:
        errors: list[str] = []

        records = data if isinstance(data, list) else [data]

        for i, record in enumerate(records):
            if not isinstance(record, dict):
                errors.append(f"Record {i}: esperado dict, recebido {type(record).__name__}")
                continue

            for field in self.required:
                if field not in record:
                    errors.append(f"Record {i}: campo obrigatório ausente '{field}'")
                    continue

                expected_type = self.schema.get(field)
                if expected_type and not isinstance(record[field], expected_type):
                    actual = type(record[field]).__name__
                    errors.append(
                        f"Record {i}: campo '{field}' esperado {expected_type.__name__}, "
                        f"recebido {actual}"
                    )

        return ValidationResult(valid=len(errors) == 0, errors=errors)
