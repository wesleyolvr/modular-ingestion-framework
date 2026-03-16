from typing import Any

from pydantic import BaseModel, ValidationError

from core.base_validator import BaseValidator, ValidationResult


class PydanticValidator(BaseValidator):
    """
    Validador usando Pydantic para validação de schemas complexos.

    Permite usar toda a expressividade do Pydantic (validações customizadas,
    Field constraints, tipos complexos, etc.) mantendo a interface BaseValidator.
    
    Também suporta validação de duplicatas entre registros via unique_by,
    combinando validação de schema (Pydantic) com validação de qualidade.

    Exemplo:
        from pydantic import BaseModel, Field
        
        class Municipio(BaseModel):
            id: int = Field(gt=0)
            nome: str = Field(min_length=1)
            populacao: int = Field(gt=0)
        
        # Apenas validação de schema
        validator = PydanticValidator(model=Municipio)
        
        # Validação de schema + duplicatas
        validator = PydanticValidator(model=Municipio, unique_by="id")
        
        result = validator.validate([{"id": 1, "nome": "Parnaíba", "populacao": 150000}])
    """

    def __init__(self, model: type[BaseModel], unique_by: str | None = None):
        """
        Inicializa o validador com um modelo Pydantic.

        Args:
            model: Classe BaseModel do Pydantic que define o schema de validação
            unique_by: Campo usado para detectar duplicatas entre registros (opcional)
        """
        if not issubclass(model, BaseModel):
            raise TypeError(f"model deve ser uma subclasse de BaseModel, recebido: {type(model)}")
        self.model = model
        self.unique_by = unique_by

    def validate(self, data: Any) -> ValidationResult:
        """
        Valida os dados usando o modelo Pydantic e verifica duplicatas se unique_by estiver definido.

        Args:
            data: Dados a validar (dict único ou lista de dicts)

        Returns:
            ValidationResult com status de validação e lista de erros formatados
        """
        errors: list[str] = []
        records = data if isinstance(data, list) else [data]
        
        # Set para rastrear valores únicos quando unique_by está definido
        seen_keys: set = set()

        for i, record in enumerate(records):
            if not isinstance(record, dict):
                errors.append(
                    f"Record {i}: esperado dict, recebido {type(record).__name__}"
                )
                continue

            # 1. Validação de schema usando Pydantic
            try:
                # Tenta criar uma instância do modelo Pydantic
                self.model(**record)
            except ValidationError as e:
                # Formata os erros do Pydantic para o formato do framework
                for err in e.errors():
                    field_path = " → ".join(str(loc) for loc in err["loc"])
                    error_msg = err["msg"]
                    error_type = err["type"]
                    
                    # Formata mensagem de erro de forma legível
                    if field_path:
                        formatted_error = (
                            f"Record {i}: campo '{field_path}' — {error_msg} "
                            f"(tipo: {error_type})"
                        )
                    else:
                        formatted_error = f"Record {i}: {error_msg} (tipo: {error_type})"
                    
                    errors.append(formatted_error)
                # Se houver erro de schema, pula a validação de duplicatas para este record
                continue
            except Exception as exc:  # pylint: disable=broad-except
                # Captura outros erros inesperados que não sejam ValidationError
                # (ex: erros de serialização, problemas de memória, etc.)
                errors.append(f"Record {i}: erro inesperado — {str(exc)}")
                continue

            # 2. Validação de duplicatas (se unique_by estiver definido)
            if self.unique_by:
                key = record.get(self.unique_by)
                if key in seen_keys:
                    errors.append(
                        f"Record {i}: valor duplicado em '{self.unique_by}' = {key!r}"
                    )
                else:
                    seen_keys.add(key)

        return ValidationResult(valid=len(errors) == 0, errors=errors)
