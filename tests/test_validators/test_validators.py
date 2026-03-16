import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pydantic import BaseModel, Field, field_validator
from typing import Optional

from validators.pydantic_validator import PydanticValidator


class TestPydanticValidator:
    """Testes para o PydanticValidator"""

    def test_valid_data(self):
        class Municipio(BaseModel):
            id: int
            nome: str

        validator = PydanticValidator(model=Municipio)
        result = validator.validate([{"id": 1, "nome": "Parnaíba"}])
        assert result.valid is True
        assert result.errors == []

    def test_missing_required_field(self):
        class Municipio(BaseModel):
            id: int
            nome: str

        validator = PydanticValidator(model=Municipio)
        result = validator.validate([{"id": 1}])
        assert result.valid is False
        assert any("nome" in e for e in result.errors)

    def test_wrong_type(self):
        class Municipio(BaseModel):
            id: int
            nome: str

        validator = PydanticValidator(model=Municipio)
        result = validator.validate([{"id": "nao_sou_int", "nome": "Parnaíba"}])
        assert result.valid is False
        assert any("id" in e for e in result.errors)

    def test_field_constraints(self):
        class Produto(BaseModel):
            id: int = Field(gt=0)
            preco: float = Field(gt=0, le=1000)

        validator = PydanticValidator(model=Produto)

        # ID inválido (<= 0)
        result = validator.validate([{"id": 0, "preco": 100.0}])
        assert result.valid is False
        assert any("id" in e for e in result.errors)

        # Preço inválido (muito alto)
        result = validator.validate([{"id": 1, "preco": 2000.0}])
        assert result.valid is False
        assert any("preco" in e for e in result.errors)

        # Dados válidos
        result = validator.validate([{"id": 1, "preco": 100.0}])
        assert result.valid is True

    def test_optional_fields(self):
        class Usuario(BaseModel):
            id: int
            nome: str
            email: Optional[str] = None

        validator = PydanticValidator(model=Usuario)

        # Sem email (válido)
        result = validator.validate([{"id": 1, "nome": "João"}])
        assert result.valid is True

        # Com email (válido)
        result = validator.validate([{"id": 1, "nome": "João", "email": "joao@example.com"}])
        assert result.valid is True

    def test_custom_validator(self):
        class Pedido(BaseModel):
            numero: int
            codigo: Optional[str] = None

            @field_validator("codigo")
            @classmethod
            def validar_codigo(cls, v):
                if v is not None and not v.startswith("PROMO"):
                    raise ValueError("Código deve começar com 'PROMO'")
                return v

        validator = PydanticValidator(model=Pedido)

        # Código válido
        result = validator.validate([{"numero": 1, "codigo": "PROMO2024"}])
        assert result.valid is True

        # Código inválido
        result = validator.validate([{"numero": 1, "codigo": "DESCONTO"}])
        assert result.valid is False
        assert any("codigo" in e for e in result.errors)

    def test_multiple_records_with_errors(self):
        class Municipio(BaseModel):
            id: int = Field(gt=0)
            nome: str

        validator = PydanticValidator(model=Municipio)
        data = [
            {"id": 1, "nome": "Parnaíba"},  # Válido
            {"id": -1, "nome": "Teresina"},  # ID inválido
            {"id": 2},  # Nome faltando
        ]
        result = validator.validate(data)
        assert result.valid is False
        assert len(result.errors) == 2

    def test_single_dict_not_list(self):
        class Municipio(BaseModel):
            id: int
            nome: str

        validator = PydanticValidator(model=Municipio)
        result = validator.validate({"id": 1, "nome": "Parnaíba"})
        assert result.valid is True

    def test_invalid_record_type(self):
        class Municipio(BaseModel):
            id: int
            nome: str

        validator = PydanticValidator(model=Municipio)
        result = validator.validate([{"id": 1, "nome": "Parnaíba"}, "não sou dict"])
        assert result.valid is False
        assert any("esperado dict" in e for e in result.errors)

    def test_invalid_model_type(self):
        """Testa que apenas BaseModel pode ser usado como model"""
        class NotAModel:
            pass

        try:
            PydanticValidator(model=NotAModel)
            assert False, "Deveria ter levantado TypeError"
        except TypeError:
            pass  # Esperado

    def test_unique_by_passes(self):
        """Testa validação de duplicatas quando não há duplicatas"""
        class Municipio(BaseModel):
            id: int
            nome: str

        validator = PydanticValidator(model=Municipio, unique_by="id")
        result = validator.validate([
            {"id": 1, "nome": "Parnaíba"},
            {"id": 2, "nome": "Teresina"}
        ])
        assert result.valid is True

    def test_unique_by_fails_on_duplicate(self):
        """Testa validação de duplicatas quando há duplicatas"""
        class Municipio(BaseModel):
            id: int
            nome: str

        validator = PydanticValidator(model=Municipio, unique_by="id")
        result = validator.validate([
            {"id": 1, "nome": "Parnaíba"},
            {"id": 1, "nome": "Teresina"}  # ID duplicado
        ])
        assert result.valid is False
        assert any("duplicado" in e for e in result.errors)

    def test_unique_by_with_schema_validation(self):
        """Testa que validação de schema acontece antes da validação de duplicatas"""
        class Municipio(BaseModel):
            id: int = Field(gt=0)
            nome: str

        validator = PydanticValidator(model=Municipio, unique_by="id")
        
        # Primeiro record tem erro de schema (id inválido), então não chega na validação de duplicatas
        result = validator.validate([
            {"id": -1, "nome": "Parnaíba"},  # ID inválido (schema error)
            {"id": 1, "nome": "Teresina"}    # Válido
        ])
        assert result.valid is False
        # Deve ter erro de schema relacionado ao campo 'id', não de duplicata
        assert any("id" in e.lower() and "duplicado" not in e.lower() for e in result.errors)
        assert not any("duplicado" in e for e in result.errors)  # Não deve ter erro de duplicata
        
        # Agora testa com schema válido mas duplicatas
        result = validator.validate([
            {"id": 1, "nome": "Parnaíba"},
            {"id": 1, "nome": "Teresina"}  # Duplicata
        ])
        assert result.valid is False
        assert any("duplicado" in e for e in result.errors)
