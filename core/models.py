from pydantic import BaseModel, Field, EmailStr, HttpUrl, field_validator
from typing import Optional

class Pedido(BaseModel):
    numero: int
    valor: float
    codigo_promocional: Optional[str] = None

    @field_validator("codigo_promocional")
    @classmethod
    def validar_codigo_promocional(cls, v):
        if v is not None and not v.startswith("PROMO"):
            raise ValueError("Código promocional deve começar com 'PROMO'")
        return v

class Contato(BaseModel):
    nome: str
    email: EmailStr
    website: Optional[HttpUrl] = None

class Municipio_IBGE(BaseModel):
    id: int = Field(gt=0)
    nome: str = Field(min_length=1)

class Selic(BaseModel):
    """Modelo Pydantic para validação de Selic."""
    data: str
    valor: str
    
class Produto(BaseModel):
    id: int = Field(gt=0, description="ID deve ser maior que zero")
    nome: str = Field(min_length=1, max_length=100)
    preco: float = Field(gt=0, le=10000, description="Preço entre 0 e 10000")
    estoque: int = Field(ge=0, description="Estoque não pode ser negativo")
    
class Usuario(BaseModel):
    id: int
    nome: str
    email: Optional[str] = None
    ativo: bool = True