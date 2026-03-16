"""
Script de exploração do PydanticValidator.

Demonstra como usar o PydanticValidator com diferentes tipos de validações
do Pydantic, incluindo constraints de Field, tipos complexos, e validações customizadas.
"""

from pydantic import Field
from core.models import Produto, Usuario, Contato, Pedido, Municipio_IBGE
from validators.pydantic_validator import PydanticValidator


print("=" * 100)
print("EXPLORANDO PYDANTIC VALIDATOR")
print("=" * 100)

# ============================================================================
# Exemplo 1: Validação básica com tipos simples
# ============================================================================
print("\n" + "-" * 100)
print("1. Validação básica com tipos simples")
print("-" * 100)


validator = PydanticValidator(model=Municipio_IBGE)

# Dados válidos
print("\n✓ Testando dados válidos:")
result = validator.validate([{"id": 1, "nome": "Parnaíba", "uf": "PI", "regiao": "Nordeste"}])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# Campo "populacao" faltando
print("\n✗ Testando campo faltando:")
result = validator.validate([{"id": 1, "nome": "Parnaíba"}])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# Tipo errado para "id"
print("\n✗ Testando tipo errado:")
result = validator.validate([{"id": "não sou int", "nome": "Parnaíba", "uf": "PI", "regiao": "Nordeste"}])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")


# ============================================================================
# Exemplo 2: Validação com Field constraints
# ============================================================================
print("\n" + "-" * 100)
print("2. Validação com Field constraints (gt, lt, min_length, etc.)")
print("-" * 100)
    

validator_produto = PydanticValidator(model=Produto)

# Dados válidos
print("\n✓ Testando dados válidos:")
result = validator_produto.validate([
    {"id": 1, "nome": "Notebook", "preco": 2500.99, "estoque": 10}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# ID inválido (menor ou igual a zero)
print("\n✗ Testando ID inválido (id <= 0):")
result = validator_produto.validate([
    {"id": 0, "nome": "Notebook", "preco": 2500.99, "estoque": 10}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# Preço fora do range
print("\n✗ Testando preço fora do range:")
result = validator_produto.validate([
    {"id": 1, "nome": "Notebook", "preco": 15000, "estoque": 10}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# Nome muito longo
print("\n✗ Testando nome muito longo:")
result = validator_produto.validate([
    {"id": 1, "nome": "A" * 150, "preco": 2500.99, "estoque": 10}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")


# ============================================================================
# Exemplo 3: Campos opcionais e valores padrão
# ============================================================================
print("\n" + "-" * 100)
print("3. Campos opcionais e valores padrão")
print("-" * 100)

validator_usuario = PydanticValidator(model=Usuario)

# Dados válidos com todos os campos
print("\n✓ Testando com todos os campos:")
result = validator_usuario.validate([
    {"id": 1, "nome": "João", "email": "joao@example.com", "ativo": True}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# Dados válidos sem campos opcionais
print("\n✓ Testando sem campos opcionais (usando defaults):")
result = validator_usuario.validate([
    {"id": 1, "nome": "Maria"}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")


# ============================================================================
# Exemplo 4: Validação de tipos especiais (Email, URL)
# ============================================================================
print("\n" + "-" * 100)
print("4. Validação de tipos especiais (Email, URL)")
print("-" * 100)





validator_contato = PydanticValidator(model=Contato)

# Dados válidos
print("\n✓ Testando dados válidos:")
result = validator_contato.validate([
    {"nome": "Empresa", "email": "contato@empresa.com", "website": "https://empresa.com"}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# Email inválido
print("\n✗ Testando email inválido:")
result = validator_contato.validate([
    {"nome": "Empresa", "email": "email-invalido", "website": "https://empresa.com"}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# URL inválida
print("\n✗ Testando URL inválida:")
result = validator_contato.validate([
    {"nome": "Empresa", "email": "contato@empresa.com", "website": "não é uma url"}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")


# ============================================================================
# Exemplo 5: Validação customizada com field_validator
# ============================================================================
print("\n" + "-" * 100)
print("5. Validação customizada com field_validator")
print("-" * 100)





validator_pedido = PydanticValidator(model=Pedido)

# Dados válidos sem código promocional
print("\n✓ Testando sem código promocional:")
result = validator_pedido.validate([
    {"numero": 1001, "valor": 150.50}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# Dados válidos com código promocional válido
print("\n✓ Testando com código promocional válido:")
result = validator_pedido.validate([
    {"numero": 1001, "valor": 150.50, "codigo_promocional": "PROMO2024"}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# Código promocional inválido
print("\n✗ Testando código promocional inválido:")
result = validator_pedido.validate([
    {"numero": 1001, "valor": 150.50, "codigo_promocional": "DESCONTO2024"}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")


# ============================================================================
# Exemplo 6: Múltiplos registros com erros
# ============================================================================
print("\n" + "-" * 100)
print("6. Múltiplos registros com alguns erros")
print("-" * 100)

# Alguns válidos, alguns inválidos
print("\n✗ Testando múltiplos registros (alguns válidos, alguns inválidos):")
result = validator_produto.validate([
    {"id": 1, "nome": "Produto A", "preco": 100.0, "estoque": 5},  # Válido
    {"id": -1, "nome": "Produto B", "preco": 200.0, "estoque": 3},  # ID inválido
    {"id": 2, "nome": "", "preco": 300.0, "estoque": 7},  # Nome vazio
    {"id": 3, "nome": "Produto D", "preco": 50000.0, "estoque": 2},  # Preço muito alto
    {"id": 4, "nome": "Produto E", "preco": 150.0, "estoque": -1},  # Estoque negativo
])
print(f"  valid: {result.valid}")
print(f"  total errors: {len(result.errors)}")
print("  errors:")
for error in result.errors:
    print(f"    - {error}")


# ============================================================================
# Exemplo 7: Validação de duplicatas (unique_by)
# ============================================================================
print("\n" + "-" * 100)
print("7. Validação de duplicatas entre registros (unique_by)")
print("-" * 100)


# Validator com validação de duplicatas
validator_municipio = PydanticValidator(model=Municipio_IBGE, unique_by="id")

# Dados válidos sem duplicatas
print("\n✓ Testando dados válidos sem duplicatas:")
result = validator_municipio.validate([
    {"id": 1, "nome": "Parnaíba", "uf": "PI", "regiao": "Nordeste"},
    {"id": 2, "nome": "Teresina", "uf": "PI", "regiao": "Nordeste"},
    {"id": 3, "nome": "Fortaleza", "uf": "CE", "regiao": "Nordeste"}
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# Duplicatas detectadas
print("\n✗ Testando duplicatas:")
result = validator_municipio.validate([
    {"id": 1, "nome": "Parnaíba", "uf": "PI", "regiao": "Nordeste"},
    {"id": 2, "nome": "Teresina", "uf": "PI", "regiao": "Nordeste"},
    {"id": 1, "nome": "Parnaíba Duplicada", "uf": "PI", "regiao": "Nordeste"}  # ID duplicado
])
print(f"  valid: {result.valid}")
print(f"  errors: {result.errors}")

# Combinando validação de schema + duplicatas
print("\n✗ Testando schema inválido + duplicatas:")
result = validator_municipio.validate([
    {"id": -1, "nome": "Parnaíba", "uf": "PI", "regiao": "Nordeste"},  # ID inválido (schema error)
    {"id": 1, "nome": "Teresina", "uf": "PI", "regiao": "Nordeste"},
    {"id": 1, "nome": "Fortaleza", "uf": "CE", "regiao": "Nordeste"}  # Duplicata
])
print(f"  valid: {result.valid}")
print(f"  total errors: {len(result.errors)}")
print("  errors:")
for error in result.errors:
    print(f"    - {error}")
