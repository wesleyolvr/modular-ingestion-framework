from validators.schema_validator import SchemaValidator
from validators.quality_validator import QualityValidator

print("-" * 100)
print("Validando o schema dos dados, ou seja, se os dados contêm os campos obrigatórios e que os tipos batem com o schema definido.")
print("-" * 100)

schema = {"id": int, "nome": str}
required = ["id", "nome"]
validator = SchemaValidator(schema=schema, required=required)
result = validator.validate([{"id": 1, "nome": "Parnaíba"}])
print("valid: ", result.valid)
print("errors: ", result.errors)
result = validator.validate([{"id": 1, "nome": 1}])
print("valid: ", result.valid)
print("errors: ", result.errors)
print("valid: ", result.valid)
print("errors: ", result.errors)
result = validator.validate([{"id": 1}])
print("valid: ", result.valid)
print("errors: ", result.errors)
result = validator.validate([{"nome": "Parnaíba", "idade": 20}])
print("valid: ", result.valid)
print("errors: ", result.errors)
result = validator.validate([{"id": 1, "nome": "Parnaíba", "idade": 20}])
print("valid: ", result.valid)
print("errors: ", result.errors)



print("-" * 100)
print("Validando a qualidade dos dados, ou seja, se os dados contêm campos nulos, duplicatas e valores fora de range.")
print("-" * 100)
validator = QualityValidator(not_null=["id"], unique_by="id", ranges={"valor": (0, 100)}, contains={"valor": [1, 2, 3]})
result = validator.validate([{"id": 1}])
print("valid: ", result.valid)
print("errors: ", result.errors)
result = validator.validate([{"id": None}])
print("valid: ", result.valid)
print("errors: ", result.errors)
result = validator.validate([{"id": 200, "valor": 99.99}])
print("valid: ", result.valid)
print("errors: ", result.errors)
result = validator.validate([{"id": 20, "valor": 250}])
print("valid: ", result.valid)
print("errors: ", result.errors)
result = validator.validate([{"id": 1}, {"id": 2}])
print("valid: ", result.valid)
print("errors: ", result.errors)
