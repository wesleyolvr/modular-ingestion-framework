# Script de exemplo usando o pipeline com dados do IBGE
# Execute com: poetry run python scripts/pipeline_ibge.py

from pydantic import BaseModel

from core.pipeline import Pipeline
from connectors.rest_connector import RESTConnector
from validators.pydantic_validator import PydanticValidator
from loaders.json_loader import JsonLoader


class Municipio(BaseModel):
    """Modelo Pydantic para validação de municípios."""
    id: int
    nome: str


pipeline = Pipeline(
    name="municipios_ibge",
    connector=RESTConnector(
        name="ibge_api",
        url="https://servicodados.ibge.gov.br/api/v1/localidades/estados/PI/municipios",
    ),
    validator=PydanticValidator(model=Municipio, unique_by="id"),
    loader=JsonLoader(path="output/municipios.json"),
)

metrics = pipeline.run()
print(metrics)
