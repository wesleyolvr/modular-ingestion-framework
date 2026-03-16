"""
Exemplo real: busca municípios do Piauí via API pública do IBGE,
valida o schema, e salva em arquivo JSON local.

API: https://servicodados.ibge.gov.br/api/v1/localidades/estados/{UF}/municipios

Execute:
    python examples/pipeline_ibge.py
"""

import sys
from pathlib import Path

# Adiciona o root do projeto ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from connectors.rest_connector import RESTConnector
from validators.pydantic_validator import PydanticValidator
from loaders.json_loader import JsonLoader
from core.pipeline import Pipeline
from core.models import Municipio_IBGE

def transform_municipios(data: list[dict]) -> list[dict]:
    """Extrai apenas os campos relevantes de cada município."""
    return [
        {
            "id": m["id"],
            "nome": m["nome"],
            "uf": m["microrregiao"]["mesorregiao"]["UF"]["sigla"],
            "regiao": m["microrregiao"]["mesorregiao"]["UF"]["regiao"]["nome"],
        }
        for m in data
    ]


if __name__ == "__main__":
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/PI/municipios"
    pipeline = Pipeline(
        name="ibge_municipios_pi",
        connector=RESTConnector(
            name="ibge_api",
            url=url,
        ),
        validator=PydanticValidator(model=Municipio_IBGE, unique_by="id"),
        loader=JsonLoader(path="output/municipios_pi_ibge.json"),
        transform=transform_municipios,
    )

    metrics = pipeline.run()
    print(metrics)
