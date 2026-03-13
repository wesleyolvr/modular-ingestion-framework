"""
Exemplo usando transformers do módulo transformers.

Demonstra como usar transformers individuais e TransformPipeline
com o Pipeline principal.
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
from transformers import (
    IBGETransformer,
    FieldMapper,
    FilterTransformer,
    EnrichTransformer,
    TransformPipeline,
)


def exemplo_transformers_individuais():
    """Exemplo usando transformers individuais."""
    print("\n" + "=" * 60)
    print("Exemplo 1: Usando IBGETransformer individual")
    print("=" * 60)
    
    pipeline = Pipeline(
        name="ibge_com_transformer",
        connector=RESTConnector(
            name="ibge_api",
            url="https://servicodados.ibge.gov.br/api/v1/localidades/estados/PI/municipios",
        ),
        validator=PydanticValidator(model=Municipio_IBGE, unique_by="id"),
        loader=JsonLoader(path="output/municipios_com_transformer.json"),
        transform=IBGETransformer(),  # Usa transformer em vez de função
    )
    
    metrics = pipeline.run()
    print(f"✓ Pipeline executado: {metrics}")


def exemplo_transform_pipeline():
    """Exemplo usando TransformPipeline para compor múltiplas transformações."""
    print("\n" + "=" * 60)
    print("Exemplo 2: Usando TransformPipeline (composição)")
    print("=" * 60)
    
    # Cria pipeline de transformers
    transform_pipeline = TransformPipeline([
        # 1. Extrai campos do IBGE
        IBGETransformer(),
        # 2. Filtra apenas municípios com ID > 2200000
        FilterTransformer(condition=lambda r: r.get("id", 0) > 2200000),
        # 3. Adiciona metadados
        EnrichTransformer(
            fields={"fonte": "API_IBGE", "versao": "1.0"},
            add_timestamp=True
        ),
    ])
    
    pipeline = Pipeline(
        name="ibge_com_pipeline_transformers",
        connector=RESTConnector(
            name="ibge_api",
            url="https://servicodados.ibge.gov.br/api/v1/localidades/estados/PI/municipios",
        ),
        validator=PydanticValidator(model=Municipio_IBGE, unique_by="id"),
        loader=JsonLoader(path="output/municipios_com_pipeline.json"),
        transform=transform_pipeline,
    )
    
    metrics = pipeline.run()
    print(f"✓ Pipeline executado: {metrics}")


def exemplo_field_mapper():
    """Exemplo usando FieldMapper para renomear campos."""
    print("\n" + "=" * 60)
    print("Exemplo 3: Usando FieldMapper para renomear campos")
    print("=" * 60)
    
    # Dados de exemplo
    dados_exemplo = [
        {"id": 1, "nome": "Parnaíba", "ativo": True},
        {"id": 2, "nome": "Teresina", "ativo": False},
    ]
    
    mapper = FieldMapper(mapping={
        "identifier": "id",
        "name": "nome",
        "is_active": "ativo",
    })
    
    resultado = mapper.transform(dados_exemplo)
    print(f"Dados originais: {dados_exemplo}")
    print(f"Dados mapeados: {resultado}")


if __name__ == "__main__":
    try:
        exemplo_field_mapper()
        exemplo_transformers_individuais()
        exemplo_transform_pipeline()
    except Exception as e:
        print(f"\n✗ Erro: {e}")
        import traceback
        traceback.print_exc()
