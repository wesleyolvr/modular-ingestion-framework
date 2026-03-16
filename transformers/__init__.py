"""
Módulo de transformers para pipelines de ingestão de dados.

Transformers são componentes reutilizáveis que transformam dados de forma padronizada.
Podem ser usados individualmente ou compostos em um TransformPipeline.

Exemplo básico:
    from transformers import FieldMapper, FilterTransformer, EnrichTransformer, TransformPipeline
    
    transformer = TransformPipeline([
        FieldMapper(mapping={"id": "identifier", "nome": "name"}),
        FilterTransformer(condition=lambda r: r.get("ativo")),
        EnrichTransformer(fields={"fonte": "API"}, add_timestamp=True),
    ])
    
    # Usar no Pipeline
    pipeline = Pipeline(
        name="exemplo",
        connector=...,
        loader=...,
        transform=transformer,  # Aceita BaseTransformer ou callable
    )
"""

from transformers.base_transformer import BaseTransformer
from transformers.transform_pipeline import TransformPipeline
from transformers.enrich_transformer import EnrichTransformer
from transformers.filter_transformer import FilterTransformer
from transformers.field_mapper import FieldMapper
from transformers.ibge_transformer import IBGETransformer
from transformers.selic_transformer import SelicTransformer

__all__ = [
    "BaseTransformer",
    "TransformPipeline",
    "EnrichTransformer",
    "FilterTransformer",
    "FieldMapper",
    "IBGETransformer",
    "SelicTransformer",
]
