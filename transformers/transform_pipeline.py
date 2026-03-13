"""
Encadeia vários transformers em sequência usando o padrão de design Pipeline.

Permite compor múltiplas transformações que são aplicadas em sequência.
Cada transformer recebe o resultado do anterior.
"""
from transformers.base_transformer import BaseTransformer
from typing import Any, Callable


class TransformPipeline(BaseTransformer):
    """
    Pipeline de transformers que aplica múltiplas transformações em sequência.
    
    Exemplo:
        pipeline = TransformPipeline([
            FieldMapper(mapping={"nome": "name", "id": "identifier"}),
            FilterTransformer(condition=lambda r: r["ativo"]),
            EnrichTransformer(fields={"fonte": "API", "timestamp": "2025-01-01"}),
        ])
    """
    
    def __init__(self, transformers: list[BaseTransformer | Callable[[Any], Any]]):
        """
        Inicializa o pipeline com uma lista de transformers.
        
        Args:
            transformers: Lista de transformers (BaseTransformer ou callable) a aplicar em sequência
        """
        self.transformers = transformers
        
    def transform(self, data: Any) -> Any:
        """
        Aplica todos os transformers em sequência.
        
        Args:
            data: Dados a transformar
            
        Returns:
            Dados após todas as transformações
        """
        result = data
        for transformer in self.transformers:
            if isinstance(transformer, BaseTransformer):
                result = transformer.transform(result)
            elif callable(transformer):
                result = transformer(result)
            else:
                raise TypeError(f"Transformer deve ser BaseTransformer ou callable, recebido: {type(transformer)}")
        return result
    
    def __repr__(self) -> str:
        return f"TransformPipeline(transformers={len(self.transformers)})"