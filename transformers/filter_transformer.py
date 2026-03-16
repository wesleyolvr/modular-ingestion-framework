"""
Filtra registros baseado em uma condição.

Útil para remover registros que não atendem critérios específicos.
"""
from transformers.base_transformer import BaseTransformer
from typing import Any, Callable


class FilterTransformer(BaseTransformer):
    """
    Filtra registros baseado em uma função de condição.
    
    Exemplo:
        # Filtra apenas registros ativos
        transformer = FilterTransformer(condition=lambda r: r.get("ativo") is True)
        
        # Filtra registros com ID maior que 1000
        transformer = FilterTransformer(condition=lambda r: r.get("id", 0) > 1000)
    """
    
    def __init__(self, condition: Callable[[dict], bool]):
        """
        Inicializa o transformer de filtro.
        
        Args:
            condition: Função que recebe um registro (dict) e retorna True para manter, False para remover
        """
        if not callable(condition):
            raise TypeError(f"condition deve ser callable, recebido: {type(condition)}")
        self.condition = condition
        
    def transform(self, data: Any) -> Any:
        """
        Filtra registros baseado na condição.
        
        Args:
            data: Lista de dicionários ou dicionário único
            
        Returns:
            Lista filtrada (se entrada era lista) ou registro único (se entrada era dict e passou no filtro)
        """
        if isinstance(data, list):
            return [record for record in data if self.condition(record)]
        elif isinstance(data, dict):
            return data if self.condition(data) else {}
        else:
            return data
    
    def __repr__(self) -> str:
        return f"FilterTransformer(condition={self.condition.__name__ if hasattr(self.condition, '__name__') else 'lambda'})"