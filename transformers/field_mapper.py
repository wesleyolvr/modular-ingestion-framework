"""
Mapeia campos de registros, incluindo campos aninhados via notação "a.b.c".

Útil para renomear campos, extrair campos aninhados, ou transformar estrutura de dados.
"""
from transformers.base_transformer import BaseTransformer
from typing import Any


class FieldMapper(BaseTransformer):
    """
    Mapeia campos de registros usando um dicionário de mapeamento.
    
    Suporta campos aninhados usando notação de ponto (ex: "microrregiao.mesorregiao.UF.sigla").
    
    Exemplo:
        transformer = FieldMapper(mapping={
            "id": "identifier",
            "nome": "name",
            "uf": "microrregiao.mesorregiao.UF.sigla",  # campo aninhado
        })
    """
    
    def __init__(self, mapping: dict[str, str]):
        """
        Inicializa o transformer de mapeamento de campos.
        
        Args:
            mapping: Dicionário {campo_destino: campo_origem} onde campo_origem pode usar notação "a.b.c"
        """
        if not isinstance(mapping, dict):
            raise TypeError(f"mapping deve ser dict, recebido: {type(mapping)}")
        self.mapping = mapping
        
    def transform(self, data: Any) -> Any:
        """
        Aplica o mapeamento de campos aos registros.
        
        Args:
            data: Lista de dicionários ou dicionário único
            
        Returns:
            Dados com campos mapeados
        """
        if isinstance(data, list):
            return [self._map_record(record) for record in data]
        elif isinstance(data, dict):
            return self._map_record(data)
        else:
            return data
    
    def _map_record(self, record: dict) -> dict:
        """Aplica mapeamento a um único registro."""
        mapped = {}
        for dest_field, source_field in self.mapping.items():
            value = self._get_nested_value(record, source_field)
            if value is not None:
                mapped[dest_field] = value
        return mapped
    
    def _get_nested_value(self, record: dict, field_path: str) -> Any:
        """
        Extrai valor de campo aninhado usando notação de ponto.
        
        Exemplo:
            _get_nested_value({"a": {"b": {"c": 123}}}, "a.b.c") -> 123
        """
        parts = field_path.split(".")
        value = record
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
                if value is None:
                    return None
            else:
                return None
        return value
    
    def __repr__(self) -> str:
        return f"FieldMapper(mapping={len(self.mapping)} fields)"