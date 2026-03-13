"""
Adiciona campos fixos aos registros (enriquecimento de dados).

Útil para adicionar metadados como fonte, timestamp, versão, etc.
"""
from transformers.base_transformer import BaseTransformer
from typing import Any
from datetime import datetime, timezone


class EnrichTransformer(BaseTransformer):
    """
    Adiciona campos fixos a cada registro.
    
    Exemplo:
        transformer = EnrichTransformer(
            fields={"fonte": "API_IBGE", "versao": "1.0"},
            add_timestamp=True
        )
        # Adiciona "fonte", "versao" e "timestamp" a cada registro
    """
    
    def __init__(self, fields: dict[str, Any] | None = None, add_timestamp: bool = False):
        """
        Inicializa o transformer de enriquecimento.
        
        Args:
            fields: Dicionário com campos fixos a adicionar a cada registro
            add_timestamp: Se True, adiciona campo "timestamp" com timestamp atual (UTC)
        """
        self.fields = fields or {}
        self.add_timestamp = add_timestamp
        
    def transform(self, data: Any) -> Any:
        """
        Adiciona campos fixos aos registros.
        
        Args:
            data: Lista de dicionários ou dicionário único
            
        Returns:
            Dados com campos adicionados
        """
        if isinstance(data, list):
            return [self._enrich_record(record) for record in data]
        elif isinstance(data, dict):
            return self._enrich_record(data)
        else:
            return data
    
    def _enrich_record(self, record: dict) -> dict:
        """Adiciona campos a um único registro."""
        enriched = {**record, **self.fields}
        if self.add_timestamp:
            enriched["timestamp"] = datetime.now(timezone.utc).isoformat()
        return enriched
    
    def __repr__(self) -> str:
        fields_str = f"fields={len(self.fields)}" if self.fields else "no_fields"
        timestamp_str = "timestamp=True" if self.add_timestamp else ""
        return f"EnrichTransformer({fields_str}, {timestamp_str})"