"""
Transformer específico para dados da API do IBGE.

Extrai campos relevantes de municípios retornados pela API do IBGE.
"""
from transformers.base_transformer import BaseTransformer
from typing import Any


class IBGETransformer(BaseTransformer):
    """
    Transforma dados de municípios do IBGE extraindo campos relevantes.
    
    Converte estrutura aninhada da API em estrutura plana com campos:
    - id: ID do município
    - nome: Nome do município
    - uf: Sigla do estado (extraído de microrregiao.mesorregiao.UF.sigla)
    - regiao: Nome da região (extraído de microrregiao.mesorregiao.UF.regiao.nome)
    
    Exemplo:
        transformer = IBGETransformer()
        dados_transformados = transformer.transform(dados_ibge)
    """
    
    def __init__(self):
        super().__init__("IBGETransformer")
        
    def transform(self, data: Any) -> Any:
        """
        Transforma dados do IBGE extraindo campos relevantes.
        
        Args:
            data: Lista de dicionários com dados da API do IBGE
            
        Returns:
            Lista de dicionários com estrutura plana
        """
        if not isinstance(data, list):
            return data
        
        return [
            {
                "id": m["id"],
                "nome": m["nome"],
                "uf": m.get("microrregiao", {})
                    .get("mesorregiao", {})
                    .get("UF", {})
                    .get("sigla"),
                "regiao": m.get("microrregiao", {})
                    .get("mesorregiao", {})
                    .get("UF", {})
                    .get("regiao", {})
                    .get("nome"),
            }
            for m in data
        ]
