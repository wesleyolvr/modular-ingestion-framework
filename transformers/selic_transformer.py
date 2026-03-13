"""
Transformer específico para dados da API do BCB (Banco Central do Brasil) - Selic.

Transforma dados da série histórica da taxa Selic em formato adequado para armazenamento.
"""
from transformers.base_transformer import BaseTransformer
from typing import Any
from datetime import datetime


class SelicTransformer(BaseTransformer):
    """
    Transforma dados da API do BCB (SGS 11 - Taxa Selic) em formato estruturado.
    
    Converte formato da API (data em DD/MM/YYYY, valor como string) para formato
    adequado para banco de dados (data em YYYY-MM-DD, cálculos de taxa anualizada).
    
    Campos gerados:
    - data: Data no formato YYYY-MM-DD
    - taxa_ao_dia: Taxa diária (float)
    - taxa_anualizada_pct: Taxa anualizada em percentual (calculada)
    - fonte: Identificação da fonte (fixo: "BCB SGS 11")
    
    Exemplo:
        transformer = SelicTransformer()
        dados_transformados = transformer.transform(dados_bcb)
    """
    
    def __init__(self, fonte: str = "BCB SGS 11"):
        """
        Inicializa o transformer da Selic.
        
        Args:
            fonte: Nome da fonte de dados (padrão: "BCB SGS 11")
        """
        self.fonte = fonte
    
    def transform(self, data: Any) -> Any:
        """
        Transforma dados da API do BCB.
        
        Args:
            data: Lista de dicionários com dados da API do BCB
                Formato esperado: [{"data": "12/03/2025", "valor": "13.75"}, ...]
        
        Returns:
            Lista de dicionários transformados
        """
        if not isinstance(data, list):
            return data
        
        resultado = []
        for r in data:
            try:
                # Converte data de DD/MM/YYYY para YYYY-MM-DD
                data_obj = datetime.strptime(r["data"], "%d/%m/%Y")
                data_formatada = data_obj.strftime("%Y-%m-%d")
                
                # Converte valor para float
                taxa_diaria = float(r["valor"])
                
                # Calcula taxa anualizada: ((1 + taxa_diaria/100) ^ 252 - 1) * 100
                # 252 = número de dias úteis em um ano
                taxa_anualizada = round(((1 + taxa_diaria / 100) ** 252 - 1) * 100, 4)
                
                resultado.append({
                    "data": data_formatada,
                    "taxa_ao_dia": taxa_diaria,
                    "taxa_anualizada_pct": taxa_anualizada,
                    "fonte": self.fonte,
                })
            except (KeyError, ValueError, TypeError) as e:
                # Ignora registros com formato inválido
                continue
        
        return resultado
    
    def __repr__(self) -> str:
        return f"SelicTransformer(fonte={self.fonte!r})"
