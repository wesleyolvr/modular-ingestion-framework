import csv
import json
from pathlib import Path
from typing import Any
import pandas as pd

from core.base_connector import BaseConnector


class FileConnector(BaseConnector):
    """
    Conector para arquivos locais: CSV, JSON, JSONL, XLSX.
    """

    SUPPORTED = {".csv", ".json", ".jsonl", ".xlsx"}

    def __init__(self, name: str, path: str | Path, encoding: str = "utf-8"):
        super().__init__(name=name)
        self.path = Path(path)
        self.encoding = encoding
        self.suffixs = {
            ".csv" : self._read_csv,
            ".json" : self._read_json,
            ".jsonl" : self._read_jsonl,
            ".xlsx" : self._read_excel,
        }

        if self.path.suffix not in self.SUPPORTED:
            raise ValueError(
                f"Formato não suportado: {self.path.suffix}. "
                f"Suportados: {self.SUPPORTED}"
            )

    def fetch(self, **kwargs) -> Any:
        """
        Lê um arquivo e retorna os dados como uma lista de dicionários.
        Lança ValueError em caso de erro ao ler o arquivo.
        """
        suffix = self.path.suffix
        try:
            return self.suffixs[suffix]()
        except Exception as e:
            raise ValueError(f"Erro ao ler o arquivo {self.path}: {e}")

    def _read_csv(self) -> list[dict]:
        """
        Lê um arquivo CSV e retorna os dados como uma lista de dicionários.
        """
        try:
            with self.path.open(encoding=self.encoding) as f:
                return list(csv.DictReader(f))
        except Exception as e:
            raise ValueError(f"Erro ao ler o arquivo CSV: {e}")

    def _read_json(self) -> Any:
        """
        Lê um arquivo JSON e retorna os dados como um dicionário.
        """
        try:
            with self.path.open(encoding=self.encoding) as f:
                return json.load(f)
        except Exception as e:
            raise ValueError(f"Erro ao ler o arquivo JSON: {e}")

    def _read_jsonl(self) -> list[dict]:
        """
        Lê um arquivo JSONL e retorna os dados como uma lista de dicionários.
        """
        try:
            with self.path.open(encoding=self.encoding) as f:
                return [json.loads(line) for line in f if line.strip()]
        except Exception as e:
            raise ValueError(f"Erro ao ler o arquivo JSONL: {e}")
        
    def _read_excel(self) -> list[dict]:
        """
        Lê um arquivo Excel e retorna os dados do pandas DataFrame.
        """
        try:
            return pd.read_excel(self.path, engine="openpyxl")
        except Exception as e:
            raise ValueError(f"Erro ao ler o arquivo Excel: {e}")