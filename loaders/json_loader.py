import json
from pathlib import Path
from typing import Any

from core.base_loader import BaseLoader


class JsonLoader(BaseLoader):
    """
    Loader que salva os dados em um arquivo JSON local.
    Útil para testes, demos e pipelines que não precisam de banco.
    """

    def __init__(self, path: str | Path, indent: int = 2, encoding: str = "utf-8"):
        self.path = Path(path)
        self.indent = indent
        self.encoding = encoding

    def load(self, data: Any) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding=self.encoding) as f:
            json.dump(data, f, indent=self.indent, ensure_ascii=False, default=str)
