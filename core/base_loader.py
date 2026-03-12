from abc import ABC, abstractmethod
from typing import Any


class BaseLoader(ABC):
    """
    Interface base para todos os loaders.

    Todo loader recebe dados transformados e os persiste
    no destino configurado (banco, S3, arquivo, etc).
    """

    @abstractmethod
    def load(self, data: Any) -> None:
        """
        Persiste os dados no destino.
        """
        ...
