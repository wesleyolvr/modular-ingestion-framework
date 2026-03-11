from abc import ABC, abstractmethod
from typing import Any


class BaseConnector(ABC):
    """
    Interface base para todos os conectores de dados.

    Todo conector deve implementar o método fetch(),
    que retorna os dados brutos da fonte.
    """

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def fetch(self, **kwargs) -> Any:
        """
        Busca dados da fonte.
        Retorna os dados brutos — a transformação é responsabilidade do pipeline.
        """
        pass

    def __repr__(self) -> str:
        """
        Retorna uma representação em string do conector.

        Returns:
            str: Representação em string do conector.
        """
        return f"{self.__class__.__name__}(name={self.name!r})"
