from abc import ABC, abstractmethod
from typing import Any, Callable


class BaseTransformer(ABC):
    """
    Interface base para todos os transformers.
    
    Um transformer recebe dados e retorna dados transformados.
    Pode ser usado diretamente no Pipeline como alternativa a funções callable.
    """
    
    def __init__(self, name: str):
        self.name = name
        
    @abstractmethod
    def transform(self, data: Any) -> Any:
        """
        Transforma os dados recebidos.
        
        Args:
            data: Dados a transformar (geralmente list[dict] ou dict)
            
        Returns:
            Dados transformados (mesmo tipo ou estrutura compatível)
        """
        ...
    
    def __call__(self, data: Any) -> Any:
        """
        Permite usar o transformer como callable diretamente.
        Facilita integração com Pipeline que aceita callable.
        """
        return self.transform(data)
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"