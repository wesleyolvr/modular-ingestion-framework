from asyncio import Task
from typing import Any

import requests

from core.base_connector import BaseConnector
from audit.logger import get_logger


class RESTConnector(BaseConnector):
    """
    Conector para APIs REST via HTTP GET/POST.

    Suporta autenticação via Bearer token ou API Key no header.
    """

    def __init__(
        self,
        name: str,
        url: str,
        method: str = "GET",
        headers: dict | None = None,
        params: dict | None = None,
        timeout: int = 30,
        bearer_token: str | None = None,
        api_key: str | None = None,
        api_key_header: str = "X-API-Key",
    ):
        super().__init__(name=name)
        self.url = url
        self.method = method.upper()
        self.headers = headers or {}
        # Adiciona Accept: application/json por padrão se não especificado
        if "Accept" not in self.headers:
            self.headers["Accept"] = "application/json"
        self.params = params or {}
        self.timeout = timeout
        self.logger = get_logger(name=name)
        if bearer_token:
            self.headers["Authorization"] = f"Bearer {bearer_token}"
        if api_key:
            self.headers[api_key_header] = api_key

    def fetch(self, **kwargs) -> Any:
        """
        Faz a requisição HTTP e retorna o JSON da resposta.
        kwargs sobrescrevem params configurados no construtor.
        
        Lança requests.exceptions.RequestException em caso de erro HTTP ou de conexão.
        """
        params = {**self.params, **kwargs}
        response = None
        try:
            response = requests.request(
                method=self.method,
                url=self.url,
                headers=self.headers,
                params=params if self.method == "GET" else None,
                json=params if self.method == "POST" else None,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json() if response.status_code == 200 else None
        except requests.exceptions.RequestException as e:
            self.logger.error("request_failed", error=str(e), exc_info=False, task="fetch", connector=self.name)
            # Re-lança a exceção para que o Pipeline possa tratá-la adequadamente
            raise