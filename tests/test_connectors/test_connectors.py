import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from connectors.rest_connector import RESTConnector
from connectors.file_connector import FileConnector


# ── RESTConnector ─────────────────────────────────────────────────────────

class TestRESTConnectorInit:
    def test_bearer_token_sets_authorization_header(self):
        connector = RESTConnector(
            name="test", url="https://api.example.com",
            bearer_token="my-token",
        )
        assert connector.headers["Authorization"] == "Bearer my-token"

    def test_api_key_sets_header(self):
        connector = RESTConnector(
            name="test", url="https://api.example.com",
            api_key="secret-key",
        )
        assert connector.headers["X-API-Key"] == "secret-key"

    def test_api_key_custom_header(self):
        connector = RESTConnector(
            name="test", url="https://api.example.com",
            api_key="secret-key", api_key_header="Authorization-Key",
        )
        assert connector.headers["Authorization-Key"] == "secret-key"

    def test_method_uppercased(self):
        connector = RESTConnector(name="test", url="https://api.example.com", method="post")
        assert connector.method == "POST"


class TestRESTConnectorFetch:
    @patch("connectors.rest_connector.requests.request")
    def test_fetch_get_success(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": 1}]
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        connector = RESTConnector(name="test", url="https://api.example.com")
        result = connector.fetch()

        assert result == [{"id": 1}]
        mock_request.assert_called_once_with(
            method="GET", url="https://api.example.com",
            headers={}, params={}, json=None, timeout=30,
        )

    @patch("connectors.rest_connector.requests.request")
    def test_fetch_post_sends_json_body(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": True}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        connector = RESTConnector(
            name="test", url="https://api.example.com",
            method="POST", params={"query": "test"},
        )
        connector.fetch()

        call_kwargs = mock_request.call_args
        assert call_kwargs.kwargs["json"] == {"query": "test"}
        assert call_kwargs.kwargs["params"] is None

    @patch("connectors.rest_connector.requests.request")
    def test_fetch_kwargs_override_params(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        connector = RESTConnector(
            name="test", url="https://api.example.com",
            params={"formato": "json"},
        )
        connector.fetch(formato="xml", extra="value")

        call_kwargs = mock_request.call_args
        assert call_kwargs.kwargs["params"]["formato"] == "xml"
        assert call_kwargs.kwargs["params"]["extra"] == "value"

    @patch("connectors.rest_connector.requests.request")
    def test_fetch_custom_timeout(self, mock_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        connector = RESTConnector(name="test", url="https://api.example.com", timeout=60)
        connector.fetch()

        assert mock_request.call_args.kwargs["timeout"] == 60


# ── FileConnector ─────────────────────────────────────────────────────────

class TestFileConnectorInit:
    def test_unsupported_format_raises(self):
        with pytest.raises(ValueError, match="Formato não suportado"):
            FileConnector(name="test", path="data.txt")

    def test_supported_formats(self):
        for ext in [".csv", ".json", ".jsonl"]:
            connector = FileConnector(name="test", path=f"data{ext}")
            assert connector.path.suffix == ext


class TestFileConnectorFetch:
    def test_fetch_csv(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,nome\n1,Parnaíba\n2,Teresina\n", encoding="utf-8")

        connector = FileConnector(name="test", path=str(csv_file))
        result = connector.fetch()

        assert len(result) == 2
        assert result[0]["id"] == "1"
        assert result[0]["nome"] == "Parnaíba"

    def test_fetch_json(self, tmp_path):
        json_file = tmp_path / "data.json"
        json_file.write_text(
            json.dumps([{"id": 1, "nome": "Parnaíba"}], ensure_ascii=False),
            encoding="utf-8",
        )

        connector = FileConnector(name="test", path=str(json_file))
        result = connector.fetch()

        assert result == [{"id": 1, "nome": "Parnaíba"}]

    def test_fetch_jsonl(self, tmp_path):
        jsonl_file = tmp_path / "data.jsonl"
        lines = [
            json.dumps({"id": 1, "nome": "Parnaíba"}),
            json.dumps({"id": 2, "nome": "Teresina"}),
        ]
        jsonl_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

        connector = FileConnector(name="test", path=str(jsonl_file))
        result = connector.fetch()

        assert len(result) == 2
        assert result[1]["nome"] == "Teresina"

    def test_fetch_file_not_found(self, tmp_path):
        connector = FileConnector(name="test", path=str(tmp_path / "inexistente.json"))
        with pytest.raises(FileNotFoundError, match="Path não encontrado"):
            connector.fetch()

    def test_fetch_path_is_directory(self, tmp_path):
        connector = FileConnector(name="test", path=str(tmp_path) + ".json")
        # Cria diretório com extensão .json para simular caso
        dir_path = Path(str(tmp_path) + ".json")
        dir_path.mkdir(parents=True, exist_ok=True)

        connector_dir = FileConnector(name="test", path=str(dir_path))
        with pytest.raises(FileNotFoundError, match="Path não é de um arquivo"):
            connector_dir.fetch()

    def test_fetch_empty_csv(self, tmp_path):
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("id,nome\n", encoding="utf-8")

        connector = FileConnector(name="test", path=str(csv_file))
        result = connector.fetch()

        assert result == []

    def test_fetch_jsonl_skips_empty_lines(self, tmp_path):
        jsonl_file = tmp_path / "data.jsonl"
        content = '{"id": 1}\n\n{"id": 2}\n\n'
        jsonl_file.write_text(content, encoding="utf-8")

        connector = FileConnector(name="test", path=str(jsonl_file))
        result = connector.fetch()

        assert len(result) == 2

    def test_fetch_json_nested(self, tmp_path):
        json_file = tmp_path / "nested.json"
        data = {"usuarios": [{"id": 1}], "meta": {"total": 1}}
        json_file.write_text(json.dumps(data), encoding="utf-8")

        connector = FileConnector(name="test", path=str(json_file))
        result = connector.fetch()

        assert result["meta"]["total"] == 1
