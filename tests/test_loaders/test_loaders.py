import pytest
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from loaders.json_loader import JsonLoader


# ── JsonLoader ────────────────────────────────────────────────────────────

class TestJsonLoader:
    def test_load_list_of_dicts(self, tmp_path):
        output = tmp_path / "output.json"
        loader = JsonLoader(path=str(output))
        data = [{"id": 1, "nome": "Parnaíba"}, {"id": 2, "nome": "Teresina"}]

        loader.load(data)

        saved = json.loads(output.read_text(encoding="utf-8"))
        assert saved == data

    def test_load_single_dict(self, tmp_path):
        output = tmp_path / "output.json"
        loader = JsonLoader(path=str(output))
        data = {"id": 1, "nome": "Parnaíba"}

        loader.load(data)

        saved = json.loads(output.read_text(encoding="utf-8"))
        assert saved == data

    def test_load_creates_directory(self, tmp_path):
        output = tmp_path / "sub" / "dir" / "output.json"
        loader = JsonLoader(path=str(output))

        loader.load([{"id": 1}])

        assert output.exists()

    def test_load_overwrites_existing(self, tmp_path):
        output = tmp_path / "output.json"
        output.write_text('["old"]', encoding="utf-8")

        loader = JsonLoader(path=str(output))
        loader.load([{"id": 1}])

        saved = json.loads(output.read_text(encoding="utf-8"))
        assert saved == [{"id": 1}]

    def test_load_custom_indent(self, tmp_path):
        output = tmp_path / "output.json"
        loader = JsonLoader(path=str(output), indent=4)

        loader.load([{"id": 1}])

        content = output.read_text(encoding="utf-8")
        assert "    " in content

    def test_load_with_datetime(self, tmp_path):
        output = tmp_path / "output.json"
        loader = JsonLoader(path=str(output))
        data = [{"created": datetime(2025, 1, 1, 12, 0, 0)}]

        loader.load(data)

        saved = json.loads(output.read_text(encoding="utf-8"))
        assert saved[0]["created"] == "2025-01-01 12:00:00"

    def test_load_empty_list(self, tmp_path):
        output = tmp_path / "output.json"
        loader = JsonLoader(path=str(output))

        loader.load([])

        saved = json.loads(output.read_text(encoding="utf-8"))
        assert saved == []

    def test_load_nested_data(self, tmp_path):
        output = tmp_path / "output.json"
        loader = JsonLoader(path=str(output))
        data = [{"id": 1, "meta": {"regiao": "Nordeste", "uf": "PI"}}]

        loader.load(data)

        saved = json.loads(output.read_text(encoding="utf-8"))
        assert saved[0]["meta"]["uf"] == "PI"

    def test_load_unicode_characters(self, tmp_path):
        output = tmp_path / "output.json"
        loader = JsonLoader(path=str(output))
        data = [{"nome": "São João do Piauí", "desc": "Município com acentuação"}]

        loader.load(data)

        content = output.read_text(encoding="utf-8")
        assert "São João do Piauí" in content
        assert "\\u" not in content


# ── PostgresLoader ────────────────────────────────────────────────────────

class TestPostgresLoaderInit:
    @patch.dict("sys.modules", {"psycopg2": MagicMock(), "psycopg2.extras": MagicMock()})
    def test_upsert_without_conflict_on_raises(self):
        from loaders.postgres_loader import PostgresLoader
        with pytest.raises(ValueError, match="conflict_on é obrigatório"):
            PostgresLoader(dsn="postgresql://localhost", table="test", mode="upsert")

    @patch.dict("sys.modules", {"psycopg2": MagicMock(), "psycopg2.extras": MagicMock()})
    def test_conflict_on_string_becomes_list(self):
        from loaders.postgres_loader import PostgresLoader
        loader = PostgresLoader(
            dsn="postgresql://localhost", table="test",
            conflict_on="id", mode="upsert",
        )
        assert loader.conflict_on == ["id"]

    @patch.dict("sys.modules", {"psycopg2": MagicMock(), "psycopg2.extras": MagicMock()})
    def test_insert_mode_no_conflict_required(self):
        from loaders.postgres_loader import PostgresLoader
        loader = PostgresLoader(dsn="postgresql://localhost", table="test", mode="insert")
        assert loader.mode == "insert"
        assert loader.conflict_on == []


class TestPostgresLoaderSQL:
    @patch.dict("sys.modules", {"psycopg2": MagicMock(), "psycopg2.extras": MagicMock()})
    def test_build_insert_sql(self):
        from loaders.postgres_loader import PostgresLoader
        loader = PostgresLoader(dsn="postgresql://localhost", table="municipios")
        sql = loader._build_insert(["id", "nome", "uf"])
        assert sql == "INSERT INTO municipios (id, nome, uf) VALUES (%s, %s, %s)"

    @patch.dict("sys.modules", {"psycopg2": MagicMock(), "psycopg2.extras": MagicMock()})
    def test_build_upsert_sql(self):
        from loaders.postgres_loader import PostgresLoader
        loader = PostgresLoader(
            dsn="postgresql://localhost", table="municipios",
            conflict_on="id", mode="upsert",
        )
        sql = loader._build_upsert(["id", "nome", "uf"])
        assert "ON CONFLICT (id)" in sql
        assert "DO UPDATE SET" in sql
        assert "nome = EXCLUDED.nome" in sql
        assert "uf = EXCLUDED.uf" in sql
        assert "id = EXCLUDED.id" not in sql

    @patch.dict("sys.modules", {"psycopg2": MagicMock(), "psycopg2.extras": MagicMock()})
    def test_build_upsert_multiple_conflict_fields(self):
        from loaders.postgres_loader import PostgresLoader
        loader = PostgresLoader(
            dsn="postgresql://localhost", table="events",
            conflict_on=["id", "date"], mode="upsert",
        )
        sql = loader._build_upsert(["id", "date", "value"])
        assert "ON CONFLICT (id, date)" in sql
        assert "value = EXCLUDED.value" in sql


class TestPostgresLoaderLoad:
    @patch.dict("sys.modules", {"psycopg2": MagicMock(), "psycopg2.extras": MagicMock()})
    def test_load_empty_records_does_nothing(self):
        from loaders.postgres_loader import PostgresLoader
        loader = PostgresLoader(dsn="postgresql://localhost", table="test")
        loader.load([])

    @patch.dict("sys.modules", {"psycopg2": MagicMock(), "psycopg2.extras": MagicMock()})
    def test_load_calls_execute_batch(self):
        from loaders.postgres_loader import PostgresLoader
        loader = PostgresLoader(dsn="postgresql://localhost", table="test")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader._psycopg2.connect.return_value = mock_conn

        data = [{"id": 1, "nome": "Parnaíba"}]
        loader.load(data)

        loader._extras.execute_batch.assert_called_once()

    @patch.dict("sys.modules", {"psycopg2": MagicMock(), "psycopg2.extras": MagicMock()})
    def test_load_single_dict_wrapped_in_list(self):
        from loaders.postgres_loader import PostgresLoader
        loader = PostgresLoader(dsn="postgresql://localhost", table="test")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader._psycopg2.connect.return_value = mock_conn

        loader.load({"id": 1, "nome": "Parnaíba"})

        loader._extras.execute_batch.assert_called_once()
