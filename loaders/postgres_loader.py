from typing import Any
from core.base_loader import BaseLoader
from audit.logger import get_logger

class PostgresLoader(BaseLoader):
    """
    Loader para PostgreSQL via psycopg2.

    Suporta dois modos:
        insert : INSERT simples (falha em duplicatas)
        upsert : INSERT ... ON CONFLICT DO UPDATE

    Parâmetros:
        dsn         : connection string PostgreSQL
        table       : nome da tabela de destino
        conflict_on : campo(s) para ON CONFLICT (requerido no modo upsert)
        mode        : "insert" ou "upsert"
    """

    def __init__(
        self,
        dsn: str,
        table: str,
        conflict_on: str | list[str] | None = None,
        mode: str = "insert",
    ):
        self._logger = get_logger(name="postgres_loader")
        try:
            import psycopg2
            import psycopg2.extras
            self._psycopg2 = psycopg2
            self._extras = psycopg2.extras
        except ImportError:
            raise ImportError("psycopg2 não instalado. Execute: pip install psycopg2-binary")

        self.dsn = dsn
        self.table = table
        self.conflict_on = [conflict_on] if isinstance(conflict_on, str) else (conflict_on or [])
        self.mode = mode

        if mode == "upsert" and not self.conflict_on:
            raise ValueError("conflict_on é obrigatório no modo upsert")

    def load(self, data: Any) -> None:
        records = data if isinstance(data, list) else [data]
        if not records:
            self._logger.warning("Nenhum registro para inserir")
            return

        columns = list(records[0].keys())
        values = [[r.get(col) for col in columns] for r in records]

        with self._psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                if self.mode == "insert":
                    sql = self._build_insert(columns)
                else:
                    sql = self._build_upsert(columns)
                self._extras.execute_batch(cur, sql, values)
                self._logger.info("records inserted into table", records=len(records), table=self.table, mode=self.mode)
    def _build_insert(self, columns: list[str]) -> str:
        cols = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        SQL = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders})"
        return SQL

    def _build_upsert(self, columns: list[str]) -> str:
        cols = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        conflict_cols = ", ".join(self.conflict_on)
        updates = ", ".join(
            f"{col} = EXCLUDED.{col}"
            for col in columns
            if col not in self.conflict_on
        )
        SQL = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates}"
        return SQL
