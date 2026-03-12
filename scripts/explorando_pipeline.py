from core.pipeline import Pipeline
from validators.pydantic_validator import PydanticValidator
from connectors.rest_connector import RESTConnector
from loaders.postgres_loader import PostgresLoader
from datetime import datetime

# ── Pipeline BCB Selic → Postgres ────────────────────────
print("\n" + "=" * 60)
print("Pipeline Selic BCB → PostgreSQL (dados reais)")
print("=" * 60)
DSN = "postgresql://pipeline:pipeline123@localhost:5432/pipeline_dev"

def transformar_selic(records):
    resultado = []
    for r in records:
        resultado.append({
            "data": datetime.strptime(r["data"], "%d/%m/%Y").strftime("%Y-%m-%d"),
            "taxa_ao_dia": float(r["valor"]),
            "taxa_anualizada_pct": round(((1 + float(r["valor"]) / 100) ** 252 - 1) * 100, 4),
            "fonte": "BCB SGS 11",
        })
    return resultado

try:
    pipeline_selic = Pipeline(
        name="selic_bcb_postgres",
        connector=RESTConnector(
            name="bcb_selic",
            url="https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=12/03/2025",
            params={"formato": "json", "ultimos": 30},
        ),
        validator=PydanticValidator(
            model=Selic,
            unique_by="data",
        ),
        loader=PostgresLoader(
            dsn=DSN,
            table="selic_diaria",
            conflict_on="data",
            mode="upsert",
        ),
        transform=transformar_selic,
    )

    metrics = pipeline_selic.run()
    print(f"\n  {metrics}")

except Exception as e:
    print(f"  ✗ Erro (sem internet?): {e}")