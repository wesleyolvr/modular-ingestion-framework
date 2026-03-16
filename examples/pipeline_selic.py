# pipeline para buscar dados da Selic
# API: https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=13/03/2025&dataFinal=13/03/2026
from core.pipeline import Pipeline
from core.models import Selic
from connectors.rest_connector import RESTConnector
from loaders.json_loader import JsonLoader
from validators.pydantic_validator import PydanticValidator
from transformers.selic_transformer import SelicTransformer

if __name__ == "__main__":
    data_inicial = "13/03/2025"
    data_final = "13/03/2026"
    params = {
        "formato": "json",
        "dataInicial": data_inicial,
        "dataFinal": data_final,
    }
    pipeline = Pipeline(
    name="selic",
    connector=RESTConnector(name="selic", url="https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados", params=params),
        validator=PydanticValidator(model=Selic, unique_by="data"),
        loader=JsonLoader(path="output/selic.json"),
        transform=SelicTransformer(),
    )

    metrics = pipeline.run(data_inicial=data_inicial, data_final=data_final)
    print(metrics)