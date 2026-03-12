
from core.pipeline import Pipeline
from connectors.rest_connector import RESTConnector
from validators.pydantic_validator import PydanticValidator
from loaders.json_loader import JsonLoader
from core.models import Municipio_IBGE


pipeline = Pipeline(
    name="municipios_ibge",
    connector=RESTConnector(
        name="ibge_api",
        url="https://servicodados.ibge.gov.br/api/v1/localidades/estados/PI/municipios",
    ),
    validator=PydanticValidator(model=Municipio_IBGE, unique_by="id"),
    loader=JsonLoader(path="output/municipios.json"),
)

metrics = pipeline.run()
print(metrics)