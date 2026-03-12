from loaders.json_loader import JsonLoader
from loaders.postgres_loader import PostgresLoader
from audit.logger import get_logger
from connectors.rest_connector import RESTConnector
from connectors.file_connector import FileConnector

logger = get_logger(name="explorando_loaders")
# use logger info to log a JSON structure para ser usado em observability tools like Datadog, CloudWatch, etc.

json_loader = JsonLoader(path="output/data_example.json")
example_data = [{"id": 1, "nome": "João"}, {"id": 2, "nome": "Maria"}]
json_loader.load(example_data)
logger.info("Arquivo JSON criado com sucesso com o nome: {path}", path=json_loader.path)

#--------------------------------
#exemplo usando o connector e o loader para salvar os dados do IBGE
# ------------------------------------------------------------------------------------------------
json_loader = JsonLoader(path="output/data_ibge.json")
connector = RESTConnector(name="ibge_api", url="https://servicodados.ibge.gov.br/api/v1/localidades/estados/PI/municipios")
data_rest_connector = connector.fetch()
json_loader.load(data_rest_connector)
logger.info("Arquivo JSON criado com sucesso com o nome: {path}", path=json_loader.path)

# --------------------------------
# exemplo usando o connector e o loader para salvar os dados do IBGE
# ------------------------------------------------------------------------------------------------
json_loader = JsonLoader(path="output/municipios_pi.json")
file_connector = FileConnector(name="file_connector", path="input/municipios_pi.csv")
data = file_connector.fetch()
json_loader.load(data)
logger.info("Arquivo JSON criado com sucesso com o nome: {path}", path=json_loader.path)

#--------------------------------
#exemplo usando o loader para salvar os dados do IBGE
# ------------------------------------------------------------------------------------------------
DSN = "postgresql://pipeline:pipeline123@localhost:5432/pipeline_dev"
postgres_loader = PostgresLoader(
    dsn=DSN,
    table="municipios",
    conflict_on="id",
    mode="upsert",
)

dados = [
    {"id": 2200400, "nome": "Altos",   "uf": "PI", "populacao": 43092, "area_km2": 624.7,  "densidade_hab_km2": 69.00, "regiao": "Centro-Norte"},
    {"id": 2201200, "nome": "Barras",  "uf": "PI", "populacao": 44242, "area_km2": 3201.2, "densidade_hab_km2": 13.82, "regiao": "Norte"},
    {"id": 2201309, "nome": "Batalha",  "uf": "PI", "populacao": 32132, "area_km2": 2001.2, "densidade_hab_km2": 16.06, "regiao": "Norte"},
    {"id": 2202307, "nome": "Campo Maior", "uf": "PI", "populacao": 45276, "area_km2": 1697.3, "densidade_hab_km2": 26.68, "regiao": "Centro-Norte"},
    {"id": 2202406, "nome": "Canto do Buriti", "uf": "PI", "populacao": 24123, "area_km2": 1001.2, "densidade_hab_km2": 24.10, "regiao": "Centro-Norte"},
]

postgres_loader.load(dados)
logger.info("Dados salvos com sucesso na tabela: {table}", table=postgres_loader.table, records=len(dados))

def transformar_ibge(records):
    return [
        {
            "id": r["id"],
            "nome": r["nome"],
            "uf": r["microrregiao"]["mesorregiao"]["UF"]["sigla"],
            "populacao": None,  # esse endpoint não retorna população
            "area_km2": None,
            "densidade_hab_km2": None,
            "regiao": r["microrregiao"]["mesorregiao"]["UF"]["regiao"]["nome"],
        }
        for r in records
    ]


#--------------------------------
#exemplo usando o loader para salvar os dados lidos do IBGE em um arquivo JSON e salvar na tabela municipios
# ------------------------------------------------------------------------------------------------
connector = RESTConnector(name="ibge_api", url="https://servicodados.ibge.gov.br/api/v1/localidades/estados/PI/municipios")
data_rest_connector = connector.fetch()
dados_transformados = transformar_ibge(data_rest_connector)
postgres_loader.load(dados_transformados)
logger.info("Dados salvos com sucesso na tabela: {table}", table=postgres_loader.table, records=len(dados_transformados))
logger.info("registros inseridos via UPSERT", records=len(dados_transformados))