from connectors.file_connector import FileConnector
from connectors.rest_connector import RESTConnector

#valor de data retornado: lista de municipios.
connector = FileConnector(name="teste", path="input/municipios_pi.csv")
data = connector.fetch()
print(data)

#valor de data retornado: dataframe de municipios.
connector = FileConnector(name="teste", path="input/municipios_pi.xlsx")
data = connector.fetch()
print(data)

#valor de json retornado: lista de municipios.
connector = RESTConnector(name="teste", url="https://servicodados.ibge.gov.br/api/v1/localidades/estados/PI/municipios")
data = connector.fetch()
print(data)

#valor de json retornado: vazio.
connector = RESTConnector(name="teste", url="https://servicodados.ibge.gov.br/api/v1/localidades/municipios/2205504")
data = connector.fetch()
print(data)