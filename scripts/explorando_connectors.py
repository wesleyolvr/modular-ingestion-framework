from connectors.file_connector import FileConnector

connector = FileConnector(name="teste", path="input/municipios_pi.csv")
data = connector.fetch()
print(data)

connector = FileConnector(name="teste", path="input/municipios_pi.xlsx")
data = connector.fetch()
print(data)