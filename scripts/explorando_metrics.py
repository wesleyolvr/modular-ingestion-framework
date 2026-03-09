from audit.metrics import PipelineMetrics

print("-" * 100)
print("Metrics PipelineMetrics")
print("-" * 100)
metrics = PipelineMetrics(pipeline="teste Sintegra", success=True, records_fetched=100, records_loaded=100, duration_seconds=10.0)
print(metrics)
print("dict: ", metrics.to_dict())
print("repr: ", metrics.__repr__())

print("-" * 100)

metrics = PipelineMetrics(pipeline="teste Receita Federal", success=False, records_fetched=100, records_loaded=100, duration_seconds=10.0)
print(metrics)
print("dict: ", metrics.to_dict())
print("repr: ", metrics.__repr__())

print("-" * 100)

metrics = PipelineMetrics(pipeline="teste IBGE", success=True, records_fetched=100, records_loaded=0, duration_seconds=10.0)
print(metrics)
print("dict: ", metrics.to_dict())
print("repr: ", metrics.__repr__())
