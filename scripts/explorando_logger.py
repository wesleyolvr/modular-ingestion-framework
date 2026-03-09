from audit.logger import get_logger
logger = get_logger(name="explorando_logger")

logger.info("Olá, mundo!")
logger.error("Este é um mensagem de erro")
logger.warning("Este é um mensagem de aviso")
logger.debug("Este é um mensagem de depuração")
# using taskName and taskId
logger.info("Executando tarefa", extra={"taskName": "teste Sintegra", "taskId": "1"})
logger.info("Executando tarefa", extra={"taskName": "teste Receita Federal", "taskId": "2"})
logger.info("Executando tarefa", extra={"taskName": "teste IBGE", "taskId": "3"})