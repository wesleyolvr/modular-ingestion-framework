# Scripts do Projeto

## Por que preciso configurar o sys.path?

Quando você executa um script Python diretamente (ex: `python scripts/explorando_logger.py`), o Python adiciona apenas o diretório do script ao `sys.path`, não o diretório raiz do projeto.

### Estrutura do projeto:
```
modular-ingestion-framework/
├── audit/          ← módulos aqui
├── core/           ← módulos aqui
├── connectors/     ← módulos aqui
└── scripts/        ← scripts aqui
    └── explorando_logger.py
```

### O problema:
- Script está em: `scripts/explorando_logger.py`
- Módulos estão em: `audit/`, `core/`, etc. (diretório pai)
- Python procura módulos apenas em `scripts/` → **não encontra!**

## Soluções

### ✅ Solução 1: Executar como módulo (Melhor prática)
```bash
# Do diretório raiz do projeto
python -m scripts.explorando_logger
```

### ✅ Solução 2: Instalar o pacote com Poetry
```bash
poetry install  # Instala o pacote em modo desenvolvimento
python scripts/explorando_logger.py  # Agora funciona sem sys.path
```

### ✅ Solução 3: Usar PYTHONPATH
```bash
# Windows PowerShell
$env:PYTHONPATH = "C:\caminho\para\modular-ingestion-framework"
python scripts/explorando_logger.py

# Linux/Mac
PYTHONPATH=. python scripts/explorando_logger.py
```

## Qual usar?

- **Com Poetry instalado** (✅ Recomendado): Use `poetry run python scripts/nome_script.py`
- **Produção/CI**: Use Poetry install + `poetry run` (Solução 2)
- **Testes**: Use `python -m` (Solução 1)

## ⚠️ Importante: Executando com Poetry

Após executar `poetry install`, você **deve** usar `poetry run` para executar os scripts:

```bash
# ✅ CORRETO - Com Poetry
poetry run python scripts/explorando_logger.py
poetry run python scripts/pipeline_ibge.py

# ❌ ERRADO - Sem Poetry (não encontra os módulos)
python scripts/explorando_logger.py
```

**Por quê?** O `poetry run` ativa o ambiente virtual onde o pacote está instalado. Sem ele, o Python usa o ambiente do sistema que não tem acesso aos módulos do projeto.

### ⚠️ Conflito de nomes

**NUNCA** nomeie um script com o mesmo nome de um módulo do projeto! Por exemplo:
- ❌ `scripts/core.py` → conflita com o módulo `core/`
- ✅ `scripts/pipeline_example.py` → sem conflito

Se você criar um script com nome conflitante, o Python tentará importar o arquivo do script ao invés do módulo, causando erros de importação.
