import logging
import json
import re
import sys
from datetime import datetime, timezone
from typing import Any


# Códigos ANSI para cores
class Colors:
    """Códigos ANSI para colorização de terminal."""
    RESET = "\033[0m"
    
    # Cores por nível
    DEBUG = "\033[36m"      # Cyan
    INFO = "\033[32m"       # Verde
    WARNING = "\033[33m"    # Amarelo
    ERROR = "\033[31m"      # Vermelho
    CRITICAL = "\033[35m"   # Magenta
    
    # Cores para campos
    FIELD_NAME = "\033[94m"  # Azul claro
    TIMESTAMP = "\033[90m"   # Cinza escuro
    STRING_VALUE = "\033[93m"  # Amarelo claro


def is_tty(stream) -> bool:
    """Verifica se o stream é um terminal (TTY)."""
    return hasattr(stream, 'isatty') and stream.isatty()


class JsonFormatter(logging.Formatter):
    """Formata logs como JSON estruturado."""

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }

        # Campos extras adicionados via get_logger().info("msg", key=value)
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "message", "pathname", "process", "processName",
                "relativeCreated", "stack_info", "thread", "threadName",
                "exc_info", "exc_text", "taskName"
            ):
                log_data[key] = value

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=str, ensure_ascii=False)


class ColoredJsonFormatter(JsonFormatter):
    """Formata logs como JSON estruturado com cores para terminal."""

    def __init__(self, use_colors: bool = True):
        super().__init__()
        self.use_colors = use_colors

    def _get_level_color(self, level: str) -> str:
        """Retorna a cor ANSI para o nível de log."""
        colors = {
            "DEBUG": Colors.DEBUG,
            "INFO": Colors.INFO,
            "WARNING": Colors.WARNING,
            "ERROR": Colors.ERROR,
            "CRITICAL": Colors.CRITICAL,
        }
        return colors.get(level, Colors.RESET)

    def _colorize_json(self, json_str: str, level: str) -> str:
        """Adiciona cores ao JSON para facilitar leitura no terminal."""
        if not self.use_colors:
            return json_str

        colored = json_str
        
        # Colore o campo "level" com a cor correspondente ao nível
        colored = re.sub(
            r'"level":\s*"([^"]+)"',
            lambda m: f'{Colors.FIELD_NAME}"level"{Colors.RESET}: {self._get_level_color(m.group(1))}"{m.group(1)}"{Colors.RESET}',
            colored
        )
        
        # Colore o campo "timestamp"
        colored = re.sub(
            r'"timestamp":\s*"([^"]+)"',
            lambda m: f'{Colors.FIELD_NAME}"timestamp"{Colors.RESET}: {Colors.TIMESTAMP}"{m.group(1)}"{Colors.RESET}',
            colored
        )
        
        # Colore o campo "message"
        colored = re.sub(
            r'"message":\s*"([^"]+)"',
            lambda m: f'{Colors.FIELD_NAME}"message"{Colors.RESET}: {Colors.STRING_VALUE}"{m.group(1)}"{Colors.RESET}',
            colored
        )
        
        # Colore outros campos (chaves)
        colored = re.sub(
            r'"([a-zA-Z_][a-zA-Z0-9_]*)":',
            lambda m: f'{Colors.FIELD_NAME}"{m.group(1)}"{Colors.RESET}:',
            colored
        )
        
        return colored

    def format(self, record: logging.LogRecord) -> str:
        json_str = super().format(record)
        return self._colorize_json(json_str, record.levelname)


class StructuredLogger:
    """Logger com suporte a campos extras por chamada."""

    def __init__(self, name: str, **context):
        self._logger = logging.getLogger(name)
        self._context = context

    def _log(self, level: int, message: str, **kwargs):
        extra = {**self._context, **kwargs}
        self._logger.log(level, message, extra=extra)

    def info(self, message: str, **kwargs):
        self._log(logging.INFO, message, **kwargs)

    def error(self, message: str, **kwargs):
        exc_info = kwargs.pop("exc_info", False)
        extra = {**self._context, **kwargs}
        self._logger.error(message, extra=extra, exc_info=exc_info)

    def warning(self, message: str, **kwargs):
        self._log(logging.WARNING, message, **kwargs)

    def debug(self, message: str, **kwargs):
        self._log(logging.DEBUG, message, **kwargs)


def setup_logging(level: int = logging.INFO, use_colors: bool | None = None) -> None:
    """
    Configura o handler raiz com JsonFormatter.
    
    Args:
        level: Nível de log (DEBUG, INFO, WARNING, ERROR)
        use_colors: Se True, usa cores. Se None, detecta automaticamente se é TTY.
                    Se False, sempre usa JSON puro (útil para redirecionamento).
    """
    handler = logging.StreamHandler()
    
    # Detecta automaticamente se deve usar cores (apenas em TTY)
    if use_colors is None:
        use_colors = is_tty(sys.stdout) or is_tty(sys.stderr)
    
    # Usa formatter colorido se for terminal, senão JSON puro
    if use_colors:
        handler.setFormatter(ColoredJsonFormatter(use_colors=True))
    else:
        handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.setLevel(level)

    if not root.handlers:
        root.addHandler(handler)


def get_logger(name: str = "pipeline", **context) -> StructuredLogger:
    """Retorna um StructuredLogger com contexto fixo."""
    setup_logging()
    return StructuredLogger(name=name, **context)
