import json
import logging
import os


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "level": record.levelname,
            "message": record.getMessage(),
            "service": "sward-lambda-notificaciones",
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def setup_logger(name: str = __name__) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(JsonFormatter())
        logger.addHandler(h)
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
    return logger
