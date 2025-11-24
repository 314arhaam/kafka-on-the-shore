import logging
from logging.handlers import RotatingFileHandler

def get_logger(
    name: str = "kafka-consumer",
    log_file: str = "logs/kafka_messages.log",
    level=logging.INFO
):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent adding handlers multiple times
    if logger.handlers:
        return logger

    # File handler
    handler = RotatingFileHandler(
        log_file,
        maxBytes=5_000_000,   # 5MB
        backupCount=5
    )
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Optional: console handler
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    return logger
