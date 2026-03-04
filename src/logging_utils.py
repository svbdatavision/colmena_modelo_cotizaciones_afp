import logging
from typing import Optional


class _DocIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "doc_idn"):
            record.doc_idn = "-"
        return True


def get_logger(name: str, log_file_path: str, with_doc_id: bool = True) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_file_path)
    if with_doc_id:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(module)s doc_idn:%(doc_idn)s %(message)s"
        )
        file_handler.addFilter(_DocIdFilter())
    else:
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(module)s %(message)s")

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def log_exception(logger: logging.Logger, err: Exception, doc_idn: Optional[str] = None) -> None:
    if doc_idn is not None:
        logger.error(err, extra={"doc_idn": doc_idn})
    else:
        logger.error(err)
