import logging
from typing import Optional


class _DocIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "doc_idn"):
            record.doc_idn = "-"
        return True


class _ResilientFileHandler(logging.FileHandler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            super().emit(record)
        except OSError:
            # Some filesystem backends (for example DBFS/FUSE) may intermittently
            # raise Illegal seek on flush. Keep pipeline execution unaffected.
            pass

    def flush(self) -> None:
        try:
            super().flush()
        except OSError:
            pass


def get_logger(name: str, log_file_path: str, with_doc_id: bool = True) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    file_handler = _ResilientFileHandler(log_file_path)
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
