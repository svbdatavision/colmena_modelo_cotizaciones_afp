from __future__ import annotations

import datetime
import glob
import os
from typing import Literal

from pipeline_config import PipelineConfig


DocType = Literal["original", "validacion"]


def _processing_date(config: PipelineConfig) -> datetime.date:
    if config.processing_date:
        return datetime.datetime.strptime(config.processing_date, "%Y-%m-%d").date()
    return datetime.date.today()


def _partitioned_pdf_dir(config: PipelineConfig, doc_type: DocType) -> str:
    dt = _processing_date(config)
    return os.path.join(
        config.base_path,
        config.bronze_pdf_prefix,
        f"year={dt.year:04d}",
        f"month={dt.month:02d}",
        f"day={dt.day:02d}",
        f"tipo_documento={doc_type}",
    )


def _legacy_pdf_path(config: PipelineConfig, doc_idn: str, doc_type: DocType) -> str:
    if doc_type == "original":
        filename = f"{doc_idn}.pdf"
    else:
        filename = f"_{doc_idn}.pdf"
    return os.path.join(config.pdfs_dir, filename)


def build_pdf_path(config: PipelineConfig, doc_idn: str, doc_type: DocType) -> str:
    if config.use_partitioned_pdf_storage:
        return os.path.join(_partitioned_pdf_dir(config, doc_type), f"{doc_idn}.pdf")
    return _legacy_pdf_path(config, doc_idn, doc_type)


def resolve_pdf_path(config: PipelineConfig, doc_idn: str, doc_type: DocType) -> str:
    preferred = build_pdf_path(config, doc_idn, doc_type)
    if os.path.exists(preferred):
        return preferred

    partitioned_pattern = os.path.join(
        config.base_path,
        config.bronze_pdf_prefix,
        "year=*",
        "month=*",
        "day=*",
        f"tipo_documento={doc_type}",
        f"{doc_idn}.pdf",
    )
    matches = glob.glob(partitioned_pattern)
    if matches:
        return max(matches, key=os.path.getmtime)

    legacy = _legacy_pdf_path(config, doc_idn, doc_type)
    if os.path.exists(legacy):
        return legacy

    return preferred


def ensure_pdf_parent(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
