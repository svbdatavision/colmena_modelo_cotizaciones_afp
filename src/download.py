import csv
import time
from typing import Optional

import requests

from logging_utils import get_logger, log_exception
from pipeline_config import PipelineConfig
from pdf_storage import build_pdf_path, ensure_pdf_parent
from source_link import normalize_source_link


def _download_content(
    session: requests.Session,
    link: str,
    timeout_seconds: int,
    retries: int,
) -> bytes:
    normalized_link = normalize_source_link(link)
    last_error: Optional[Exception] = None
    for _ in range(retries + 1):
        try:
            response = session.get(normalized_link, timeout=timeout_seconds)
            if response.status_code == 200:
                return response.content
            last_error = Exception(f"status code {response.status_code}")
        except Exception as err:
            last_error = err
        time.sleep(1)

    raise last_error if last_error else Exception("download error")


def run(config: Optional[PipelineConfig] = None) -> str:
    config = config or PipelineConfig()
    config.ensure_directories()
    logger = get_logger(__name__, config.log_file_path, with_doc_id=True)
    downloaded = 0
    failed = 0

    with open(config.input_csv_path, "r", encoding="utf-8") as handler:
        reader = csv.DictReader(handler, delimiter=";", escapechar="\\")
        with requests.Session() as session:
            for row in reader:
                doc_idn = row["doc_idn"]
                link = row["link"]
                try:
                    content = _download_content(
                        session=session,
                        link=link,
                        timeout_seconds=config.request_timeout_seconds,
                        retries=config.request_retries,
                    )
                    pdf_path = build_pdf_path(config, doc_idn, "original")
                    ensure_pdf_parent(pdf_path)
                    with open(pdf_path, "wb") as output_pdf:
                        output_pdf.write(content)
                    downloaded += 1
                except Exception as err:
                    log_exception(logger, err, doc_idn=doc_idn)
                    failed += 1
                time.sleep(config.sleep_seconds_between_downloads)

    print(f"2_download: OK (downloaded={downloaded}, failed={failed})")
    return config.pdfs_dir


if __name__ == "__main__":
    run()
