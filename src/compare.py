import csv
import difflib
import unicodedata
from typing import Optional

from pdfminer.high_level import extract_text

from logging_utils import get_logger, log_exception
from pipeline_config import PipelineConfig


def diff(x, y):
    differ = difflib.Differ()
    difs = list(differ.compare(x.splitlines(), y.splitlines()))
    res = []
    for d in difs:
        if d.startswith("-"):
            res.append(d)
        elif d.startswith("+"):
            res.append(d)
    return " | ".join(res)


HEADERS = [
    "doc_idn",
    "link",
    "periodo_produccion",
    "fecha_ingreso",
    "metadata_creator",
    "metadata_producer",
    "metadata_creadate",
    "metadata_moddate",
    "es_metadata",
    "afp",
    "es_cert_cot",
    "codver",
    "rut",
    "res_afp",
    "es_dif",
    "res_dif",
]


def run(config_runtime: Optional[PipelineConfig] = None) -> str:
    config_runtime = config_runtime or PipelineConfig()
    config_runtime.ensure_directories()
    logger = get_logger(__name__, config_runtime.log_file_path, with_doc_id=True)

    output = []
    with open(config_runtime.output_csv_path, "r", encoding="utf-8") as handler:
        reader = csv.DictReader(handler, delimiter=";", escapechar="\\", quotechar='"')
        for row in reader:
            out = row
            if row["res_afp"] == "ok":
                try:
                    txt_afp = (
                        unicodedata.normalize("NFKD", extract_text(f"{config_runtime.pdfs_dir}/_{row['doc_idn']}.pdf"))
                        .encode("ascii", "ignore")
                        .decode("utf-8")
                    )
                    txt_ori = (
                        unicodedata.normalize("NFKD", extract_text(f"{config_runtime.pdfs_dir}/{row['doc_idn']}.pdf"))
                        .encode("ascii", "ignore")
                        .decode("utf-8")
                    )
                    if txt_afp != txt_ori:
                        dif = diff(txt_afp, txt_ori)
                    else:
                        dif = ""
                    out["es_dif"] = txt_afp != txt_ori
                    out["res_dif"] = dif
                except Exception as err:
                    log_exception(logger, err, doc_idn=row["doc_idn"])
            output.append(out)

    with open(config_runtime.output_csv_path, "w", encoding="utf-8") as handler:
        writer = csv.DictWriter(
            handler,
            fieldnames=HEADERS,
            delimiter=";",
            escapechar="\\",
            quotechar='"',
        )
        writer.writeheader()
        writer.writerows(output)

    print("5_compare: OK")
    return config_runtime.output_csv_path


if __name__ == "__main__":
    run()
