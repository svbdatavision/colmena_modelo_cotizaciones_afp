import csv
import json
import os
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from pipeline import run_pipeline
from pipeline_config import PipelineConfig
from pdf_storage import resolve_pdf_path


def _write_seed_input(
    config: PipelineConfig,
    doc_idn: str,
    link: str,
    periodo_produccion: str,
    fecha_ingreso: str,
) -> None:
    with open(config.input_csv_path, "w", encoding="utf-8") as handler:
        writer = csv.DictWriter(
            handler,
            fieldnames=["doc_idn", "link", "periodo_produccion", "fecha_ingreso"],
            delimiter=";",
            escapechar="\\",
        )
        writer.writeheader()
        writer.writerow(
            {
                "doc_idn": doc_idn,
                "link": link,
                "periodo_produccion": periodo_produccion,
                "fecha_ingreso": fecha_ingreso,
            }
        )


def _read_output_first_row(output_csv_path: str) -> Optional[Dict[str, str]]:
    if not os.path.exists(output_csv_path):
        return None
    with open(output_csv_path, "r", encoding="utf-8") as handler:
        reader = csv.DictReader(handler, delimiter=";", escapechar="\\", quotechar='"')
        for row in reader:
            return row
    return None


def run_validation(
    config: PipelineConfig,
    spark: SparkSession,
    run_extract: bool = False,
    seed_doc_idn: str = "166088887",
    seed_link: str = "https://w3.provida.cl/validador/descarga.ashx?Id=245756274-188906699",
    seed_periodo_produccion: str = "2024-12-01",
    seed_fecha_ingreso: str = "2026-03-04",
) -> Dict[str, Any]:
    config.ensure_directories()

    results: Dict[str, Any] = {
        "storage_base_path": config.storage_base_path,
        "source_table": config.source_table,
        "target_table": config.target_table,
        "chromedriver_path": config.chromedriver_path,
        "run_extract": run_extract,
        "checks": {},
    }

    results["checks"]["storage_base_exists"] = os.path.isdir(config.base_path)
    results["checks"]["chromedriver_exists"] = os.path.exists(config.chromedriver_path)

    if not run_extract:
        _write_seed_input(
            config=config,
            doc_idn=seed_doc_idn,
            link=seed_link,
            periodo_produccion=seed_periodo_produccion,
            fecha_ingreso=seed_fecha_ingreso,
        )
        results["seed_input"] = {
            "doc_idn": seed_doc_idn,
            "link": seed_link,
            "periodo_produccion": seed_periodo_produccion,
            "fecha_ingreso": seed_fecha_ingreso,
        }

    run_pipeline(config=config, spark=spark, run_extract=run_extract)

    first_row = _read_output_first_row(config.output_csv_path)
    results["checks"]["output_csv_exists"] = os.path.exists(config.output_csv_path)
    results["checks"]["output_first_row"] = first_row

    doc_idn = first_row["doc_idn"] if first_row else seed_doc_idn
    original_pdf = resolve_pdf_path(config, doc_idn, "original")
    validated_pdf = resolve_pdf_path(config, doc_idn, "validacion")

    results["checks"]["original_pdf_path"] = original_pdf
    results["checks"]["validated_pdf_path"] = validated_pdf

    results["checks"]["original_pdf_exists"] = os.path.exists(original_pdf)
    results["checks"]["validated_pdf_exists"] = os.path.exists(validated_pdf)
    results["checks"]["original_pdf_size"] = os.path.getsize(original_pdf) if os.path.exists(original_pdf) else 0
    results["checks"]["validated_pdf_size"] = os.path.getsize(validated_pdf) if os.path.exists(validated_pdf) else 0
    results["checks"]["target_table_count"] = spark.sql(
        f"select count(*) as c from {config.target_table}"
    ).collect()[0]["c"]

    if first_row:
        results["checks"]["res_afp"] = first_row.get("res_afp")
        results["checks"]["es_dif"] = first_row.get("es_dif")
        results["checks"]["afp"] = first_row.get("afp")
        results["checks"]["rut"] = first_row.get("rut")
        results["checks"]["codver"] = first_row.get("codver")

    return results


def format_results(results: Dict[str, Any]) -> str:
    return json.dumps(results, indent=2, ensure_ascii=True)
