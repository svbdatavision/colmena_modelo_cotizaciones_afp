# Databricks notebook source
# COMMAND ----------
import csv
import json
import os
import sys

# COMMAND ----------
repo_root = os.getcwd()
src_path = os.path.join(repo_root, "src")
if not os.path.isdir(src_path):
    src_path = os.path.abspath(os.path.join(repo_root, "..", "src"))
if src_path not in sys.path:
    sys.path.append(src_path)

from pipeline import run_pipeline
from pipeline_config import PipelineConfig

# COMMAND ----------
try:
    dbutils.widgets.text("storage_base_path", os.getenv("AFP_STORAGE_BASE_PATH", "dbfs:/tmp/modelo_cotizaciones_afp"))
    dbutils.widgets.text("source_table", os.getenv("AFP_SOURCE_TABLE", "opx.p_ddv_opx.afp_certificados"))
    dbutils.widgets.text("target_table", os.getenv("AFP_TARGET_TABLE", "opx.p_ddv_opx.afp_certificados_output"))
    dbutils.widgets.text("extract_days", os.getenv("AFP_EXTRACT_DAYS", "0"))
    dbutils.widgets.text("extract_limit", os.getenv("AFP_EXTRACT_LIMIT", "240"))
    dbutils.widgets.text("chromedriver_path", os.getenv("AFP_CHROMEDRIVER_PATH", "/databricks/driver/chromedriver"))
    dbutils.widgets.dropdown("run_extract", "true", ["true", "false"])
except Exception:
    pass

# COMMAND ----------
def _widget(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


def _count_csv_rows(path: str) -> int:
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as handler:
        reader = csv.DictReader(handler, delimiter=";", escapechar="\\", quotechar='"')
        return sum(1 for _ in reader)


def _read_doc_ids(path: str, max_items: int = 1000):
    if not os.path.exists(path):
        return []
    items = []
    with open(path, "r", encoding="utf-8") as handler:
        reader = csv.DictReader(handler, delimiter=";", escapechar="\\", quotechar='"')
        for row in reader:
            doc_idn = (row.get("doc_idn") or "").strip()
            if doc_idn:
                items.append(doc_idn)
            if len(items) >= max_items:
                break
    return items


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


run_extract = _widget("run_extract", "true").lower() == "true"
config = PipelineConfig(
    storage_base_path=_widget("storage_base_path", os.getenv("AFP_STORAGE_BASE_PATH", "dbfs:/tmp/modelo_cotizaciones_afp")),
    source_table=_widget("source_table", "opx.p_ddv_opx.afp_certificados"),
    target_table=_widget("target_table", "opx.p_ddv_opx.afp_certificados_output"),
    extract_days=int(_widget("extract_days", os.getenv("AFP_EXTRACT_DAYS", "0"))),
    extract_limit=int(_widget("extract_limit", os.getenv("AFP_EXTRACT_LIMIT", "240"))),
    chromedriver_path=_widget("chromedriver_path", os.getenv("AFP_CHROMEDRIVER_PATH", "/databricks/driver/chromedriver")),
)

summary = {
    "run_extract": run_extract,
    "storage_base_path": config.storage_base_path,
    "source_table": config.source_table,
    "target_table": config.target_table,
    "extract_days": config.extract_days,
    "extract_limit": config.extract_limit,
    "input_csv_path": config.input_csv_path,
    "output_csv_path": config.output_csv_path,
}

if run_extract:
    try:
        summary["source_candidates_before_run"] = spark.sql(
            f"""
            SELECT COUNT(*) AS c
            FROM {config.source_table}
            WHERE CAST(FECHA_INGRESO AS DATE) >= date_sub(current_date(), {config.extract_days})
              AND DOC_IDN NOT IN (SELECT DOC_IDN FROM {config.target_table})
            """
        ).collect()[0]["c"]
    except Exception as err:
        summary["source_candidates_before_run_error"] = str(err)

run_pipeline(config=config, spark=spark, run_extract=run_extract)

summary["input_csv_rows"] = _count_csv_rows(config.input_csv_path)
summary["output_csv_rows"] = _count_csv_rows(config.output_csv_path)

doc_ids = _read_doc_ids(config.input_csv_path, max_items=1000)
summary["input_doc_idn_sample_size"] = len(doc_ids)

if doc_ids:
    ids_sql = ",".join([_sql_quote(item) for item in doc_ids])
    batch_metrics = spark.sql(
        f"""
        SELECT
            COUNT(*) AS total_target_rows_for_batch,
            SUM(CASE WHEN AFP IS NULL OR TRIM(AFP) = '' THEN 1 ELSE 0 END) AS afp_vacio_en_batch,
            SUM(CASE WHEN LOWER(COALESCE(RES_AFP, '')) = 'ok' THEN 1 ELSE 0 END) AS res_afp_ok_en_batch
        FROM {config.target_table}
        WHERE DOC_IDN IN ({ids_sql})
        """
    ).collect()[0]
    summary["target_rows_for_batch"] = int(batch_metrics["total_target_rows_for_batch"] or 0)
    summary["afp_empty_for_batch"] = int(batch_metrics["afp_vacio_en_batch"] or 0)
    summary["res_afp_ok_for_batch"] = int(batch_metrics["res_afp_ok_en_batch"] or 0)

print(json.dumps(summary, indent=2, ensure_ascii=True))

if doc_ids:
    display(
        spark.sql(
            f"""
            SELECT DOC_IDN, AFP, RUT, CODVER, RES_AFP, ES_DIF, FECHA_INGRESO
            FROM {config.target_table}
            WHERE DOC_IDN IN ({ids_sql})
            ORDER BY FECHA_INGRESO DESC, DOC_IDN
            LIMIT 200
            """
        )
    )
