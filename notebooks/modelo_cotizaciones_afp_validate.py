# Databricks notebook source
# COMMAND ----------
import os
import sys

# COMMAND ----------
repo_root = os.getcwd()
src_path = os.path.join(repo_root, "src")
if not os.path.isdir(src_path):
    src_path = os.path.abspath(os.path.join(repo_root, "..", "src"))
if src_path not in sys.path:
    sys.path.append(src_path)

from pipeline_config import PipelineConfig
from validation import format_results, run_validation

# COMMAND ----------
try:
    dbutils.widgets.text("storage_base_path", os.getenv("AFP_STORAGE_BASE_PATH", "dbfs:/tmp/modelo_cotizaciones_afp"))
    dbutils.widgets.text("source_document_base_url", os.getenv("AFP_SOURCE_DOCUMENT_BASE_URL", "http://10.0.15.58:8080/notifEnvioMailRest/public/documento"))
    dbutils.widgets.text("source_table", os.getenv("AFP_SOURCE_TABLE", "opx.p_ddv_opx.afp_certificados"))
    dbutils.widgets.text("target_table", os.getenv("AFP_TARGET_TABLE", "opx.p_ddv_opx.afp_certificados_output"))
    dbutils.widgets.text("extract_days", os.getenv("AFP_EXTRACT_DAYS", "0"))
    dbutils.widgets.text("extract_limit", os.getenv("AFP_EXTRACT_LIMIT", "240"))
    dbutils.widgets.text("table_provider", os.getenv("AFP_TABLE_PROVIDER", "delta"))
    dbutils.widgets.text("chromedriver_path", os.getenv("AFP_CHROMEDRIVER_PATH", "/databricks/driver/chromedriver"))
    dbutils.widgets.dropdown("run_extract", "true", ["true", "false"])
    dbutils.widgets.text("seed_doc_idn", "166088887")
    dbutils.widgets.text("seed_link", "https://w3.provida.cl/validador/descarga.ashx?Id=245756274-188906699")
    dbutils.widgets.text("seed_periodo_produccion", "2024-12-01")
    dbutils.widgets.text("seed_fecha_ingreso", "2026-03-04")
except Exception:
    pass

# COMMAND ----------
def _widget(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


config = PipelineConfig(
    storage_base_path=_widget("storage_base_path", os.getenv("AFP_STORAGE_BASE_PATH", "dbfs:/tmp/modelo_cotizaciones_afp")),
    source_document_base_url=_widget(
        "source_document_base_url",
        os.getenv("AFP_SOURCE_DOCUMENT_BASE_URL", "http://10.0.15.58:8080/notifEnvioMailRest/public/documento"),
    ),
    source_table=_widget("source_table", "opx.p_ddv_opx.afp_certificados"),
    target_table=_widget("target_table", "opx.p_ddv_opx.afp_certificados_output"),
    extract_days=int(_widget("extract_days", os.getenv("AFP_EXTRACT_DAYS", "0"))),
    extract_limit=int(_widget("extract_limit", os.getenv("AFP_EXTRACT_LIMIT", "240"))),
    table_provider=_widget("table_provider", os.getenv("AFP_TABLE_PROVIDER", "delta")),
    chromedriver_path=_widget("chromedriver_path", os.getenv("AFP_CHROMEDRIVER_PATH", "/databricks/driver/chromedriver")),
)

results = run_validation(
    config=config,
    spark=spark,
    run_extract=_widget("run_extract", "true").lower() == "true",
    seed_doc_idn=_widget("seed_doc_idn", "166088887"),
    seed_link=_widget("seed_link", "https://w3.provida.cl/validador/descarga.ashx?Id=245756274-188906699"),
    seed_periodo_produccion=_widget("seed_periodo_produccion", "2024-12-01"),
    seed_fecha_ingreso=_widget("seed_fecha_ingreso", "2026-03-04"),
)

print(format_results(results))
display(spark.sql(f"select * from {config.target_table} order by FECHA_INGRESO desc limit 20"))
