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

from pipeline import run_pipeline
from pipeline_config import PipelineConfig

# COMMAND ----------
try:
    dbutils.widgets.text("storage_base_path", os.getenv("AFP_STORAGE_BASE_PATH", "dbfs:/tmp/modelo_cotizaciones_afp"))
    dbutils.widgets.text("source_table", os.getenv("AFP_SOURCE_TABLE", "opx.p_ddv_opx.afp_certificados"))
    dbutils.widgets.text("target_table", os.getenv("AFP_TARGET_TABLE", "opx.p_ddv_opx.afp_certificados_output"))
    dbutils.widgets.text("table_provider", os.getenv("AFP_TABLE_PROVIDER", "delta"))
    dbutils.widgets.text("chromedriver_path", os.getenv("AFP_CHROMEDRIVER_PATH", "/databricks/driver/chromedriver"))
    dbutils.widgets.text(
        "modelo_api_url",
        os.getenv("AFP_MODELO_API_URL", "https://api-kong.afpmodelo.net/mwd/wsAFPHerramientas/wmValidarCertificados"),
    )
    dbutils.widgets.text("modelo_api_key", os.getenv("AFP_MODELO_API_KEY", ""))
    dbutils.widgets.dropdown("run_extract", "true", ["true", "false"])
except Exception:
    pass

# COMMAND ----------
def _widget(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


run_extract = _widget("run_extract", "true").lower() == "true"
os.environ["AFP_MODELO_API_URL"] = _widget(
    "modelo_api_url",
    os.getenv("AFP_MODELO_API_URL", "https://api-kong.afpmodelo.net/mwd/wsAFPHerramientas/wmValidarCertificados"),
)
modelo_api_key = _widget("modelo_api_key", os.getenv("AFP_MODELO_API_KEY", ""))
if modelo_api_key:
    os.environ["AFP_MODELO_API_KEY"] = modelo_api_key

config = PipelineConfig(
    storage_base_path=_widget("storage_base_path", os.getenv("AFP_STORAGE_BASE_PATH", "dbfs:/tmp/modelo_cotizaciones_afp")),
    source_table=_widget("source_table", "opx.p_ddv_opx.afp_certificados"),
    target_table=_widget("target_table", "opx.p_ddv_opx.afp_certificados_output"),
    table_provider=_widget("table_provider", os.getenv("AFP_TABLE_PROVIDER", "delta")),
    chromedriver_path=_widget("chromedriver_path", os.getenv("AFP_CHROMEDRIVER_PATH", "/databricks/driver/chromedriver")),
)

run_pipeline(config=config, spark=spark, run_extract=run_extract)

