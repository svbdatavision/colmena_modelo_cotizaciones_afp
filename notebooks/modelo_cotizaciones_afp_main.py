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
    dbutils.widgets.text("storage_base_path", os.getenv("AFP_STORAGE_BASE_PATH", "/Volumes/main/default/modelo_cotizaciones_afp"))
    dbutils.widgets.text("source_table", os.getenv("AFP_SOURCE_TABLE", "opx.p_ddv_opx.afp_certificados"))
    dbutils.widgets.text("target_table", os.getenv("AFP_TARGET_TABLE", "opx.p_ddv_opx.afp_certificados_output"))
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


run_extract = _widget("run_extract", "true").lower() == "true"
config = PipelineConfig(
    storage_base_path=_widget("storage_base_path", os.getenv("AFP_STORAGE_BASE_PATH", "/workspace")),
    source_table=_widget("source_table", "opx.p_ddv_opx.afp_certificados"),
    target_table=_widget("target_table", "opx.p_ddv_opx.afp_certificados_output"),
    chromedriver_path=_widget("chromedriver_path", os.getenv("AFP_CHROMEDRIVER_PATH", "/databricks/driver/chromedriver")),
)

run_pipeline(config=config, spark=spark, run_extract=run_extract)

