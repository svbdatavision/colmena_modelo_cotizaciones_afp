import csv
from typing import Dict, List, Optional

from pyspark.sql import SparkSession

from logging_utils import get_logger, log_exception
from pipeline_config import PipelineConfig


TARGET_COLUMNS = [
    "DOC_IDN",
    "LINK",
    "PERIODO_PRODUCCION",
    "FECHA_INGRESO",
    "METADATA_CREATOR",
    "METADATA_PRODUCER",
    "METADATA_CREADATE",
    "METADATA_MODDATE",
    "ES_METADATA",
    "AFP",
    "ES_CERT_COT",
    "CODVER",
    "RUT",
    "RUT_L11",
    "RES_AFP",
    "ES_DIF",
    "RES_DIF",
]


def _normalize_row(row: Dict[str, str]) -> Dict[str, object]:
    out = row
    out["RUT_L11"] = row["rut"].replace(".", "").zfill(11) if row["rut"] != "" else ""
    out["es_metadata"] = row["es_metadata"] == "True"
    out["es_cert_cot"] = row["es_cert_cot"] == "True"
    out["es_dif"] = row["es_dif"] == "True"

    upper = {key.upper(): value for key, value in out.items()}
    upper["METADATA_CREADATE"] = str(upper.get("METADATA_CREADATE", "")).replace("'", "")
    upper["METADATA_MODDATE"] = str(upper.get("METADATA_MODDATE", "")).replace("'", "")
    return upper


def _create_target_if_not_exists(spark: SparkSession, table_name: str, table_provider: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DOC_IDN STRING,
            LINK STRING,
            PERIODO_PRODUCCION STRING,
            FECHA_INGRESO STRING,
            METADATA_CREATOR STRING,
            METADATA_PRODUCER STRING,
            METADATA_CREADATE STRING,
            METADATA_MODDATE STRING,
            ES_METADATA BOOLEAN,
            AFP STRING,
            ES_CERT_COT BOOLEAN,
            CODVER STRING,
            RUT STRING,
            RUT_L11 STRING,
            RES_AFP STRING,
            ES_DIF BOOLEAN,
            RES_DIF STRING
        )
        USING {table_provider}
        """
    )


def run(config_runtime: Optional[PipelineConfig] = None, spark: Optional[SparkSession] = None) -> str:
    config_runtime = config_runtime or PipelineConfig()
    config_runtime.ensure_directories()
    logger = get_logger(__name__, config_runtime.log_file_path, with_doc_id=False)
    spark = spark or SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    output: List[Dict[str, object]] = []
    try:
        with open(config_runtime.output_csv_path, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile, delimiter=";", escapechar="\\", quotechar='"')
            for row in reader:
                output.append(_normalize_row(row))
    except Exception as err:
        log_exception(logger, Exception(f"Error al procesar CSV: {err}"))
        raise

    if not output:
        print("6_upload: OK")
        return config_runtime.target_table

    try:
        df = spark.createDataFrame(output)
        _create_target_if_not_exists(
            spark=spark,
            table_name=config_runtime.target_table,
            table_provider=config_runtime.table_provider,
        )
        df.select(*TARGET_COLUMNS).write.mode("append").insertInto(config_runtime.target_table)
        print("6_upload: OK")
        return config_runtime.target_table
    except Exception as err:
        log_exception(logger, Exception(f"Error al cargar datos en Spark SQL: {err}"))
        raise


if __name__ == "__main__":
    run()
