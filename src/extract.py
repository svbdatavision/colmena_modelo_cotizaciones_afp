import datetime
from typing import Optional

from pyspark.sql import SparkSession

from logging_utils import get_logger, log_exception
from pipeline_config import PipelineConfig


HEADERS = "doc_idn;link;periodo_produccion;fecha_ingreso\n"


def _to_csv_value(value) -> str:
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d")
    return str(value)


def run(config: Optional[PipelineConfig] = None, spark: Optional[SparkSession] = None) -> str:
    config = config or PipelineConfig()
    config.ensure_directories()
    logger = get_logger(__name__, config.log_file_path, with_doc_id=False)

    date_from = datetime.date.today() - datetime.timedelta(days=config.extract_days)
    query = f"""
    select DOC_IDN,LINK,PERIODO_PRODUCCION,FECHA_INGRESO
    from {config.source_table}
    where cast(FECHA_INGRESO as date) >= date('{date_from.strftime("%Y-%m-%d")}') and
    DOC_IDN not in (select DOC_IDN from {config.target_table})
    order by DOC_IDN
    limit {config.extract_limit}
    """

    spark = spark or SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    try:
        rows = spark.sql(query).collect()
        out = []
        for row in rows:
            values = [_to_csv_value(res) for res in row]
            out.append(";".join(values) + "\n")

        with open(config.input_csv_path, "w", encoding="utf-8") as handler:
            handler.write(HEADERS)
            handler.writelines(out)
        print("1_extract: OK")
        return config.input_csv_path
    except Exception as err:
        log_exception(logger, err)
        raise


if __name__ == "__main__":
    run()
