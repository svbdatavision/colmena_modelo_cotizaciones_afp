from typing import Optional

from pyspark.sql import SparkSession

import afp as step4_afp
import compare as step5_compare
import download as step2_download
import extract as step1_extract
import parse as step3_parse
import upload as step6_upload
from pipeline_config import PipelineConfig


def run_pipeline(
    config: Optional[PipelineConfig] = None,
    spark: Optional[SparkSession] = None,
    run_extract: bool = True,
) -> None:
    config = config or PipelineConfig()
    spark = spark or SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    config.ensure_directories()

    if run_extract:
        step1_extract.run(config=config, spark=spark)
    step2_download.run(config=config)
    step3_parse.run(config_runtime=config)
    step4_afp.run(config_runtime=config)
    step5_compare.run(config_runtime=config)
    step6_upload.run(config_runtime=config, spark=spark)
