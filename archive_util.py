# Databricks notebook source
# MAGIC %run /Workspace/Shared/util/storage_account_access

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *

now = datetime.now()
date_str = now.strftime("%Y/%m/%d")
current_year = int(now.strftime("%Y"))

tu_archive_base_path = archive_root + "transunion/"
aiq_archive_base_path = archive_root + "analyticsiq/"
mri_archive_base_path = archive_root + "mrisimmons/"


def archive_csv(
    source_input: str, source_output: str, table_name: str, delimiter="|"
) -> None:
    df = spark.read.csv(source_input, header=True, sep=delimiter)
    spark.sql(f"drop table if exists {table_name}")
    df.write.format("delta").option("compression", "snappy").option(
        "path", source_output
    ).saveAsTable(table_name)


def archive_parquet(source_input: str, source_output: str, table_name: str) -> None:
    df = spark.read.parquet(source_input)
    spark.sql(f"drop table if exists {table_name}")
    df.write.format("delta").option("compression", "snappy").option(
        "path", source_output
    ).saveAsTable(table_name)


def upper_case_columns(df) -> None:
    cols = df.columns
    for x in cols:
        tmp = (
            x.upper()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(",", "")
        )
        df = df.withColumnRenamed(x, tmp)
    return df
