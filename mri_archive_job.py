# Databricks notebook source
# MAGIC %run ./archive_util

# COMMAND ----------

# DBTITLE 1,Claritas - Fall 2023
source_input = sandbox_root + "karen_swift/Fall23DB/General/claritas.parquet"
source_output = mri_archive_base_path + "claritas/Fall2023"
table_name = "bronze_alwayson.archive_mri_claritas_fall2023"

df = spark.read.parquet(source_input)

df = upper_case_columns(df)
display(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Block Groups - Fall 2023
source_input = sandbox_root + "karen_swift/Fall23DB/General/bg_fusiondata.csv"
source_output = mri_archive_base_path + "bg/Fall2023"
table_name = "bronze_alwayson.archive_mri_bg_fall2023"

df = spark.read.csv(source_input, header=True, sep=",")
df = upper_case_columns(df)
display(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Recode - Fall 2023
source_input = sandbox_root + "karen_swift/Fall23DB/General/recode.parquet"
source_output = mri_archive_base_path + "recode/Fall2023"
table_name = "bronze_alwayson.archive_mri_recode_fall2023"

df = spark.read.parquet(source_input)
df = upper_case_columns(df)
display(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,FIPS- Fall 2023
source_input = sandbox_root + "karen_swift/Fall23DB/General/fips.parquet"
source_output = mri_archive_base_path + "fips/Fall2023"
table_name = "bronze_alwayson.archive_mri_fips_fall2023"

df = spark.read.parquet(source_input)
df = upper_case_columns(df)
display(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Fall DB 2023
source_input = bronze_root + "bronze_schema/mrisimmons/2070_3301/2024/05/08/0/2070_3301.dat"
source_output = mri_archive_base_path + "mri_respondents/Fall2023"
table_name = "bronze_alwayson.archive_mri_respondents_fall2023"

df = spark.read.csv(source_input, header=True, sep="\t")
display(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,FA23_DBL_Layout
source_input = bronze_root + "bronze_schema/mrisimmons/FA23_DBL_Layout/2024/05/08/0/"
source_output = mri_archive_base_path + "mri_respondents_layout/Fall2023"
table_name = "bronze_alwayson.archive_mri_respondents_layout_fall2023"

df = spark.read.csv(source_input, header=True, sep="\t")
display(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,ACS - Fall 2023
source_input = sandbox_root + "karen_swift/ACS_2020/bg_acs.parquet"
source_output = mri_archive_base_path + "mri_acs/Fall2023"
table_name = "bronze_alwayson.archive_mri_acs_fall2023"

df = spark.read.parquet(source_input)
df = upper_case_columns(df)
display(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table)
