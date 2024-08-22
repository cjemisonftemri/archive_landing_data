# Databricks notebook source
# MAGIC %run ./archive_util

# COMMAND ----------

# DBTITLE 1,Full Population - April 2024
source_output = aiq_archive_base_path + "full_population/April2024"
table_name = "bronze_alwayson.archive_aiq_full_population_april2024"

df = spark.sql("SELECT * from bronze_alwayson.aiq_full_population_april24")
spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Full Population - July 2024
source_output = aiq_archive_base_path + "full_population/July2024"
table_name = "bronze_alwayson.archive_aiq_full_population_july2024"

df = spark.sql("SELECT * from bronze_alwayson.aiq_full_population_july24")
spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,ID Resolution - April 2024
source_output = aiq_archive_base_path + "id_resolution/April2024"
table_name = "bronze_alwayson.archive_aiq_id_resolution_april2024"

df = spark.sql("SELECT * from bronze_alwayson.aiq_id_resolution_april24")
spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,AIQ / MRI Fusion - April 2024
source_output = aiq_archive_base_path + "aiq_mri_fusion/April2024"
table_name = "bronze_alwayson.archive_aiq_mri_fusion_april2024"

df = spark.sql("SELECT * from bronze_alwayson.aiq_fusion_dma_match")
spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,HEM - April 2024
source_input = analyticsiq_root + "AIQ_HEM_20240715"
source_output = aiq_archive_base_path + "aiq_hem/April2024"
table_name = "bronze_alwayson.archive_aiq_hem_april2024"

df = spark.read.parquet(source_input)
df = upper_case_columns(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,PII - April 2024
source_input = analyticsiq_root + "AIQ_PII_20240715"
source_output = aiq_archive_base_path + "aiq_pii/April2024"
table_name = "bronze_alwayson.archive_aiq_pii_april2024"

df = spark.read.parquet(source_input)
df = upper_case_columns(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Race and Auxiliary
source_input = analyticsiq_root + "AIQ_Race&Ethincity_Auxillary_07232024"
source_output = aiq_archive_base_path + "aiq_race_ethinicity_auxiliary/April2024"
table_name = "bronze_alwayson.archive_aiq_race_ethinicity_auxiliary_april2024"

df = spark.read.parquet(source_input)
spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,MRI TO AIQ ID Resolution
source_input = analyticsiq_root + "1_AIQ_March2024/TO_AIQ_March2024/1037_10000301_INPUT_240229_Part1.psv"
source_output = aiq_archive_base_path + "mri_to_aiq_id_resolution/March2024"
table_name = "bronze_alwayson.archive_mri_to_aiq_id_resolution_march2024"

df = spark.read.csv(source_input, header=True, sep="|")
df = upper_case_columns(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,AIQ Block Groups
source_input = analyticsiq_root + "MRI_dyn100_Block_skinny.txt.gz"
source_output = aiq_archive_base_path + "aiq_block_groups/April2024"
table_name = "bronze_alwayson.archive_aiq_block_groups_april2024"

df = spark.read.csv(source_input, header=True, sep="|")
df = upper_case_columns(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,AIQ Girl Scouts Crosswalk
source_input = analyticsiq_root + "Extract_GirlScouts_Crosswalk_052024.txt.gz"
source_output = aiq_archive_base_path + "aiq_girlscout_crosswalk/April2024"
table_name = "bronze_alwayson.archive_aiq_girlscout_crosswalk_april2024"

df = spark.read.csv(source_input, header=True, sep="|")
df = upper_case_columns(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,AIQ Girl Scouts Append
source_input = analyticsiq_root + "Append_GirlScouts_Jun2024.txt.gz"
source_output = aiq_archive_base_path + "aiq_girlscout_append/April2024"
table_name = "bronze_alwayson.archive_aiq_girlscout_append_april2024"

df = spark.read.csv(source_input, header=True, sep="|")
df = upper_case_columns(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Single Race & Ethnicity
source_input = analyticsiq_root + "1037_10000301_INPUT_AIQ_D20240724_ace&E.psv"
source_output = aiq_archive_base_path + "aiq_race_ethinicity_auxiliary2/April2024"
table_name = "bronze_alwayson.archive_aiq_race_ethinicity_auxiliary_2_april2024"

df = spark.read.parquet(source_input)
df = upper_case_columns(df)

display(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,AIQ Wave
source_input = analyticsiq_root + "AIQ_Wave82to89_FullPopulationIDResolution_March2024.txt.gz"
source_output = aiq_archive_base_path + "aiq_id_resolution_wave/April2024"
table_name = "bronze_alwayson.archive_aiq_id_resolution_wave_april2024"

df = spark.read.csv(source_input, header=True, sep="|")
df = upper_case_columns(df)

display(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)
