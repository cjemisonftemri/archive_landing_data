# Databricks notebook source
# DBTITLE 1,Init
# MAGIC %run ./archive_util

# COMMAND ----------

# DBTITLE 1,TU - Full Population - April 2024
source_input = transunion_root + "FullPopulation/Attribute" 
source_output = tu_archive_base_path + "full_population/April2024"
table_name = "bronze_alwayson.archive_tu_full_population_april24"

archive_csv(source_input, 
            source_output, 
            table_name)

# COMMAND ----------

# DBTITLE 1,TU - Full Population - July 2024
source_input = transunion_root + "TU_Spring2024_Q32024Refresh_July2024" 
source_output = tu_archive_base_path + "full_population/July2024"
table_name = "bronze_alwayson.archive_tu_full_population_july24"

archive_parquet(source_input, source_output, table_name)

# COMMAND ----------

# DBTITLE 1,TU - Resolution - April 2024
source_input = transunion_root + "Resolution" 
source_output = tu_archive_base_path + "id_resolution/April2024"
table_name = "bronze_alwayson.archive_tu_id_resolution_april24"

archive_csv(source_input, 
            source_output, 
            table_name)

# COMMAND ----------

# DBTITLE 1,TU/AIQ Crosswalk - August 2024
source_input = transunion_root + "Crosswalk/MRI_TU_AIQ_Crosswalk_August2024_08.05.2024" 
source_output = tu_archive_base_path + "tu_aiq_crosswalk/August2024"
table_name = "bronze_alwayson.archive_tu_aiq_crosswalk_august2024"

archive_parquet(source_input, source_output, table_name)

# COMMAND ----------

# DBTITLE 1,TU - Dictionary - June 2024
source_input = transunion_root + "Dictionary"
source_output = tu_archive_base_path + "dictionary/June2024"
table_name = "bronze_alwayson.archive_tu_dictionary_june2024"

df = spark.read.csv(source_input, header=True, sep=",")

df = upper_case_columns(df)
display(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Full Population - July 2024
source_input = transunion_root + "TU_AlwaysOnFiles_April2024"
source_output = tu_archive_base_path + "dictionary/June2024"
table_name = "bronze_alwayson.archive_tu_dictionary_june2024"

df = spark.read.parquet(source_input)


# COMMAND ----------

source_input = transunion_root + "AIQ_HEM_TO_TU_20240730"
source_output = tu_archive_base_path + "aiq_to_tu_hem/July2024"
table_name = "bronze_alwayson.archive_aiq_to_tu_hem_July2024"

df = spark.read.csv(source_input, header=True, sep="|")

df = upper_case_columns(df)
display(df)

spark.sql(f"drop table if exists {table_name}")
df.write.format("delta").option("compression", "snappy").option(
    "path", source_output
).saveAsTable(table_name)
