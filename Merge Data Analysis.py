# Databricks notebook source
# MAGIC %md
# MAGIC #Overview
# MAGIC In this video, we explore how analyzing MERGE queries in SQL can often be a complex and opaque process. To address this, we’ll leverage row-level tracking using a row_id to make MERGE operations more transparent, traceable, and easier to debug. We’ll walk through practical techniques to both understand and optimize your MERGE workflows.
# MAGIC
# MAGIC Topics Covered:
# MAGIC - Introducing Row-Level Tracking – Using row_id to track data changes 
# MAGIC - Overwrite vs Merge 
# MAGIC - Effective merges leveraging Row Level Tracking

# COMMAND ----------

# DBTITLE 1,Create a  sample table with Row Tracking
# MAGIC %sql
# MAGIC DROP TABLE if exists spark_catalog.default.sample_data ;
# MAGIC CREATE TABLE spark_catalog.default.sample_data (
# MAGIC   ID INT,
# MAGIC   Description STRING,
# MAGIC   Start_Date DATE,
# MAGIC   End_Date DATE)
# MAGIC USING delta
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2',
# MAGIC   'delta.enableRowTracking' = true)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Loading Initial Data
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date

# Define schema
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Description", StringType(), True),
    StructField("Start_Date", DateType(), True),
    StructField("End_Date", DateType(), True)
])

# Create data
data_existing = [
    (1, "abc", date(2021, 1, 1), date(9999, 12, 31)),
    (2, "def", date(2022, 3, 15), date(9999, 12, 31)),
    (3, "ghi", date(2023, 5, 10), date(9999, 12, 31)),
    (4, "pqr", date(2023, 5, 10), date(9999, 12, 31)),
]

# Create DataFrame
df_existing = spark.createDataFrame(data_existing, schema)

# Show DataFrame
df_existing.display()


# COMMAND ----------

df_existing.write.mode("append").saveAsTable("sample_data")

# COMMAND ----------

# DBTITLE 1,Viewing Row IDs
# MAGIC %sql
# MAGIC select _metadata.row_id,* from sample_data

# COMMAND ----------

# DBTITLE 1,df_new_data
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date

# Define schema
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Description", StringType(), True),
    StructField("Start_Date", DateType(), True),
    StructField("End_Date", DateType(), True)
])

# Create data
new_data = [
    (1, "abc", date(2021, 1, 1), date(9999, 12, 31)), # no change 
    (2, "xyz", date(2022, 3, 15), date(9999, 12, 31)), # value change with no changes to time attributes
    (3, "mno", date(2021, 5, 10), date(2022, 5, 9)),
    (3, "ghi", date(2022, 5, 10), date(9999, 12, 31)),
    (5, "pqr", date(2023, 5, 10), date(9999, 12, 31)),
]

# Create DataFrame
df_new = spark.createDataFrame(new_data, schema)

# Show DataFrame
df_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Update using ![overwrite](path)

# COMMAND ----------

df_new.write.mode("overwrite").saveAsTable("sample_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select _metadata.row_id,* from sample_data 

# COMMAND ----------

# MAGIC %md
# MAGIC # Limitation
# MAGIC Overwrite option is simple to implement but it results in loss of complete tracking as new row_ids are created

# COMMAND ----------

# MAGIC %sql
# MAGIC restore sample_data version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select _metadata.row_id,* from sample_data

# COMMAND ----------

df_new.createOrReplaceTempView("new_data")

# COMMAND ----------

# DBTITLE 1,Merge Option - I
# MAGIC
# MAGIC %sql
# MAGIC merge into spark_catalog.default.sample_data tgt
# MAGIC using new_data src 
# MAGIC on src.id=tgt.id
# MAGIC and src.description = tgt.description
# MAGIC and src.start_date=tgt.start_date
# MAGIC and src.end_date=tgt.end_date
# MAGIC when not matched then insert *  -- this is for new record (highlighted in yellow in excel)
# MAGIC when not matched by source then delete   -- (this is for records which are not present today red color in excel)

# COMMAND ----------

# MAGIC %sql
# MAGIC select _metadata.row_id,* from sample_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Limitation
# MAGIC In the above approach recording is coming correct but the traces of IDs are  completely removed and re-written a lot except for changed records

# COMMAND ----------

# MAGIC %sql
# MAGIC restore sample_data version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select _metadata.row_id,* from sample_data

# COMMAND ----------

# DBTITLE 1,Effective Merge
# MAGIC
# MAGIC %sql
# MAGIC merge into spark_catalog.default.sample_data tgt
# MAGIC using new_data src 
# MAGIC on src.id=tgt.id
# MAGIC --and src.description = tgt.description
# MAGIC and src.start_date=tgt.start_date
# MAGIC --and src.end_date=tgt.end_date
# MAGIC WHEN MATCHED and (src.end_date!= tgt.end_date or src.description != tgt.description) THEN UPDATE set  tgt.end_date= src.end_date ,  tgt.description=src.description
# MAGIC when not matched then insert *
# MAGIC when not matched by source then delete  

# COMMAND ----------

# MAGIC %sql
# MAGIC select _metadata.row_id,* from spark_catalog.default.sample_data

# COMMAND ----------

