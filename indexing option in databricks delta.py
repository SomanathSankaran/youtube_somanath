# Databricks notebook source
# MAGIC %md 
# MAGIC #Overview

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks doesn’t use traditional database indexes, but it provides powerful data skipping and layout optimizations through features like Z-Ordering, Partitioning, and Bloom Filters to drastically speed up queries on large datasets.
# MAGIC - Partitoning 
# MAGIC - z-order
# MAGIC - liquid clustering
# MAGIC - bloom filter

# COMMAND ----------

# MAGIC %md
# MAGIC # Partitioning

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table if exists people_partitioned;
# MAGIC CREATE TABLE people_partitioned (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   age INT,
# MAGIC   country STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (country);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##_Data_ Insert

# COMMAND ----------

from pyspark.sql.functions import expr, rand, monotonically_increasing_id
from pyspark.sql.types import *
import random

# COMMAND ----------

def load_data(table,partition_cols=None,limit=None):
  names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hannah", "Ivan", "Jack"]
  countries = ["USA", "UK", "India", "Germany", "Canada"]
  if limit:
    limit=limit
  else:
    limit=10000000
  # Generate 100 mock rows
  data = [
      (
          i + 1,
          random.choice(names),
          random.randint(18, 60),
          random.choice(countries)
      )
      for i in range(10000000)
  ]

  # Create DataFrame
  schema = StructType([
      StructField("id", IntegerType(), False),
      StructField("name", StringType(), True),
      StructField("age", IntegerType(), True),
      StructField("country", StringType(), True)
  ])

  df = spark.createDataFrame(data, schema)

  # Write to partitioned Delta table
  if partition_cols:
    df.write \
      .format("delta") \
      .partitionBy(f"{partition_cols}").mode("append").saveAsTable(f"{table}")
  else:
    df.write \
      .format("delta") \
      .mode("append").saveAsTable(f"{table}")

# COMMAND ----------

load_data("people_partitioned","country")

# COMMAND ----------

# MAGIC %md
# MAGIC #Analyzing Partitions

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended people_partitioned

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/people_partitioned"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/people_partitioned/country=Canada/"))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/people_partitioned/_delta_log/'))

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_partitioned/_delta_log/00000000000000000000.json"))

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_partitioned/_delta_log/00000000000000000001.json"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people_partitioned where lower(country)="canada"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from people_partitioned where lower(country)="canada"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from people_partitioned where name='Alice'

# COMMAND ----------

# MAGIC %md
# MAGIC ##Update Partition

# COMMAND ----------

# MAGIC %sql
# MAGIC update people_partitioned set country='canada' where country="Canada"

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/people_partitioned"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/people_partitioned/_delta_log/"))

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_partitioned/_delta_log/00000000000000000002.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Update Value

# COMMAND ----------

# MAGIC %sql
# MAGIC update people_partitioned set name='alice' where name='Alice'

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_partitioned/_delta_log/00000000000000000003.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##delete value

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from people_partitioned where name="Diana"

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/people_partitioned/_delta_log/"))

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_partitioned/_delta_log/00000000000000000004.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Partition limitation
# MAGIC - It can work only on low cardinality(less no of distinct value)
# MAGIC - skewness cannot be effectively handled
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Z-Order

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize people_partitioned zorder by (name,id)

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize people_partitioned zorder by (id)

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_partitioned/_delta_log/00000000000000000005.json"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people_partitioned where name='alice' and id between 1 and 100

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into people_partitioned 
# MAGIC select * from people_partitioned where country='canada' limit 10

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_partitioned/_delta_log/00000000000000000006.json"))

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize people_partitioned zorder by (name,id)

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_partitioned/_delta_log/00000000000000000007.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Z-Order limitation
# MAGIC - It can work only on high cardinality
# MAGIC - it is not incremental making a small change results in re-writing of entire files
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Liquid Clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table if exists people_liquid;
# MAGIC CREATE TABLE people_liquid (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   age INT,
# MAGIC   country STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC cluster BY (country,name);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##_Data_ Insert

# COMMAND ----------

load_data("people_liquid")

# COMMAND ----------

# MAGIC %md
# MAGIC #Analyzing liquid clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended people_liquid

# COMMAND ----------

[clusteringColumns=[["country"],["name"]],delta.checkpointPolicy=v2,delta.enableDeletionVectors=true,delta.enableRowTracking=true,delta.feature.clustering=supported,delta.feature.deletionVectors=supported,delta.feature.domainMetadata=supported,delta.feature.rowTracking=supported,delta.feature.v2Checkpoint=supported,delta.minReaderVersion=3,delta.minWriterVersion=7,delta.rowTracking.materializedRowCommitVersionColumnName=_row-commit-version-col-c1079ef7-fdc4-40bd-bf9e-bfee0d6ac6a8,delta.rowTracking.materializedRowIdColumnName=_row-id-col-1fdc49d3-800e-4ed0-a262-4bb479ee2676]

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_liquid/_delta_log/00000000000000000000.json"))

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_liquid/_delta_log/00000000000000000001.json"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people_liquid where country='Canada'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from people_liquid where name='Alice'

# COMMAND ----------

# MAGIC %md
# MAGIC ##Update Liquudi Clustering
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update people_liquid set country='canada' where country="Canada"

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_liquid/_delta_log/00000000000000000002.json"))

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize people_liquid

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delete Liquid Clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from people_liquid where name="Diana"

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/people_liquid/_delta_log/00000000000000000005.json"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/people_liquid/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Liquid Clustering Incremental Refresh

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC insert into people_liquid
# MAGIC select * from people_liquid  limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize people_liquid

# COMMAND ----------

# MAGIC %md
# MAGIC # Row Tracking in Liquid Cluster
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select _metadata from people_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC select _metadata.row_id,min(_metadata.row_id) over w as min_row_track,max(_metadata.row_id) over w as max_row_track from people_liquid
# MAGIC window w as ()

# COMMAND ----------

# MAGIC %md
# MAGIC #Concurrency update in Liquid clustering

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

# Define update function using Spark SQL
def update_name_to_lowercase(table,target_name):
    print(f"🔁 Starting update for: {target_name} in table {table}")
    spark.sql(f"""
      UPDATE {table}
      SET name = LOWER(name)
      WHERE name = '{target_name}'
    """)
    print(f"✅ Completed update for: {target_name}")

# List of target names to update concurrently


# COMMAND ----------

names_to_update = ['Frank', 'Grace']

# Use ThreadPoolExecutor for parallel updates
with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [executor.submit(update_name_to_lowercase, *("people_liquid",name)) for name in names_to_update]
    for future in futures:
        future.result()  # Ensures any exceptions are raised

print("🚀 All updates completed.")


# COMMAND ----------

names_to_update = ['Frank', 'Grace']

# Use ThreadPoolExecutor for parallel updates
with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [executor.submit(update_name_to_lowercase, *("people_partitioned",name)) for name in names_to_update]
    for future in futures:
        future.result()  # Ensures any exceptions are raised

print("🚀 All updates completed.")


# COMMAND ----------

# MAGIC %md
# MAGIC #Why Liquid clustering
# MAGIC - It can work only on high and low cardinality
# MAGIC - it is incremental and does re-write only when needed
# MAGIC - handles concurrency using deletion vectors and row_id tracking
# MAGIC - Use **Partitioning** only for staging tables 
# MAGIC

# COMMAND ----------

