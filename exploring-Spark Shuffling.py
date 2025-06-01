# Databricks notebook source
# MAGIC %md
# MAGIC # Overview 

# COMMAND ----------

# MAGIC %md
# MAGIC In this video , we will see how does shuffling works in spark 
# MAGIC - what does aqe bring to the shuffle mechanism
# MAGIC - Dynamically coalesces partitions (combine small partitions into reasonably sized partitions) after shuffle exchange. Very small tasks have worse I/O throughput and tend to suffer more from scheduling overhead and task setup overhead. Combining small tasks saves resources and improves cluster throughput.
# MAGIC - when not to use aqe

# COMMAND ----------

# MAGIC %md
# MAGIC #Enabling Disk IO Cache

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")


# COMMAND ----------

# MAGIC %md
# MAGIC #Load Sample Data

# COMMAND ----------

# MAGIC %md
# MAGIC Data is loaded using azure open Dataset
# MAGIC [url](https://learn.microsoft.com/en-us/azure/open-datasets/dataset-taxi-yellow?tabs=pyspark#volume-and-retention)

# COMMAND ----------

  def load_data():
    # Azure storage access info
    blob_account_name = "azureopendatastorage"
    blob_container_name = "nyctlc"
    blob_relative_path = "yellow"
    blob_sas_token = "r"

    # Allow SPARK to read from Blob remotely
    wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
    spark.conf.set(
      'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
      blob_sas_token)
    print('Remote blob path: ' + wasbs_path)

    # SPARK read parquet, note that it won't load any data yet by now
    df = spark.read.parquet(wasbs_path).where("puyear between 2000 and 2012")
    
    return df

# COMMAND ----------

df=load_data()

# COMMAND ----------

# MAGIC %md
# MAGIC # Check AQE Enablement

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled
# MAGIC

# COMMAND ----------

spark.conf.get('spark.databricks.optimizer.adaptive.enabled')

# COMMAND ----------

df_limit=df.limit(10)

# COMMAND ----------

# MAGIC %md
# MAGIC - Important Note AQE Works only when there is a shuffle

# COMMAND ----------

df_limit.display()

# COMMAND ----------

df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC # Concept of Shuffle Exchange

# COMMAND ----------

df_shuffle=df.where("puyear=2012").groupBy("vendorId").count()

# COMMAND ----------

df_shuffle.explain()

# COMMAND ----------

375%200

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled=false
# MAGIC

# COMMAND ----------

df_shuffle.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.shuffle.partitions

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.shuffle.partitions=1

# COMMAND ----------

df_shuffle.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled=true
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.shuffle.partitions=200

# COMMAND ----------

df_shuffle.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.shuffle.partitions=auto

# COMMAND ----------

df_shuffle.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Where AQE Shines

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled=false
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.shuffle.partitions=200

# COMMAND ----------

df_sample=df.where("puyear=2012").selectExpr("*",
"row_number() over (partition by vendorid order by tpepPickupDateTime desc ) as rn").where("rn<200").groupby("passengerCount").count()

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.shuffle.partitions=200

# COMMAND ----------

df_sample.explain()

# COMMAND ----------

df_sample.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.shuffle.partitions=1

# COMMAND ----------

df_sample.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled=true;
# MAGIC set spark.sql.shuffle.partitions=auto

# COMMAND ----------

df_sample.display()

# COMMAND ----------

