# Databricks notebook source
# MAGIC %md 
# MAGIC # Load Data

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
    df = spark.read.parquet(wasbs_path)
    print('Register the DataFrame as a SQL temporary view: source')
    df.createOrReplaceTempView('source')

    # Display top 10 rows
    print('Displaying top 10 rows: ')
    display(spark.sql('SELECT * FROM source LIMIT 10'))
    return df

# COMMAND ----------

df=load_data()

# COMMAND ----------

display(dbutils.fs.ls(wasbs_path))

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

dbutils.fs.ls("wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2001/")

# COMMAND ----------

df2=spark.read.parquet("wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2001/")

# COMMAND ----------

(df.filter("puYear=0").explain())

# COMMAND ----------

df.where("puYear=2012").selectExpr("_metadata.file_path","_metadata.file_size","*").display()

# COMMAND ----------

df.where("puYear=2012").selectExpr("_metadata.file_path").distinct().display()

# COMMAND ----------

df.where("puYear=2012  and tripDistance=2.1").selectExpr("_metadata.file_path","_metadata.file_size","*").display()

# COMMAND ----------

display(dbutils.fs.ls("wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2001/puMonth=1/")+dbutils.fs.ls("wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2001/puMonth=2/"))

# COMMAND ----------

df.filter("puYear=2001").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC update peopel