# Databricks notebook source
# MAGIC %md
# MAGIC # Overview

# COMMAND ----------

# MAGIC %md
# MAGIC As part of this Video ,We will understand  
# MAGIC
# MAGIC | **Feature**               | **Description** |
# MAGIC |---------------------------|---------------|
# MAGIC | **Custom Spark DataSource** | Implements a custom Spark DataSource for reading YAML files . |
# MAGIC | **Schema Definition**      | Defines a structured schema using PySpark `StructType` for data validation. |
# MAGIC | **Parallel Processing**    | Distributes YAML file reading across Spark executors using partitioning. |
# MAGIC | **Schema Validation**      | Utilizes Pydantic for YAML schema validation and error handling. |
# MAGIC | **Flexible Data Loading**  | Supports reading YAML data with Spark’s `format()` API and configurable partitioning & Partitioning. |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC [custom_source](https://github.com/apache/spark/blob/0d7c07047a628bd42eb53eb49935f5e3f81ea1a1/python/pyspark/sql/datasource.py)

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader,InputPartition
from pyspark.sql.types import *
from pyspark.errors import PySparkNotImplementedError
import yaml,os
from collections import defaultdict
from pyspark import SparkFiles



# COMMAND ----------

# MAGIC %md
# MAGIC # helper notebook to convert pyspark to pydantic schema

# COMMAND ----------

# MAGIC %run ./pyspark_schema_to_pydantic

# COMMAND ----------

# MAGIC %md
# MAGIC #Defining Schema

# COMMAND ----------

column_schema = StructType([
    StructField("description", StringType(), True),
    StructField("name", StringType(), True),
    StructField("tests", ArrayType(StringType(), True), True)  # Array of strings
])

# Define the schema for "models" elements
model_schema = StructType([
    StructField("columns", ArrayType(column_schema, True), True),  # Array of column structs
    StructField("description", StringType(), True),
    StructField("name", StringType(), True)
])

# Define the root schema
dbt_schema = StructType([
    StructField("models", ArrayType(model_schema, True), False),  # Array of model structs
    StructField("version", LongType(), True) , # Version as long
    StructField("_corrupt",StringType(),True)
])

type(dbt_schema)

# COMMAND ----------

# MAGIC %md 
# MAGIC We are copying the local files so that it will be available in executor

# COMMAND ----------

dbfs_path="dbfs:/FileStore/custom_source_demo/"
local_path="/tmp/custom_source_demo/"
dbutils.fs.cp(dbfs_path, f"file://{local_path}",recurse=True)
executor_root_dir=SparkFiles.getRootDirectory()

# COMMAND ----------

print(executor_root_dir)

# COMMAND ----------

spark.sparkContext.addFile("/tmp/custom_source_demo/",True)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/custom_source_demo/

# COMMAND ----------

# MAGIC %sh 
# MAGIC cat  /tmp/custom_source_demo/corrupt.yaml
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Defining basic Custom Source & Source Reader

# COMMAND ----------

class YamlConfigReader(DataSourceReader):

    def __init__(self, schema, options,path):
        self.options = options
        self.schema = schema
        self.options = options
        self.path=path
    def partitions(self):
      
      return  [InputPartition(0)]
    
    def _bucket_files_by_name(self,folder_path, num_buckets):
      """Distributes files in a folder into n buckets based on filename."""
      if not os.path.isdir(folder_path):
          raise ValueError("Invalid folder path")

      files = sorted(os.listdir(folder_path))  # Sort files for consistency
      buckets = defaultdict(list)

      # Assign files to buckets based on a hashing function (e.g., modulo)
      for file in files:
          print(file)
          if file.endswith("yaml"):
            print(hash(file))
            bucket_index = hash(file) % num_buckets  # Hashing ensures even distribution
            buckets[bucket_index].append(folder_path+file)

      return buckets
  
    def _read_yaml(self,file_path):
      """Reads a YAML file and returns its content as a dictionary."""
      try:
          with open(file_path, "r", encoding="utf-8") as file:
              data = yaml.safe_load(file)  # Safe loading to prevent security risks
          return data
      except Exception as e:
          print(f"Error reading YAML file: {e}")
      
    def read(self,partition):
      num_buckets=1
      files_dict=self._bucket_files_by_name(self.path,num_buckets)
      print(files_dict,"soma")
      for file in files_dict[partition.value]:

        #for file in files:
        data=self._read_yaml(file)
        print(data)
        YamlModel = spark_schema_to_pydantic("Yaml_Schema", self.schema)
        try:
          validated_data=YamlModel(**data).dict()
          validated_data["_corrupt"]=None
          yield tuple(validated_data.values())
        except:
          
          for field, field_type in YamlModel.__annotations__.items():
            if getattr(field_type, "__origin__", None) == list:  # If it's a list type
                validated_data[field] = []  # Replace with an empty list
            else:
                validated_data[field] = None
          print(validated_data)
          validated_data["_corrupt"]=data
          yield tuple(validated_data.values())







# COMMAND ----------

class YamlReader(DataSource):
    def __init__(self, options: dict):
        self.options = options
        self.path=self.options["loc"]
        
 

    @classmethod
    def name(cls):
        return "yaml"

    def schema(self):
        #print(self.schema,type(schema))
        return dbt_schema #Hardcoded_schema

    def reader(self, schema: StructType):
        return YamlConfigReader(schema, self.options,self.path)

# COMMAND ----------

column_schema = StructType([
    StructField("description", StringType(), True),
    StructField("name", StringType(), True),
    StructField("tests", ArrayType(StringType(), True), True)  # Array of strings
])

# Define the schema for "models" elements
model_schema = StructType([
    StructField("columns", ArrayType(column_schema, True), True),  # Array of column structs
    StructField("description", StringType(), True),
    StructField("name", StringType(), True)
])

# Define the root schema
dbt_schema = StructType([
    StructField("models", ArrayType(model_schema, True), False),  # Array of model structs
    StructField("version", LongType(), True) , # Version as long
    StructField("_corrupt",StringType(),True)
])

type(dbt_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #Register Custom Source

# COMMAND ----------

spark.dataSource.register(YamlReader) 


# COMMAND ----------

executor_root_dir

# COMMAND ----------


df=spark.read.format("yaml").options( loc=f"{executor_root_dir}/custom_source_demo/").load()

# COMMAND ----------

df.display()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dynamic Schema Handling & Parallelism

# COMMAND ----------

dbt_schema.json()

# COMMAND ----------

class YamlReader2(DataSource):
    
    def __init__(self, options: dict[str,Any]):
        self.options=options
        
        self.yaml_schema = self.options["yaml_schema"]
        print("checking ",type(self.yaml_schema))
        self.path=self.options["loc"]
        

    @classmethod
    def name(cls):
        return "yaml-parallel"

    def schema(self):
        print("check ing ",self.yaml_schema,type(self.yaml_schema))
        return json_schema_to_spark(json.loads( self.yaml_schema)) #dbt_schema

    def reader(self, schema: StructType):
        return YamlConfigReader2(schema, self.options,self.path)

# COMMAND ----------

class YamlConfigReader2(DataSourceReader):

    def __init__(self, schema, options,path):
        self.options = options
        self.schema = schema
        self.options = options
        self.path=path
        self.num_partitions=self.options["num_partitions"]
    def partitions(self):
      partition_list=[]
      for i in range(int(self.num_partitions)):
          partition_list.append(InputPartition(i))
      return  partition_list
    
    def _bucket_files_by_name(self,folder_path, num_buckets):
      """Distributes files in a folder into n buckets based on filename."""
      if not os.path.isdir(folder_path):
          raise ValueError("Invalid folder path")

      files = sorted(os.listdir(folder_path))  # Sort files for consistency
      buckets = defaultdict(list)

      # Assign files to buckets based on a hashing function (e.g., modulo)
      for file in files:
          print(file)
          if file.endswith("yaml"):
            print(hash(file))
            bucket_index = hash(file) % num_buckets  # Hashing ensures even distribution
            buckets[bucket_index].append(folder_path+file)

      return buckets
  
    def _read_yaml(self,file_path):
      """Reads a YAML file and returns its content as a dictionary."""
      try:
          with open(file_path, "r", encoding="utf-8") as file:
              data = yaml.safe_load(file)  # Safe loading to prevent security risks
          return data
      except Exception as e:
          print(f"Error reading YAML file: {e}")
      
    def read(self,partition):
      #num_buckets=1
      files_dict=self._bucket_files_by_name(self.path,int(self.num_partitions))
      print(files_dict,"soma")
      for file in files_dict[partition.value]:

        #for file in files:
        data=self._read_yaml(file)
        print(data)
        YamlModel = spark_schema_to_pydantic("Yaml_Schema", self.schema)
        try:
          validated_data=YamlModel(**data).dict()
          validated_data["_corrupt"]=None
          yield tuple(validated_data.values())
        except:
          validated_data={}
          for field, field_type in YamlModel.__annotations__.items():
            if getattr(field_type, "__origin__", None) == list:  # If it's a list type
                validated_data[field] = []  # Replace with an empty list
            else:
                validated_data[field] = None
          validated_data["_corrupt"]=data
          print(validated_data)
          yield tuple(validated_data.values())







# COMMAND ----------



# COMMAND ----------

spark.dataSource.register(YamlReader2)

# COMMAND ----------

dbt_schema.json()

# COMMAND ----------


df=spark.read.format("yaml-parallel").options(yaml_schema=dbt_schema.json(), loc=f"{executor_root_dir}/custom_source_demo/",
                                              num_partitions=3).load()

# COMMAND ----------

df.display()

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

df.select(F.spark_partition_id()).display()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

