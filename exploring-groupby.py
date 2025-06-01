# Databricks notebook source
# MAGIC %md
# MAGIC #Exploring GroupBy in Spark
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC GroupBy has below signature  
# MAGIC     ` def groupBy(self, *cols: "ColumnOrName") -> "GroupedData":`  
# MAGIC     `def groupBy(self, __cols: Union[List[Column], List[str]]) -> "GroupedData"`:
# MAGIC
# MAGIC    `def groupBy(self, *cols: "ColumnOrName") -> "GroupedData"`  
# MAGIC [source](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)
# MAGIC
# MAGIC
# MAGIC GroupedData is a class
# MAGIC `pyspark.sql.GroupedData(jgd: py4j.java_gateway.JavaObject, df: pyspark.sql.dataframe.DataFrame)`  
# MAGIC [source](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.html?highlight=groupeddata#pyspark.sql.GroupedData)  

# COMMAND ----------

# MAGIC %md
# MAGIC ColumnName has a type as this  
# MAGIC `ColumnOrName = Union[Column, str]`  
# MAGIC [source](https://github.com/apache/spark/blob/master/python/pyspark/sql/_typing.pyi)

# COMMAND ----------

# MAGIC %md 
# MAGIC Grouped Data has the following main methods
# MAGIC - direct agg functions like min,max
# MAGIC - indirect agg function and it uses
# MAGIC - identifying dups 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Load dataset from online 

# COMMAND ----------



# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime 

# COMMAND ----------

# MAGIC %md
# MAGIC #Download Dataset

# COMMAND ----------

from pyspark import SparkFiles

# COMMAND ----------

spark.sparkContext.addFile("https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv")
df_data_path=SparkFiles.get("titanic.csv")
print(df_data_path)
                           

# COMMAND ----------

df=spark.read.csv(f"file://{df_data_path}",header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Simple Groupby with explicit agg function

# COMMAND ----------

type(df.groupBy("Survived"))

# COMMAND ----------

# MAGIC %md
# MAGIC count() has below signature  
# MAGIC     `GroupedData.count() → pyspark.sql.dataframe.DataFrame`
# MAGIC min has below signature  
# MAGIC
# MAGIC `GroupedData.min(*cols: str) → pyspark.sql.dataframe.DataFrame`  
# MAGIC [source](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.min.html#pyspark.sql.GroupedData.min)  

# COMMAND ----------

df.groupBy("Survived").count().display()
df.groupBy("Survived").min("Age").display()

# COMMAND ----------

df.groupBy("Survived").min("Age","Fare").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #GroupBy Multiple columns

# COMMAND ----------

df.groupBy("Survived","Pclass").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Making it more powerful with ColumnExpr  
# MAGIC
# MAGIC [source](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.expr.html?highlight=expr#pyspark.sql.functions.expr)

# COMMAND ----------

df.groupBy(F.expr("case when Survived=1 then 'alive' else 'dead'end as survive_status"),"Pclass").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Advanced Chaining with agg function

# COMMAND ----------

# MAGIC %md 
# MAGIC Agg function has below signature  
# MAGIC `GroupedData.agg(*exprs: Union[pyspark.sql.column.Column, Dict[str, str]]) → pyspark.sql.dataframe.DataFrame`

# COMMAND ----------

df.groupBy("survived").agg({"age":"min","age":"max","fare":"sum"}).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Overcoming all issues with Expr 

# COMMAND ----------

df.groupBy("survived").agg(F.expr("min(age) as min_age"),
                           F.expr("max(age) as max_age"),
                           F.expr("sum(fare) as fare_sum"),
                           F.expr("min(case when fare is null then -1 else fare end) as null_fare")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Unique Value Analysis with CollectSet,CollectList

# COMMAND ----------

df.groupBy("survived").agg(F.collect_set("pclass").alias("unique_survived_class")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Check Dups using groupby All columns

# COMMAND ----------

df.groupBy().count().where("count>1").display()

# COMMAND ----------

