# Databricks notebook source
# MAGIC %md
# MAGIC #Exploring OrberBy(Sort) in Spark
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC Filter has below signature  
# MAGIC     `DataFrame.sort(*cols: Union[str, pyspark.sql.column.Column, List[Union[str, pyspark.sql.column.Column]]], **kwargs: Any) → pyspark.sql.dataframe.DataFrame`
# MAGIC
# MAGIC [source](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sort.html?highlight=sort#pyspark.sql.DataFrame.sort)

# COMMAND ----------

# MAGIC %md
# MAGIC #Create a dummy data Frame with 4 columns:name ,age,place,join_date

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime 

# COMMAND ----------

df = spark.createDataFrame([
         (2, "Alice","chennai",datetime(2012,10,1)), (5, "Bob","delhi",datetime(2024,10,10)), (25, "Michele","Mumbai",None),(10,None,"Delhi",datetime(2025,10,1)),(10,'  ',"Delhi",datetime(2025,10,1))], schema=["age", "name","place","join_date"])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Single Column Sort

# COMMAND ----------

df.sort("age").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Multi Column Sort

# COMMAND ----------

df.sort("age","join_date").display()
df.sort(["age","join_date"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Changing order of sorting

# COMMAND ----------

# MAGIC %md
# MAGIC By Default Spark uses sorting asc   we can use **kwargs to specify the ordering using boolean values

# COMMAND ----------

# MAGIC %md
# MAGIC ##Other Parameters
# MAGIC - ascendingbool or list, optional, default True  
# MAGIC - boolean or list of boolean.   
# MAGIC - Sort ascending vs. descending. Specify list for multiple sort orders. If a list is specified, the length of the list must equal the length of the cols.  

# COMMAND ----------

df.sort(["age","join_date"],**{"ascending":[0,0]}).display()

# COMMAND ----------

df.sort(["age","join_date"],**{"ascending":[0,0],"nulls":"FIRST"}).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Handling Nulls while sorting

# COMMAND ----------

df.sort(F.col("join_date").asc_nulls_last()).display()

# COMMAND ----------

