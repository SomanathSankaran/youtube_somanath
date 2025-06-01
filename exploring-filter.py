# Databricks notebook source
# MAGIC %md
# MAGIC #Exploring Filter in Spark
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC Filter has below signature  
# MAGIC     `def filter(self, condition: "ColumnOrName") -> "DataFrame":`
# MAGIC [source](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html?highlight=filter#pyspark.sql.DataFrame.filter)
# MAGIC
# MAGIC
# MAGIC ColumnName has a type as this  
# MAGIC `ColumnOrName = Union[Column, str]`  
# MAGIC [source](https://github.com/apache/spark/blob/master/python/pyspark/sql/_typing.pyi)  
# MAGIC ColumnType-[source](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.html#pyspark.sql.Column)  

# COMMAND ----------

# MAGIC %md 
# MAGIC As per documentation,filter accepts a Column of **types.BooleanType** or a string of SQL expressions.
# MAGIC
# MAGIC So, We need to explore columnType which returns bool below are few of the things 
# MAGIC - isNull
# MAGIC - isNotNull
# MAGIC - ilike
# MAGIC - like
# MAGIC - startswith
# MAGIC - isin  
# MAGIC SqlExpr String- Or some of the sql expression meaning where condition which returns true or false for eg
# MAGIC where age=10 age is not null etc

# COMMAND ----------

# MAGIC %md
# MAGIC #Create a dummy data Frame with 4 columns:name ,age,place,join_date

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime 

# COMMAND ----------

datetime(2023,1,1)

# COMMAND ----------

df = spark.createDataFrame([
         (2, "Alice","chennai",datetime(2012,10,1)), (5, "Bob","delhi",datetime(2024,10,10)), (25, "Michele","Mumbai",None),(10,None,"Delhi",datetime(2025,10,1)),(10,'  ',"Delhi",datetime(2025,10,1))], schema=["age", "name","place","join_date"])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Simple String Filter

# COMMAND ----------

df.filter("age=2").display()
df.filter(F.col("age").eqNullSafe("2")).display()
df.filter(F.col("age").eqNullSafe(2)).display()
df.filter(F.col("join_date").eqNullSafe(datetime(2025,10,1))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter for Null ![Values](path)

# COMMAND ----------

df.filter(F.col("name").isNull()).display()
df.filter("name is null").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter using Like and ilike(case insensitive) values for partial matching

# COMMAND ----------

df.filter("place like '%del%'").display()
df.filter(F.col("place").like("del%")).display()
df.filter(F.col("place").ilike("del%")).display()


# COMMAND ----------

df.filter(F.col("place").ilike(2)).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Range Filter using ![between](path)

# COMMAND ----------

df.filter("age between 5 and 10").display()
df.filter(F.col("age").between(5,10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Anything that return boolean right!!

# COMMAND ----------

df.filter("1=1").display()
df.filter("1=0").display()
#this will help in creating empty_df for capture target schema in unioning multiple df

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding Python Dynamism

# COMMAND ----------

# MAGIC %md
# MAGIC We will get the lower and upperbound using Fstring and store this in config table for incremental loading

# COMMAND ----------

startdate=datetime(2025,1,1)
endtime=datetime(2025,12,1)

# COMMAND ----------

print(f"join_date between '{startdate}' and '{endtime}'")
df.filter(f"join_date between '{startdate}' and '{endtime}'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding SqlExpr Dynamism

# COMMAND ----------

df.filter(f"case when Name is null or trim(name)='' then 'Unknown' else name end='Unknown'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding Negation with != and tilde(~)

# COMMAND ----------

df.filter(f"case when Name is null or trim(name)='' then 'Unknown' else name end!='Unknown'").display()
df.filter(~F.expr("case when Name is null or trim(name)='' then 'Unknown' else name end").eqNullSafe("Unknown")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Combining multiple filters using and /or

# COMMAND ----------

df.filter((F.col("age")<10) & (F.expr("place='chennai'"))).selectExpr("*","'and filter' as filtertype").display() #and filer
df.filter("age< 10 and place='chennai' ").selectExpr("*","'and filter' as filtertype").display()
df.filter((F.col("age")<10) | (F.expr("place='chennai'"))).selectExpr("*","'or filter' as filtertype").display() #and filer
df.filter("age< 10 or place='chennai' ").selectExpr("*","'or filter' as filtertype").display()

# COMMAND ----------

