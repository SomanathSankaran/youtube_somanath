# Databricks notebook source
# MAGIC %md
# MAGIC #Exploring Select in Spark
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC Select has mainly 2 Apis.
# MAGIC -   Select
# MAGIC -   SelectExpr

# COMMAND ----------

# MAGIC %md
# MAGIC Select has a signature as below  
# MAGIC `DataFrame.select(*cols: ColumnOrName) → DataFrame`  
# MAGIC     ` @overload
# MAGIC     def select(self, *cols: "ColumnOrName") -> "DataFrame":`
# MAGIC    
# MAGIC `   @overload
# MAGIC     def select(self, __cols: Union[List[Column], List[str]]) -> "DataFrame":`  
# MAGIC [source](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html?highlight=select#pyspark.sql.DataFrame.select)

# COMMAND ----------

# MAGIC %md
# MAGIC ColumnName has a type as this  
# MAGIC `ColumnOrName = Union[Column, str]`  
# MAGIC [source](https://github.com/apache/spark/blob/master/python/pyspark/sql/_typing.pyi)

# COMMAND ----------

# MAGIC %md
# MAGIC It means we can select
# MAGIC - Any no of  column as column or column as string(* cols (stargs) means any no of columns)
# MAGIC - List of Columns
# MAGIC - List of Expr (since expr also return a column type pyspark.sql.functions.expr(str: str) → pyspark.sql.column.Column)
# MAGIC - Since python is loosely typed .We can also Mix and match types 

# COMMAND ----------

# MAGIC %md
# MAGIC #Create a dummy data Frame with 3 columns:name ,age

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime

# COMMAND ----------

df = spark.createDataFrame([
         (2, "Alice"), (5, "Bob"), (25, "Michele")], schema=["age", "name"])
df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #Exploring select Options

# COMMAND ----------

help(F.col)

# COMMAND ----------

#Select any no of column/string name => *cols: ColumnOrName
df.select(F.col("name"),F.col("age")).display() 
df.select("name","age").display()

# COMMAND ----------

help(F.expr)

# COMMAND ----------

#mix and match String and column
df.select(F.col("name"),F.col("age"),F.expr("'columms selected as column_type'  as type")).display() 
df.select("name","age",F.expr("'columms selected as string'  as type")).display()

# COMMAND ----------

#list of columns
df.select([F.col("name"),F.col("age"),F.expr("'columms selected as list'  as type")]).display() 



# COMMAND ----------

#list of columns
df.select(["name","age",F.expr("'string name selected as list'  as type")]).display() 


# COMMAND ----------

# MAGIC %md
# MAGIC #Why SelectExpr

# COMMAND ----------

# MAGIC %md 
# MAGIC Assume we need to do a transformation say lower of Name ,and create an is_adult column additionally we can use below syntax

# COMMAND ----------

df.select("*",F.expr("case when age<18 then 'young' else 'adult' end as adult_ind"),F.expr("lower(name) as name_lowered")).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC But this is ugly!!

# COMMAND ----------

# MAGIC %md 
# MAGIC ##let us do it better using List Comprehension

# COMMAND ----------

def multiplier_comprehension(X):return X*2
input=[1,2,3]
output=[multiplier_comprehension(i) for i in input]
print(input,output)

# COMMAND ----------

transformation=["case when age<18 then 'young' else 'adult' end as adult_ind","lower(name) as name_lowered"]
df.select(["*"]+[F.expr(x) for x in transformation]).display()

# COMMAND ----------

datetime.now().day

# COMMAND ----------

# MAGIC %md
# MAGIC ## SelectExpr is the same for sql folks

# COMMAND ----------

transformation=["case when age<18 then 'young' else 'adult' end as adult_ind","lower(name) as name_lowered"]
df.selectExpr(["*"]+transformation).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Finally adding constant with Lit

# COMMAND ----------

# MAGIC %md
# MAGIC Lit has below Signature  
# MAGIC `def lit(col: Any) -> Column:`
# MAGIC    
# MAGIC [source](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lit.html?highlight=lit)

# COMMAND ----------

#It means we can truly select anything including dates!!!
ingested_time=datetime.now()
print(ingested_time)
df2=df.select("*",F.lit(1).alias("int_column"),F.expr("1 as int_column"),F.lit(ingested_time).alias("date_column"))
display(df2)

# COMMAND ----------

df2.dtypes

# COMMAND ----------

