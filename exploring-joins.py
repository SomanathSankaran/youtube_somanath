# Databricks notebook source
# MAGIC %md
# MAGIC #Exploring Join in Spark
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC Join has below signature  
# MAGIC `DataFrame.join(other: pyspark.sql.dataframe.DataFrame, on: Union[str, List[str], pyspark.sql.column.Column, List[pyspark.sql.column.Column], None] = None, how: Optional[str] = None) → pyspark.sql.dataframe.DataFrame
# MAGIC `  
# MAGIC [source](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join)

# COMMAND ----------

# MAGIC %md
# MAGIC So basically join will take 3 arguments  
# MAGIC - other- which is the other df with which we need to join
# MAGIC - on- condition on which it joins which can be column of list of str or list of column
# MAGIC - how-it is optional str ,meaning 

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Left and Right Dataframe

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime

# COMMAND ----------

left_df = spark.createDataFrame([
    (1, 2, "Alice", "chennai", datetime(2012, 10, 1)),
    (2, 5, "Bob", "delhi", datetime(2024, 10, 10)),
    (3, 25, "Michele", "Mumbai", None),
    (4, 10, None, "Delhi", datetime(2025, 10, 1)),
    (5, 10, '  ', "Delhi", datetime(2025, 10, 1)),
    (None, 10, '  ', "Delhi", datetime(2025, 10, 1))
     
], schema=["id", "age", "name", "place", "join_date"])
left_df.display()

# COMMAND ----------

right_df=spark.createDataFrame([
    (1, "CompanyA", "New York"),
    (2, "CompanyB", "Delhi"),
    (4, "CompanyD", "Bangalore"),
    (5, "CompanyE", "Chennai"),
    (None,"CompanyF","Chennai")
], schema=["id", "company_name", "location"])


# COMMAND ----------

right_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Simple InnerJoin with Str

# COMMAND ----------

left_df.join(right_df,on="id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Simple InnerJoin with list of Str

# COMMAND ----------

left_df.join(right_df,on=["id","place"]).display()

# COMMAND ----------

left_df.join(right_df.withColumnRenamed("location","place"),on=["id","place"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #list of [Column]

# COMMAND ----------

left_df["id"]==right_df["id"],type(left_df["id"]==right_df["id"])

# COMMAND ----------

left_df.join(right_df,on=left_df["id"]==right_df["id"]).display()

# COMMAND ----------

left_df.join(right_df,on=[left_df["id"]==right_df["id"],left_df["place"]==right_df["location"]]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Let us fix the case insensitivity

# COMMAND ----------

left_df.join(right_df,on=[left_df["id"]==right_df["id"],left_df["place"]==F.lower(right_df["location"])]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Null Culprit Fixing

# COMMAND ----------

# MAGIC %sql
# MAGIC select null=null,null=true,null=false;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select null <=> null,null <=> true,null <=> false

# COMMAND ----------

left_df.join(right_df,on=[left_df["id"].eqNullSafe(right_df["id"])]).display()

# COMMAND ----------

left_df.join(right_df,on=[left_df["id"]==right_df["id"]]).display()

# COMMAND ----------

left_df.alias("l").join(right_df.alias("r"),on=F.expr("r.id<=>l.id")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Doing things easily with Expr (sql _way_)

# COMMAND ----------

left_df.alias("l").join(right_df.alias("r"),on=[F.expr("r.id<=>l.id"),F.expr("lower(r.location)<=>l.place")]).display()
left_df.alias("l").join(right_df.alias("r"),on=F.expr("r.id<=>l.id and l.place=lower(r.location)")).display()
left_df.alias("l").join(right_df.alias("r"),on=F.expr("r.id<=>l.id or l.place=lower(r.location)")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Changing Join condition
# MAGIC

# COMMAND ----------

left_df.alias("l").join(right_df.alias("r"),on=[F.expr("r.id<=>l.id"),F.expr("lower(r.location)<=>l.place")],how='left').selectExpr("'left' as join","*").display()
left_df.alias("l").join(right_df.alias("r"),on=[F.expr("r.id<=>l.id"),F.expr("lower(r.location)<=>l.place")],how='right').selectExpr("'right' as join","*").display()
left_df.alias("l").join(right_df.alias("r"),on=[F.expr("r.id<=>l.id"),F.expr("lower(r.location)<=>l.place")],how='full').selectExpr("'full' as join","*").display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Fast Lookups using Left Semi Join/Anti Join
# MAGIC

# COMMAND ----------

left_df.alias("l").join(right_df.alias("r"),on=[F.expr("r.id<=>l.id"),F.expr("lower(r.location)<=>l.place")],how='leftsemi').selectExpr("'left' as join","*").display()
left_df.alias("l").join(right_df.alias("r"),on=[F.expr("r.id<=>l.id"),F.expr("lower(r.location)<=>l.place")],how='leftanti').selectExpr("'left' as join","*").display()

# COMMAND ----------

