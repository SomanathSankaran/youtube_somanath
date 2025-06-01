# Databricks notebook source
# MAGIC %md
# MAGIC ### 🚀 SQL Higher-Order Functions in Spark SQL
# MAGIC
# MAGIC SQL Higher-Order Functions such as `transform()`, `filter()`, `exists()`, and `reduce()` are powerful tools for processing and transforming arrays or nested data types in Spark SQL.  
# MAGIC They make your code more concise and expressive, especially when working with array columns or complex nested structures.
# MAGIC z
# MAGIC | Function   | Description |
# MAGIC |------------|-------------|
# MAGIC | **transform(array, function)** | Applies an anonymous function to each element of the input array and returns a new array with the transformed results. |
# MAGIC | **filter(array, predicate)**  | Constructs a new array by keeping only the elements that satisfy the given condition (predicate function). |
# MAGIC | **exists(array, predicate)**  | Returns `true` if at least one element in the array satisfies the predicate function; otherwise, returns `false`. |
# MAGIC | **reduce(array , init,mergeFunc, [finishFunc])** | Aggregates array elements into a single result. <br>•  <br>• `zero`: Starting value for aggregation.`mergeFunc`: Combines elements into a buffer. <br>• `finishFunc` (optional): Final transformation on the result. If not provided, the identity function is used. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 💡 Use these to simplify complex data transformations, especially when dealing with nested or repeated fields in Spark SQL tables!
# MAGIC

# COMMAND ----------

transform
[1,2,3],x => x*2 ,[2,4,6]
filter
[1,2,3],x => x%2!=0,[1,3]
exists
[1,2,3],x => x%2!=0,[true,false,true]

[1,2,3],sum
[6]


# COMMAND ----------

# MAGIC %md #About the Data

# COMMAND ----------

data=[
  {
    "pullRequestId": 438,
    "pr_createdBy": "user1@example.com",
    "pr_creationDate": "2022-02-02T06:55:49.934Z",
    "pr_closedDate": "2022-02-02T11:36:24.658Z",
    "pr_title": "Structure update for cost analysis",
    "reviewers": [
      {
        "teamName": "DataPlatform Code Approval",
        "reviewerName": "reviewer1@example.com"
      },
      {
        "teamName": "DataPlatform Code Reviewers",
        "reviewerName": "reviewer2@example.com"
      }
    ],
    "commitId": "commit123",
    "commit_username": "committer1@example.com",
    "commit_date": "2022-02-02T05:43:42.000Z",
    "commit_comment": "Updated column types",
    "notebook": [
      {
        "entityName": "/notebooks/Project1/DataCleaning.py",
        "changeType": "edit"
      }
    ],
    "values": [1, 2, 3]
  },
  {
    "pullRequestId": 438,
    "pr_createdBy": "user1@example.com",
    "pr_creationDate": "2022-02-02T06:55:49.934Z",
    "pr_closedDate": "2022-02-02T11:36:24.658Z",
    "pr_title": "Structure update for cost analysis",
    "reviewers": [
      {
        "teamName": "DataPlatform Code Approval",
        "reviewerName": "reviewer1@example.com"
      },
      {
        "teamName": "DataPlatform Code Reviewers",
        "reviewerName": "reviewer2@example.com"
      }
    ],
    "commitId": "commit124",
    "commit_username": "committer1@example.com",
    "commit_date": "2022-02-02T06:53:40.000Z",
    "commit_comment": "Merged PR: Structural changes",
    "notebook": [
      {
        "entityName": "/notebooks/Project1/DataIngestion.py",
        "changeType": "edit"
      },
      {
        "entityName": "/notebooks/Project1/UsageTrendsAnalysis.py",
        "changeType": "edit"
      },
      {
        "entityName": "/notebooks/Project1/CostBreakdownReport.py",
        "changeType": "edit"
      }
    ],
    "values": [3, 4, 5]
  },
  {
    "pullRequestId": 438,
    "pr_createdBy": "user1@example.com",
    "pr_creationDate": "2022-02-02T06:55:49.934Z",
    "pr_closedDate": "2022-02-02T11:36:24.658Z",
    "pr_title": "Structure update for cost analysis",
    "reviewers": [
      {
        "teamName": "DataPlatform Code Approval",
        "reviewerName": "reviewer1@example.com"
      },
      {
        "teamName": "DataPlatform Code Reviewers",
        "reviewerName": "reviewer2@example.com"
      }
    ],
    "commitId": "commit125",
    "commit_username": "committer2@example.com",
    "commit_date": "2022-02-02T11:16:06.000Z",
    "commit_comment": "Refactored widget names",
    "notebook": [
      {
        "entityName": "/notebooks/Project2/DataCleaning.py",
        "changeType": "edit"
      },
      {
        "entityName": "/notebooks/Project2/DataCleaning.py",
        "changeType": "edit"
      }
    ],
    "values": [7, 8, 9]
  },
  {
    "pullRequestId": 434,
    "pr_createdBy": "user2@example.com",
    "pr_creationDate": "2022-01-31T14:40:18.154Z",
    "pr_closedDate": "2022-01-31T14:46:11.765Z",
    "pr_title": "Initial cost analysis commit",
    "reviewers": [
      {
        "teamName": "DataPlatform Code Reviewers",
        "reviewerName": "reviewer3@example.com"
      },
      {
        "teamName": "DataPlatform Code Approval",
        "reviewerName": "reviewer4@example.com"
      }
    ],
    "commitId": "commit126",
    "commit_username": "committer1@example.com",
    "commit_date": "2022-01-31T14:37:59.000Z",
    "commit_comment": "Merged PR: Initial cost analysis",
    "notebook": [
      {
        "entityName": "/notebooks/Project1/DataIngestion.py",
        "changeType": "add"
      },
      {
        "entityName": "/notebooks/Project1/UsageTrendsAnalysis.py",
        "changeType": "add"
      },
      {
        "entityName": "/notebooks/Project1/CostBreakdownReport.py",
        "changeType": "delete"
      }
    ],
    "values": [1, 2, 9]
  },
  {
    "pullRequestId": 434,
    "pr_createdBy": "user2@example.com",
    "pr_creationDate": "2022-01-31T14:40:18.154Z",
    "pr_closedDate": "2022-01-31T14:46:11.765Z",
    "pr_title": "Initial Usage analysis commit",
    "reviewers": [
      {
        "teamName": "DataPlatform Code Reviewers",
        "reviewerName": "reviewer3@example.com"
      },
      {
        "teamName": "DataPlatform Code Approval",
        "reviewerName": "reviewer4@example.com"
      }
    ],
    "commitId": "commit127",
    "commit_username": "committer1@example.com",
    "commit_date": "2022-01-31T13:11:07.000Z",
    "commit_comment": "DE ingestion notebook Commits",
    "notebook": [
      {
        "entityName": "/notebooks/Project1/DataIngestion.py",
        "changeType": "add"
      },
      {
        "entityName": "/notebooks/Project1/UsageTrendsAnalysis.py",
        "changeType": "add"
      },
      {
        "entityName": "/notebooks/Project1/CostBreakdownReport.py",
        "changeType": "add"
      }
    ],
    "values": [2, 4, 6]
  }
]


# COMMAND ----------


schema ="commitId string, commit_comment string, commit_date string, commit_username string, notebook array<map<string,string>>, pr_closedDate string, pr_createdBy string, pr_creationDate string, pr_title string, pullRequestId bigint, reviewers array<map<string,string>>, values array<int>"

df=spark.createDataFrame(data, schema=schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md #### Function - `transform(array<T>, function<T, U>): array<U>`

# COMMAND ----------

[{"entityName":"/notebooks/Project1/DataIngestion.py","changeType":"edit"},{"entityName":"/notebooks/Project1/UsageTrendsAnalysis.py","changeType":"edit"},{"entityName":"/notebooks/Project1/CostBreakdownReport.py","changeType":"edit"}]

# COMMAND ----------

# final_df = (df
#             .withColumn("is_new_notebook",expr('transform(notebook,t ->  case when t.changeType = "add" then 1 else 0 end)'))
#                         .withColumn("is_notebook_deleted",expr('transform(notebook,t ->  case when t.changeType = "add" then 1 else 0 end)'))
          #  )
final_df = (df
            .withColumn("is_new_notebook",expr('array_max(transform(notebook,t ->  case when t.changeType = "add" then 1 else 0 end))'))
                        .withColumn("is_notebook_deleted",expr('array_max(transform(notebook,t ->  case when t.changeType = "add" then 1 else 0 end))')))            
final_df.display()           

# COMMAND ----------

# MAGIC %md #### Function - `exists(array<T>, function<T, V, Boolean>): Boolean`

# COMMAND ----------

final_df = (df
            .withColumn("value_exists",expr('exists(notebook,t -> lower(t.entityName) like "%analysis%")'))
            #.filter("value_exists = true")
           )
final_df.display()           

# COMMAND ----------

# MAGIC %md #### Function - `filter(array<T>, function<T, U>): array<U>`

# COMMAND ----------

final_df = (df
            .withColumn("edit_noebooks", expr('filter(notebook, t -> t.changeType LIKE "%edit%")')        
           ))
final_df.display()           

# COMMAND ----------

# MAGIC %md #### Function - `reduce(array<T>, B, function<B, T, B>, function<B, R>): R`

# COMMAND ----------

final_df = (df
            .withColumn("summed_values",expr("REDUCE(values, 0, ( acc,value) -> value + acc,acc->acc/3)"))
           )  
final_df.display()

# COMMAND ----------

df = spark.createDataFrame([
    (1, "update", [
        {"a": 1, "b": 2},
        {"c": 3, "d": 4}
    ]),
    (2, "update", [
        {"x": 10},
        {"y": 5, "z": 1}
    ])
], ["id", "op_type", "changes"])

# COMMAND ----------

df.display()

# COMMAND ----------



# COMMAND ----------

df_with_merged = df.withColumn("merged_map", expr("""
  REDUCE(
    changes,
    CAST(map() AS MAP<STRING, INT>),
    (acc, x) -> map_concat(acc, CAST(x AS MAP<STRING, INT>))
  )
"""))
df_with_merged.display()

# COMMAND ----------

df_with_merged = df.withColumn("merged_map", expr("""
  REDUCE(
    changes,
    CAST(map() AS MAP<STRING, INT>),
    (acc, x) -> map_concat(acc, CAST(x AS MAP<STRING, INT>)), acc ->  cast(acc as string))
"""))
df_with_merged.display()

# COMMAND ----------

