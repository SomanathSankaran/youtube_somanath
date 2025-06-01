# Databricks notebook source
# MAGIC %md
# MAGIC #  Overview  
# MAGIC
# MAGIC In this video, we dive deep into **how joins work** in **Apache Spark** and the different strategies used to optimize performance. We'll explore:  
# MAGIC
# MAGIC ### Topics Covered:  
# MAGIC 1. **How Joins Work & Join Strategies** – Understanding the execution process.  
# MAGIC 2. **Loading Data** – Preparing datasets for efficient joins.  
# MAGIC 3. **Tuning Broadcast Thresholds** – Adjusting Spark settings to optimize **Broadcast Hash Joins**.  
# MAGIC 4. **AQE & Broadcast Thresholds** – How **Adaptive Query Execution (AQE)** dynamically convert joins .  
# MAGIC 5. **Disabling vs. Enabling AQE** – When to use AQE and when to disable it.  
# MAGIC 6. **Overriding with Hints** – Forcing Spark to use specific join strategies.  
# MAGIC
# MAGIC

# COMMAND ----------

!pip install mermaid-py

# COMMAND ----------

# MAGIC %md
# MAGIC #Join Strategies

# COMMAND ----------

import mermaid as md
from mermaid.graph import Graph

sequence = Graph('Sequence-diagram',"""graph TD;
    A[Start Equi-Join] -->|Check Join Hints| B{Hint Type?};
    B -->|Broadcast Hint Present?| C[Broadcast Hash Join - Only Equi-Join];
    B -->|Sort Merge Hint Present?| D[Sort Merge Join - Sortable Keys];
    B -->|Shuffle Hash Hint Present?| E[Shuffle Hash Join - Only Equi-Join];
    B -->|Shuffle Replicate NL Hint?| F[Cartesian Product - Inner Joins Only];
    B -->|No Hint Present| G{Apply Default Rules};
    
    G -->|Small Table Available?| C;
    G -->|Small Table for Hash Map & Sort Merge Disabled?| E;
    G -->|Sortable Join Keys?| D;
    G -->|Inner-Like Join?| F;
    G -->|None Apply| H[Broadcast Nested Loop Join - Last Resort];
    
    H -->|May Cause OOM| I[Final Join Strategy]


""")
render = md.Mermaid(sequence)
render # !! note this only works in the notebook that rendered the html.

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md 
# MAGIC #Join Working
# MAGIC In this Video we will see the various types of join 
# MAGIC and how does joins works and we will do a deep dive on broadcast join and join hints

# COMMAND ----------

sequence = Graph('Sequence-diagram',"""graph TD;
    
      subgraph Broadcast Hash Join
        direction TB
        C0[Driver Node] -->|Receives Small Table| C1[Small Table];
        C1 -->|Build Hash Table| C2[Hash Table in Driver];
        C2 -->|Broadcast to All Worker Nodes| C3[Worker Nodes];
        C3 -->|Probe Larger Table| C4[Join Result]
    end
    
    
    subgraph Shuffle Hash Join
        direction TB
        E1[Table 1] -->|Shuffled on Join Key| E3[Worker Nodes];
        E2[Table 2] -->|Shuffled on Join Key| E3;
        E3 -->|Build Hash Table & Probe| E4[Join Result]
    end
    
    subgraph Sort Merge Join
        direction TB
        D1[Table 1] -->|Shuffle & Sort on Join Key| D3[Sorted Partitions];
        D2[Table 2] -->|Shuffle Sort on Join Key| D3;
        D3 -->|Merge & Join| D4[Join Result]
    end
    
    """
    )
render = md.Mermaid(sequence)
render # !! note this only works in the notebook that rendered the html.


# COMMAND ----------

# MAGIC %md
# MAGIC #Load Data
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/retail-org/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/retail-org/products/

# COMMAND ----------

products_df=spark.read.format("csv").load("dbfs:/databricks-datasets/retail-org/products/",sep=";",header=True)
display(products_df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/retail-org/sales_orders/

# COMMAND ----------

spark.read.format("json").load("dbfs:/databricks-datasets/retail-org/sales_orders/").display()

# COMMAND ----------

spark.read.format("json").load("dbfs:/databricks-datasets/retail-org/sales_orders/").selectExpr("explode(Ordered_products)","order_number").display()

# COMMAND ----------

df_ord_details=spark.read.format("json").load("dbfs:/databricks-datasets/retail-org/sales_orders/").selectExpr("explode(Ordered_products) as ord_struct ","order_number").select("order_number","ord_struct.*")
display(df_ord_details)

# COMMAND ----------

df_ord_details.join(products_df,on=(df_ord_details["id"]==products_df["product_id"])).selectExpr("*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Changing Broadcast ![Threshold](path)

# COMMAND ----------

# MAGIC %sql set spark.sql.autoBroadcastJoinThreshold

# COMMAND ----------

# MAGIC %sql set spark.sql.autoBroadcastJoinThreshold=10b

# COMMAND ----------

df_ord_details.join(products_df,on=(df_ord_details["id"]==products_df["product_id"])).selectExpr("*").display()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC # Changing Aqe Broadcast Threshold

# COMMAND ----------

# MAGIC %sql set spark.sql.adaptive.autoBroadcastJoinThreshold

# COMMAND ----------

# MAGIC %sql set spark.sql.adaptive.autoBroadcastJoinThreshold=20kb

# COMMAND ----------

df_ord_details.join(products_df,on=(df_ord_details["id"]==products_df["product_id"])).selectExpr("*").display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql set spark.sql.adaptive.autoBroadcastJoinThreshold=25kb

# COMMAND ----------

df_ord_details.join(products_df,on=(df_ord_details["id"]==products_df["product_id"])).selectExpr("*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Disable AQE Enablement

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled=false

# COMMAND ----------

df_ord_details.join(products_df,on=(df_ord_details["id"]==products_df["product_id"])).selectExpr("*").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled=true

# COMMAND ----------

# MAGIC %md
# MAGIC #OverPower With Hint

# COMMAND ----------

df_ord_details.hint("SHUFFLE_HASH").join(products_df,on=(df_ord_details["id"]==products_df["product_id"])).selectExpr("*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC In this video 
# MAGIC we saw various join type and how does it work

# COMMAND ----------

df_ord_details.hint("broadcast").join(products_df,on=(df_ord_details["id"]<=products_df["product_id"])).selectExpr("*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ✅ Conclusion  
# MAGIC
# MAGIC Optimizing join selection is **crucial** for improving **query performance and resource utilization** in Spark.  
# MAGIC
# MAGIC ### 🔹 Key Takeaways:  
# MAGIC - Use **Broadcast Joins** for **small tables** whenever possible.  
# MAGIC - **Leverage AQE** to allow Spark to **dynamically optimize join strategies**.  
# MAGIC - **Manually override join strategies** with hints when confident to avoid un-necessary sort merge join.  
# MAGIC
# MAGIC By mastering these strategies, you can **significantly boost performance** and handle large-scale data processing more efficiently! 🚀  
# MAGIC
# MAGIC ---

# COMMAND ----------

