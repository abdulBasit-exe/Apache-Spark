# Databricks notebook source
dfServerName = spark.read.format("csv").load("dbfs:/FileStore/tables/server_name.csv", inferSchema="true", header="true")

dfServerName.show()

# COMMAND ----------

dfServerName.createOrReplaceTempView("ServerName")

# COMMAND ----------

dfUtilization = spark.read.format("json") \
    .option("header", "true") \
    .load("dbfs:/FileStore/utilization.json")

display(dfUtilization)

# COMMAND ----------

dfUtilization.count()

# COMMAND ----------

from pyspark.sql.functions import col

dfUtilization = dfUtilization.select(col("event_datetime"), col("cpu_utilization"), col("free_memory"), col("server_id"), col("session_count"))

# COMMAND ----------

dfUtilization.show()

# COMMAND ----------

from pyspark.sql.functions import round
dfUtilization = dfUtilization.withColumn("total_memory", round(col("cpu_utilization") + col("free_memory"), 2))

# COMMAND ----------

dfUtilization.columns

# COMMAND ----------

dfUtilization = dfUtilization.select(['event_datetime',
 'cpu_utilization',
 'free_memory',
 'total_memory',
 'server_id',
 'session_count'])

# COMMAND ----------

dfUtilization.show(10)

# COMMAND ----------

dfUtilization.createOrReplaceTempView("Utilization")

# COMMAND ----------

dfServerName.show()

# COMMAND ----------

dfServerName.createOrReplaceTempView("ServerName")

# COMMAND ----------

dfcount = spark.sql("SELECT DISTINCT server_id FROM Utilization ORDER BY server_id")

dfcount.show()

# COMMAND ----------

dfcount = spark.sql("SELECT  min(server_id) , max(server_id) FROM Utilization ")

dfcount.show()

# COMMAND ----------

display(spark.catalog.listTables())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joins 

# COMMAND ----------

dfJoins = spark.sql("""
                    SELECT u.server_id, sn.server_name, u.session_count
                    FROM Utilization u 
                    INNER JOIN servername sn 
                    on sn.server_id = u.server_id
                    order by u.server_id 
                    """)

dfJoins.show()