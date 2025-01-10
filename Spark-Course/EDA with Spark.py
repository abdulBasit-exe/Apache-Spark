# Databricks notebook source
df = spark.read.format("json").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/utilization.json")

# COMMAND ----------

display(df.describe())

# COMMAND ----------

df.stat.corr('cpu_utilization', 'free_memory')

# COMMAND ----------

df.stat.corr('session_count', 'free_memory')

# COMMAND ----------

df.stat.freqItems(('server_id', 'session_count')).show()

# COMMAND ----------

df.createOrReplaceTempView("Utilization")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

spark.sql("SELECT min(cpu_utilization) as min_cpu_utilization, max(cpu_utilization) as max_cpu_utilization, min(session_count) as min_session_count, max(session_count) as max_session_count FROM Utilization").show()