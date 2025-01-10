# Databricks notebook source
from pyspark.sql import Row
df_dup = sc.parallelize([Row(server_name = "101 Server", cpu_utilization = 85, session_count = 80), 
                        Row(server_name = "101 Server", cpu_utilization = 80, session_count = 90),
                        Row(server_name = "102 Server", cpu_utilization = 85, session_count = 80),
                        Row(server_name = "102 Server", cpu_utilization = 85, session_count = 80)]).toDF()

# COMMAND ----------

df_dup.show()

# COMMAND ----------

df_dup.dropDuplicates().show()

# COMMAND ----------

df_dup = df_dup.dropDuplicates(['server_name'])

display(df_dup)

# COMMAND ----------

df_dup.dropDuplicates(['cpu_utilization']).show()