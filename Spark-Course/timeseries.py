# Databricks notebook source
df_util = spark.read.format('json').options(inferSchema="true").load("dbfs:/FileStore/utilization.json")

# COMMAND ----------

display(df_util)

# COMMAND ----------

df_util.createOrReplaceTempView("utilization")

# COMMAND ----------

df_util.printSchema()

# COMMAND ----------

sql_window = spark.sql(
    "Select event_datetime, server_id, cpu_utilization, \
            avg(cpu_utilization) OVER(PARTITION BY server_id) avg_server_util \
                from utilization"
)

# COMMAND ----------

sql_window.show()

# COMMAND ----------

sql_window2 = spark.sql(
    "Select event_datetime, server_id, cpu_utilization, \
            avg(cpu_utilization) OVER(PARTITION BY server_id) avg_server_util, \
            cpu_utilization- avg(cpu_utilization) OVER(PARTITION BY server_id) delta_server_util \
                from utilization"
)

# COMMAND ----------

sql_window2.show()

# COMMAND ----------

sql_window3 = spark.sql(
    "Select event_datetime, server_id, cpu_utilization, \
     avg(cpu_utilization) OVER(PARTITION BY server_id order by event_datetime\
                          rows between 1 preceding and 1 following) avg_server_util \
    from utilization"
)

# COMMAND ----------

sql_window3.show(5)

# COMMAND ----------

display(sql_window3)