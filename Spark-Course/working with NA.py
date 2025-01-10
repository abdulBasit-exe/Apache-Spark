# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import lit
df = sc.parallelize([Row(server_name = "101 Server", cpu_utilization = 85, session_count = 80), 
                        Row(server_name = "101 Server", cpu_utilization = 80, session_count = 90),
                        Row(server_name = "102 Server", cpu_utilization = 85, session_count = 80),
                        Row(server_name = "102 Server", cpu_utilization = 85, session_count = 80)]).toDF()

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.types import StringType

# COMMAND ----------

df_na= df.withColumn('na_col', lit(None).cast(StringType()))

# COMMAND ----------

df_na.show()

# COMMAND ----------

df_na.fillna('A').show()

# COMMAND ----------

df2 = df_na.na.fill('A').union(df_na)

# COMMAND ----------

df2.show()

# COMMAND ----------

df_final = df2.na.drop()

# COMMAND ----------

df_final.show()

# COMMAND ----------

df_final.createOrReplaceTempView("df_final")

# COMMAND ----------

spark.sql("SELECT * from df_final").show()