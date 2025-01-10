# Databricks notebook source
spark.version

# COMMAND ----------

df = spark.read.format("csv").load("dbfs:/FileStore/tables/billing_dataset.csv", inferSchema=True, header=True)

# COMMAND ----------

from pyspark.sql.functions import col, column 
df.select(col("customerid"), col("country")).show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC sorting 

# COMMAND ----------

df_sort=df.sort("billedamount")
df_sort.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### filtering

# COMMAND ----------

df_Pakistan = df.filter(col("country") == "Pakistan")
df_Pakistan.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregating

# COMMAND ----------

df_grpBy_country = df.groupBy("country").count()
df_grpBy_country.orderBy(col("count").desc()).show()

# COMMAND ----------

from pyspark.sql.functions import avg, round
df_grpBy_agg = df.groupBy("country").agg(
    round(avg("billedamount"))
)

df_grpBy_agg.show()

df_grpBy_agg.columns

# COMMAND ----------

# MAGIC %md
# MAGIC Sampling

# COMMAND ----------

df.count()

# COMMAND ----------

df_sample = df.sample(fraction=0.1,withReplacement=False)

# COMMAND ----------

df_sample.count()

# COMMAND ----------

df.write.csv("df.csv")

# COMMAND ----------

! ls dfwrite1.csv

# COMMAND ----------

df.write.json("df.json")

# COMMAND ----------

! ls df.json

# COMMAND ----------

